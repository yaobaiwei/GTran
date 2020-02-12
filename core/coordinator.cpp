/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "coordinator.hpp"

void Uint64CLineWithTag::SetValue(uint64_t val, uint64_t tag) {
    uint64_t tmp_data[8] __attribute__((aligned(64)));
    tmp_data[0] = tmp_data[6] = tag;
    tmp_data[1] = tmp_data[7] = tag + 1;
    tmp_data[2] = val;
    memcpy(data, tmp_data, 64);
}

bool Uint64CLineWithTag::GetValue(uint64_t& val, uint64_t tag) {
    uint64_t tmp_data[8] __attribute__((aligned(64)));
    memcpy(tmp_data, data, 64);

    if (tmp_data[0] == tmp_data[6] && tmp_data[1] == tmp_data[7] && tmp_data[0] + 1 == tmp_data[1] && tmp_data[0] == tag) {
        val = data[2];
        return true;
    }

    return false;
}

Coordinator::Coordinator() {
    next_trx_id_ = 1;
    distributed_clock_ = DistributedClock::GetInstance();
    distributed_clock_initialized_ = false;
}

void Coordinator::Init(Node* node) {
    node_ = node;
    MPI_Comm_size(node_->local_comm, &comm_sz_);
    MPI_Comm_rank(node_->local_comm, &my_rank_);

    config_ = Config::GetInstance();

    if (config_->global_use_rdma) {
        Buffer* buf = Buffer::GetInstance();
        rdma_mem_ = buf->GetTSSyncBuf();
        rdma_mem_offset_ = config_->ts_sync_buffer_offset;
        ts_cline_ = (Uint64CLineWithTag*)rdma_mem_;
    }
}

// get trx id
void Coordinator::RegisterTrx(uint64_t& trx_id) {
    trx_id = TRX_ID_MASK | (((next_trx_id_++) * comm_sz_ + my_rank_) << QID_BITS);
}

int Coordinator::GetWorkerFromTrxID(const uint64_t& trx_id) {
    CHECK(IS_VALID_TRX_ID(trx_id));
    return ((trx_id ^ TRX_ID_MASK) >> QID_BITS) % comm_sz_;
}

void Coordinator::WaitForDistributedClockInit() {
    while (true) {
        usleep(1000);
        bool initialized = distributed_clock_initialized_;
        if (initialized)
            return;
    }
}

void Coordinator::ProcessTimestampRequest() {
    // To ensure rdtsc works
    DistributedClock::BindToLogicalCore(CPUInfoUtil::GetInstance()->GetTotalThreadCount() - 1);

    distributed_clock_->Init(node_->local_comm, 3, 1000);

    distributed_clock_initialized_ = true;

    // To ensure the correctness, only one thread can call GetTimestamp.
    while (true) {
        TimestampRequest req;
        pending_timestamp_request_->WaitAndPop(req);

        AllocatedTimestamp allocated_ts = AllocatedTimestamp(req.trx_id, req.ts_type, distributed_clock_->GetTimestamp());
        pending_allocated_timestamp_->Push(allocated_ts);
    }
}

// Only called in PerformCalibration
void Coordinator::WriteTimestampToWorker(int worker_id, uint64_t ts, uint64_t tag) {
    RDMA &rdma = RDMA::get_rdma();
    int t_id = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);
    ts_cline_->SetValue(ts, tag);
    rdma.dev->RdmaWrite(t_id, worker_id, rdma_mem_, sizeof(Uint64CLineWithTag), rdma_mem_offset_);
}

// Only called in PerformCalibration
uint64_t Coordinator::ReadTimestampFromRDMAMem(uint64_t tag) {
    uint64_t ret;
    while (true) {
        if (ts_cline_->GetValue(ret, tag))
            return ret;
    }
}

void Coordinator::PerformCalibration() {
    // Only one worker, do not need to calibrate
    if (comm_sz_ == 1)
        return;

    TidPoolManager::GetInstance()->Register(TID_TYPE::RDMA, config_->global_num_threads + Config::perform_calibration_tid);

    // To ensure rdtsc works
    DistributedClock::BindToLogicalCore(CPUInfoUtil::GetInstance()->GetTotalThreadCount() - 1);

    const int calibration_interval = 500;

    uint64_t remote_ns, ns;
    uint64_t loop = 0;
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(calibration_interval));
        if (config_->global_use_rdma) {
            /*
                Clock calibration for RDMA:
                    Assume that node with rank 0 is the master of the calibration.
                    And other nodes are the worker.

                    Phase 0:
                        All nodes are sleeping.
                    Phase 1:
                        The workers wake up first, and wait for the signal from the master.
                    Phase 2:
                        Master:
                            The master wakes up. And for each workers:
                                Send a signal to the worker.
                                Recv timestamp from it. Then get a local timestamp.
                                    If the local timestamp is less than the remote timestamp:
                                        Increase the offset of the local clock.
                                Get a local timestamp and send to the worker.
                                Wait for the signal from the worker to ensure the safety of RDMA mem.
                        Worker:
                            Wait for the signal from the master.
                            Send the local timestamp back to the master.
                            Wait for the timestamp from the master. Then get a local timestamp.
                                If the local timestamp is less than the remote timestamp:
                                    Increase the offset of the local clock.
                            Send a signal to the master.
                        When phase 2 is finished, the Master will have the fastest clock.
                        So, the procedure need to be repeated (phase 3), to sync all workers to the fastest clock.
                    Phase 3:
                        Do the same thing as phase 2 (round == 1).
             */

            for (int round = 0; round < 2; round++) {
                if (my_rank_ == 0) {
                    for (int partner = 1; partner < comm_sz_; partner++) {
                        uint64_t tag = (loop * (comm_sz_ - 1) * 2 * 4) + (partner - 1) * 4 + (round * (comm_sz_ - 1) * 4);
                        // Send greeting signal to the partner
                        WriteTimestampToWorker(partner, 0, tag);
                        // Wait for the remote time from the partner
                        remote_ns = ReadTimestampFromRDMAMem(tag + 1);
                        // Get local time and send to tht partner
                        ns = distributed_clock_->GetRefinedNS();
                        WriteTimestampToWorker(partner, ns, tag + 2);
                        // Recv finishing signal from the partner to make sure that RDMA mem can be used for the next partner
                        ReadTimestampFromRDMAMem(tag + 3);
                        int64_t delta = remote_ns - ns;
                        if (delta > 0) {
                            distributed_clock_->IncreaseGlobalNSOffset(delta);
                            // printf("[Worker%d, partner = %d] ns = %lu, remote_ns = %lu, delta = %ld\n", my_rank_, partner, ns, remote_ns, delta);
                        }
                    }
                } else {
                    uint64_t tag = (loop * (comm_sz_ - 1) * 2 * 4) + (my_rank_ - 1) * 4 + (round * (comm_sz_ - 1) * 4);
                    const int partner = 0;
                    // Recv greeting signal from the partner
                    ReadTimestampFromRDMAMem(tag);
                    // Get local time and send to tht partner
                    ns = distributed_clock_->GetRefinedNS();
                    WriteTimestampToWorker(partner, ns, tag + 1);
                    // Wait for the remote time from the partner
                    remote_ns = ReadTimestampFromRDMAMem(tag + 2);
                    // Get local time after receiving the remote time
                    ns = distributed_clock_->GetRefinedNS();
                    // Send finishing signal to the partner
                    WriteTimestampToWorker(partner, 0, tag + 3);
                    int64_t delta = remote_ns - ns;
                    if (delta > 0) {
                        distributed_clock_->IncreaseGlobalNSOffset(delta);
                        // printf("[Worker%d, partner = %d] ns = %lu, remote_ns = %lu, delta = %ld\n", my_rank_, partner, ns, remote_ns, delta);
                    }
                }
            }
        } else {
            distributed_clock_->GlobalCalibrateMeasure(40, 0.1, false);
            int64_t delta = distributed_clock_->GetMeasuredGlobalNSOffset();

            if (delta != 0) {
                // printf("[<PerformCalibration>] node %d, delta = %ld\n", my_rank_, delta);
                distributed_clock_->IncreaseGlobalNSOffset(delta);
            }
        }
        loop++;
    }
}

void Coordinator::PrepareSockets() {
    char addr[64];

    if (!config_->global_use_rdma) {
        trx_read_recv_socket_ = new zmq::socket_t(context_, ZMQ_PULL);
        // Ports with node_->tcp_port + 3 + 1 * config_->global_num_threads are occupied by TCPMailbox
        snprintf(addr, sizeof(addr), "tcp://*:%d", node_->tcp_port + 3 + 2 * config_->global_num_threads);
        trx_read_recv_socket_->bind(addr);
        DLOG(INFO) << "[Master] bind " << string(addr);

        trx_read_rep_sockets_.resize(config_->global_num_threads *
                                    config_->global_num_workers);

        // connect to p+3+global_num_threads ~ p+2+2*global_num_threads
        for (int i = 0; i < config_->global_num_workers; ++i) {
            Node& r_node = GetNodeById(workers_, i + 1);

            for (int j = 0; j < config_->global_num_threads; ++j) {
                trx_read_rep_sockets_[socket_code(i, j)] =
                    new zmq::socket_t(context_, ZMQ_PUSH);
                snprintf(
                    addr, sizeof(addr), "tcp://%s:%d",
                    workers_[i].ibname.c_str(),
                    r_node.tcp_port + j + 3 + config_->global_num_threads);
                trx_read_rep_sockets_[socket_code(i, j)]->connect(addr);
                DLOG(INFO) << "[Master] connects to " << string(addr);
            }
        }
    }
}

void Coordinator::ProcessQueryRCTRequest() {
    while (true) {
        QueryRCTRequest request;
        // Pop RCT query request from other workers
        pending_rct_query_request_->WaitAndPop(request);

        vector<uint64_t> trx_ids;
        rct_->query_trx(request.bt, request.ct - 1, trx_ids);

        ibinstream in;
        int notification_type = (int)(NOTIFICATION_TYPE::RCT_TIDS);
        in << notification_type << request.trx_id;
        in << trx_ids;
        mailbox_->SendNotification(request.n_id, in);
    }
}

void Coordinator::ProcessTrxTableWriteReqs() {
    while (true) {
        // pop a req
        UpdateTrxStatusReq req;
        pending_trx_updates_->WaitAndPop(req);

        CHECK(req.new_status != TRX_STAT::VALIDATING);

        trx_table_->modify_status(req.trx_id, req.new_status);
    }
}

void Coordinator::ListenTCPTrxReads() {
    while (true) {
        ReadTrxStatusReq req;
        obinstream out;
        zmq::message_t zmq_req_msg;
        if (trx_read_recv_socket_->recv(&zmq_req_msg, 0) < 0) {
            CHECK(false) << "[Coordinator::ListenTCPTrxReads] Coordinator failed to recv TCP read";
        }
        char* buf = new char[zmq_req_msg.size()];
        memcpy(buf, zmq_req_msg.data(), zmq_req_msg.size());
        out.assign(buf, zmq_req_msg.size(), 0);

        out >> req.n_id >> req.t_id >> req.trx_id >> req.read_ct;
        pending_trx_reads_->Push(req);
    }
}

void Coordinator::ProcessTCPTrxReads() {
    while (true) {
        // pop a req
        ReadTrxStatusReq req;
        pending_trx_reads_->WaitAndPop(req);
        // printf("[Worker%d ProcessTCPTrxReads] %s\n", node_->get_local_rank(), req.DebugString().c_str());

        ibinstream in;
        if (req.read_ct) {
            uint64_t ct_;
            TRX_STAT status;
            trx_table_->query_ct(req.trx_id, ct_);
            trx_table_->query_status(req.trx_id, status);
            int status_i = (int) status;
            in << ct_;
            in << status_i;
        } else {
            TRX_STAT status;
            trx_table_->query_status(req.trx_id, status);
            int status_i = (int) status;
            in << status_i;
        }
        zmq::message_t zmq_send_msg(in.size());
        memcpy(reinterpret_cast<void*>(zmq_send_msg.data()), in.get_buf(),
               in.size());
        trx_read_rep_sockets_[socket_code(req.n_id, req.t_id)]->send(zmq_send_msg);
    }
}
