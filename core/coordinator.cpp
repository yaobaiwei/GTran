/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "coordinator.hpp"

void Uint64CLineWithTag::SetValue(uint64_t val, uint64_t tag) {
    uint64_t tmp_data[8] __attribute__((aligned(64)));
    tmp_data[0] = tmp_data[6] = tag;
    tmp_data[1] = tmp_data[7] = tag + 1;
    tmp_data[2] = tmp_data[4] = val;
    tmp_data[3] = tmp_data[5] = val + 1;
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
    timestamper_ = MPITimestamper::GetInstance();
    mpi_timestamper_initialized_ = false;
}

void Coordinator::Init(Node* node, ThreadSafeQueue<TimestampRequest>* pending_timestamp_request,
                       ThreadSafeQueue<AllocatedTimestamp>* pending_allocated_timestamp) {
    node_ = node;
    MPI_Comm_size(node_->local_comm, &comm_sz_);
    MPI_Comm_rank(node_->local_comm, &my_rank_);

    pending_timestamp_request_ = pending_timestamp_request;
    pending_allocated_timestamp_ = pending_allocated_timestamp;

    config_ = Config::GetInstance();

    if (config_->global_use_rdma) {
        Buffer* buf = Buffer::GetInstance();
        rdma_mem_ = buf->GetTSSyncBuf();
        rdma_mem_offset_ = config_->ts_sync_buffer_offset;
        ts_cline_ = (Uint64CLineWithTag*)rdma_mem_;
    }
}

void Coordinator::RegisterTrx(uint64_t& trx_id) {
    trx_id = 0x8000000000000000 | (((next_trx_id_) * comm_sz_ + my_rank_) << 8);
    next_trx_id_++;
}

int Coordinator::GetWorkerFromTrxID(const uint64_t& trx_id) {
    return ((trx_id ^ 0x8000000000000000) >> 8) % comm_sz_;
}

void Coordinator::WaitForMPITimestamperInit() {
    while (true) {
        usleep(1000);
        bool initialized = mpi_timestamper_initialized_;
        if (initialized)
            return;
    }
}

void Coordinator::ProcessObtainingTimestamp() {
    MPITimestamper::BindToLogicalCore(CPUInfoUtil::GetInstance()->GetTotalThreadCount() - 1);

    timestamper_->Init(node_->local_comm, 3, 1000);

    mpi_timestamper_initialized_ = true;

    while (true) {
        TimestampRequest req;
        pending_timestamp_request_->WaitAndPop(req);

        AllocatedTimestamp allocated_ts = AllocatedTimestamp(req.trx_id, req.is_ct, timestamper_->GetTimestamp());
        pending_allocated_timestamp_->Push(allocated_ts);
    }
}

void Coordinator::WriteTimestampToWorker(int worker_id, uint64_t ts, uint64_t tag) {
    RDMA &rdma = RDMA::get_rdma();
    int t_id = config_->global_num_threads + 2;
    ts_cline_->SetValue(ts, tag);
    rdma.dev->RdmaWrite(t_id, worker_id, rdma_mem_, sizeof(Uint64CLineWithTag), rdma_mem_offset_);
}

uint64_t Coordinator::ReadTimestampFromRDMAMem(uint64_t tag) {
    uint64_t ret;
    while (true) {
        if (ts_cline_->GetValue(ret, tag))
            return ret;
    }
}

void Coordinator::PerformCalibration() {
    MPITimestamper::BindToLogicalCore(CPUInfoUtil::GetInstance()->GetTotalThreadCount() - 1);
    if (comm_sz_ == 1)
        return;

    const int ms_to_sleep = 500;

    uint64_t remote_ns, ns;
    uint64_t loop = 0;
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms_to_sleep));
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

                    Phase 3:
                        Do the same thing as phase 2 (round = 1).
             */

            for (int round = 0; round < 2; round++) {
                if (my_rank_ == 0) {
                    // send greeting msg
                    for (int partner = 1; partner < comm_sz_; partner++) {
                        uint64_t tag = (loop * (comm_sz_ - 1) * 2 * 4) + (partner - 1) * 4 + (round * (comm_sz_ - 1) * 4);
                        WriteTimestampToWorker(partner, 0, tag);
                        remote_ns = ReadTimestampFromRDMAMem(tag + 1);
                        ns = timestamper_->GetRefinedNS();
                        WriteTimestampToWorker(partner, ns, tag + 2);
                        ReadTimestampFromRDMAMem(tag + 3);
                        int64_t delta = remote_ns - ns;
                        if (delta > 0) {
                            timestamper_->IncreaseGlobalNSOffset(delta);
                            // printf("[Worker%d, partner = %d] ns = %lu, remote_ns = %lu, delta = %ld\n", my_rank_, partner, ns, remote_ns, delta);
                        }
                    }
                } else {
                    uint64_t tag = (loop * (comm_sz_ - 1) * 2 * 4) + (my_rank_ - 1) * 4 + (round * (comm_sz_ - 1) * 4);
                    const int partner = 0;
                    ReadTimestampFromRDMAMem(tag);
                    ns = timestamper_->GetRefinedNS();
                    WriteTimestampToWorker(partner, ns, tag + 1);
                    remote_ns = ReadTimestampFromRDMAMem(tag + 2);
                    ns = timestamper_->GetRefinedNS();
                    WriteTimestampToWorker(partner, 4396, tag + 3);
                    int64_t delta = remote_ns - ns;
                    if (delta > 0) {
                        timestamper_->IncreaseGlobalNSOffset(delta);
                        // printf("[Worker%d, partner = %d] ns = %lu, remote_ns = %lu, delta = %ld\n", my_rank_, partner, ns, remote_ns, delta);
                    }
                }
            }
        } else {
            timestamper_->GlobalCalibrateMeasure(40, 0.1, false);
            int64_t delta = timestamper_->GetMeasuredGlobalNSOffset();

            if (delta != 0) {
                // printf("[<PerformCalibration>] node %d, delta = %ld\n", my_rank_, delta);
                timestamper_->IncreaseGlobalNSOffset(delta);
            }
        }
        // if (my_rank_ == 0)
        //     printf("[<PerformCalibration>] finished loop %lu\n", loop);
        loop++;
    }
}
