// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <utility>
#include "core/rdma_mailbox.hpp"

RdmaMailbox::~RdmaMailbox() {
    for (int i = 0; i < config_->global_num_threads; i++) {
        delete local_msgs[i];
    }

    free(local_msgs);
    free(schedulers);
    free(recv_locks);
    free(lmetas);
    free(rmetas);
}

void RdmaMailbox::Init(vector<Node> & nodes) {
    // Init RDMA
    rdma_mem_t mem_info;
    mem_info.mem_conn = config_->kvstore;
    mem_info.mem_conn_sz = config_->conn_buf_sz;
    mem_info.mem_dgram = config_->dgram_send_buf;
    mem_info.mem_dgram_sz = config_->dgram_buf_sz;
    mem_info.mem_dgram_recv = config_->dgram_recv_buf;
    mem_info.mem_dgram_recv_sz = config_->dgram_recv_buffer_sz;

    int nid = node_.get_local_rank();


    // Other threads may call RDMARead or RDMAWrite in:
    //      Worker::Start (with tid = config_->global_num_threads)
    //      Worker::ProcessAllocatedTimestamp (with tid = config_->global_num_threads + 1)
    //      Worker::RecvNotification (with tid = config_->global_num_threads + 2)
    //      RunningTrxList::UpdateMinBT (with tid = config_->global_num_threads + 3)
    //      Coordinator::PerformCalibration (with tid = config_->global_num_threads + 4)
    RDMA_init(config_->global_num_workers, config_->global_num_threads + Config::extra_rdma_rc_thread_count, nid, mem_info, nodes);

    int nrbfs = (config_->global_num_workers - 1) * config_->global_num_threads;

    rmetas = (rbf_rmeta_t *)malloc(sizeof(rbf_rmeta_t) * nrbfs);
    memset(rmetas, 0, sizeof(rbf_rmeta_t) * nrbfs);
    for (int i = 0; i < nrbfs; i++) {
        rmetas[i].tail = 0;
        pthread_spin_init(&rmetas[i].lock, 0);
    }

    lmetas = (rbf_lmeta_t *)malloc(sizeof(rbf_lmeta_t) * nrbfs);
    memset(lmetas, 0, sizeof(rbf_lmeta_t) * nrbfs);
    for (int i = 0; i < nrbfs; i++) {
        lmetas[i].head = 0;
        pthread_spin_init(&lmetas[i].lock, 0);
    }

    recv_locks = (pthread_spinlock_t *)malloc(sizeof(pthread_spinlock_t) * config_->global_num_threads);
    for (int i = 0; i < config_->global_num_threads; i++) {
        pthread_spin_init(&recv_locks[i], 0);
    }

    schedulers = (scheduler_t *)malloc(sizeof(scheduler_t) * config_->global_num_threads);
    memset(schedulers, 0, sizeof(scheduler_t) * config_->global_num_threads);

    local_msgs = reinterpret_cast<ThreadSafeQueue<Message> **>(
                malloc(sizeof(ThreadSafeQueue<Message>*) * config_->global_num_threads));
    for (int i = 0; i < config_->global_num_threads; i++) {
        local_msgs[i] = new ThreadSafeQueue<Message>();
    }

    // 1 more thread for worker to send init msg
    pending_msgs.resize(config_->global_num_threads + Config::extra_send_buf_count);
    rr_size = 3;

    pthread_spin_init(&send_notification_lock_, 0);
}

bool RdmaMailbox::IsBufferFull(int dst_nid, int dst_tid, uint64_t tail, uint64_t msg_sz) {
    static uint64_t old_head = 0;
    uint64_t rbf_sz = MiB2B(config_->global_per_recv_buffer_sz_mb);
    uint64_t head = *(uint64_t *)buffer_->GetRemoteHeadBuf(dst_tid, dst_nid);

    return rbf_sz < (tail - head + msg_sz);
}

void RdmaMailbox::Sweep(int tid) {
    if (pending_msgs[tid].size() == 0) {
        return;
    }

    for (auto it = pending_msgs[tid].begin(); it != pending_msgs[tid].end();) {
        if (SendData(tid, *it)) {
            it = pending_msgs[tid].erase(it);
        } else {
            it++;
        }
    }
}

int RdmaMailbox::Send(int tid, const Message & msg) {
    if (msg.meta.recver_nid == node_.get_local_rank()) {
        local_msgs[msg.meta.recver_tid]->Push(msg);
    } else {
        mailbox_data_t data;
        data.dst_nid = msg.meta.recver_nid;
        data.dst_tid = msg.meta.recver_tid;

        data.stream << msg;

        pending_msgs[tid].push_back(move(data));
    }
}

bool RdmaMailbox::SendData(int tid, const mailbox_data_t& data) {
    // Send data to remote machine only
    int dst_nid = data.dst_nid;
    int dst_tid = data.dst_tid;

    size_t data_sz = data.stream.size();
    uint64_t msg_sz = sizeof(uint64_t) + ceil(data_sz, sizeof(uint64_t)) + sizeof(uint64_t);

    rbf_rmeta_t *rmeta = &rmetas[GetIndex(dst_tid, dst_nid)];

    pthread_spin_lock(&rmeta->lock);
     // detect overflow
    if (IsBufferFull(dst_nid, dst_tid, rmeta->tail, msg_sz)) {
        pthread_spin_unlock(&rmeta->lock);
        return false;
    }
    // update tail
    uint64_t off = rmeta->tail;
    rmeta->tail += msg_sz;
    pthread_spin_unlock(&rmeta->lock);

    uint64_t rbf_sz = MiB2B(config_->global_per_recv_buffer_sz_mb);
    char *rdma_buf = buffer_->GetSendBuf(tid);

    *((uint64_t *)rdma_buf) = data_sz;  // header
    rdma_buf += sizeof(uint64_t);

    memcpy(rdma_buf, data.stream.get_buf(), data_sz);    // data
    rdma_buf += ceil(data_sz, sizeof(uint64_t));

    *((uint64_t*)rdma_buf) = data_sz;   // footer

    RDMA &rdma = RDMA::get_rdma();
    uint64_t rdma_off = buffer_->GetRecvBufOffset(dst_tid, dst_nid);
    pthread_spin_lock(&rmeta->lock);
    if (off / rbf_sz == (off + msg_sz - 1) / rbf_sz) {
        rdma.dev->RdmaWrite(dst_tid, dst_nid, buffer_->GetSendBuf(tid), msg_sz, rdma_off + (off % rbf_sz));
    } else {
        uint64_t _sz = rbf_sz - (off % rbf_sz);
        rdma.dev->RdmaWrite(dst_tid, dst_nid, buffer_->GetSendBuf(tid), _sz, rdma_off + (off % rbf_sz));
        rdma.dev->RdmaWrite(dst_tid, dst_nid, buffer_->GetSendBuf(tid) + _sz, msg_sz - _sz, rdma_off);
    }
    pthread_spin_unlock(&rmeta->lock);
    return true;
}

void RdmaMailbox::Recv(int tid, Message & msg) {
    while (true) {
        int machine_id = (schedulers[tid].rr_cnt++) % node_.get_local_size();
        if (machine_id != node_.get_local_rank() && CheckRecvBuf(tid, machine_id)) {
            obinstream um;
            FetchMsgFromRecvBuf(tid, machine_id, um);
            um >> msg;
        }
    }
}


bool RdmaMailbox::TryRecv(int tid, Message & msg) {
    pthread_spin_lock(&recv_locks[tid]);
    int type = (schedulers[tid].rr_cnt++) % rr_size;

    // Try local message queue in higher priority
    // Use round-robin to avoid starvation
    if (type != 0) {
        if (local_msgs[tid]->Size() != 0) {
            local_msgs[tid]->WaitAndPop(msg);
            pthread_spin_unlock(&recv_locks[tid]);
            return true;
        }
    }

    // Try rdma memory
    for (int i = 0; i < node_.get_local_size(); i++) {
        int machine_id = (schedulers[tid].machine_rr_cnt++) % node_.get_local_size();
        if (machine_id != node_.get_local_rank() && CheckRecvBuf(tid, machine_id)) {
            obinstream um;
            FetchMsgFromRecvBuf(tid, machine_id, um);
            pthread_spin_unlock(&recv_locks[tid]);

            um >> msg;
            return true;
        }
    }
    pthread_spin_unlock(&recv_locks[tid]);
    return false;
}

bool RdmaMailbox::CheckRecvBuf(int tid, int nid) {
    rbf_lmeta_t *lmeta = &lmetas[GetIndex(tid, nid)];
    char * rbf = buffer_->GetRecvBuf(tid, nid);
    uint64_t rbf_sz = buffer_->GetRecvBufSize();
    volatile uint64_t msg_size = *(volatile uint64_t *)(rbf + lmeta->head % rbf_sz);  // header
    return msg_size != 0;
}

void RdmaMailbox::FetchMsgFromRecvBuf(int tid, int nid, obinstream & um) {
    rbf_lmeta_t *lmeta = &lmetas[GetIndex(tid, nid)];
    char * rbf = buffer_->GetRecvBuf(tid, nid);
    uint64_t rbf_sz = buffer_->GetRecvBufSize();
    volatile uint64_t pop_msg_size = *(volatile uint64_t *)(rbf + lmeta->head % rbf_sz);  // header

    uint64_t to_footer = sizeof(uint64_t) + ceil(pop_msg_size, sizeof(uint64_t));
    volatile uint64_t * footer = (volatile uint64_t *)(rbf + (lmeta->head  + to_footer) % rbf_sz);  // footer

    if (pop_msg_size) {
        // Make sure RDMA trans is done
        while (*footer != pop_msg_size) {
            _mm_pause();
            CHECK(*footer == 0 || *footer == pop_msg_size);
        }

        // IF it is a ring(rare situation)
        uint64_t start = (lmeta->head + sizeof(uint64_t)) % rbf_sz;
        uint64_t end = (lmeta->head + sizeof(uint64_t) + pop_msg_size) % rbf_sz;
        if (start > end) {
            char* tmp_buf = new char[pop_msg_size];
            memcpy(tmp_buf, rbf + start, pop_msg_size - end);
            memcpy(tmp_buf + pop_msg_size - end, rbf, end);

            // register tmp_buf into obinstream,
            // the obinstream will charge the memory of buf, including memory release
            um.assign(tmp_buf, pop_msg_size, 0);

            // clean
            memset(rbf + start, 0, pop_msg_size - end);
            memset(rbf, 0, ceil(end, sizeof(uint64_t)));
        } else {
            char* tmp_buf = new char[pop_msg_size];
            memcpy(tmp_buf, rbf + start, pop_msg_size);

            um.assign(tmp_buf, pop_msg_size, 0);

            // clean the data
            memset(rbf + start, 0, ceil(pop_msg_size, sizeof(uint64_t)));
        }

        // clear header and footer
        *(uint64_t *)(rbf + lmeta->head % rbf_sz) = 0;
        *footer = 0;

        // advance the pointer
        lmeta->head += 2 * sizeof(uint64_t) + ceil(pop_msg_size, sizeof(uint64_t));

        // update heads of ring buffer to writer to help it detect overflow
        const uint64_t threshold = rbf_sz / 16;
        char *head = buffer_->GetLocalHeadBuf(tid, nid);
        if (lmeta->head - *(uint64_t *)head > threshold) {
            *(uint64_t *)head = lmeta->head;
            if (node_.get_local_rank() == nid) {
                *(uint64_t *)buffer_->GetRemoteHeadBuf(tid, nid) = lmeta->head;
            } else {
                rbf_rmeta_t *rmeta = &rmetas[GetIndex(tid, nid)];
                RDMA &rdma = RDMA::get_rdma();
                uint64_t off = buffer_->GetRemoteHeadBufOffset(tid, nid);
                pthread_spin_lock(&rmeta->lock);
                rdma.dev->RdmaWrite(tid, nid, head, sizeof(uint64_t), off);
                pthread_spin_unlock(&rmeta->lock);
            }
        }
    }
}

void RdmaMailbox::SendNotification(int dst_nid, ibinstream& in) {
    RDMA &rdma = RDMA::get_rdma();
    int failed = 0;
    SimpleSpinLockGuard lock_guard(&send_notification_lock_);

    while (rdma.dev->RdmaSend(dst_nid, config_->dgram_send_buf, in.get_buf(), in.size()) != 0) {
        failed++;
        cout << "Fail to send msg from " << node_.get_world_rank() << " to "
            << dst_nid << ", retry " << failed << " times"<< endl;
        CHECK_LT(failed, 10) << "Node " << node_.get_world_rank() << " fail sending msg 10 times!";
    }
}

void RdmaMailbox::RecvNotification(obinstream& out) {
    RDMA &rdma = RDMA::get_rdma();
    int failed = 0;
    uint64_t received_sz = 0;
    char* received;
    while (rdma.dev->RdmaRecv(received, received_sz) != 0) {
        failed++;
        cout << "Node " << node_.get_world_rank() <<" fail to post recv request " << failed << " times!"<< endl;
        CHECK_LT(failed, 10) << "Node " << node_.get_world_rank() << " fail post recv request 10 times!";
    }
    out.assign(received, received_sz, 0);
}
