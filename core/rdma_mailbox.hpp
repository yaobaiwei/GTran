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

#pragma once

#include <emmintrin.h>
#include <vector>
#include <string>
#include <mutex>

#include "core/buffer.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/abstract_id_mapper.hpp"
#include "base/node.hpp"
#include "base/rdma.hpp"
#include "base/serialization.hpp"
#include "base/thread_safe_queue.hpp"
#include "utils/config.hpp"
#include "utils/global.hpp"
#include "utils/simple_spinlock_guard.hpp"

#include "glog/logging.h"

#define CLINE 64

class RdmaMailbox : public AbstractMailbox {
 public:
    RdmaMailbox(Node & node, Buffer * buffer) :
        node_(node), buffer_(buffer) {
        config_ = Config::GetInstance();
    }

    ~RdmaMailbox();

    void Init(vector<Node> & nodes) override;

    // When sent to the same recv buffer, the consistency relies on
    // the lock in the id_mapper
    int Send(int tid, const Message & msg) override;

    void Recv(int tid, Message & msg) override;

    bool TryRecv(int tid, Message & msg) override;

    void Sweep(int tid) override;

    void SendNotification(int dst_nid, ibinstream& in) override;

    void RecvNotification(obinstream& out) override;

 private:
    struct rbf_rmeta_t {
        uint64_t tail;  // write from here
        pthread_spinlock_t lock;
    } __attribute__((aligned(CLINE)));

    struct rbf_lmeta_t {
        uint64_t head;  // read from here
        pthread_spinlock_t lock;
    } __attribute__((aligned(CLINE)));

    // each thread uses a round-robin strategy to check its physical-queues
    struct scheduler_t {
        // round-robin
        uint64_t rr_cnt;  // choosing local or remote
        uint64_t machine_rr_cnt;  // choosing machine id
    } __attribute__((aligned(CLINE)));

    // round-robin size for choosing local or remote msg
    // TODO(nick): Move to config
    int rr_size;

    struct mailbox_data_t{
        ibinstream stream;
        int dst_nid;
        int dst_tid;
    };

    bool CheckRecvBuf(int tid, int nid);
    void FetchMsgFromRecvBuf(int tid, int nid, obinstream & um);
    bool IsBufferFull(int dst_nid, int dst_tid, uint64_t tail, uint64_t msg_sz);
    bool SendData(int tid, const mailbox_data_t& data);

    inline int GetIndex(int tid, int nid) {
        nid = nid < node_.get_local_rank() ? nid : nid - 1;
        return nid * config_->global_num_threads + tid;
    }

    Node & node_;
    Config* config_;
    Buffer * buffer_;

    vector<vector<mailbox_data_t>> pending_msgs;

    // Fail to use vector as copy constructors of ThreadSafeQueue are deleted
    ThreadSafeQueue<Message>** local_msgs;

    rbf_rmeta_t *rmetas = NULL;
    rbf_lmeta_t *lmetas = NULL;
    pthread_spinlock_t *recv_locks = NULL;
    scheduler_t *schedulers;
    pthread_spinlock_t send_notification_lock_;
};
