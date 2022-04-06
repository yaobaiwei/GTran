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

#include <iostream>
#include "base/node.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/buffer.hpp"
#include "core/common.hpp"
#include "core/coordinator.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/tcp_mailbox.hpp"
#include "core/trx_table_stub.hpp"
#include "glog/logging.h"
#include "tbb/concurrent_hash_map.h"
#include "utils/config.hpp"

class TcpTrxTableStub : public TrxTableStub {
 private:
    vector<Node> workers_;

    static TcpTrxTableStub *instance_;
    vector<zmq::socket_t *> senders_;
    vector<zmq::socket_t *> receivers_;  // global_num_threads

    Coordinator* coordinator_;

    TcpTrxTableStub(AbstractMailbox *mailbox, vector<Node> workers, ThreadSafeQueue<UpdateTrxStatusReq>* pending_trx_updates) :
                    workers_(workers) {
        mailbox_ = mailbox;
        pending_trx_updates_ = pending_trx_updates;
        config_ = Config::GetInstance();
        node_ = Node::StaticInstance();
        coordinator_ = Coordinator::GetInstance();
        trx_table_ = TransactionStatusTable::GetInstance();
    }

    inline int socket_code(int n_id, int t_id) {
        return (config_ -> global_num_threads + 1) * n_id + t_id;
    }

    void send_req(int n_id, int t_id, ibinstream &in);
    bool recv_rep(int t_id, obinstream &out);

 public:
    static TcpTrxTableStub * GetInstance(AbstractMailbox * mailbox,
                                         vector<Node>& workers,
                                         ThreadSafeQueue<UpdateTrxStatusReq>* pending_trx_updates) {
        assert(instance_ == nullptr);
        // << "[TcpTrxTableStub::GetInstance] duplicate initilization";
        assert(mailbox != nullptr);
        // << "[TcpTrxTableStub::GetInstance] must provide valid mailbox";
        assert(pending_trx_updates != nullptr);
        DLOG(INFO) << "[TcpTrxTableStub::GetInstance] Initialize instance_" << mailbox;

        instance_ = new TcpTrxTableStub(mailbox, workers, pending_trx_updates);

        assert(instance_ != nullptr);
        CHECK(instance_) << "[TcpTrxTableStub::GetInstance] Null instance_";
        return instance_;
    }

    static TcpTrxTableStub * GetInstance() {
        assert(instance_!=nullptr);
        //  << "[TcpTrxTableStub::GetInstance] must initialze instance_ first";
        DLOG(INFO) << "[TcpTrxTableStub::GetInstance] Get instance_";
        return instance_;
    }

    bool Init() override;
    bool update_status(uint64_t trx_id, TRX_STAT new_status, bool is_read_only = false) override;
    bool read_status(uint64_t trx_id, TRX_STAT &status) override;
    bool read_ct(uint64_t trx_id, TRX_STAT & status, uint64_t & ct) override;
};
