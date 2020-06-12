// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
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
#include "core/running_trx_list.hpp"
#include "core/trx_table_stub.hpp"
#include "glog/logging.h"
#include "utils/config.hpp"

class RDMATrxTableStub : public TrxTableStub{
 private:
    Buffer* buf_;
    static RDMATrxTableStub * instance_;

    uint64_t trx_num_total_buckets_;
    uint64_t trx_num_main_buckets_;
    uint64_t trx_num_indirect_buckets_;
    uint64_t trx_num_slots_;

    uint64_t mem_sz_;
    uint64_t base_offset_;

    uint64_t ASSOCIATIVITY_;

    Coordinator* coordinator_;

    RDMATrxTableStub(AbstractMailbox * mailbox, ThreadSafeQueue<UpdateTrxStatusReq>* pending_trx_updates) {
        config_ = Config::GetInstance();
        mailbox_ = mailbox;
        node_ = Node::StaticInstance();
        buf_ = Buffer::GetInstance();
        pending_trx_updates_ = pending_trx_updates;
        trx_table_ = TransactionStatusTable::GetInstance();

        trx_num_total_buckets_ = config_ -> trx_num_total_buckets;
        trx_num_main_buckets_ = config_ -> trx_num_main_buckets;
        trx_num_indirect_buckets_ = config_ -> trx_num_indirect_buckets;
        trx_num_slots_ = config_ -> trx_num_slots;

        mem_sz_ = config_ -> trx_table_sz;
        base_offset_ = config_ -> trx_table_offset;

        ASSOCIATIVITY_ = config_ -> ASSOCIATIVITY;

        coordinator_ = Coordinator::GetInstance();
    }

 public:
    static RDMATrxTableStub* GetInstance(AbstractMailbox * mailbox = nullptr, ThreadSafeQueue<UpdateTrxStatusReq>* pending_trx_updates = nullptr) {
        if(instance_ == nullptr && mailbox != nullptr && pending_trx_updates != nullptr) {
            instance_ = new RDMATrxTableStub(mailbox, pending_trx_updates);
        }
        return instance_;
    }

    bool Init() override{};

    bool update_status(uint64_t trx_id, TRX_STAT new_status, bool is_read_only = false) override;

    bool read_status(uint64_t trx_id, TRX_STAT& status) override;
    bool read_ct(uint64_t trx_id, TRX_STAT & status, uint64_t & ct) override;
};
