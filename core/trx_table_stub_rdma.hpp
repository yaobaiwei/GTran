/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

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
#include "utils/tid_mapper.hpp"

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

    mutex update_mutex_;

    Coordinator* coordinator_;

    RDMATrxTableStub(AbstractMailbox * mailbox, ThreadSafeQueue<UpdateTrxStatusReq>* pending_trx_updates) {
        config_ = Config::GetInstance();
        mailbox_ = mailbox;
        node_ = Node::StaticInstance();
        buf_ = Buffer::GetInstance();
        pending_trx_updates_ = pending_trx_updates;
        trx_table_ = TransactionTable::GetInstance();

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
