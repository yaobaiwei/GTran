/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#pragma once

#include <core/common.hpp>
#include <base/node.hpp>
#include <core/abstract_mailbox.hpp>
#include <core/rdma_mailbox.hpp>
#include <core/buffer.hpp>
#include <utils/config.hpp>
#include <utils/tid_mapper.hpp>
#include "glog/logging.h"
#include <iostream>

class TrxTableStub{
private:
    AbstractMailbox * mailbox_;
    Config* config_;
    Node node_;
    Buffer* buf_;
    static TrxTableStub * instance_;

    uint64_t trx_num_total_buckets_;
    uint64_t trx_num_main_buckets_;
    uint64_t trx_num_indirect_buckets_;
    uint64_t trx_num_slots_;

    uint64_t mem_sz_;
    uint64_t base_offset_;

    uint64_t ASSOCIATIVITY_;

    TrxTableStub(AbstractMailbox * mailbox){
        config_ = Config::GetInstance();
        mailbox_ = mailbox;
        node_ = Node::StaticInstance();
        buf_ = Buffer::GetInstance();

        trx_num_total_buckets_ = config_ -> trx_num_total_buckets;
        trx_num_main_buckets_ = config_ -> trx_num_main_buckets;
        trx_num_indirect_buckets_ = config_ -> trx_num_indirect_buckets;
        trx_num_slots_ = config_ -> trx_num_slots;

        mem_sz_ = config_ -> trx_table_sz;
        base_offset_ = config_ -> trx_table_offset;

        ASSOCIATIVITY_ = 8;

        // ASSOCIATIVITY_ = config_ -> ASSOCIATIVITY;
    }

public:

    static TrxTableStub* GetInstance(AbstractMailbox * mailbox){
        CHECK(mailbox);
        if(instance_ == nullptr){
            instance_ = new TrxTableStub(mailbox);
        }
        return instance_;
    }

    bool TrxTableStub::update_status(uint64_t trx_id, TRX_STAT new_status, std::vector<uint64_t> * trx_ids = nullptr);

    bool read_status(uint64_t trx_id, TRX_STAT& status);
};


