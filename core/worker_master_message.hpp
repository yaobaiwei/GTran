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

class TrxTableStubImpl{
private:
    AbstractMailbox * mailbox_;
    Config* config_;
    Node node_;
    Buffer* buf_;
    static TrxTableStubImpl * instance_;

    uint64_t num_total_buckets_;
    uint64_t num_main_buckets_;
    uint64_t num_indirect_buckets_;
    uint64_t num_slots_;

    uint64_t mem_sz_;
    uint64_t base_offset_;

    uint64_t ASSOCIATIVITY_;

    TrxTableStubImpl(AbstractMailbox * mailbox){
        config_ = Config::GetInstance();
        mailbox_ = mailbox;
        node_ = Node::StaticInstance();
        buf_ = Buffer::GetInstance();

        num_total_buckets_ = config_ -> num_total_buckets;
        num_main_buckets_ = config_ -> num_main_buckets;
        num_indirect_buckets_ = config_ -> num_indirect_buckets;
        num_slots_ = config_ -> num_slots;

        mem_sz_ = config_ -> trx_table_sz;
        base_offset_ = config_ -> trx_table_offset;

        ASSOCIATIVITY_ = 8;

        // ASSOCIATIVITY_ = config_ -> ASSOCIATIVITY;
    }

public:

    static TrxTableStubImpl* GetInstance(AbstractMailbox * mailbox){
        CHECK(mailbox);
        if(instance_ == nullptr){
            instance_ = new TrxTableStubImpl(mailbox);
        }
        return instance_;
    }

    bool update_status(uint64_t trx_id, TRX_STAT new_status);

    bool read_status(uint64_t trx_id, TRX_STAT& status);
};



