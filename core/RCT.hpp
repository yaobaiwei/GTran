/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#pragma once

#include <inttypes.h>
#include <stdint.h>
#include <iostream>
#include <set>
#include "bplustreelib/bplustree.hpp"
#include "core/common.hpp"
#include "glog/logging.h"
#include "utils/simple_spinlock_guard.hpp"

struct bplus_tree_config {
        int order;
        int entries;
};

// TODO(zj): modify the B+ tree lib to support uint64
class RCTable {  // RecentCommittedTrxTable
 private:
    RCTable();
    // ~RCTable();

    struct bplus_tree * tree_;
    struct bplus_tree_config bpt_config_;

    static RCTable * rct_;
    pthread_spinlock_t lock_;

    long long trxid2llint(uint64_t trx_id);
    uint64_t llint2trxid(long long trx_id);

 public:
    static RCTable * GetInstance() {
        if (rct_ == nullptr) {
            rct_ = new RCTable();
        }
        return rct_;
    }
    bool insert_trx(uint64_t ct, uint64_t t_id);
    bool query_trx(uint64_t bt, uint64_t ct, std::set<uint64_t>& trx_ids);
    bool delete_transaction(uint64_t ct);
};

