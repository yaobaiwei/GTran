/* Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#pragma once

#include <stdint.h>
#include <set>
#include <iostream>
#include <inttypes.h>
#include "glog/logging.h"
#include "core/common.hpp"

extern "C" {
    #include "bplustreelib/bplustree.h"
}


using namespace std;

struct bplus_tree_config {
        int order;
        int entries;
};

class RCTable {  // RecentCommittedTrxTable
 private:
    RCTable();
    // ~RCTable();

    struct bplus_tree * tree_;
    struct bplus_tree_config bpt_config_;

    static RCTable * rct_;

    int trxid2int(uint64_t trx_id);
    uint64_t int2trxid(int trx_id);

    int ct2int(uint64_t ct);

 public:
    static RCTable * GetInstance() {
        if (rct_ == nullptr) {
            rct_ = new RCTable();
        }
        return rct_;
    }
    bool insertTransaction(uint64_t ct, uint64_t t_id);
    bool queryTransactions(uint64_t bt, uint64_t ct, std::set<uint64_t>& trx_ids);
    bool delete_single_transaction(uint64_t ct);
};

