/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#pragma once

#include <inttypes.h>
#include <stdint.h>
#include <iostream>
#include <set>
#include <map>
#include "bplustreelib/bplustree.hpp"
#include "core/common.hpp"
#include "glog/logging.h"
#include "utils/write_prior_rwlock.hpp"

class GCProducer;
class GCConsumer;

class RCTable {
 private:
    std::map<uint64_t, uint64_t> rct_map_;
    
    mutable WritePriorRWLock lock_;
    
    RCTable() {}
    RCTable(const RCTable&);  // not to def
    RCTable& operator=(const RCTable&);  // not to def
    ~RCTable() {}
 
 public:
    static RCTable* GetInstance() {
        static RCTable instance;
        return &instance;
    }

    void insert_trx(uint64_t ct, uint64_t trx_id);

    void query_trx(uint64_t bt, uint64_t ct, std::vector<uint64_t>& trx_ids) const;

    // Erase all transactions with CT < min-bt
    void erase_trxs(uint64_t min_bt);

    friend class GCProducer;
    friend class GCConsumer;
};
