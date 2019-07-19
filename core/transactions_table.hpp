/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 *          Modified by Changji LI (cjli@cse.cuhk.edu.hk)
 */
#pragma once

#include <pthread.h>
#include <stdint.h>
#include <tbb/concurrent_hash_map.h>
#include <map>
#include <utility>
#include "base/type.hpp"
#include "core/common.hpp"
#include "glog/logging.h"
#include "utils/config.hpp"

struct Trx {
    uint64_t trx_id;
    uint64_t bt;
    uint64_t ct;
    TRX_STAT status;
};

/*
 * A table to record the status of transactions
 * logic schema : trx_id, status, bt(Begin Time)
 * 
 * the actual memory region: buffer
 * This class is responsible for managing this region and provide public interfaces   * to access this memory region
 */

class TrxGlobalCoordinator{
 private:
    static TrxGlobalCoordinator * trx_coordinator;

    TrxGlobalCoordinator();
    // ~TrxGlobalCoordinator();

    /* core fields */
    uint64_t next_trx_id_;

    // bt table is separated from status region since we must keep status region in RDMA
    tbb::concurrent_hash_map<uint64_t, uint64_t> bt_table_;
    typedef tbb::concurrent_hash_map<uint64_t, uint64_t>::accessor bt_table_accessor;
    typedef tbb::concurrent_hash_map<uint64_t, uint64_t>::const_accessor bt_table_const_accessor;

    uint64_t next_time_;

    char * buffer_;
    uint64_t buffer_sz_;
    TidStatus * table_;

    const uint64_t ASSOCIATIVITY_ = 8;
    const double MI_RATIO_ = 0.8;  // the ratio of main buckets vs indirect buckets
    uint64_t trx_num_total_buckets_;
    uint64_t trx_num_main_buckets_;
    uint64_t trx_num_indirect_buckets_;
    uint64_t trx_num_slots_;

    // the next available bucket.
    // table[last_ext]
    uint64_t last_ext_;

    /* secondary fields: used to operate on external objects and the objects above*/
    Config * config_;

    bool find_trx(uint64_t trx_id, TidStatus** p);

    bool allocate_trx_id(uint64_t& trx_id);

    // allocate bt and trx_id
    // Note that trx_id must begin with 1, not 0
    bool allocate_bt(uint64_t &bt);

    // assign a ct for a existing trx
    // can only be called for once for some specific Transsaction undefined behavior if some transaction call it over once
    bool allocate_ct(uint64_t &ct);

    bool register_ct(uint64_t trx_id, uint64_t ct);
    bool register_bt(uint64_t trx_id, uint64_t bt);
    bool deregister_bt(uint64_t trx_id);
    uint64_t next_trx_id();

 public:
    static TrxGlobalCoordinator* GetInstance();

    bool insert_single_trx(uint64_t trx_id, uint64_t& bt);

    bool delete_single_item(uint64_t trx_id);

    // called if not P->V
    bool modify_status(uint64_t trx_id, TRX_STAT new_status);

    // called if p->V
    bool modify_status(uint64_t trx_id, TRX_STAT new_status, uint64_t& ct, bool is_read_only);

    bool query_bt(uint64_t trx_id, uint64_t& bt);

    bool query_ct(uint64_t trx_id, uint64_t& ct);

    bool query_status(uint64_t trx_id, TRX_STAT & status);

    bool query_single_item(uint64_t trx_id, Trx & trx);

    bool print_single_item(uint64_t trx_id);
};

