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

#include <pthread.h>
#include <stdint.h>
#include <tbb/concurrent_hash_map.h>
#include <map>
#include <utility>
#include "base/type.hpp"
#include "core/common.hpp"
#include "glog/logging.h"
#include "utils/config.hpp"
#include "tbb/atomic.h"

struct TsPtrNode {
    uint64_t ts;
    TidStatus* ptr;
    TsPtrNode* next = nullptr;
};

extern uint64_t TrxIDHash(uint64_t trx_id);

/*
 * A table to record the status of transactions
 * logic schema : trx_id, status, bt(Begin Time)
 *
 * the actual memory region: buffer
 * This class is responsible for managing this region and provide public interfaces   * to access this memory region
 */
class TransactionStatusTable {
 public:
    static TransactionStatusTable* GetInstance() {
        static TransactionStatusTable instance;
        return &instance;
    }

    bool insert_single_trx(const uint64_t& trx_id, const uint64_t& bt, const bool& readonly);

    // called if not P->V
    bool modify_status(uint64_t trx_id, TRX_STAT new_status);

    // called if p->V
    bool modify_status(uint64_t trx_id, TRX_STAT new_status, const uint64_t& ct);

    bool query_ct(uint64_t trx_id, uint64_t& ct);

    bool query_status(uint64_t trx_id, TRX_STAT & status);

    // =================GC related=================
    // called by GC thread
    void erase_trx_via_min_bt(uint64_t global_min_bt, vector<uint64_t> *non_readonly_trx_ids);
    // For non-readonly transaction, record its finish time.
    void record_nro_trx_with_et(uint64_t trx_id, uint64_t endtime);

 private:
    TransactionStatusTable();
    TransactionStatusTable(const TransactionStatusTable&);  // not to def
    TransactionStatusTable& operator=(const TransactionStatusTable&);  // not to def
    ~TransactionStatusTable() {}

    bool find_trx(uint64_t trx_id, TidStatus** p);
    bool register_ct(uint64_t trx_id, uint64_t ct);

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

    tbb::atomic<TsPtrNode*> ro_trxs_head_ = nullptr, ro_trxs_tail_ = nullptr;
    tbb::atomic<TsPtrNode*> nro_trxs_head_ = nullptr, nro_trxs_tail_ = nullptr;

    // For readonly transaction, record its begin time.
    void record_ro_trx_with_bt(TidStatus* ptr, uint64_t bt);
    // Perform the erasure of TrxTable. Return a new head.
    TsPtrNode* erase(TsPtrNode* head, TsPtrNode* tail, uint64_t global_min_bt, vector<uint64_t> *non_readonly_trx_ids = nullptr);
};
