/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <cstdio>

#include "base/type.hpp"
#include "core/buffer.hpp"
#include "layout/layout_type.hpp"

// for both EP and VP, in case of implementing two KVStore classes.

struct KVKey {
    MVCCHeader mvcc_header;
    ptr_t ptr;

    bool is_empty() { return mvcc_header.pid == 0; }
};

class MVCCKVStore {
 private:
    MVCCKVStore(const MVCCKVStore&);

    Config * config_;

    static const int NUM_LOCKS = 1024;
    static const int ASSOCIATIVITY = 8;  // the associativity of slots in each bucket
    static const int MHD_RATIO = 80;  // main-header / (main-header + indirect-header)
    int HD_RATIO_;  // header / (header + entry)

    char* mem_;
    uint64_t mem_sz_;
    // uint64_t offset_;

    // kvstore key
    KVKey *keys_;
    // kvstore value
    char* values_;

    uint64_t num_slots_;  // 1 bucket = ASSOCIATIVITY slots
    uint64_t num_buckets_;  // main-header region (static)
    uint64_t num_buckets_ext_;  // indirect-header region (dynamical)
    uint64_t num_entries_;  // entry region (dynamical)

    uint64_t last_ext_;
    uint64_t last_entry_;

    pthread_spinlock_t entry_lock_;
    pthread_spinlock_t bucket_ext_lock_;
    pthread_spinlock_t bucket_locks_[NUM_LOCKS];

    uint64_t SyncFetchAndAllocValues(uint64_t n);
    uint64_t InsertId(const MVCCHeader& _mvcc_header);

 public:
    MVCCKVStore(char* mem, uint64_t mem_sz);
    ptr_t Insert(const MVCCHeader& key, const value_t& value);
    void Get(ptr_t, value_t&);
};
