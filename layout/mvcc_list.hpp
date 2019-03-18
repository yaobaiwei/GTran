/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdio>

#include "layout/concurrent_mem_pool.hpp"
#include "layout/mvcc_definition.hpp"

template<class MVCC>
class MVCCList {
    static_assert(std::is_base_of<AbstractMVCC, MVCC>::value, "MVCC must derive from AbstractMVCC");
 public:
    typedef decltype(MVCC::val) ValueType;

    // TODO(entityless): Implement thread safe when modifying data
    MVCC* GetCurrentVersion(const uint64_t& trx_id, const uint64_t& begin_time);

    // If nullptr, then append failed.
    ValueType* AppendVersion(const uint64_t& trx_id, const uint64_t& begin_time);
    ValueType* AppendInitialVersion();
    void CommitVersion(const uint64_t& trx_id, const uint64_t& commit_time);  // TODO(entityless): Finish this
    ValueType AbortVersion(const uint64_t& trx_id);

    static void SetGlobalMemoryPool(OffsetConcurrentMemPool<MVCC>* pool_ptr) {
        pool_ptr_ = pool_ptr;
    }

 private:
    static OffsetConcurrentMemPool<MVCC>* pool_ptr_;  // Initialized in data_storage.cpp

    MVCC* head_ = nullptr;
    MVCC* tail_ = nullptr;
};

#include "mvcc_list.tpp"
