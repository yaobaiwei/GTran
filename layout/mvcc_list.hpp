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
    // TODO(entityless): Implement thread safe when modifying data
    // TODO(entityless): Function naming?
    // The initial value of an MVCC
    MVCC* GetCurrentVersion(const uint64_t& trx_id, const uint64_t& begin_time);

    void AppendVersion(decltype(MVCC::val) val, const uint64_t& trx_id, const uint64_t& begin_time);
    void CommitVersion(const uint64_t& trx_id, const uint64_t& begin_time);

    static void SetGlobalMemoryPool(OffsetConcurrentMemPool<MVCC>* pool_ptr) {
        pool_ptr_ = pool_ptr;
    }

 private:
    static OffsetConcurrentMemPool<MVCC>* pool_ptr_;  // Initialized in data_storage.cpp
    // TODO(entityless): enable GC

    MVCC* head_ = nullptr;
};

#include "mvcc_list.tpp"
