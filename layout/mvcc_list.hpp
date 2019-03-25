/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdio>

#include "core/factory.hpp"
#include "layout/concurrent_mem_pool.hpp"
#include "layout/mvcc_definition.hpp"
#include "layout/layout_type.hpp"

template<class MVCC>
class MVCCList {
    static_assert(std::is_base_of<AbstractMVCC, MVCC>::value, "MVCC must derive from AbstractMVCC");

 public:
    typedef decltype(MVCC::val) ValueType;
    // Invoked only when data loading
    MVCC* GetInitVersion();

    // TODO(entityless): Implement thread safe when modifying data
    MVCC* GetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time);

    // If nullptr, then append failed.
    ValueType* AppendVersion(const uint64_t& trx_id, const uint64_t& begin_time);
    ValueType* AppendInitialVersion();
    void CommitVersion(const uint64_t& trx_id, const uint64_t& commit_time);  // TODO(entityless): Finish this
    ValueType AbortVersion(const uint64_t& trx_id);

    static void SetGlobalMemoryPool(OffsetConcurrentMemPool<MVCC>* mem_pool) {
        mem_pool_ = mem_pool;
    }

    MVCC* GetHead();

 private:
    static OffsetConcurrentMemPool<MVCC>* mem_pool_;  // Initialized in data_storage.cpp

    MVCC* head_ = nullptr;
    MVCC* tail_ = nullptr;
    MVCC* tail_last_ = nullptr;
    MVCC* tail_last_buffer_ = nullptr;
};

#include "mvcc_list.tpp"
