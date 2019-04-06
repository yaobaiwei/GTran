/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <pthread.h>

#include <cstdio>

#include "core/factory.hpp"
#include "layout/concurrent_mem_pool.hpp"
#include "layout/mvcc_definition.hpp"
#include "layout/layout_type.hpp"

template<class MVCC>
class MVCCList {
    static_assert(std::is_base_of<AbstractMVCC, MVCC>::value, "MVCC must derive from AbstractMVCC");

 public:
    typedef MVCC MVCCType;
    typedef MVCC* MVCC_PTR;

    MVCCList();

    typedef decltype(MVCC::val) ValueType;
    // Invoked only when data loading
    MVCC* GetInitVersion();

    // TODO(entityless): Implement thread safe when modifying data
    bool GetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time, const bool& read_only, MVCC_PTR& ret);

    // If nullptr, then append failed.
    ValueType* AppendVersion(const uint64_t& trx_id, const uint64_t& begin_time);
    ValueType* AppendInitialVersion();
    void CommitVersion(const uint64_t& trx_id, const uint64_t& commit_time);  // TODO(entityless): Finish this
    ValueType AbortVersion(const uint64_t& trx_id);

    static void SetGlobalMemoryPool(OffsetConcurrentMemPool<MVCC>* mem_pool) {
        mem_pool_ = mem_pool;
    }

    MVCC* GetHead();

    void SelfGarbageCollect();

    struct SimpleSpinLockGuard {
        pthread_spinlock_t* lock_ptr;
        explicit SimpleSpinLockGuard(pthread_spinlock_t* _lock_ptr) {
            lock_ptr = _lock_ptr;
            pthread_spin_lock(lock_ptr);
        }
        ~SimpleSpinLockGuard() {
            pthread_spin_unlock(lock_ptr);
        }
    };

 private:
    static OffsetConcurrentMemPool<MVCC>* mem_pool_;  // Initialized in data_storage.cpp

    // when a MVCCList is visible outside who created it, head_ must != nullptr,
    // this can only be guaranteed by the developer who use it.
    MVCC* head_ = nullptr;
    MVCC* tail_ = nullptr;
    MVCC* tail_last_ = nullptr;
    MVCC* tail_last_buffer_ = nullptr;
    // tail_last_buffer_ this is not nullptr only when the tail is uncommited
    pthread_spinlock_t lock_;
};

#include "mvcc_list.tpp"
