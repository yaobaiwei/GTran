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
#include "utils/tid_mapper.hpp"

template<class Item>
class MVCCList {
    static_assert(std::is_base_of<AbstractMVCCItem, Item>::value, "Item must derive from AbstractMVCCItem");

 public:
    typedef Item MVCCItemType;
    typedef Item* MVCCItem_PTR;

    MVCCList();

    typedef decltype(Item::val) ValueType;
    // Invoked only when data loading
    Item* GetInitVersion();

    bool GetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time,
                           const bool& read_only, MVCCItem_PTR& ret);

    // If nullptr, then append failed.
    ValueType* AppendVersion(const uint64_t& trx_id, const uint64_t& begin_time);
    ValueType* AppendInitialVersion();
    void CommitVersion(const uint64_t& trx_id, const uint64_t& commit_time);  // TODO(entityless): Finish this
    ValueType AbortVersion(const uint64_t& trx_id);

    static void SetGlobalMemoryPool(ConcurrentMemPool<Item>* mem_pool) {
        mem_pool_ = mem_pool;
    }

    Item* GetHead();

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
    static ConcurrentMemPool<Item>* mem_pool_;  // Initialized in data_storage.cpp

    // when a MVCCList is visible outside who created it, head_ must != nullptr,
    // this can only be guaranteed by the developer who use it.
    Item* head_ = nullptr;
    Item* tail_ = nullptr;
    Item* pre_tail_ = nullptr;
    Item* tmp_pre_tail_ = nullptr;
    // tmp_pre_tail_ is not nullptr only when the tail is uncommitted
    pthread_spinlock_t lock_;
};

#include "mvcc_list.tpp"
