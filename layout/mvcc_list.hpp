/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <pthread.h>

#include <cstdio>

#include "core/factory.hpp"
#include "layout/concurrent_mem_pool.hpp"
#include "layout/mvcc_definition.hpp"
#include "utils/config.hpp"
#include "utils/simple_spinlock_guard.hpp"
#include "utils/tid_pool_manager.hpp"

class GCProducer;
class GCConsumer;

// record dependencies in MVCCList::TryPreReadUncommittedTail, used in validation phase
struct depend_trx_lists {
    std::set<uint64_t> homo_trx_list;  // commit -> commit
    std::set<uint64_t> hetero_trx_list;  // abort -> commit
};

extern tbb::concurrent_hash_map<uint64_t, depend_trx_lists> dep_trx_map;  // defined here, instantiated in data_storage.cpp
typedef tbb::concurrent_hash_map<uint64_t, depend_trx_lists>::accessor dep_trx_accessor;
typedef tbb::concurrent_hash_map<uint64_t, depend_trx_lists>::const_accessor dep_trx_const_accessor;

template<class Item>
class MVCCList {
    static_assert(std::is_base_of<AbstractMVCCItem, Item>::value, "Item must derive from AbstractMVCCItem");

 public:
    typedef Item MVCCItemType;

    MVCCList();

    typedef decltype(Item::val) ValueType;
    // Invoked only when data loading
    Item* GetInitVersion();

    // ReturnValue
    //  first: false if abort
    //  second: false if no version visible
    pair<bool, bool> GetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time,
                                       const bool& read_only, ValueType& ret);
    pair<bool, bool> SerializableLevelGetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time,
                                                        const bool& read_only, ValueType& ret);
    bool SnapshotLevelGetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time, ValueType& ret);

    // Called by GetVisibleVersion. Check if the tail_ (uncommited) is to be read.
    // if ret.first == false, abort;
    // If pre-read is performed, dependency will be recorded in this function.
    pair<bool, bool> TryPreReadUncommittedTail(const uint64_t& trx_id, const uint64_t& begin_time, const bool& read_only);

    // If nullptr, then append failed.
    ValueType* AppendVersion(const uint64_t& trx_id, const uint64_t& begin_time, ValueType* old_val_header = nullptr, bool* old_val_exists = nullptr);
    ValueType* AppendInitialVersion();
    void CommitVersion(const uint64_t& trx_id, const uint64_t& commit_time);
    void AbortVersion(const uint64_t& trx_id);

    static void SetGlobalMemoryPool(ConcurrentMemPool<Item>* mem_pool) {
        mem_pool_ = mem_pool;
    }

    Item* GetHead();

    // Clear the MVCCList
    void SelfGarbageCollect();

 private:
    static ConcurrentMemPool<Item>* mem_pool_;  // Initialized in data_storage.cpp
    static Config* config_;

    // when a MVCCList is visible outside who created it, head_ must != nullptr,
    // this can only be guaranteed by the developer who use it.
    Item* head_ = nullptr;
    Item* tail_ = nullptr;
    Item* pre_tail_ = nullptr;
    Item* tmp_pre_tail_ = nullptr;
    // tmp_pre_tail_ is not nullptr only when the tail_ is uncommitted
    pthread_spinlock_t lock_;

    friend class GCProducer;
    friend class GCConsumer;
};

#include "mvcc_list.tpp"
