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

#include <atomic>

#include "layout/concurrent_mem_pool.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/mvcc_value_store.hpp"
#include "utils/tid_pool_manager.hpp"
#include "utils/write_prior_rwlock.hpp"
#include "tbb/atomic.h"
#include "tbb/concurrent_hash_map.h"

class GCProducer;
class GCConsumer;

template <class PropertyRow>
class PropertyRowList {
 private:
    typedef typename remove_all_extents<decltype(PropertyRow::cells_)>::type CellType;
    typedef decltype(CellType::pid) PidType;
    typedef typename CellType::MVCCItemType MVCCItemType;
    typedef MVCCList<MVCCItemType> MVCCListType;

    // Initialized in data_storage.cpp
    static ConcurrentMemPool<PropertyRow>* mem_pool_;
    static MVCCValueStore* value_store_;

    std::atomic_int property_count_;
    tbb::atomic<PropertyRow*> head_, tail_;

    CellType* AllocateCell(PidType pid, int* property_count_ptr, PropertyRow** tail_ptr);

    // property_count_ptr and tail_ptr will not be nullptr when called by ProcessedModifyProperty
    CellType* LocateCell(PidType pid, int* property_count_ptr = nullptr, PropertyRow** tail_ptr = nullptr);

    typedef tbb::concurrent_hash_map<label_t, CellType*> CellMap;
    CellMap* cell_map_;
    typedef typename tbb::concurrent_hash_map<label_t, CellType*>::accessor CellAccessor;
    typedef typename tbb::concurrent_hash_map<label_t, CellType*>::const_accessor CellConstAccessor;
    static constexpr int MAP_THRESHOLD = PropertyRow::ROW_CELL_COUNT;

    // This lock is implemented to guarantee the consistency of 4 variables: head_, tail_, property_count_ and cell_map_.
    // These variables will be changed in AllocateCell(). Thus, in AllocateCell(), a write lock will be acquired.
    // In other functions, a read lock will be acquired when reading a snapshot of those variables.
    WritePriorRWLock rwlock_;

    // This lock is only used to avoid conflict between gc operation (including delete all and defrag) and all other operations:
    // write_lock -> gc; read_lock -> others
    WritePriorRWLock gc_rwlock_;

 public:
    void Init();

    // used when loading data from hdfs
    void InsertInitialCell(const PidType& pid, const value_t& value);

    READ_STAT ReadProperty(const PidType& pid, const uint64_t& trx_id,
                           const uint64_t& begin_time, const bool& read_only, value_t& ret);
    READ_STAT ReadPropertyByPKeyList(const vector<label_t>& p_key, const uint64_t& trx_id,
                                     const uint64_t& begin_time, const bool& read_only,
                                     vector<pair<label_t, value_t>>& ret);
    READ_STAT ReadAllProperty(const uint64_t& trx_id, const uint64_t& begin_time,
                              const bool& read_only, vector<pair<label_t, value_t>>& ret);
    READ_STAT ReadPidList(const uint64_t& trx_id, const uint64_t& begin_time,
                          const bool& read_only, vector<PidType>& ret);

    // Return value:
    //  bool:           True if "Modify", false if "Add"
    //  MVCCListType* : Pointer to MVCC list, nullptr if abort
    pair<bool, MVCCListType*> ProcessModifyProperty(const PidType& pid, const value_t& value, value_t& old_value,
                                                    const uint64_t& trx_id, const uint64_t& begin_time);
    MVCCListType* ProcessDropProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& old_value);

    static void SetGlobalMemoryPool(ConcurrentMemPool<PropertyRow>* mem_pool) {
        mem_pool_ = mem_pool;
    }
    static void SetGlobalValueStore(MVCCValueStore* value_store_ptr) {
        value_store_ = value_store_ptr;
    }

    void SelfGarbageCollect();
    void SelfDefragment();

    friend class GCProducer;
    friend class GCConsumer;
};

#include "property_row_list.tpp"
