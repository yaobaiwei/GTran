/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <atomic>

#include "layout/concurrent_mem_pool.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/mvcc_value_store.hpp"
#include "utils/tid_mapper.hpp"
#include "utils/write_prior_rwlock.hpp"
#include "tbb/concurrent_hash_map.h"

template <class PropertyRow>
class PropertyRowList {
 private:
    typedef typename remove_all_extents<decltype(PropertyRow::cells_)>::type CellType;
    typedef decltype(CellType::pid) PidType;
    typedef typename CellType::MVCCItemType MVCCItemType;
    typedef MVCCList<MVCCItemType> MVCCListType;

    static ConcurrentMemPool<PropertyRow>* mem_pool_;  // Initialized in data_storage.cpp
    static MVCCValueStore* value_storage_;

    std::atomic_int property_count_;
    PropertyRow* head_, *tail_;
    WritePriorRWLock rwlock_;

    CellType* AllocateCell(PidType pid, int* property_count_ptr = nullptr, PropertyRow** tail_ptr = nullptr);
    CellType* LocateCell(PidType pid, int* property_count_ptr = nullptr, PropertyRow** tail_ptr = nullptr);

    typedef tbb::concurrent_hash_map<label_t, CellType*> CellMap;
    CellMap* cell_map_;
    typedef typename tbb::concurrent_hash_map<label_t, CellType*>::accessor CellAccessor;
    typedef typename tbb::concurrent_hash_map<label_t, CellType*>::const_accessor CellConstAccessor;
    static constexpr int MAP_THRESHOLD = PropertyRow::ROW_ITEM_COUNT;

 public:
    void Init();

    // This function will only be called when loading data from hdfs
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
    //  MVCCListType* : Pointer to MVCC list, need to abort if is nullptr
    pair<bool, MVCCListType*> ProcessModifyProperty(const PidType& pid, const value_t& value,
                                                    const uint64_t& trx_id, const uint64_t& begin_time);
    MVCCListType* ProcessDropProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time);

    static void SetGlobalMemoryPool(ConcurrentMemPool<PropertyRow>* mem_pool) {
        mem_pool_ = mem_pool;
    }
    static void SetGlobalValueStore(MVCCValueStore* value_storage_ptr) {
        value_storage_ = value_storage_ptr;
    }

    void SelfGarbageCollect();
};

#include "property_row_list.tpp"
