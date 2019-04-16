/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <atomic>

#include "layout/concurrent_mem_pool.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/mvcc_value_store.hpp"
#include "utils/tid_mapper.hpp"
#include "tbb/concurrent_hash_map.h"

template <class PropertyRow>
class PropertyRowList {
 private:
    typedef decltype(PropertyRow::cells_[0].pid) PidType;
    typedef typename remove_pointer<decltype(PropertyRow::cells_[0].mvcc_list)>::type MVCCListType;
    typedef typename MVCCListType::MVCCItemType MVCCItemType;
    typedef typename remove_reference<decltype(PropertyRow::cells_[0])>::type CellType;

    static OffsetConcurrentMemPool<PropertyRow>* mem_pool_;  // Initialized in data_storage.cpp
    static MVCCValueStore* value_storage_;

    std::atomic_int property_count_;
    PropertyRow* head_, *tail_;
    pthread_spinlock_t lock_;

    CellType* AllocateCell(PidType pid, int* property_count_ptr = nullptr, PropertyRow** tail_ptr = nullptr);
    CellType* LocateCell(PidType pid, int* property_count_ptr = nullptr, PropertyRow** tail_ptr = nullptr);

    typedef tbb::concurrent_hash_map<label_t, CellType*> CellMap;
    CellMap* cell_map_;
    typedef typename tbb::concurrent_hash_map<label_t, CellType*>::accessor CellAccessor;
    typedef typename tbb::concurrent_hash_map<label_t, CellType*>::const_accessor CellConstAccessor;
    static constexpr int MAP_THRESHOLD = PropertyRow::ROW_ITEM_COUNT;

 public:
    void Init();

    // this function will only be called when loading data from hdfs
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

    // the bool value is true if "Modify", false if "Add"
    pair<bool, MVCCListType*> ProcessModifyProperty(const PidType& pid, const value_t& value,
                                                    const uint64_t& trx_id, const uint64_t& begin_time);
    MVCCListType* ProcessDropProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time);

    static void SetGlobalMemoryPool(OffsetConcurrentMemPool<PropertyRow>* mem_pool) {
        mem_pool_ = mem_pool;
    }
    static void SetGlobalValueStore(MVCCValueStore* value_storage_ptr) {
        value_storage_ = value_storage_ptr;
    }

    void SelfGarbageCollect();
};

#include "property_row_list.tpp"
