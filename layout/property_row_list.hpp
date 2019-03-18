/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <atomic>

#include "layout/concurrent_mem_pool.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/mvcc_value_store.hpp"

template <class PropertyRow>
class PropertyRowList {
 private:
    static OffsetConcurrentMemPool<PropertyRow>* pool_ptr_;  // Initialized in data_storage.cpp
    static MVCCValueStore* value_storage_ptr_;

    std::atomic_int property_count_;
    PropertyRow* head_;

 public:
    void Init();

    typedef decltype(PropertyRow::elements_[0].pid) PidType;

    // this function will only be called when loading data from hdfs
    void InsertInitialElement(const PidType& pid, const value_t& value);

    void ReadProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& ret);
    void ReadAllProperty(const uint64_t& trx_id, const uint64_t& begin_time, vector<pair<label_t, value_t>>& ret);
    void ReadPidList(const uint64_t& trx_id, const uint64_t& begin_time, vector<PidType>& ret);

    // TODO(entityless): Implement 2 function below
    bool ProcessModifyProperty(const PidType& pid, const value_t& value, const uint64_t& trx_id, const uint64_t& begin_time);
    bool ProcessDropProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time);

    void Commit(const PidType& pid, const uint64_t& trx_id, const uint64_t& commit_time);
    void Abort(const PidType& pid, const uint64_t& trx_id);

    static void SetGlobalMemoryPool(OffsetConcurrentMemPool<PropertyRow>* pool_ptr) {
        pool_ptr_ = pool_ptr;
    }
    static void SetGlobalKVS(MVCCValueStore* value_storage_ptr) {
        value_storage_ptr_ = value_storage_ptr;
    }
};

#include "property_row_list.tpp"
