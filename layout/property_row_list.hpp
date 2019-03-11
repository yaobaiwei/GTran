/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <atomic>

#include "layout/concurrent_mem_pool.hpp"
#include "layout/mvcc_kv_store.hpp"
#include "layout/mvcc_list.hpp"

template <class PropertyRow>
class PropertyRowList {
 private:
    static OffsetConcurrentMemPool<PropertyRow>* pool_ptr_;  // Initialized in data_storage.cpp
    static MVCCKVStore* kvs_ptr_;

    std::atomic_int property_count_;
    PropertyRow* head_;

 public:
    void Init() {head_ = pool_ptr_->Get(); property_count_ = 0;}

    typedef decltype(PropertyRow::elements_[0].pid) PidType;

    // this function will only be called when loading data from hdfs
    void InsertElement(const PidType& pid, const value_t& value);

    // TODO(entityless): Implement how to deal with deleted property in MVCC
    void ReadProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& ret);
    void ReadAllProperty(const uint64_t& trx_id, const uint64_t& begin_time, vector<pair<label_t, value_t>>& ret);

    static void SetGlobalMemoryPool(OffsetConcurrentMemPool<PropertyRow>* pool_ptr) {
        pool_ptr_ = pool_ptr;
    }
    static void SetGlobalKVS(MVCCKVStore* kvs_ptr) {
        kvs_ptr_ = kvs_ptr;
    }
}  __attribute__((aligned(64)));

#include "property_row_list.tpp"
