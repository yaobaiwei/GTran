/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <atomic>

#include "layout/mvcc_definition.hpp"
#include "layout/mvcc_kv_store.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/layout_type.hpp"

struct EPHeader {
    epid_t pid;
    // PropertyMVCC* mvcc_head = nullptr;
    MVCCList<PropertyMVCC>* mvcc_list;
};

#define EP_ROW_ITEM_COUNT std::InferElementCount<EPHeader>(256, sizeof(std::atomic_int) + sizeof(void*))

// always fetch the object from memory pool
class EdgePropertyRow {
 private:
    std::atomic_int property_count_;
    EdgePropertyRow* next_;
    EPHeader elements_[EP_ROW_ITEM_COUNT];

 public:
    void Initial() {property_count_ = 0; next_ = nullptr;}

    // this function will only be called when loading data from hdfs
    void InsertElement(const epid_t& pid, const value_t& value);

    // TODO(entityless): Implement how to deal with deleted property in MVCC
    value_t ReadProperty(epid_t pid, const uint64_t& trx_id, const uint64_t& begin_time);
    vector<pair<label_t, value_t>> ReadAllProperty(const uint64_t& trx_id, const uint64_t& begin_time);
}  __attribute__((aligned(64)));
