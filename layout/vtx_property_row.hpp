/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <atomic>

#include "layout/mvcc_definition.hpp"
#include "layout/mvcc_kv_store.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/layout_type.hpp"

struct VPHeader {
    vpid_t pid;
    // PropertyMVCC* mvcc_head = nullptr;
    MVCCList<PropertyMVCC>* mvcc_list;
};

#define VP_ROW_ITEM_COUNT std::InferElementCount<VPHeader>(256, sizeof(std::atomic_int) + sizeof(void*))

// always fetch the object from memory pool
class VertexPropertyRow {
 private:
    std::atomic_int property_count_;
    VertexPropertyRow* next_;
    VPHeader elements_[VP_ROW_ITEM_COUNT];

 public:
    void Initial() {property_count_ = 0; next_ = nullptr;}

    // this function will only be called when loading data from hdfs
    void InsertElement(const vpid_t& pid, const value_t& value);

    // TODO(entityless): Implement how to deal with deleted property in MVCC
    value_t ReadProperty(vpid_t pid, const uint64_t& trx_id, const uint64_t& begin_time);
    vector<pair<label_t, value_t>> ReadAllProperty(const uint64_t& trx_id, const uint64_t& begin_time);
}  __attribute__((aligned(64)));
