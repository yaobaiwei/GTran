/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <atomic>

#include "mvcc_concurrent_ll.hpp"
#include "mvcc_definition.hpp"
#include "layout/layout_type.hpp"

struct VPHeader
{
    vpid_t pid;
    PropertyMVCC* mvcc_head = nullptr;
};

#define VP_ROW_ITEM_COUNT std::InferElementCount<VPHeader>(256, sizeof(std::atomic_int) + sizeof(void*))

// always fetch the object from memory pool
class VertexPropertyRow
{
 private:
    std::atomic_int property_count_;
    VertexPropertyRow* next_;
    VPHeader elements_[VP_ROW_ITEM_COUNT];

 public:
    void Initial(){property_count_ = 0; next_ = nullptr;}

    // this function will only be called at the first node
    void InsertElement(const vpid_t& pid, const value_t& value);

}  __attribute__((aligned(64)));
