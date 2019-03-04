/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <atomic>

#include "mvcc_concurrent_ll.hpp"
#include "mvcc_definition.hpp"
#include "layout/layout_type.hpp"

struct EPHeader
{
    epid_t pid;
    PropertyMVCC* mvcc_head = nullptr;
};

#define EP_ROW_ITEM_COUNT std::InferElementCount<EPHeader>(256, sizeof(std::atomic_int) + sizeof(void*))

// always fetch the object from memory pool
class EdgePropertyRow
{
 private:
    std::atomic_int property_count_;
    EdgePropertyRow* next_;
    EPHeader elements_[EP_ROW_ITEM_COUNT];

 public:
    // this function will only be called at the first node
    void InsertElement(const epid_t& pid, const value_t& value);
}  __attribute__((aligned(64)));
