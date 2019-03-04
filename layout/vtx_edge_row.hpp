/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <atomic>

#include "edge_property_row.hpp"
#include "mvcc_concurrent_ll.hpp"
#include "mvcc_definition.hpp"
#include "layout/layout_type.hpp"

struct EdgeHeader {
    bool is_out;//if this vtx is a, true: a -> b, false: a <- b
    label_t label;
    vid_t conn_vtx_id;
    TopoMVCC* mvcc_head = nullptr;
    EdgePropertyRow* row_head = nullptr;
};

#define VE_ROW_ITEM_COUNT std::InferElementCount<EdgeHeader>(320, sizeof(std::atomic_int) + sizeof(void*))

class VertexEdgeRow {
    //notice that there are default value of some member variable
private:
    // TODO(entityless): disable initial outside memory pool
    std::atomic_int edge_count_;  // need to set to 0 after MemPool::Get()
    VertexEdgeRow* next_;  // need to set to nullptr after MemPool::Get()
    EdgeHeader elements_[VE_ROW_ITEM_COUNT];

public:

    // call this after MemPool::Get()
    void Initial() {edge_count_ = 0; next_ = nullptr;}

    // TODO(entityless): add thread safety
    void InsertElement(const bool& is_out, const vid_t& conn_vtx_id, const label_t& label, EdgePropertyRow* ep_row);

}  __attribute__((aligned(64)));
