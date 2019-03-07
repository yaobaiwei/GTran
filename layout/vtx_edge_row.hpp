/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <atomic>

#include "layout/edge_property_row.hpp"
#include "layout/mvcc_definition.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/layout_type.hpp"

struct EdgeHeader {
    // TODO(entityless): Remove label and row_head if not used in the future "release version"
    bool is_out;  // if this vtx is a, true: a -> b, false: a <- b
    label_t label;
    vid_t conn_vtx_id;
    // TopoMVCC* mvcc_head = nullptr;
    MVCCList<TopoMVCC>* mvcc_list;
    EdgePropertyRow* row_head = nullptr;
};

#define VE_ROW_ITEM_COUNT std::InferElementCount<EdgeHeader>(320, sizeof(std::atomic_int) + sizeof(void*))

// always fetch the object from memory pool
class VertexEdgeRow {
 private:
    std::atomic_int edge_count_;  // need to set to 0 after MemPool::Get()
    VertexEdgeRow* next_;  // need to set to nullptr after MemPool::Get()
    EdgeHeader elements_[VE_ROW_ITEM_COUNT];

 public:
    // call this after MemPool::Get()
    void Initial() {edge_count_ = 0; next_ = nullptr;}

    // this function will only be called when loading data from hdfs
    void InsertElement(const bool& is_out, const vid_t& conn_vtx_id, const label_t& label, EdgePropertyRow* ep_row);

    // TODO(entityless): Implement how to deal with deleted edge in MVCC
    vector<vid_t> ReadInVertex(const label_t& edge_label, const uint64_t& trx_id, const uint64_t& begin_time);
    vector<vid_t> ReadOutVertex(const label_t& edge_label, const uint64_t& trx_id, const uint64_t& begin_time);
    vector<vid_t> ReadBothVertex(const label_t& edge_label, const uint64_t& trx_id, const uint64_t& begin_time);
    // my_vid is needed as VertexEdgeRow won't know the vid of itself
    vector<eid_t> ReadInEdge(const label_t& edge_label, const vid_t& my_vid,
                             const uint64_t& trx_id, const uint64_t& begin_time);
    vector<eid_t> ReadOutEdge(const label_t& edge_label, const vid_t& my_vid,
                              const uint64_t& trx_id, const uint64_t& begin_time);
    vector<eid_t> ReadBothEdge(const label_t& edge_label, const vid_t& my_vid,
                               const uint64_t& trx_id, const uint64_t& begin_time);
}  __attribute__((aligned(64)));
