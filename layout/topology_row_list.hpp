/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <atomic>

#include "layout/concurrent_mem_pool.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/row_definition.hpp"

class TopologyRowList {
 private:
    static OffsetConcurrentMemPool<VertexEdgeRow>* pool_ptr_;  // Initialized in data_storage.cpp

    std::atomic_int edge_count_;
    VertexEdgeRow* head_;
    vid_t my_vid_;

 public:
    void Init(const vid_t& vid) {head_ = pool_ptr_->Get(); edge_count_ = 0; my_vid_ = vid;}

    // this function will only be called when loading data from hdfs
    MVCCList<TopologyMVCC>* InsertElement(const bool& is_out, const vid_t& conn_vtx_id, const label_t& label);

    // TODO(entityless): Implement how to deal with deleted edge in MVCC
    void ReadConnectedVertex(const Direction_T& direction, const label_t& edge_label,
                             const uint64_t& trx_id, const uint64_t& begin_time, vector<vid_t>& ret);

    void ReadConnectedEdge(const Direction_T& direction, const label_t& edge_label,
                           const uint64_t& trx_id, const uint64_t& begin_time, vector<eid_t>& ret);

    static void SetGlobalMemoryPool(OffsetConcurrentMemPool<VertexEdgeRow>* pool_ptr) {
        pool_ptr_ = pool_ptr;
    }
}  __attribute__((aligned(64)));
