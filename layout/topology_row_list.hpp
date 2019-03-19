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
    void Init();
    // this function will only be called when loading data from hdfs
    MVCCList<EdgeMVCC>* InsertInitialElement(const bool& is_out, const vid_t& conn_vtx_id,
                                             const label_t& edge_label,
                                             PropertyRowList<EdgePropertyRow>* ep_row_list_ptr);

    void ReadConnectedVertex(const Direction_T& direction, const label_t& edge_label,
                             const uint64_t& trx_id, const uint64_t& begin_time, vector<vid_t>& ret);

    void ReadConnectedEdge(const vid_t& my_vid, const Direction_T& direction, const label_t& edge_label,
                           const uint64_t& trx_id, const uint64_t& begin_time, vector<eid_t>& ret);

    // nullptr for failed
    MVCCList<EdgeMVCC>* ProcessAddEdge(const eid_t& eid, const label_t& edge_label,
                                           const uint64_t& trx_id, const uint64_t& begin_time);
    bool ProcessDropEdge(const eid_t& eid,
                         const uint64_t& trx_id, const uint64_t& begin_time);

    static void SetGlobalMemoryPool(OffsetConcurrentMemPool<VertexEdgeRow>* pool_ptr) {
        pool_ptr_ = pool_ptr;
    }
};
