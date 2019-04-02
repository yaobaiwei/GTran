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
    static OffsetConcurrentMemPool<VertexEdgeRow>* mem_pool_;  // Initialized in data_storage.cpp

    std::atomic_int edge_count_;
    VertexEdgeRow* head_, *tail_;
    vid_t my_vid_;

    EdgeHeader* AllocateCell();
    EdgeHeader* LocateCell(eid_t eid);

 public:
    void Init(const vid_t& my_vid);

    // this function will only be called when loading data from hdfs
    MVCCList<EdgeMVCC>* InsertInitialCell(const bool& is_out, const vid_t& conn_vtx_id,
                                          const label_t& edge_label,
                                          PropertyRowList<EdgePropertyRow>* ep_row_list_ptr);

    READ_STAT ReadConnectedVertex(const Direction_T& direction, const label_t& edge_label,
                             const uint64_t& trx_id, const uint64_t& begin_time,
                             const bool& read_only, vector<vid_t>& ret);

    READ_STAT ReadConnectedEdge(const Direction_T& direction, const label_t& edge_label,
                           const uint64_t& trx_id, const uint64_t& begin_time,
                           const bool& read_only, vector<eid_t>& ret);

    MVCCList<EdgeMVCC>* ProcessAddEdge(const bool& is_out, const vid_t& conn_vtx_id,
                                       const label_t& edge_label,
                                       PropertyRowList<EdgePropertyRow>* ep_row_list_ptr,
                                       const uint64_t& trx_id, const uint64_t& begin_time);

    static void SetGlobalMemoryPool(OffsetConcurrentMemPool<VertexEdgeRow>* mem_pool) {
        mem_pool_ = mem_pool;
    }
};
