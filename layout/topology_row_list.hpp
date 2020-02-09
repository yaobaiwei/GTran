/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <atomic>

#include "layout/mvcc_list.hpp"
#include "tbb/atomic.h"
#include "utils/tid_mapper.hpp"

class GCProducer;
class GCConsumer;
struct VertexEdgeRow;

class TopologyRowList {
 private:
    // Initialized in data_storage.cpp
    static ConcurrentMemPool<VertexEdgeRow>* mem_pool_;

    atomic_int edge_count_;
    tbb::atomic<VertexEdgeRow*> head_, tail_;
    vid_t my_vid_;

    void AllocateCell(const bool& is_out, const vid_t& conn_vtx_id, MVCCList<EdgeMVCCItem>* mvcc_list);

    // this lock is only used in AllocateCell. Traversal in the row list is thread-safe
    pthread_spinlock_t lock_;

    // This lock is only used to avoid conflict between gc operation (including delete all and defrag) and all other operations:
    // write_lock -> gc; read_lock -> others
    WritePriorRWLock gc_rwlock_;

 public:
    void Init(const vid_t& my_vid);

    // This function will only be called when loading data from hdfs
    MVCCList<EdgeMVCCItem>* InsertInitialCell(const bool& is_out, const vid_t& conn_vtx_id,
                                              const label_t& edge_label,
                                              PropertyRowList<EdgePropertyRow>* ep_row_list_ptr);

    READ_STAT ReadConnectedVertex(const Direction_T& direction, const label_t& edge_label,
                                  const uint64_t& trx_id, const uint64_t& begin_time,
                                  const bool& read_only, vector<vid_t>& ret);

    READ_STAT ReadConnectedEdge(const Direction_T& direction, const label_t& edge_label,
                                const uint64_t& trx_id, const uint64_t& begin_time,
                                const bool& read_only, vector<eid_t>& ret);

    /* For an specific eid on a vertex, ProcessAddEdge will only be called once.
     * This is guaranteed by DataStorage::ProcessAddE
     */
    MVCCList<EdgeMVCCItem>* ProcessAddEdge(const bool& is_out, const vid_t& conn_vtx_id,
                                           const label_t& edge_label,
                                           PropertyRowList<EdgePropertyRow>* ep_row_list_ptr,
                                           const uint64_t& trx_id, const uint64_t& begin_time);

    static void SetGlobalMemoryPool(ConcurrentMemPool<VertexEdgeRow>* mem_pool) {
        mem_pool_ = mem_pool;
    }

    void SelfGarbageCollect(const vid_t& vid, vector<pair<eid_t, bool>>* vec);
    void SelfDefragment(const vid_t&, vector<pair<eid_t, bool>>*);

    friend class GCProducer;
    friend class GCConsumer;
};
