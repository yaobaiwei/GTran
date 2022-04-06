// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>

#include "layout/mvcc_list.hpp"
#include "tbb/atomic.h"
#include "utils/tid_pool_manager.hpp"

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
    ~TopologyRowList();

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

    void SelfGarbageCollect(vector<pair<eid_t, bool>>* vec);
    void SelfDefragment(vector<pair<eid_t, bool>>*);

    friend class GCProducer;
    friend class GCConsumer;
};
