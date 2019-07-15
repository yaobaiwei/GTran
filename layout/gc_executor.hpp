/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <vector>

#include "layout/concurrent_mem_pool.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/mvcc_value_store.hpp"
#include "layout/property_row_list.hpp"
#include "layout/row_definition.hpp"
#include "layout/topology_row_list.hpp"
#include "utils/concurrent_unordered_map.hpp"
#include "utils/config.hpp"
#include "utils/mymath.hpp"
#include "utils/tid_mapper.hpp"

class GCExecutor {
 private:
    GCExecutor() {}
    GCExecutor(const GCExecutor&);
    ~GCExecutor() {}

    ConcurrentUnorderedMap<uint64_t, OutEdge>* out_edge_map_;
    ConcurrentUnorderedMap<uint64_t, InEdge>* in_edge_map_;
    typedef ConcurrentUnorderedMap<uint64_t, OutEdge>::accessor OutEdgeAccessor;
    typedef ConcurrentUnorderedMap<uint64_t, OutEdge>::const_accessor OutEdgeConstAccessor;
    typedef ConcurrentUnorderedMap<uint64_t, InEdge>::accessor InEdgeAccessor;
    typedef ConcurrentUnorderedMap<uint64_t, InEdge>::const_accessor InEdgeConstAccessor;
    ConcurrentUnorderedMap<uint32_t, Vertex>* vertex_map_;
    typedef ConcurrentUnorderedMap<uint32_t, Vertex>::accessor VertexAccessor;
    typedef ConcurrentUnorderedMap<uint32_t, Vertex>::const_accessor VertexConstAccessor;

    MVCCValueStore* vp_store_ = nullptr;
    MVCCValueStore* ep_store_ = nullptr;
    ConcurrentMemPool<EdgePropertyRow>* ep_row_pool_ = nullptr;
    ConcurrentMemPool<VertexEdgeRow>* ve_row_pool_ = nullptr;
    ConcurrentMemPool<VertexPropertyRow>* vp_row_pool_ = nullptr;
    ConcurrentMemPool<VPropertyMVCCItem>* vp_mvcc_pool_ = nullptr;
    ConcurrentMemPool<EPropertyMVCCItem>* ep_mvcc_pool_ = nullptr;
    ConcurrentMemPool<VertexMVCCItem>* vertex_mvcc_pool_ = nullptr;
    ConcurrentMemPool<EdgeMVCCItem>* edge_mvcc_pool_ = nullptr;

    void VPropertyMVCCItemGC(VPropertyMVCCItem*);
    void EPropertyMVCCItemGC(EPropertyMVCCItem*);
    void VertexMVCCItemGC(VertexMVCCItem*);
    void EdgeMVCCItemGC(EdgeMVCCItem*);
    void VertexGC(Vertex*);
    void EdgeGC(EdgeVersion*);

 public:
    void Init(ConcurrentUnorderedMap<uint64_t, OutEdge>*,
              ConcurrentUnorderedMap<uint64_t, InEdge>*,
              ConcurrentUnorderedMap<uint32_t, Vertex>*,
              MVCCValueStore*, MVCCValueStore*);

    static GCExecutor* GetInstance() {
        static GCExecutor executor;
        return &executor;
    }
};
