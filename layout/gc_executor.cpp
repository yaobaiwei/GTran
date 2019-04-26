/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "gc_executor.hpp"

using namespace std;

void GCExecutor::Init(tbb::concurrent_hash_map<uint64_t, MVCCList<EdgeMVCCItem>*>* out_edge_map,
                      tbb::concurrent_hash_map<uint64_t, MVCCList<EdgeMVCCItem>*>* in_edge_map,
                      tbb::concurrent_hash_map<uint32_t, VertexItem>* vertex_map,
                      MVCCValueStore* vp_store, MVCCValueStore* ep_store) {
    out_edge_map_ = out_edge_map;
    in_edge_map_ = in_edge_map;
    vertex_map_ = vertex_map;
    vp_store_ = vp_store;
    ep_store_ = ep_store;

    ve_row_pool_ = ConcurrentMemPool<VertexEdgeRow>::GetInstance();
    vp_row_pool_ = ConcurrentMemPool<VertexPropertyRow>::GetInstance();
    ep_row_pool_ = ConcurrentMemPool<EdgePropertyRow>::GetInstance();
    vp_mvcc_pool_ = ConcurrentMemPool<VPropertyMVCCItem>::GetInstance();
    ep_mvcc_pool_ = ConcurrentMemPool<EPropertyMVCCItem>::GetInstance();
    vertex_mvcc_pool_ = ConcurrentMemPool<VertexMVCCItem>::GetInstance();
    edge_mvcc_pool_ = ConcurrentMemPool<EdgeMVCCItem>::GetInstance();

    // TODO(entityless): spawn threads here
}

void GCExecutor::VPropertyMVCCItemGC(VPropertyMVCCItem* item) {
    auto* cur_item = item;
    while (cur_item != nullptr) {
        vp_store_->FreeValue(cur_item->GetValue());

        auto* to_free = cur_item;
        cur_item = item->next;
        vp_mvcc_pool_->Free(to_free);
    }
}

void GCExecutor::EPropertyMVCCItemGC(EPropertyMVCCItem* item) {
    auto* cur_item = item;
    while (cur_item != nullptr) {
        ep_store_->FreeValue(cur_item->GetValue());

        auto* to_free = cur_item;
        cur_item = item->next;
        ep_mvcc_pool_->Free(to_free);
    }
}

void GCExecutor::VertexMVCCItemGC(VertexMVCCItem* item) {
    auto* cur_item = item;
    while (cur_item != nullptr) {
        auto* to_free = cur_item;
        cur_item = item->next;
        vertex_mvcc_pool_->Free(to_free);
    }
}

void GCExecutor::EdgeMVCCItemGC(EdgeMVCCItem* item) {
    auto* cur_item = item;
    while (cur_item != nullptr) {
        EdgeItemGC(&cur_item->GetValue());

        auto* to_free = cur_item;
        cur_item = item->next;
        edge_mvcc_pool_->Free(to_free);
    }
}

void GCExecutor::VertexItemGC(VertexItem* item) {
    item->vp_row_list->SelfGarbageCollect();
    delete item->vp_row_list;
    item->mvcc_list->SelfGarbageCollect();
    delete item->mvcc_list;
    item->ve_row_list->SelfGarbageCollect();
    delete item->ve_row_list;
}

void GCExecutor::EdgeItemGC(EdgeItem* item) {
    if (item->ep_row_list == nullptr)
        return;
    item->ep_row_list->SelfGarbageCollect();
    delete item->ep_row_list;
}
