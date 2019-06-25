/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <atomic>

#include "layout/mvcc_definition.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/mvcc_value_store.hpp"
#include "layout/layout_type.hpp"
#include "layout/property_row_list.hpp"

struct VPHeader {
    vpid_t pid;
    std::atomic<MVCCList<VPropertyMVCCItem>*> mvcc_list;
    typedef VPropertyMVCCItem MVCCItemType;
};

/* Use constexpr function InferElementCount to get row capacity during compilation
 * Similarly hereinafter.
 */
#define VP_ROW_ITEM_COUNT InferElementCount<VPHeader>(256, sizeof(void*))


struct EPHeader {
    epid_t pid;
    std::atomic<MVCCList<EPropertyMVCCItem>*> mvcc_list;
    typedef EPropertyMVCCItem MVCCItemType;
};

#define EP_ROW_ITEM_COUNT InferElementCount<EPHeader>(256, sizeof(void*))


struct EdgeHeader {
    bool is_out;  // If this vtx is a, true: a -> b, false: a <- b
    // EdgeVersion labels are stored in EdgeMVCCItem->val
    vid_t conn_vtx_id;
    MVCCList<EdgeMVCCItem>* mvcc_list;
};

#define VE_ROW_ITEM_COUNT InferElementCount<EdgeHeader>(256, sizeof(void*))


// always fetch rows from memory pools


struct VertexPropertyRow {
 private:
    VertexPropertyRow* next_;
    VPHeader cells_[VP_ROW_ITEM_COUNT];

    template<class PropertyRow> friend class PropertyRowList;

 public:
    static constexpr int ROW_ITEM_COUNT = VP_ROW_ITEM_COUNT;
}  __attribute__((aligned(64)));


struct EdgePropertyRow {
 private:
    EdgePropertyRow* next_;
    EPHeader cells_[EP_ROW_ITEM_COUNT];

    template<class PropertyRow> friend class PropertyRowList;

 public:
    static constexpr int ROW_ITEM_COUNT = EP_ROW_ITEM_COUNT;
}  __attribute__((aligned(64)));


struct VertexEdgeRow {
 private:
    VertexEdgeRow* next_;
    EdgeHeader cells_[VE_ROW_ITEM_COUNT];

    friend class TopologyRowList;
}  __attribute__((aligned(64)));
