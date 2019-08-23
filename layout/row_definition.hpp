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

class GCProducer;
class GCConsumer;

struct VPHeader {
    vpid_t pid;
    std::atomic<MVCCList<VPropertyMVCCItem>*> mvcc_list;

    VPHeader& operator= (const VPHeader& other) {
        pid = other.pid;
        MVCCList<VPropertyMVCCItem>* non_atomic = other.mvcc_list;
        mvcc_list = non_atomic;

        return *this;
    }

    typedef VPropertyMVCCItem MVCCItemType;
};

/* Use constexpr function InferElementCount to get row capacity during compilation
 * Similarly hereinafter.
 */
#define VP_ROW_ITEM_COUNT InferElementCount<VPHeader>(128, sizeof(void*))


struct EPHeader {
    epid_t pid;
    std::atomic<MVCCList<EPropertyMVCCItem>*> mvcc_list;

    EPHeader& operator= (const EPHeader& other) {
        pid = other.pid;
        MVCCList<EPropertyMVCCItem>* non_atomic = other.mvcc_list;
        mvcc_list = non_atomic;

        return *this;
    }

    typedef EPropertyMVCCItem MVCCItemType;
};

#define EP_ROW_ITEM_COUNT InferElementCount<EPHeader>(64, sizeof(void*))


struct EdgeHeader {
 public:
    bool is_out;  // If this vtx is a, true: a -> conn_vtx_id, false: a <- conn_vtx_id
    // EdgeVersion labels are stored in EdgeMVCCItem->val
    vid_t conn_vtx_id;
    MVCCList<EdgeMVCCItem>* mvcc_list;

    EdgeHeader& operator= (const EdgeHeader& _edge_header) {
        this->is_out = _edge_header.is_out;
        this->conn_vtx_id = _edge_header.conn_vtx_id;
        this->mvcc_list = _edge_header.mvcc_list;
    }
};

#define VE_ROW_ITEM_COUNT InferElementCount<EdgeHeader>(128, sizeof(void*))


// always fetch rows from memory pools


struct VertexPropertyRow {
 private:
    VertexPropertyRow* next_;
    VPHeader cells_[VP_ROW_ITEM_COUNT];

    template<class PropertyRow> friend class PropertyRowList;
    friend class GCProducer;
    friend class GCConsumer;

 public:
    static constexpr int ROW_ITEM_COUNT = VP_ROW_ITEM_COUNT;
}  __attribute__((aligned(64)));


struct EdgePropertyRow {
 private:
    EdgePropertyRow* next_;
    EPHeader cells_[EP_ROW_ITEM_COUNT];

    template<class PropertyRow> friend class PropertyRowList;
    friend class GCProducer;
    friend class GCConsumer;

 public:
    static constexpr int ROW_ITEM_COUNT = EP_ROW_ITEM_COUNT;
}  __attribute__((aligned(64)));


struct VertexEdgeRow {
 private:
    VertexEdgeRow* next_;
    EdgeHeader cells_[VE_ROW_ITEM_COUNT];

    friend class TopologyRowList;
    friend class GCProducer;
    friend class GCConsumer;
}  __attribute__((aligned(64)));
