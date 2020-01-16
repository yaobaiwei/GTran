/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <atomic>
#include <vector>

#include "tbb/atomic.h"

#include "base/serialization.hpp"
#include "base/type.hpp"
#include "layout/mvcc_definition.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/mvcc_value_store.hpp"
#include "layout/property_row_list.hpp"
#include "layout/topology_row_list.hpp"
#include "utils/tool.hpp"
#include "utils/write_prior_rwlock.hpp"

/* =================== VP, EP, VE row related =================== */

/*
InferElementCount is implemented to infer how many cells that a row contains during compilation.
@param:
    T: The cell datatype
    preferred_size: the size of the row
    taken_size: the size of non-cell elements in a row
@return: the number of cells in each row
*/
template <class T>
constexpr int InferElementCount(int preferred_size, int taken_size) {
    return (preferred_size - taken_size) / sizeof(T);
}

class GCProducer;
class GCConsumer;

// VPHeader is the cell in VPRow
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

/*
InferElementCount<VPHeader>(128, sizeof(void*)):
    For VPRow containing VPHeader as the cell, we want to know how many cells will be in VPHeader if sizeof(VPRow) == 128.
    There will be a next pointer in each row. Thus, we need to pass sizeof(void*) as the second parameter to InferElementCount.
    Similarly hereinafter.
*/
#define VP_ROW_CELL_COUNT InferElementCount<VPHeader>(128, sizeof(void*))

// EPHeader is the cell in EPRow
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

#define EP_ROW_CELL_COUNT InferElementCount<EPHeader>(64, sizeof(void*))

// EPHeader is the cell in VERow
struct EdgeHeader {
 public:
    bool is_out;
    vid_t conn_vtx_id;
    MVCCList<EdgeMVCCItem>* mvcc_list;

    EdgeHeader& operator= (const EdgeHeader& _edge_header) {
        this->is_out = _edge_header.is_out;
        this->conn_vtx_id = _edge_header.conn_vtx_id;
        this->mvcc_list = _edge_header.mvcc_list;
    }
};

#define VE_ROW_CELL_COUNT InferElementCount<EdgeHeader>(128, sizeof(void*))

struct VertexPropertyRow {
 private:
    VertexPropertyRow* next_;
    VPHeader cells_[VP_ROW_CELL_COUNT];

    template<class PropertyRow> friend class PropertyRowList;
    friend class GCProducer;
    friend class GCConsumer;

 public:
    static constexpr int ROW_CELL_COUNT = VP_ROW_CELL_COUNT;
}  __attribute__((aligned(64)));


struct EdgePropertyRow {
 private:
    EdgePropertyRow* next_;
    EPHeader cells_[EP_ROW_CELL_COUNT];

    template<class PropertyRow> friend class PropertyRowList;
    friend class GCProducer;
    friend class GCConsumer;

 public:
    static constexpr int ROW_CELL_COUNT = EP_ROW_CELL_COUNT;
}  __attribute__((aligned(64)));


struct VertexEdgeRow {
 private:
    VertexEdgeRow* next_;
    EdgeHeader cells_[VE_ROW_CELL_COUNT];

    friend class TopologyRowList;
    friend class GCProducer;
    friend class GCConsumer;
}  __attribute__((aligned(64)));


/* =================== Data types for vertex map and edge map in DataStorage =================== */

struct Vertex {
    label_t label;
    // container to hold connected edges
    TopologyRowList* ve_row_list = nullptr;
    // container to hold properties
    PropertyRowList<VertexPropertyRow>* vp_row_list = nullptr;
    /* Two version in the list at most:
     *  the first version is "true", means "visible";
     *  the second version is "false", means "deleted";
     */
    tbb::atomic<MVCCList<VertexMVCCItem>*> mvcc_list;

    Vertex() : mvcc_list(nullptr) {}
};

struct InEdge {
    tbb::atomic<MVCCList<EdgeMVCCItem>*> mvcc_list;
    InEdge() : mvcc_list(nullptr) {}
};

struct OutEdge {
    tbb::atomic<MVCCList<EdgeMVCCItem>*> mvcc_list;
    OutEdge() : mvcc_list(nullptr) {}
};
