/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <vector>

#include "base/serialization.hpp"
#include "base/type.hpp"
#include "utils/tool.hpp"

// tmp datatype for HDFSDataLoader
struct TMPVertex {
    vid_t id;
    label_t label;
    std::vector<vid_t> in_nbs;  // this_vtx <- in_nbs
    std::vector<vid_t> out_nbs;  // this_vtx -> out_nbs
    std::vector<label_t> vp_label_list;
    std::vector<value_t> vp_value_list;

    std::string DebugString() const;
};

// tmp datatype for HDFSDataLoader
struct TMPEdge {
    eid_t id;  // id.out_v -> e -> id.in_v, follows out_v
    label_t label;
    std::vector<label_t> ep_label_list;
    std::vector<value_t> ep_value_list;

    std::string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const TMPVertex& v);
obinstream& operator>>(obinstream& m, TMPVertex& v);
ibinstream& operator<<(ibinstream& m, const TMPEdge& v);
obinstream& operator>>(obinstream& m, TMPEdge& v);

// To infer how many elements a row contains during compilation
template <class T>
constexpr int InferElementCount(int preferred_size, int taken_size) {
    return (preferred_size - taken_size) / sizeof(T);
}

// For dependency read, whether commit depends on others' status
struct depend_trx_lists {
    std::vector<uint64_t> homo_trx_list;  // commit -> commit
    std::vector<uint64_t> hetero_trx_list;  // abort -> commit
};

extern tbb::concurrent_hash_map<uint64_t, depend_trx_lists> dep_trx_map;
typedef tbb::concurrent_hash_map<uint64_t, depend_trx_lists>::accessor dep_trx_accessor;
typedef tbb::concurrent_hash_map<uint64_t, depend_trx_lists>::const_accessor dep_trx_const_accessor;

struct TMPVertexInfo {
    vid_t id;
    // label_t label;
    vector<vid_t> in_nbs;
    vector<vid_t> out_nbs;
    vector<label_t> vp_list;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const TMPVertexInfo& v);

obinstream& operator>>(obinstream& m, TMPVertexInfo& v);

struct TMPEdgeInfo {
    eid_t id;
    // label_t label;
    vector<label_t> ep_list;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const TMPEdgeInfo& e);

obinstream& operator>>(obinstream& m, TMPEdgeInfo& e);

struct V_KVpair {
    vpid_t key;
    value_t value;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const V_KVpair& pair);

obinstream& operator>>(obinstream& m, V_KVpair& pair);

struct VProperty{
    vid_t id;
    vector<V_KVpair> plist;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const VProperty& vp);

obinstream& operator>>(obinstream& m, VProperty& vp);

struct E_KVpair {
    epid_t key;
    value_t value;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const E_KVpair& pair);

obinstream& operator>>(obinstream& m, E_KVpair& pair);

struct EProperty {
    eid_t id;
    vector<E_KVpair> plist;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const EProperty& ep);

obinstream& operator>>(obinstream& m, EProperty& ep);

struct EdgePropertyRow;
struct VertexPropertyRow;
template <class T>
class PropertyRowList;
class TopologyRowList;
struct VertexMVCCItem;
template <class Item>
class MVCCList;

struct Vertex {
    label_t label;
    TopologyRowList* ve_row_list = nullptr;
    PropertyRowList<VertexPropertyRow>* vp_row_list = nullptr;
    MVCCList<VertexMVCCItem>* mvcc_list = nullptr;
};

struct Edge {
    label_t label;  // if 0, then the edge is deleted
    // for in_e or deleted edge, this is always nullptr
    PropertyRowList<EdgePropertyRow>* ep_row_list = nullptr;

    bool Exist() const {return label != 0;}
    Edge() {}

    constexpr Edge(label_t _label, PropertyRowList<EdgePropertyRow>* _ep_row_list) :
    label(_label), ep_row_list(_ep_row_list) {}
};
