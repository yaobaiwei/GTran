/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <vector>

#include "base/serialization.hpp"
#include "base/type.hpp"
#include "utils/tool.hpp"

namespace std {
// tmp datatype for HDFSDataLoader
struct TMPVertex {
    vid_t id;
    label_t label;
    vector<vid_t> in_nbs;  // this_vtx <- in_nbs
    vector<vid_t> out_nbs;  // this_vtx -> out_nbs
    vector<label_t> vp_label_list;
    vector<value_t> vp_value_list;

    string DebugString() const;
};

// tmp datatype for HDFSDataLoader
struct TMPEdge {
    eid_t id;  // id.out_v -> e -> id.in_v, follows out_v
    label_t label;
    vector<label_t> ep_label_list;
    vector<value_t> ep_value_list;

    string DebugString() const;
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
    vector<uint64_t> homo_trx_list;  // commit -> commit
    vector<uint64_t> hetero_trx_list;  // abort -> commit
};

extern tbb::concurrent_hash_map<uint64_t, depend_trx_lists> dep_trx_map;
typedef tbb::concurrent_hash_map<uint64_t, depend_trx_lists>::accessor dep_trx_accessor;
typedef tbb::concurrent_hash_map<uint64_t, depend_trx_lists>::const_accessor dep_trx_const_accessor;

}    // namespace std
