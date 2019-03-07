/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <vector>

#include "base/serialization.hpp"
#include "base/type.hpp"
#include "utils/tool.hpp"

namespace std {
// tmp datatype for shuffle
struct TMPVertex {
    vid_t id;
    label_t label;
    vector<vid_t> in_nbs;  // this_vtx <- in_nbs
    vector<vid_t> out_nbs;  // this_vtx -> out_nbs
    vector<label_t> vp_label_list;
    vector<value_t> vp_value_list;

    string DebugString() const;
};

struct TMPEdge {
    eid_t id;  // id.out_v -> e -> id.in_v, follows out_v
    label_t label;
    vector<label_t> ep_label_list;
    vector<value_t> ep_value_list;

    string DebugString() const;
};

// used in KVStore
struct MVCCHeader {
    uint64_t begin_time;
    uint64_t pid;  // for ep and vp

    MVCCHeader(uint64_t _begin_time, uint64_t _pid) : begin_time(_begin_time), pid(_pid) {}

    inline uint64_t HashToUint64() const {
        const uint64_t k_mul = 0x9ddfea08eb382d69ULL;
        uint64_t a = (begin_time ^ pid) * k_mul;
        a ^= (a >> 47);
        uint64_t b = (pid ^ a) * k_mul;
        b ^= (b >> 47);
        b *= k_mul;
        return b;
    }

    bool operator==(const MVCCHeader& right_header) {
        return (begin_time == right_header.begin_time) && (pid == right_header.pid);
    }
};

ibinstream& operator<<(ibinstream& m, const TMPVertex& v);
obinstream& operator>>(obinstream& m, TMPVertex& v);
ibinstream& operator<<(ibinstream& m, const TMPEdge& v);
obinstream& operator>>(obinstream& m, TMPEdge& v);

// To infer how many elements a row contains at compiling
template <class T>
constexpr int InferElementCount(int preferred_size, int taken_size) {
    return (preferred_size - taken_size) / sizeof(T);
}

}    // namespace std

