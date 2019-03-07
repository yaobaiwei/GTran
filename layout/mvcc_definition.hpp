/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdio>

#include "storage/layout.hpp"

// this should be managed by memory pool & mvcc_concurrent_ll

struct AbstractMVCC {
 private:
    uint64_t begin_time;
    uint64_t end_time;
    AbstractMVCC* next;

    // no trx_id will be 0
    // TODO(entityless): Test these functions if they're right
    uint64_t GetTransactionID() {return (begin_time >> 63) * (begin_time & 0xEFFFFFFFFFFFFFFF);}

    uint64_t Initial(const uint64_t& _trx_id, const uint64_t& _begin_time) {
        begin_time = _trx_id | 0x8000000000000000;
        end_time = _begin_time;
    }

    uint64_t Commit(AbstractMVCC* last) {
        uint64_t tmp_begin_time = end_time;
        end_time = MAX_TIME;
        begin_time = tmp_begin_time;

        if (last != nullptr) {
            last->end_time = begin_time;
        }
    }

    // TODO(entityless): How to abort?

    static const uint64_t MIN_TIME = 0;
    static const uint64_t MAX_TIME = 0x7FFFFFFFFFFFFFFF;

    template<class MVCC> friend class MVCCList;
};

struct PropertyMVCC : public AbstractMVCC {
 private:
    ptr_t val;  // stores ptr_t for KVStore
    void SetValue(const ptr_t& _val) {val = _val;}
 public:
    ptr_t GetValue() {return val;}

    template<class MVCC> friend class MVCCList;
};

// TopoMVCC is only used in VertexEdgeRowTable
struct TopoMVCC : public AbstractMVCC {
 private:
    bool val;  // whether the edge exists or not
    void SetValue(const bool& _val) {val = _val;}
 public:
    bool GetValue() {return val;}

    template<class MVCC> friend class MVCCList;
};
