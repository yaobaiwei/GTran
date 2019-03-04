/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdio>

#include "storage/layout.hpp"

// this should be managed by memory pool & mvcc_concurrent_ll
struct AbstractMVCC {
 public:
    uint64_t begin_time;
    uint64_t end_time;
    uint64_t tid;  // TODO(entityless): remove this later
    AbstractMVCC* next;

    bool aborted;

    static const uint64_t MIN_TIME = 0;
    static const uint64_t MAX_TIME = 0xFFFFFFFFFFFFFFFF;
    static const uint64_t INITIAL_TID = 0;

    bool IsFinished();  // TODO(entityless): how much states? {commited, processing, aborted}
    void MarkFinished();

    // virtual void MemPoolInitial() = 0;
    // virtual void MemPoolFree() = 0;
};

struct PropertyMVCC : public AbstractMVCC {
 public:
    // this is not pid!
    ptr_t kv_ptr;  // TODO(entityless): if kv_ptr == nullptr, then the property is deleted.
};

// TopoMVCC is only used in VertexEdgeRowTable
struct TopoMVCC : public AbstractMVCC {
 public:
    bool action;  // whether the edge exists or not
};
