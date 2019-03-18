/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <assert.h>
#include <cstdio>
#include <memory.h>
#include <pthread.h>
#include <stdint.h>

#include <atomic>
#include <cstdio>
#include <string>

#include "base/type.hpp"

// A single class to store both VP and EP

#define OffsetT uint32_t
#define MemItemSize 8
static_assert(MemItemSize % 8 == 0, "mvcc_value_store.hpp, MemItemSize % 8 != 0");

struct ValueHeader {
    OffsetT head_offset;
    OffsetT count;

    bool IsEmpty() {return count == 0;}
    ValueHeader() {count = 0;}
};

// TODO(entityless): implement this as a template class instread of hardcode with macro
// template<size_t ItemSize = 8, class OffsetT = uint32_t>
class MVCCValueStore {
 private:
    MVCCValueStore(const MVCCValueStore&);

    // memory pool elements
    char* attached_mem_ = nullptr;
    OffsetT item_count_;
    OffsetT head_, tail_;
    OffsetT* next_offset_ = nullptr;
    pthread_spinlock_t head_lock_, tail_lock_;

    OffsetT get_counter_, free_counter_;

    OffsetT Get(const OffsetT& count);
    void Free(const OffsetT& offset, const OffsetT& count);
    char* GetItemPtr(const OffsetT& offset);

    void Init(char* mem, OffsetT item_count);

 public:
    // TODO(entityless): store value.type in somewhere else.
    ValueHeader Insert(const value_t& value);
    void GetValue(const ValueHeader& header, value_t& value);
    void FreeValue(const ValueHeader& header);
    MVCCValueStore(char* mem, OffsetT item_count) {Init(mem, item_count);}
};
