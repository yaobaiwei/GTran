/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <assert.h>
#include <memory.h>
#include <pthread.h>
#include <stdint.h>

#if defined(__INTEL_COMPILER)
#include <malloc.h>
#else
#include <mm_malloc.h>
#endif  // defined(__GNUC__)

#include <atomic>
#include <cstdio>
#include <string>

#include "base/type.hpp"

#define MVCC_VALUE_STORE_DEBUG

// A single class to store both VP and EP

#define OffsetT uint32_t
#define MemItemSize 8
static_assert(MemItemSize % 8 == 0, "mvcc_value_store.hpp, MemItemSize % 8 != 0");

struct ValueHeader {
    OffsetT head_offset;
    OffsetT count;

    bool IsEmpty() {return count == 0;}
    ValueHeader() {count = 0;}
    constexpr ValueHeader(OffsetT _head_offset, OffsetT _count) : head_offset(_head_offset), count(_count) {}
};

// TODO(entityless): implement this as a template class instread of hardcode with macro
// template<size_t ItemSize = 8, class OffsetT = uint32_t>
class MVCCValueStore {
 private:
    MVCCValueStore(const MVCCValueStore&);
    ~MVCCValueStore();

    // memory pool elements
    char* attached_mem_ = nullptr;
    OffsetT item_count_;
    OffsetT head_, tail_;
    OffsetT* next_offset_ = nullptr;
    bool mem_allocated_ = false;
    pthread_spinlock_t head_lock_, tail_lock_;

    #ifdef MVCC_VALUE_STORE_DEBUG
    OffsetT get_counter_, free_counter_;
    #endif  // MVCC_VALUE_STORE_DEBUG

    OffsetT Get(const OffsetT& count);
    void Free(const OffsetT& offset, const OffsetT& count);
    char* GetItemPtr(const OffsetT& offset);

    void Init(char* mem, OffsetT item_count);

 public:
    // TODO(entityless): store value.type in somewhere else.
    ValueHeader InsertValue(const value_t& value);
    void GetValue(const ValueHeader& header, value_t& value);
    void FreeValue(const ValueHeader& header);
    MVCCValueStore(char* mem, OffsetT item_count);

    #ifdef MVCC_VALUE_STORE_DEBUG
    std::string UsageString();
    #endif  // MVCC_VALUE_STORE_DEBUG
};
