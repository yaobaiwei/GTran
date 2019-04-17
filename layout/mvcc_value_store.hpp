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

// #define MVCC_VALUE_STORE_DEBUG

#define OffsetT uint32_t
#define MEM_ITEM_SIZE 8
static_assert(MEM_ITEM_SIZE % 8 == 0, "mvcc_value_store.hpp, MEM_ITEM_SIZE % 8 != 0");

// empty value is not allowed.
struct ValueHeader {
    OffsetT head_offset;
    OffsetT count;

    bool IsEmpty() {return count == 0;}
    ValueHeader() {count = 0;}
    constexpr ValueHeader(OffsetT _head_offset, OffsetT _count) : head_offset(_head_offset), count(_count) {}
};

class MVCCValueStore {
 private:
    MVCCValueStore(const MVCCValueStore&);
    ~MVCCValueStore();

    bool mem_allocated_ __attribute__((aligned(16))) = false;
    char* attached_mem_ __attribute__((aligned(16))) = nullptr;
    OffsetT* next_offset_ __attribute__((aligned(32))) = nullptr;

    #ifdef MVCC_VALUE_STORE_DEBUG
    OffsetT item_count_;
    OffsetT get_counter_, free_counter_;
    #endif  // MVCC_VALUE_STORE_DEBUG

    OffsetT head_ __attribute__((aligned(64)));
    OffsetT tail_ __attribute__((aligned(64)));

    pthread_spinlock_t lock_ __attribute__((aligned(64)));

    // the user should guarantee that a specific tid will only be used by one specific thread.
    struct ThreadStat {
        OffsetT block_head __attribute__((aligned(16)));
        OffsetT block_tail __attribute__((aligned(16)));
        OffsetT free_cell_count __attribute__((aligned(16)));
    } __attribute__((aligned(64)));

    static_assert(sizeof(ThreadStat) % 64 == 0, "concurrent_mem_pool.hpp, sizeof(ThreadStat) % 64 != 0");

    ThreadStat* thread_stat_ __attribute__((aligned(64)));

    OffsetT Get(const OffsetT& count, int tid);
    void Free(const OffsetT& offset, const OffsetT& count, int tid);
    char* GetItemPtr(const OffsetT& offset);

    void Init(char* mem, OffsetT item_count, int nthreads);

 public:
    ValueHeader InsertValue(const value_t& value, int tid = 0);
    void GetValue(const ValueHeader& header, value_t& value, int tid = 0);
    void FreeValue(const ValueHeader& header, int tid = 0);
    MVCCValueStore(char* mem, OffsetT item_count, int nthreads);

    static constexpr int BLOCK_SIZE = 1024;

    #ifdef MVCC_VALUE_STORE_DEBUG
    std::string UsageString();
    #endif  // MVCC_VALUE_STORE_DEBUG
};
