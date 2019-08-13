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

#define OffsetT uint32_t
#define MEM_ITEM_SIZE 8
static_assert(MEM_ITEM_SIZE % 8 == 0, "mvcc_value_store.hpp, MEM_ITEM_SIZE % 8 != 0");

struct ValueHeader {
    OffsetT head_offset;
    OffsetT count;

    // True when the property has been dropped.
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

    OffsetT item_count_;
    int nthreads_;
    bool utilization_record_;

    OffsetT head_ __attribute__((aligned(64)));
    OffsetT tail_ __attribute__((aligned(64)));

    pthread_spinlock_t lock_ __attribute__((aligned(64)));

    // the user should guarantee that a specific tid will only be used by one specific thread.
    struct ThreadStat {
        OffsetT block_head __attribute__((aligned(16)));
        OffsetT block_tail __attribute__((aligned(16)));
        OffsetT free_cell_count __attribute__((aligned(16)));
        OffsetT get_counter, free_counter;
    } __attribute__((aligned(64)));

    static_assert(sizeof(ThreadStat) % 64 == 0, "concurrent_mem_pool.hpp, sizeof(ThreadStat) % 64 != 0");

    ThreadStat* thread_stat_ __attribute__((aligned(64)));

    OffsetT Get(const OffsetT& count, int tid);
    void Free(const OffsetT& offset, const OffsetT& count, int tid);
    char* GetItemPtr(const OffsetT& offset);

    void Init(char* mem, OffsetT item_count, int nthreads, bool utilization_record);

 public:
    // Insert a value_t to the MVCCValueStore, returns a ValueHeader used to fetch and free this value_t
    ValueHeader InsertValue(const value_t& value, int tid = 0);
    // Free spaces allocated for the ValueHeader
    void FreeValue(const ValueHeader& header, int tid = 0);
    /* There is no lock in each thread local block, thus if different threads call above 2
     * functions and pass the same tid, error may occurs.
     */

    void ReadValue(const ValueHeader& header, value_t& value);

    /* In the constructor, nthreads thread local blocks will be allocated, which means that
     * the concurrency of MVCCValueStore is nthreads at most (InsertValue and FreeValue).
     */
    MVCCValueStore(char* mem, OffsetT item_count, int nthreads, bool utilization_record);

    static constexpr int BLOCK_SIZE = 1024;

    std::string UsageString();
    std::pair<OffsetT, OffsetT> GetUsage();
};
