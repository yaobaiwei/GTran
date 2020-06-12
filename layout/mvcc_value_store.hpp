// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
#define MEM_CELL_SIZE 8

// MEM_CELL_SIZE should be divisible by 8, to ensure good memory alignment.
static_assert(MEM_CELL_SIZE % 8 == 0, "mvcc_value_store.hpp, MEM_CELL_SIZE % 8 != 0");

/*
MVCCValueStore is used for storing value_t in a pre-allocated memory.
-----------------------------------------------------------------------------------
The logic of MVCCValueStore is like ConcurrentMemPool. A value_t can be stored in one or multiple cells, depending on the size of value_t.
Like ConcurrentMemPool, MVCCValueStore also has thread-local blocks and a shared block.
-----------------------------------------------------------------------------------
Usage:
    1. Use InsertValue() to insert a value_t into the MVCCValueStore. Insert() will return a ValueHeader.
    2. Use ReadValue() to get the inserted value_t by a ValueHeader.
    3. Use FreeValue() to free cells related to a ValueHeader.
-----------------------------------------------------------------------------------
Cautious: the same as ConcurrentMemoryPool, a thread id can only be used by one thread to avoid undefined behavior.
*/

struct ValueHeader {
    OffsetT head_offset;
    OffsetT byte_count;

    // True when the property has been dropped.
    // For any property, byte_count should be >= 1, since value_t::type will occupy 1 byte
    bool IsEmpty() {return byte_count == 0;}
    ValueHeader() {byte_count = 0;}
    constexpr ValueHeader(OffsetT _head_offset, OffsetT _byte_count) : head_offset(_head_offset), byte_count(_byte_count) {}
    inline OffsetT GetCellCount() const __attribute__((always_inline)) {
        OffsetT cell_count = byte_count / MEM_CELL_SIZE;
        if (cell_count * MEM_CELL_SIZE != byte_count)
            cell_count++;
        return cell_count;
    }
};

class MVCCValueStore {
 private:
    MVCCValueStore(const MVCCValueStore&);
    ~MVCCValueStore();

    bool mem_allocated_ __attribute__((aligned(16))) = false;
    char* attached_mem_ __attribute__((aligned(16))) = nullptr;
    OffsetT* next_offset_ __attribute__((aligned(32))) = nullptr;

    OffsetT cell_count_;
    int nthreads_;
    bool utilization_record_;

    OffsetT head_ __attribute__((aligned(64)));
    OffsetT tail_ __attribute__((aligned(64)));

    pthread_spinlock_t lock_ __attribute__((aligned(64)));

    // the user should guarantee that a specific tid will only be used by one specific thread.
    struct ThreadLocalBlock {
        OffsetT block_head __attribute__((aligned(16)));
        OffsetT block_tail __attribute__((aligned(16)));
        OffsetT free_cell_count __attribute__((aligned(16)));
        OffsetT get_counter, free_counter;
    } __attribute__((aligned(64)));

    static_assert(sizeof(ThreadLocalBlock) % 64 == 0, "mvcc_value_store.hpp, sizeof(ThreadLocalBlock) % 64 != 0");

    ThreadLocalBlock* thread_local_block_ __attribute__((aligned(64)));

    // inline this for better performance
    inline char* GetCellPtr(const OffsetT& offset) __attribute__((always_inline)) {
        return attached_mem_ + ((size_t) offset) * MEM_CELL_SIZE;
    }

    // Allocate cells, called by InsertValue
    OffsetT Get(const OffsetT& count, int tid);
    // Free cells, called by FreeValue
    void Free(const OffsetT& offset, const OffsetT& count, int tid);

    /*
    Allocate memories and initialize blocks.
    @param:
        char* mem: Pre-allocated memory. If mem == nullptr, memory will be allocated in Init()
        size_t cell_count: The number of cells
        int nthreads: The count of thread that will use this ConcurrentMemPool
        bool utilization_record_: If true, the count of got and free cells will be recorded.
    */
    void Init(char* mem, size_t cell_count, int nthreads, bool utilization_record);

 public:
    // Insert a value_t to the MVCCValueStore, returns a ValueHeader used to fetch and free this value_t
    ValueHeader InsertValue(const value_t& value, int tid = 0);
    // Free spaces allocated for the ValueHeader
    void FreeValue(const ValueHeader& header, int tid = 0);

    void ReadValue(const ValueHeader& header, value_t& value);

    MVCCValueStore(char* mem, size_t cell_count, int nthreads, bool utilization_record);

    static constexpr int BLOCK_SIZE = 1024;

    std::string UsageString();
    std::pair<OffsetT, OffsetT> GetUsage();  // <get_count, free_count>
    std::pair<uint64_t, uint64_t> UsageStatistic();  // <usage_byte, total_byte>
};
