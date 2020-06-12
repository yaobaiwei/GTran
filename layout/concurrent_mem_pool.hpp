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

constexpr int CONCURRENT_MEM_POOL_DEFAULT_BLOCK_SIZE = 2048;  // 2K
constexpr int CONCURRENT_MEM_POOL_ARRAY_MEMORY_ALIGNMENT = 4096;  // 4KB

/*
ConcurrentMemPool is a high-throughput multi-thread container for allocating objects.
-----------------------------------------------------------------------------------
Description:
A simple memory pool can be defined as follow:
class MemPool {
    struct Node {
        Object obj;
        Node* next;
    };
    Node *head_, *tail_;
    Object * Get() { assert(head_ != tail_); Object* ret = (Object*)head_; head_ = head->next; return ret; }  // notice that head_ == &head_->obj
    void * Free(Object* obj) { tail_->next_ = (Node*)obj; tail_ = tail_->next; }
}
However, above implementation has below drawbacks:
    1. Memory of objects is not continual. Even if all nodes are allocated from an array, the alignment of Node could be bad.
    2. Thread safety is not guaranteed.
Thus, ConcurrentMemPool needs to address the 2 abovementioned problems.:
    1. To address the memory layout problem, an array of Node is split into two arrays: one array contains all objects, and another array contains the offset of the next object.
        (1). Objects are stored in attached_mem_ = Object[N], next offsets are stored in next_offsets_ = OffsetT[N], and head_ & tail_ with type of OffsetT. OffsetT can be any type of integer.
        (2). For example, for a memory pool containing 3 objects: head_ = 0, tail_ = 2, next_offset_ = {1, 2, ANY_VALUE}  // next_offset_[tail_] will never be used.
        (3). Object * Get() { assert(head_ != tail_); Object * ret = attached_mem_[head_]; head_ = next_offset_[head_]; return ret; }
        (4). void * Free(Object* obj) { assert(head_ != tail_); Object * ret = attached_mem_[head_]; head_ = next_offset_[head_]; return ret; }
    2. To ensure thread safety, a lock is needed, but each Get() and Free() operation will need to lock the whole memory pool. To address this:
        (1). Assuming that N threads will use ConcurrentMemPool. N thread-local block will be created.
        (2). Thread-local block will get objects from and free objects to the shared block by batch, with the granularity of BLOCK_SIZE.
        (3). During initialization, each thread-local block will get BLOCK_SIZE objects from the shared block. Each thread will directly Get() and Free() in the thread-local block.
        (4). If a thread-local block holds more than 2 * BLOCK_SIZE objects, it will free BLOCK_SIZE objects to the shared block (lock required).
        (5). If a thread-local block has no objects available, it will get BLOCK_SIZE objects from the shared block (lock required).
        (6). Only (4) and (5) requires lock, ensuring high throughput.
-----------------------------------------------------------------------------------
Usage:
    1. Use Get() to allocate an object, Free() to free the allocated object.
    2. ConcurrentMemPool is a template class. The object type, the integer type to record next offset, and the size of the thread-local block can be configured via the template parameter.
    3. Thread count (N) and the size of the memory pool need to be specified during initialization.
    4. When calling Get() and Free() function, a valid thread id in [0, N - 1] is needed:
        One tid should be only used by one thread. The undefined behavior will occur if multiple threads use the same tid in Get() or Free().
*/


// if cell_count is small than 65535, OffsetT can be set to uint16_t
// if cell_count is larger than 4294967296, OffsetT should be set to uint64_t
template<class CellT, class OffsetT = uint32_t, int BLOCK_SIZE = 2048>
class ConcurrentMemPool {
 private:
    ConcurrentMemPool() {}
    ConcurrentMemPool(const ConcurrentMemPool&);
    ~ConcurrentMemPool();

    /*
    Allocate memories and initialize blocks.
    @param:
        CellT* mem: Pre-allocated memory. If mem == nullptr, memory will be allocated in Init()
        size_t cell_count: The capacity of ConcurrentMemPool
        int nthreads: The count of thread that will use this ConcurrentMemPool
        bool utilization_record_: If true, the count of got and free cells will be recorded.
    */
    void Init(CellT* mem, size_t cell_count, int nthreads, bool utilization_record);

    bool mem_allocated_ __attribute__((aligned(16))) = false;
    CellT* attached_mem_ __attribute__((aligned(16))) = nullptr;
    OffsetT* next_offset_ __attribute__((aligned(16))) = nullptr;

    OffsetT cell_count_;
    int nthreads_;
    bool utilization_record_;

    // Next avaliable cell index, modified in Get()
    OffsetT head_ __attribute__((aligned(64)));
    // Last avaliable cell index, modified in Free()
    OffsetT tail_ __attribute__((aligned(64)));

    pthread_spinlock_t lock_ __attribute__((aligned(64)));

    struct ThreadLocalBlock {
        OffsetT block_head __attribute__((aligned(16)));
        OffsetT block_tail __attribute__((aligned(16)));
        OffsetT free_cell_count __attribute__((aligned(16)));
        OffsetT get_counter, free_counter;
    } __attribute__((aligned(64)));

    static_assert(sizeof(ThreadLocalBlock) % 64 == 0, "concurrent_mem_pool.hpp, sizeof(ThreadLocalBlock) % 64 != 0");

    ThreadLocalBlock* thread_local_block_ __attribute__((aligned(64)));

 public:
    static ConcurrentMemPool* GetInstance(CellT* mem, size_t cell_count,
                                          int nthreads, bool utilization_record) {
        static ConcurrentMemPool* p = nullptr;

        if (p == nullptr && cell_count > 0) {
            p = new ConcurrentMemPool();
            p->Init(mem, cell_count, nthreads, utilization_record);
        }

        return p;
    }

    CellT * Get(int tid = 0);
    void Free(CellT* cell, int tid = 0);

    std::string UsageString();
    std::pair<uint64_t, uint64_t> UsageStatistic();  // <usage_byte, total_byte>
    std::pair<OffsetT, OffsetT> GetUsage();  // <get_count, free_count>
};

#include "concurrent_mem_pool.tpp"
