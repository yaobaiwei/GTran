// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
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

template<class CellT, class OffsetT, int BLOCK_SIZE>
ConcurrentMemPool<CellT, OffsetT, BLOCK_SIZE>::~ConcurrentMemPool() {
    if (next_offset_ != nullptr)
        _mm_free(next_offset_);
    if (mem_allocated_)
        _mm_free(attached_mem_);
}

template<class CellT, class OffsetT, int BLOCK_SIZE>
void ConcurrentMemPool<CellT, OffsetT, BLOCK_SIZE>::Init(CellT* mem, size_t cell_count,
                                                         int nthreads, bool utilization_record) {
    // make sure that enough cells are available for all threads
    assert(cell_count > nthreads * (BLOCK_SIZE + 2));

    cell_count_ = cell_count;
    // Make sure that OffsetT can hold cell_count
    assert(cell_count_ == cell_count);

    if (mem != nullptr) {
        attached_mem_ = mem;
        mem_allocated_ = false;
    } else {
        attached_mem_ = reinterpret_cast<CellT*>(_mm_malloc(sizeof(CellT) * cell_count, CONCURRENT_MEM_POOL_ARRAY_MEMORY_ALIGNMENT));
        mem_allocated_ = true;
    }

    next_offset_ = reinterpret_cast<OffsetT*>(_mm_malloc(sizeof(OffsetT) * cell_count, CONCURRENT_MEM_POOL_ARRAY_MEMORY_ALIGNMENT));

    for (OffsetT i = 0; i < cell_count; i++) {
        next_offset_[i] = i + 1;
    }

    head_ = 0;
    tail_ = cell_count - 1;

    thread_local_block_ = reinterpret_cast<ThreadLocalBlock*>(_mm_malloc(sizeof(ThreadLocalBlock) * nthreads, CONCURRENT_MEM_POOL_ARRAY_MEMORY_ALIGNMENT));

    // initialize and allocate objects for the thread-local block
    for (int tid = 0; tid < nthreads; tid++) {
        auto& local_block = thread_local_block_[tid];

        local_block.free_cell_count = BLOCK_SIZE;
        local_block.block_head = head_;
        local_block.block_tail = head_ + BLOCK_SIZE - 1;
        local_block.get_counter = 0;
        local_block.free_counter = 0;

        head_ = head_ + BLOCK_SIZE;
    }

    pthread_spin_init(&lock_, 0);

    nthreads_ = nthreads;
    utilization_record_ = utilization_record;
}

template<class CellT, class OffsetT, int BLOCK_SIZE>
CellT* ConcurrentMemPool<CellT, OffsetT, BLOCK_SIZE>::Get(int tid) {
    CellT* ret;
    auto& local_block = thread_local_block_[tid];

    if (next_offset_[local_block.block_head] == local_block.block_tail) {
        // insufficient cells in the thred local block
        // fetch BLOCK_SIZE cells from the shared block to the tail of of the thread-local block
        pthread_spin_lock(&lock_);
        OffsetT tmp_head = head_;
        local_block.free_cell_count += BLOCK_SIZE;
        next_offset_[local_block.block_tail] = tmp_head;
        for (OffsetT i = 0; i < BLOCK_SIZE; i++) {
            if (i == BLOCK_SIZE - 1)
                local_block.block_tail = tmp_head;
            tmp_head = next_offset_[tmp_head];
            assert(tmp_head != tail_);
        }
        head_ = tmp_head;
        pthread_spin_unlock(&lock_);
    }

    // fetch one cell from the head of thread-local block
    ret = attached_mem_ + local_block.block_head;
    local_block.block_head = next_offset_[local_block.block_head];
    local_block.free_cell_count--;

    if (utilization_record_)
        local_block.get_counter++;

    return ret;
}

template<class CellT, class OffsetT, int BLOCK_SIZE>
void ConcurrentMemPool<CellT, OffsetT, BLOCK_SIZE>::Free(CellT* cell, int tid) {
    OffsetT mem_off = cell - attached_mem_;

    auto& local_block = thread_local_block_[tid];

    // attach the free cell to the tail of thread-local block
    next_offset_[local_block.block_tail] = mem_off;
    local_block.block_tail = mem_off;
    local_block.free_cell_count++;

    if (local_block.free_cell_count == 2 * BLOCK_SIZE) {
        // too many free cells in the thread-local block
        // get BLOCK_SIZE cells from the head of the thread-local block
        OffsetT tmp_head = local_block.block_head;
        OffsetT tmp_tail = tmp_head;
        for (int i = 0; i < BLOCK_SIZE - 1; i++)
            tmp_tail = next_offset_[tmp_tail];
        local_block.block_head = next_offset_[tmp_tail];
        local_block.free_cell_count -= BLOCK_SIZE;

        // attach those cells to the tail of the shared block
        pthread_spin_lock(&lock_);
        next_offset_[tail_] = tmp_head;
        tail_ = tmp_tail;
        pthread_spin_unlock(&lock_);
    }

    if (utilization_record_)
        local_block.free_counter++;
}

template<class CellT, class OffsetT, int THREAD_BLOCK_SIZE>
std::string ConcurrentMemPool<CellT, OffsetT, THREAD_BLOCK_SIZE>::UsageString() {
    OffsetT get_counter = 0;
    OffsetT free_counter = 0;
    for (int tid = 0; tid < nthreads_; tid++) {
        get_counter += thread_local_block_[tid].get_counter;
        free_counter += thread_local_block_[tid].free_counter;
    }
    OffsetT cell_avail = cell_count_ - get_counter + free_counter - 2;

    return "Get: " + std::to_string(get_counter) + ", Free: " + std::to_string(free_counter)
           + ", Total: " + std::to_string(cell_count_) + ", Avail: " + std::to_string(cell_avail);
}

template<class CellT, class OffsetT, int THREAD_BLOCK_SIZE>
std::pair<uint64_t, uint64_t> ConcurrentMemPool<CellT, OffsetT, THREAD_BLOCK_SIZE>::UsageStatistic() {
    OffsetT get_counter = 0;
    OffsetT free_counter = 0;
    for (int tid = 0; tid < nthreads_; tid++) {
        get_counter += thread_local_block_[tid].get_counter;
        free_counter += thread_local_block_[tid].free_counter;
    }
    uint64_t usage_counter = get_counter - free_counter + 2;

    return std::pair<uint64_t, uint64_t>(usage_counter * (sizeof(CellT) + sizeof(OffsetT)), cell_count_ * (sizeof(CellT) + sizeof(OffsetT)));
}

template<class CellT, class OffsetT, int THREAD_BLOCK_SIZE>
std::pair<OffsetT, OffsetT> ConcurrentMemPool<CellT, OffsetT, THREAD_BLOCK_SIZE>::GetUsage() {
    OffsetT get_counter = 0;
    OffsetT free_counter = 0;
    for (int tid = 0; tid < nthreads_; tid++) {
        get_counter += thread_local_block_[tid].get_counter;
        free_counter += thread_local_block_[tid].free_counter;
    }

    return std::make_pair(get_counter, free_counter);
}
