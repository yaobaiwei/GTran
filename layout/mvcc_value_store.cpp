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


#include "mvcc_value_store.hpp"

MVCCValueStore::MVCCValueStore(char* mem, size_t cell_count, int nthreads, bool utilization_record) {
    Init(mem, cell_count, nthreads, utilization_record);
}

MVCCValueStore::~MVCCValueStore() {
    if (next_offset_ != nullptr)
        _mm_free(next_offset_);
    if (mem_allocated_)
        _mm_free(attached_mem_);
}

void MVCCValueStore::Init(char* mem, size_t cell_count, int nthreads, bool utilization_record) {
    // make sure that enough cells are available for all threads
    assert(cell_count > nthreads * (BLOCK_SIZE + 2));

    cell_count_ = cell_count;
    // Make sure that OffsetT can hold cell_count
    assert(cell_count_ == cell_count);

    if (mem != nullptr) {
        attached_mem_ = mem;
        mem_allocated_ = false;
    } else {
        attached_mem_ = reinterpret_cast<char*>(_mm_malloc(cell_count * MEM_CELL_SIZE, 4096));
        mem_allocated_ = true;
    }

    next_offset_ = reinterpret_cast<OffsetT*>(_mm_malloc(sizeof(OffsetT) * cell_count, 4096));

    for (OffsetT i = 0; i < cell_count; i++) {
        next_offset_[i] = i + 1;
    }

    head_ = 0;
    tail_ = cell_count - 1;

    thread_local_block_ = reinterpret_cast<ThreadLocalBlock*>(_mm_malloc(sizeof(ThreadLocalBlock) * nthreads, 4096));

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

ValueHeader MVCCValueStore::InsertValue(const value_t& value, int tid) {
    ValueHeader ret;
    ret.byte_count = value.content.size() + 1;

    // Calculate how many cells will be used to store this value_t
    OffsetT cell_count = ret.GetCellCount();

    // Allocate cells
    ret.head_offset = Get(cell_count, tid);

    OffsetT current_offset = ret.head_offset;
    const char* value_content_ptr = &value.content[0];
    OffsetT value_len = value.content.size(), value_off = 0;

    for (OffsetT i = 0; i < cell_count; i++) {
        // get the pointer of the cell via current_offset
        char* cell_ptr = GetCellPtr(current_offset);
        if (i == 0) {
            // the first cell; we need to insert value_t::type
            cell_ptr[0] = static_cast<char>(value.type);

            // insert part of value_t::content to the remaining spaces in the first cell
            if (value_len < MEM_CELL_SIZE - 1) {
                // value_t::content cannot fill the remaining spaces
                memcpy(cell_ptr + 1, value_content_ptr + value_off, value_len);
            } else {
                memcpy(cell_ptr + 1, value_content_ptr + value_off, MEM_CELL_SIZE - 1);
                value_off += MEM_CELL_SIZE - 1;
            }
        } else if (i == cell_count - 1) {
            // The last cell to insert, copy the remaining part of value_t::content
            memcpy(cell_ptr, value_content_ptr + value_off, value_len - value_off);
        } else {
            // neither the first cell nor the last cell
            memcpy(cell_ptr, value_content_ptr + value_off, MEM_CELL_SIZE);
            value_off += MEM_CELL_SIZE;
        }
        // get the offset of the next cell
        current_offset = next_offset_[current_offset];
    }

    return ret;
}

void MVCCValueStore::ReadValue(const ValueHeader& header, value_t& value) {
    if (header.byte_count == 0)
        return;

    OffsetT value_len = header.byte_count - 1, value_off = 0;
    value.content.resize(value_len);
    OffsetT cell_count = header.GetCellCount();
    OffsetT current_offset = header.head_offset;

    char* value_content_ptr = &value.content[0];

    // Please refer to InsertValue()
    for (OffsetT i = 0; i < cell_count; i++) {
        const char* cell_ptr = GetCellPtr(current_offset);
        if (i == 0) {
            value.type = cell_ptr[0];
            if (value_len < MEM_CELL_SIZE - 1) {
                memcpy(value_content_ptr + value_off, cell_ptr + 1, value_len);
            } else {
                memcpy(value_content_ptr + value_off, cell_ptr + 1, MEM_CELL_SIZE - 1);
                value_off += MEM_CELL_SIZE - 1;
            }
        } else if (i == cell_count - 1) {
            const char* cell_ptr = GetCellPtr(current_offset);
            memcpy(value_content_ptr + value_off, cell_ptr, value_len - value_off);
        } else {
            memcpy(value_content_ptr + value_off, cell_ptr, MEM_CELL_SIZE);
            value_off += MEM_CELL_SIZE;
        }
        current_offset = next_offset_[current_offset];
    }
}

void MVCCValueStore::FreeValue(const ValueHeader& header, int tid) {
    OffsetT cell_count = header.GetCellCount();

    Free(header.head_offset, cell_count, tid);
}

// Called by InsertValue
OffsetT MVCCValueStore::Get(const OffsetT& count, int tid) {
    auto& local_block = thread_local_block_[tid];

    if (utilization_record_)
        local_block.get_counter += count;

    // count + 2: make sure at least 2 free cells for each thread, reserved for head and tail

    if (count > BLOCK_SIZE && local_block.free_cell_count < count + 2) {
        // get cells from global space directly
        pthread_spin_lock(&lock_);
        OffsetT ori_head = head_;
        for (OffsetT i = 0; i < count; i++) {
            head_ = next_offset_[head_];
            assert(head_ != tail_);
        }
        pthread_spin_unlock(&lock_);
        return ori_head;
    }

    if (local_block.free_cell_count < count + 2) {
        // no enough cells
        // fetch BLOCK_SIZE free cells from the shared block, append to the tail of the thread-local block
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

    OffsetT ori_head = local_block.block_head;
    local_block.free_cell_count -= count;
    for (OffsetT i = 0; i < count; i++)
        local_block.block_head = next_offset_[local_block.block_head];

    return ori_head;
}

// Called by FreeValue
void MVCCValueStore::Free(const OffsetT& offset, const OffsetT& count, int tid) {
    auto& local_block = thread_local_block_[tid];

    if (utilization_record_)
        local_block.free_counter += count;

    if (count > BLOCK_SIZE && local_block.free_cell_count + count >= 2 * BLOCK_SIZE) {
        // free cells to shared block directly when count is too large
        OffsetT tmp_tail = offset;
        for (OffsetT i = 0; i < count - 1; i++) {
            tmp_tail = next_offset_[tmp_tail];
        }

        // append cells to the tail of the shared block
        pthread_spin_lock(&lock_);
        next_offset_[tail_] = offset;
        tail_ = tmp_tail;
        pthread_spin_unlock(&lock_);

        return;
    }

    // attach free cells to the tail of the thread-local block
    next_offset_[local_block.block_tail] = offset;
    local_block.free_cell_count += count;
    for (OffsetT i = 0; i < count; i++) {
        local_block.block_tail = next_offset_[local_block.block_tail];
    }

    if (local_block.free_cell_count >= 2 * BLOCK_SIZE) {
        // too many free cells in the thread-local block
        // get BLOCK_SIZE cells from the head of the thread-local block
        OffsetT to_free_count = local_block.free_cell_count - BLOCK_SIZE;
        OffsetT tmp_head = local_block.block_head;
        OffsetT tmp_tail = tmp_head;
        for (int i = 0; i < to_free_count - 1; i++)
            tmp_tail = next_offset_[tmp_tail];
        local_block.block_head = next_offset_[tmp_tail];
        local_block.free_cell_count -= to_free_count;

        // attach those cells to the tail of the shared block
        pthread_spin_lock(&lock_);
        next_offset_[tail_] = tmp_head;
        tail_ = tmp_tail;
        pthread_spin_unlock(&lock_);
    }
}

std::string MVCCValueStore::UsageString() {
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

std::pair<OffsetT, OffsetT> MVCCValueStore::GetUsage() {
    OffsetT get_counter = 0;
    OffsetT free_counter = 0;
    for (int tid = 0; tid < nthreads_; tid++) {
        get_counter += thread_local_block_[tid].get_counter;
        free_counter += thread_local_block_[tid].free_counter;
    }

    return std::make_pair(get_counter, free_counter);
}

std::pair<uint64_t, uint64_t> MVCCValueStore::UsageStatistic() {
    OffsetT get_counter = 0;
    OffsetT free_counter = 0;
    for (int tid = 0; tid < nthreads_; tid++) {
        get_counter += thread_local_block_[tid].get_counter;
        free_counter += thread_local_block_[tid].free_counter;
    }
    uint64_t usage_counter = get_counter - free_counter + 2;

    return std::pair<uint64_t, uint64_t>(usage_counter * (MEM_CELL_SIZE + sizeof(OffsetT)), cell_count_ * (MEM_CELL_SIZE + sizeof(OffsetT)));
}
