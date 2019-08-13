/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include "mvcc_value_store.hpp"

MVCCValueStore::MVCCValueStore(char* mem, OffsetT item_count, int nthreads, bool utilization_record) {
    Init(mem, item_count, nthreads, utilization_record);
}

MVCCValueStore::~MVCCValueStore() {
    if (next_offset_ != nullptr)
        _mm_free(next_offset_);
    if (mem_allocated_)
        _mm_free(attached_mem_);
}

void MVCCValueStore::Init(char* mem, OffsetT item_count, int nthreads, bool utilization_record) {
    assert(item_count > nthreads * (BLOCK_SIZE + 2));

    if (mem != nullptr) {
        attached_mem_ = mem;
        mem_allocated_ = false;
    } else {
        attached_mem_ = reinterpret_cast<char*>(_mm_malloc(item_count * MEM_ITEM_SIZE, 4096));
        mem_allocated_ = true;
    }

    next_offset_ = reinterpret_cast<OffsetT*>(_mm_malloc(sizeof(OffsetT) * item_count, 4096));

    for (OffsetT i = 0; i < item_count; i++) {
        next_offset_[i] = i + 1;
    }

    head_ = 0;
    tail_ = item_count - 1;

    thread_stat_ = reinterpret_cast<ThreadStat*>(_mm_malloc(sizeof(ThreadStat) * nthreads, 4096));

    for (int tid = 0; tid < nthreads; tid++) {
        thread_stat_[tid].free_cell_count = 0;
        auto& local_stat = thread_stat_[tid];
        OffsetT tmp_head = head_;
        if (next_offset_[tmp_head] == tail_) {
            assert(false);
        } else {
            local_stat.free_cell_count = BLOCK_SIZE;
            local_stat.block_head = tmp_head;
            for (OffsetT i = 0; i < BLOCK_SIZE; i++) {
                local_stat.block_tail = tmp_head;
                tmp_head = next_offset_[tmp_head];
            }
            head_ = tmp_head;
        }
        local_stat.get_counter = 0;
        local_stat.free_counter = 0;
    }

    pthread_spin_init(&lock_, 0);

    item_count_ = item_count;
    nthreads_ = nthreads;
    utilization_record_ = utilization_record;
}

char* MVCCValueStore::GetItemPtr(const OffsetT& offset) {
    return attached_mem_ + ((size_t) offset) * MEM_ITEM_SIZE;
}

ValueHeader MVCCValueStore::InsertValue(const value_t& value, int tid) {
    ValueHeader ret;
    ret.count = value.content.size() + 1;

    // Calculate how many items will be used to store this value_t
    OffsetT item_count = ret.count / MEM_ITEM_SIZE;
    if (item_count * MEM_ITEM_SIZE != ret.count)
        item_count++;

    // Allocate items
    ret.head_offset = Get(item_count, tid);

    OffsetT current_offset = ret.head_offset;
    const char* value_content_ptr = &value.content[0];
    OffsetT value_len = value.content.size(), value_off = 0;

    for (OffsetT i = 0; i < item_count; i++) {
        char* item_ptr = GetItemPtr(current_offset);
        if (i == 0) {  // The first item; we need to insert value_t::type
            item_ptr[0] = static_cast<char>(value.type);

            // insert part of value_t::content to the remaining spaces in the first item
            if (value_len < MEM_ITEM_SIZE - 1) {
                // value_t::content cannot fill the remaining spaces
                memcpy(item_ptr + 1, value_content_ptr + value_off, value_len);
            } else {
                memcpy(item_ptr + 1, value_content_ptr + value_off, MEM_ITEM_SIZE - 1);
                value_off += MEM_ITEM_SIZE - 1;
            }
        } else if (i == item_count - 1) {
            // The last item to insert, copy the remaining part of value_t::content
            memcpy(item_ptr, value_content_ptr + value_off, value_len - value_off);
        } else {
            // neither the first item nor the last item
            memcpy(item_ptr, value_content_ptr + value_off, MEM_ITEM_SIZE);
            value_off += MEM_ITEM_SIZE;
        }
        current_offset = next_offset_[current_offset];
    }

    return ret;
}

void MVCCValueStore::ReadValue(const ValueHeader& header, value_t& value) {
    if (header.count == 0)
        return;

    OffsetT value_len = header.count - 1, value_off = 0;
    value.content.resize(value_len);
    OffsetT item_count = header.count / MEM_ITEM_SIZE;
    if (item_count * MEM_ITEM_SIZE != header.count)
        item_count++;
    OffsetT current_offset = header.head_offset;

    char* value_content_ptr = &value.content[0];

    // Please refer to InsertValue
    for (OffsetT i = 0; i < item_count; i++) {
        const char* item_ptr = GetItemPtr(current_offset);
        if (i == 0) {
            value.type = item_ptr[0];
            if (value_len < MEM_ITEM_SIZE - 1) {
                memcpy(value_content_ptr + value_off, item_ptr + 1, value_len);
            } else {
                memcpy(value_content_ptr + value_off, item_ptr + 1, MEM_ITEM_SIZE - 1);
                value_off += MEM_ITEM_SIZE - 1;
            }
        } else if (i == item_count - 1) {
            const char* item_ptr = GetItemPtr(current_offset);
            memcpy(value_content_ptr + value_off, item_ptr, value_len - value_off);
        } else {
            memcpy(value_content_ptr + value_off, item_ptr, MEM_ITEM_SIZE);
            value_off += MEM_ITEM_SIZE;
        }
        current_offset = next_offset_[current_offset];
    }
}

void MVCCValueStore::FreeValue(const ValueHeader& header, int tid) {
    OffsetT item_count = header.count / MEM_ITEM_SIZE;
    if (item_count * MEM_ITEM_SIZE != header.count)
        item_count++;

    Free(header.head_offset, item_count, tid);
}

// Called by InsertValue
OffsetT MVCCValueStore::Get(const OffsetT& count, int tid) {
    auto& local_stat = thread_stat_[tid];

    if (utilization_record_)
        local_stat.get_counter += count;

    // count + 2: make sure at least 2 free cells for each thread, reserved for head and tail

    // Get cells from global space directly when count is too large
    if (count > BLOCK_SIZE && local_stat.free_cell_count < count + 2) {
        pthread_spin_lock(&lock_);
        OffsetT ori_head = head_;
        for (OffsetT i = 0; i < count; i++) {
            head_ = next_offset_[head_];
            assert(head_ != tail_);
        }
        pthread_spin_unlock(&lock_);
        return ori_head;
    }

    if (local_stat.free_cell_count < count + 2) {
        // fetch a new block, append to the local block tail
        pthread_spin_lock(&lock_);
        OffsetT tmp_head = head_;
        local_stat.free_cell_count += BLOCK_SIZE;
        next_offset_[local_stat.block_tail] = tmp_head;
        for (OffsetT i = 0; i < BLOCK_SIZE; i++) {
            local_stat.block_tail = tmp_head;
            tmp_head = next_offset_[tmp_head];
            assert(tmp_head != tail_);
        }
        head_ = tmp_head;
        pthread_spin_unlock(&lock_);
    }

    OffsetT ori_head = local_stat.block_head;
    local_stat.free_cell_count -= count;
    for (OffsetT i = 0; i < count; i++)
        local_stat.block_head = next_offset_[local_stat.block_head];

    return ori_head;
}

// Called by FreeValue
void MVCCValueStore::Free(const OffsetT& offset, const OffsetT& count, int tid) {
    auto& local_stat = thread_stat_[tid];

    if (utilization_record_)
        local_stat.free_counter += count;

    // Free cells to global space directly when count is too large
    if (count > 2 * BLOCK_SIZE) {
        OffsetT tmp_tail = offset;
        for (OffsetT i = 0; i < count - 1; i++) {
            tmp_tail = next_offset_[tmp_tail];
        }

        pthread_spin_lock(&lock_);
        next_offset_[tail_] = offset;
        tail_ = tmp_tail;
        pthread_spin_unlock(&lock_);

        return;
    }

    next_offset_[local_stat.block_tail] = offset;
    local_stat.free_cell_count += count;

    for (OffsetT i = 0; i < count; i++) {
        local_stat.block_tail = next_offset_[local_stat.block_tail];
    }
    if (local_stat.free_cell_count >= 2 * BLOCK_SIZE) {
        OffsetT to_free_count = local_stat.free_cell_count - BLOCK_SIZE;
        OffsetT tmp_head = local_stat.block_head;
        OffsetT tmp_tail = tmp_head;
        for (int i = 0; i < to_free_count - 1; i++)
            tmp_tail = next_offset_[tmp_tail];
        local_stat.block_head = next_offset_[tmp_tail];
        local_stat.free_cell_count -= to_free_count;
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
        get_counter += thread_stat_[tid].get_counter;
        free_counter += thread_stat_[tid].free_counter;
    }
    OffsetT cell_avail = item_count_ - get_counter + free_counter - 2;

    return "Get: " + std::to_string(get_counter) + ", Free: " + std::to_string(free_counter)
           + ", Total: " + std::to_string(item_count_) + ", Avail: " + std::to_string(cell_avail);
}

std::pair<OffsetT, OffsetT> MVCCValueStore::GetUsage() {
    OffsetT get_counter = 0;
    OffsetT free_counter = 0;
    for (int tid = 0; tid < nthreads_; tid++) {
        get_counter += thread_stat_[tid].get_counter;
        free_counter += thread_stat_[tid].free_counter;
    }

    return std::make_pair(get_counter, free_counter);
}
