/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include "mvcc_value_store.hpp"

MVCCValueStore::MVCCValueStore(char* mem, OffsetT item_count, int nthreads) {
    Init(mem, item_count, nthreads);
}

MVCCValueStore::~MVCCValueStore() {
    if (next_offset_ != nullptr)
        _mm_free(next_offset_);
    if (mem_allocated_)
        _mm_free(attached_mem_);
}

void MVCCValueStore::Init(char* mem, OffsetT item_count, int nthreads) {
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
    }

    pthread_spin_init(&lock_, 0);

    #ifdef MVCC_VALUE_STORE_DEBUG
    get_counter_ = 0;
    free_counter_ = 0;
    item_count_ = item_count;
    #endif  // MVCC_VALUE_STORE_DEBUG
}

char* MVCCValueStore::GetItemPtr(const OffsetT& offset) {
    return attached_mem_ + ((size_t) offset) * MEM_ITEM_SIZE;
}

ValueHeader MVCCValueStore::InsertValue(const value_t& value, int tid) {
    ValueHeader ret;
    ret.count = value.content.size() + MEM_ITEM_SIZE;

    OffsetT item_count = ret.count / MEM_ITEM_SIZE;
    if (item_count * MEM_ITEM_SIZE != ret.count)
        item_count++;
    ret.head_offset = Get(item_count, tid);
    // allocate finished.

    OffsetT current_offset = ret.head_offset;
    const char* value_content_ptr = &value.content[0];
    OffsetT value_len = value.content.size(), value_off = 0;

    for (OffsetT i = 0; i < item_count - 1; i++) {
        char* item_ptr = GetItemPtr(current_offset);
        if (i == 0) {
            // insert type; the first item only stores type.
            item_ptr[0] = static_cast<char>(value.type);
        } else {
            memcpy(item_ptr, value_content_ptr + value_off, MEM_ITEM_SIZE);
            value_off += MEM_ITEM_SIZE;
        }
        current_offset = next_offset_[current_offset];
    }
    char* item_ptr = GetItemPtr(current_offset);
    memcpy(item_ptr, value_content_ptr + value_off, value_len - (item_count - 2) * MEM_ITEM_SIZE);

    return ret;
}

void MVCCValueStore::GetValue(const ValueHeader& header, value_t& value, int tid) {
    if (header.count == 0)
        return;

    OffsetT value_len = header.count - MEM_ITEM_SIZE, value_off = 0;
    value.content.resize(value_len);
    OffsetT item_count = header.count / MEM_ITEM_SIZE;
    if (item_count * MEM_ITEM_SIZE != header.count)
        item_count++;
    OffsetT current_offset = header.head_offset;

    char* value_content_ptr = &value.content[0];

    for (OffsetT i = 0; i < item_count - 1; i++) {
        const char* item_ptr = GetItemPtr(current_offset);
        if (i == 0) {
            value.type = item_ptr[0];
        } else {
            memcpy(value_content_ptr + value_off, item_ptr, MEM_ITEM_SIZE);
            value_off += MEM_ITEM_SIZE;
        }
        current_offset = next_offset_[current_offset];
    }
    const char* item_ptr = GetItemPtr(current_offset);
    memcpy(value_content_ptr + value_off, item_ptr, value_len - (item_count - 2) * MEM_ITEM_SIZE);
}

void MVCCValueStore::FreeValue(const ValueHeader& header, int tid) {
    OffsetT item_count = header.count / MEM_ITEM_SIZE;
    if (item_count * MEM_ITEM_SIZE != header.count)
        item_count++;

    Free(header.head_offset, item_count, tid);
}

OffsetT MVCCValueStore::Get(const OffsetT& count, int tid) {
    auto& local_stat = thread_stat_[tid];
    
    #ifdef MVCC_VALUE_STORE_DEBUG
    get_counter_ += count;
    #endif  // MVCC_VALUE_STORE_DEBUG

    if (count > BLOCK_SIZE && local_stat.free_cell_count < count + 3) {
        pthread_spin_lock(&lock_);
        OffsetT ori_head = head_;
        for (OffsetT i = 0; i < count; i++) {
            head_ = next_offset_[head_];
            assert(head_ != tail_);
        }
        pthread_spin_unlock(&lock_);
        return ori_head;
    }

    if (local_stat.free_cell_count < count + 3) {
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

void MVCCValueStore::Free(const OffsetT& offset, const OffsetT& count, int tid) {
    #ifdef MVCC_VALUE_STORE_DEBUG
    free_counter_ += count;
    #endif  // MVCC_VALUE_STORE_DEBUG

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

    auto& local_stat = thread_stat_[tid];

    next_offset_[local_stat.block_tail] = offset;
    local_stat.block_tail = offset;
    local_stat.free_cell_count += count;

    for (OffsetT i = 0; i < count - 1; i++) {
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

#ifdef MVCC_VALUE_STORE_DEBUG
std::string MVCCValueStore::UsageString() {
    int get_counter = get_counter_;
    int free_counter = free_counter_;
    return "Get: " + std::to_string(get_counter) + ", Free: " + std::to_string(free_counter)
           + ", Total: " + std::to_string(item_count_);
}
#endif  // MVCC_VALUE_STORE_DEBUG
