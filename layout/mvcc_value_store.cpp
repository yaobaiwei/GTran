/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include "layout/mvcc_value_store.hpp"

MVCCValueStore::MVCCValueStore(char* mem, OffsetT item_count) {
    Init(mem, item_count);
}

MVCCValueStore::~MVCCValueStore() {
    if (next_offset_ != nullptr)
        _mm_free(next_offset_);
    if (mem_allocated_)
        _mm_free(attached_mem_);
}

void MVCCValueStore::Init(char* mem, OffsetT item_count) {
    if (mem != nullptr) {
        attached_mem_ = mem;
        mem_allocated_ = false;
    } else {
        attached_mem_ = reinterpret_cast<char*>(_mm_malloc(item_count * MemItemSize, 4096));
        mem_allocated_ = true;
    }

    item_count_ = item_count;

    next_offset_ = reinterpret_cast<OffsetT*>(_mm_malloc(sizeof(OffsetT) * item_count, 4096));

    for (OffsetT i = 0; i < item_count; i++) {
        next_offset_[i] = i + 1;
    }

    head_ = 0;
    tail_ = item_count - 1;

    pthread_spin_init(&head_lock_, 0);
    pthread_spin_init(&tail_lock_, 0);

    #ifdef MVCC_VALUE_STORE_DEBUG
    get_counter_ = 0;
    free_counter_ = 0;
    #endif  // MVCC_VALUE_STORE_DEBUG
}

char* MVCCValueStore::GetItemPtr(const OffsetT& offset) {
    return attached_mem_ + ((size_t) offset) * MemItemSize;
}

ValueHeader MVCCValueStore::InsertValue(const value_t& value) {
    ValueHeader ret;
    ret.count = value.content.size() + MemItemSize;

    OffsetT item_count = ret.count / MemItemSize;
    if (item_count * MemItemSize != ret.count)
        item_count++;
    ret.head_offset = Get(item_count);
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
            memcpy(item_ptr, value_content_ptr + value_off, MemItemSize);
            value_off += MemItemSize;
        }
        current_offset = next_offset_[current_offset];
    }
    char* item_ptr = GetItemPtr(current_offset);
    memcpy(item_ptr, value_content_ptr + value_off, value_len - (item_count - 2) * MemItemSize);

    return ret;
}

void MVCCValueStore::GetValue(const ValueHeader& header, value_t& value) {
    if (header.count == 0)
        return;

    OffsetT value_len = header.count - MemItemSize, value_off = 0;
    value.content.resize(value_len);
    OffsetT item_count = header.count / MemItemSize;
    if (item_count * MemItemSize != header.count)
        item_count++;
    OffsetT current_offset = header.head_offset;

    char* value_content_ptr = &value.content[0];

    for (OffsetT i = 0; i < item_count - 1; i++) {
        const char* item_ptr = GetItemPtr(current_offset);
        if (i == 0) {
            value.type = item_ptr[0];
        } else {
            memcpy(value_content_ptr + value_off, item_ptr, MemItemSize);
            value_off += MemItemSize;
        }
        current_offset = next_offset_[current_offset];
    }
    const char* item_ptr = GetItemPtr(current_offset);
    memcpy(value_content_ptr + value_off, item_ptr, value_len - (item_count - 2) * MemItemSize);
}

void MVCCValueStore::FreeValue(const ValueHeader& header) {
    OffsetT item_count = header.count / MemItemSize;
    if (item_count * MemItemSize != header.count)
        item_count++;

    Free(header.head_offset, item_count);
}

OffsetT MVCCValueStore::Get(const OffsetT& count) {
    pthread_spin_lock(&head_lock_);
    OffsetT ori_head = head_;
    for (OffsetT i = 0; i < count; i++) {
        head_ = next_offset_[head_];
        assert(head_ != tail_);
    }
    #ifdef MVCC_VALUE_STORE_DEBUG
    get_counter_ += count;
    #endif  // MVCC_VALUE_STORE_DEBUG
    pthread_spin_unlock(&head_lock_);
    return ori_head;
}

void MVCCValueStore::Free(const OffsetT& offset, const OffsetT& count) {
    pthread_spin_lock(&tail_lock_);
    next_offset_[tail_] = offset;
    // TODO(entityless): This loop can actually be moved to outside the critical region
    for (OffsetT i = 0; i < count - 1; i++) {
        tail_ = next_offset_[tail_];
    }
    #ifdef MVCC_VALUE_STORE_DEBUG
    free_counter_ += count;
    #endif  // MVCC_VALUE_STORE_DEBUG
    pthread_spin_unlock(&tail_lock_);
}

#ifdef MVCC_VALUE_STORE_DEBUG
std::string MVCCValueStore::UsageString() {
    int get_counter = get_counter_;
    int free_counter = free_counter_;
    return "Get: " + std::to_string(get_counter) + ", Free: " + std::to_string(free_counter)
           + ", Total: " + std::to_string(item_count_);
}
#endif  // MVCC_VALUE_STORE_DEBUG
