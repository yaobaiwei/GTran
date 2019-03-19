/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include "layout/mvcc_value_store.hpp"

void MVCCValueStore::Init(char* mem, OffsetT item_count) {
    if (mem != nullptr)
        attached_mem_ = mem;
    else
        attached_mem_ = new char[item_count * MemItemSize];

    item_count_ = item_count;

    next_offset_ = new OffsetT[item_count];

    for (OffsetT i = 0; i < item_count; i++) {
        next_offset_[i] = i + 1;
    }

    head_ = 0;
    tail_ = item_count - 1;

    pthread_spin_init(&head_lock_, 0);
    pthread_spin_init(&tail_lock_, 0);

    get_counter_ = 0;
    free_counter_ = 0;
}

char* MVCCValueStore::GetItemPtr(const OffsetT& offset) {
    return attached_mem_ + ((size_t) offset) * MemItemSize;
}

ValueHeader MVCCValueStore::Insert(const value_t& value) {
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
    get_counter_ += count;
    pthread_spin_unlock(&head_lock_);
    return ori_head;
}

void MVCCValueStore::Free(const OffsetT& offset, const OffsetT& count) {
    pthread_spin_lock(&tail_lock_);
    next_offset_[tail_] = offset;
    for (OffsetT i = 0; i < count - 1; i++) {
        tail_ = next_offset_[tail_];
    }
    pthread_spin_unlock(&tail_lock_);
}
