/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

template<class ItemT, class OffsetT>
OffsetConcurrentMemPool<ItemT, OffsetT>::~OffsetConcurrentMemPool() {
    if(next_offset_ != nullptr)
        _mm_free(next_offset_);
    if (mem_allocated_)
        _mm_free(attached_mem_);
}

template<class ItemT, class OffsetT>
void OffsetConcurrentMemPool<ItemT, OffsetT>::Init(ItemT* mem, size_t element_count) {
    if (mem != nullptr) {
        attached_mem_ = mem;
        mem_allocated_ = false;
    }
    else {
        attached_mem_ = (ItemT*)_mm_malloc(sizeof(ItemT) * element_count, 4096);
        mem_allocated_ = true;
    }

    element_count_ = element_count;

    next_offset_ = (OffsetT*)_mm_malloc(sizeof(OffsetT) * element_count, 4096);

    for (OffsetT i = 0; i < element_count; i++) {
        next_offset_[i] = i + 1;
    }

    head_ = 0;
    tail_ = element_count - 1;

    pthread_spin_init(&head_lock_, 0);
    pthread_spin_init(&tail_lock_, 0);

    #ifdef OFFSET_MEMORY_POOL_DEBUG
    get_counter_ = 0;
    free_counter_ = 0;
    #endif  // OFFSET_MEMORY_POOL_DEBUG
}

template<class ItemT, class OffsetT>
ItemT* OffsetConcurrentMemPool<ItemT, OffsetT>::Get() {
    ItemT* ret;

    pthread_spin_lock(&head_lock_);
    OffsetT ori_head = head_;
    if (next_offset_[ori_head] == tail_) {
        ret = nullptr;
    } else {
        ret = attached_mem_ + ori_head;
        head_ = next_offset_[ori_head];
    }
    pthread_spin_unlock(&head_lock_);
    assert(ret != nullptr);
    #ifdef OFFSET_MEMORY_POOL_DEBUG
    get_counter_++;
    #endif  // OFFSET_MEMORY_POOL_DEBUG
    return ret;
}

template<class ItemT, class OffsetT>
void OffsetConcurrentMemPool<ItemT, OffsetT>::Free(ItemT* element) {
    OffsetT mem_off = element - attached_mem_;
    pthread_spin_lock(&tail_lock_);
    OffsetT ori_tail = tail_;
    next_offset_[ori_tail] = mem_off;
    tail_ = mem_off;
    pthread_spin_unlock(&tail_lock_);
    #ifdef OFFSET_MEMORY_POOL_DEBUG
    free_counter_++;
    #endif  // OFFSET_MEMORY_POOL_DEBUG
}

#ifdef OFFSET_MEMORY_POOL_DEBUG
template<class ItemT, class OffsetT>
std::string OffsetConcurrentMemPool<ItemT, OffsetT>::UsageString() {
    int get_counter = get_counter_;
    int free_counter = free_counter_;
    return "Get: " + std::to_string(get_counter) + ", Free: " + std::to_string(free_counter)
           + ", Total: " + std::to_string(element_count_);
}
#endif  // OFFSET_MEMORY_POOL_DEBUG
