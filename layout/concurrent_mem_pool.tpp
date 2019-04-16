/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

template<class ItemT, class OffsetT, int BLOCK_SIZE>
ConcurrentMemPool<ItemT, OffsetT, BLOCK_SIZE>::~ConcurrentMemPool() {
    if (next_offset_ != nullptr)
        _mm_free(next_offset_);
    if (mem_allocated_)
        _mm_free(attached_mem_);
}

template<class ItemT, class OffsetT, int BLOCK_SIZE>
void ConcurrentMemPool<ItemT, OffsetT, BLOCK_SIZE>::Init(ItemT* mem, OffsetT element_count, int nthreads) {
    assert(element_count > nthreads * (BLOCK_SIZE + 2));

    if (mem != nullptr) {
        attached_mem_ = mem;
        mem_allocated_ = false;
    } else {
        attached_mem_ = reinterpret_cast<ItemT*>(_mm_malloc(sizeof(ItemT) * element_count, 4096));
        mem_allocated_ = true;
    }

    next_offset_ = reinterpret_cast<OffsetT*>(_mm_malloc(sizeof(OffsetT) * element_count, 4096));

    for (OffsetT i = 0; i < element_count; i++) {
        next_offset_[i] = i + 1;
    }

    head_ = 0;
    tail_ = element_count - 1;

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

    #ifdef OFFSET_MEMORY_POOL_DEBUG
    nthreads_ = nthreads;
    element_count_ = element_count;
    get_counter_ = 0;
    free_counter_ = 0;
    #endif  // OFFSET_MEMORY_POOL_DEBUG
}

template<class ItemT, class OffsetT, int BLOCK_SIZE>
ItemT* ConcurrentMemPool<ItemT, OffsetT, BLOCK_SIZE>::Get(int tid) {
    ItemT* ret;
    auto& local_stat = thread_stat_[tid];

    if (next_offset_[local_stat.block_head] == local_stat.block_tail) {
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

    ret = attached_mem_ + local_stat.block_head;
    local_stat.block_head = next_offset_[local_stat.block_head];
    local_stat.free_cell_count--;

    #ifdef OFFSET_MEMORY_POOL_DEBUG
    get_counter_++;
    #endif  // OFFSET_MEMORY_POOL_DEBUG
    return ret;
}

template<class ItemT, class OffsetT, int BLOCK_SIZE>
void ConcurrentMemPool<ItemT, OffsetT, BLOCK_SIZE>::Free(ItemT* element, int tid) {
    OffsetT mem_off = element - attached_mem_;

    auto& local_stat = thread_stat_[tid];

    next_offset_[local_stat.block_tail] = mem_off;
    local_stat.block_tail = mem_off;
    local_stat.free_cell_count++;

    if (local_stat.free_cell_count == 2 * BLOCK_SIZE) {
        OffsetT tmp_head = local_stat.block_head;
        OffsetT tmp_tail = tmp_head;
        for (int i = 0; i < BLOCK_SIZE - 1; i++)
            tmp_tail = next_offset_[tmp_tail];
        local_stat.block_head = next_offset_[tmp_tail];
        local_stat.free_cell_count -= BLOCK_SIZE;
        pthread_spin_lock(&lock_);
        next_offset_[tail_] = tmp_head;
        tail_ = tmp_tail;
        pthread_spin_unlock(&lock_);
    }

    #ifdef OFFSET_MEMORY_POOL_DEBUG
    free_counter_++;
    #endif  // OFFSET_MEMORY_POOL_DEBUG
}

#ifdef OFFSET_MEMORY_POOL_DEBUG
template<class ItemT, class OffsetT, int BLOCK_SIZE>
std::string ConcurrentMemPool<ItemT, OffsetT, BLOCK_SIZE>::UsageString() {
    int get_counter = get_counter_;
    int free_counter = free_counter_;
    return "Get: " + std::to_string(get_counter) + ", Free: " + std::to_string(free_counter)
           + ", Total: " + std::to_string(element_count_);
}
#endif  // OFFSET_MEMORY_POOL_DEBUG
