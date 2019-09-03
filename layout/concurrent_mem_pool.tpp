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
void ConcurrentMemPool<ItemT, OffsetT, BLOCK_SIZE>::Init(ItemT* mem, size_t element_count,
                                                         int nthreads, bool utilization_record) {
    assert(element_count > nthreads * (BLOCK_SIZE + 2));

    element_count_ = element_count;
    // Make sure that OffsetT can hold element_count
    assert(element_count_ == element_count);

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
        auto& local_stat = thread_stat_[tid];
        local_stat.free_cell_count = 0;
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

    nthreads_ = nthreads;
    utilization_record_ = utilization_record;
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

    if (utilization_record_)
        local_stat.get_counter++;

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

    if (utilization_record_)
        local_stat.free_counter++;
}

template<class ItemT, class OffsetT, int THREAD_BLOCK_SIZE>
std::string ConcurrentMemPool<ItemT, OffsetT, THREAD_BLOCK_SIZE>::UsageString() {
    OffsetT get_counter = 0;
    OffsetT free_counter = 0;
    for (int tid = 0; tid < nthreads_; tid++) {
        get_counter += thread_stat_[tid].get_counter;
        free_counter += thread_stat_[tid].free_counter;
    }
    OffsetT cell_avail = element_count_ - get_counter + free_counter - 2;

    return "Get: " + std::to_string(get_counter) + ", Free: " + std::to_string(free_counter)
           + ", Total: " + std::to_string(element_count_) + ", Avail: " + std::to_string(cell_avail);
}

// Temporal Use
template<class ItemT, class OffsetT, int THREAD_BLOCK_SIZE>
std::pair<uint64_t, uint64_t> ConcurrentMemPool<ItemT, OffsetT, THREAD_BLOCK_SIZE>::UsageStatistic(uint64_t unit_size) {
    OffsetT get_counter = 0;
    OffsetT free_counter = 0;
    for (int tid = 0; tid < nthreads_; tid++) {
        get_counter += thread_stat_[tid].get_counter;
        free_counter += thread_stat_[tid].free_counter;
    }
    OffsetT cell_avail = element_count_ - get_counter + free_counter - 2;

    return std::pair<uint64_t, uint64_t>((std::stoi(std::to_string(cell_avail)) * unit_size), (std::stoi(std::to_string(element_count_)) * unit_size));
    // return "Get: " + std::to_string(get_counter) + ", Free: " + std::to_string(free_counter)
    //        + ", Total: " + std::to_string(element_count_) + ", Avail: " + std::to_string(cell_avail);
}

template<class ItemT, class OffsetT, int THREAD_BLOCK_SIZE>
std::pair<OffsetT, OffsetT> ConcurrentMemPool<ItemT, OffsetT, THREAD_BLOCK_SIZE>::GetUsage() {
    OffsetT get_counter = 0;
    OffsetT free_counter = 0;
    for (int tid = 0; tid < nthreads_; tid++) {
        get_counter += thread_stat_[tid].get_counter;
        free_counter += thread_stat_[tid].free_counter;
    }

    return std::make_pair(get_counter, free_counter);
}
