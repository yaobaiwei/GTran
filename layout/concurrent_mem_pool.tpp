/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

template<class ItemT, class OffsetT>
void OffsetConcurrentMemPool<ItemT, OffsetT>::Initial(ItemT* mem, size_t element_cnt) {
    if (mem != nullptr)
        attached_mem_ = mem;
    else
        attached_mem_ = new ItemT[element_cnt];  // TODO(entityless): optimization

    element_cnt_ = element_cnt;

    next_offset_ = new OffsetT[element_cnt];

    for (OffsetT i = 0; i < element_cnt; i++) {
        next_offset_[i] = i + 1;
    }

    head_ = 0;
    tail_ = element_cnt - 1;

    pthread_spin_init(&head_lock_, 0);
    pthread_spin_init(&tail_lock_, 0);
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
}

