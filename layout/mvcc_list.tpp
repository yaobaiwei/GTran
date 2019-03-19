/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

template<class MVCC>
MVCC* MVCCList<MVCC>::GetCurrentVersion(const uint64_t& trx_id, const uint64_t& begin_time) {
    if (tail_->GetTransactionID() == trx_id)
        return tail_;

    MVCC* ret = head_;

    // TODO(entityless): add Optimistic Pre-read

    while (true) {
        // no suitable version found
        if (ret == nullptr)
            break;
        // if suitable, break
        if (ret->GetTransactionID() == trx_id || begin_time < ret->GetEndTime())
            break;

        ret = (MVCC*)ret->next;
    }

    return ret;
}

template<class MVCC>
decltype(MVCC::val)* MVCCList<MVCC>::AppendVersion(const uint64_t& trx_id, const uint64_t& begin_time) {
    if(head_ == nullptr) {
        MVCC* head_mvcc = pool_ptr_->Get();

        head_mvcc->Init(trx_id, begin_time);
        head_mvcc->next = nullptr;

        head_ = head_mvcc;
        tail_ = head_mvcc;

        return &head_mvcc->val;
    }

    if (tail_->GetBeginTime() > MVCC::MAX_TIME)
        return nullptr;

    MVCC* mvcc = pool_ptr_->Get();

    mvcc->Init(trx_id, begin_time);
    mvcc->next = nullptr;

    tail_->next = mvcc;
    tail_ = mvcc;

    return &mvcc->val;
}

template<class MVCC>
decltype(MVCC::val)* MVCCList<MVCC>::AppendInitialVersion() {
    // load data from HDFS
    MVCC* initial_mvcc = pool_ptr_->Get();

    initial_mvcc->Init(MVCC::MIN_TIME, MVCC::MAX_TIME);
    initial_mvcc->next = nullptr;

    head_ = initial_mvcc;
    tail_ = initial_mvcc;

    return &initial_mvcc->val;
}

template<class MVCC>
void MVCCList<MVCC>::CommitVersion(const uint64_t& trx_id, const uint64_t& commit_time) {
    assert(tail_->GetTransactionID() == trx_id);

    MVCC *tail_previous = head_;
    while (true) {
        if (tail_previous->next == tail_ || tail_previous->next == nullptr)
            break;
        tail_previous = tail_previous->next;
    }

    tail_->Commit(tail_previous, commit_time);
}

template<class MVCC>
decltype(MVCC::val) MVCCList<MVCC>::AbortVersion(const uint64_t& trx_id) {
    assert(tail_->GetTransactionID() == trx_id);

    MVCC *tail_previous = head_;
    while (true) {
        if (tail_previous->next == tail_ || tail_previous->next == nullptr)
            break;
        tail_previous = tail_previous->next;
    }

    decltype(MVCC::val) ret = ((MVCC*)tail_)->val;

    if (tail_ != tail_previous) {
        // abort modification
        tail_previous->next = nullptr;

        pool_ptr_->Free(tail_);
        tail_ = tail_previous;
    } else {
        // abort add
        tail_->val = MVCC::EMPTY_VALUE;
        tail_->Commit(tail_previous, 0);
    }

    return ret;
}
