/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

template<class MVCC>
void MVCCList<MVCC>::AppendVersion(decltype(MVCC::val) val, const uint64_t& trx_id, const uint64_t& begin_time) {
    if(head_ == nullptr) {
        // load data
        MVCC* initial_mvcc = pool_ptr_->Get();

        initial_mvcc->begin_time = MVCC::MIN_TIME;
        initial_mvcc->end_time = MVCC::MAX_TIME;
        initial_mvcc->SetValue(val);
        initial_mvcc->next = nullptr;

        head_ = initial_mvcc;
    }
    else {
        // TODO(entityless): Implement this
    }
}

template<class MVCC>
MVCC* MVCCList<MVCC>::GetCurrentVersion(const uint64_t& trx_id, const uint64_t& begin_time) {
    MVCC* ret = head_;

    while (true) {
        // no suitable version found
        if (ret == nullptr)
            break;
        // if suitable, break
        if (ret->GetTransactionID() == trx_id || begin_time < ret->end_time)
            break;

        ret = (MVCC*)ret->next;
    }

    return ret;
}
