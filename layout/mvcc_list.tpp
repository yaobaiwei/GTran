/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

template<class MVCC>
MVCC* MVCCList<MVCC>::GetInitVersion() {
    return head_;
}

template<class MVCC>
MVCC* MVCCList<MVCC>::GetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time) {
    if (tail_->GetTransactionID() == trx_id)
        return tail_;

    MVCC* ret = head_;

    while (true) {
        // no suitable version found
        if (ret == nullptr)
            break;

        // if suitable, break
        if (ret->GetTransactionID() == trx_id || begin_time < ret->GetEndTime()) {
            // Check whether there is next version
            if (ret->next != nullptr) {
                uint64_t next_ver_trx_id = (static_cast<MVCC*>(ret->next))->GetTransactionID();
                TrxTableStub * trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();

                if (next_ver_trx_id == 0) {   // Next version committed
                    // Abort directly
                    trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
                    ret = nullptr;
                } else {   // Next version NOT committed
                    /** Need to compare current_transaction_bt(BT) and next_version_tranasction_ct(NCT)
                     *   case 1. Processing ---> Ignore ( Will valid in normal validation )
                     *   case 2. Validation && BT > NCT ---> Optimistic read, read next_version ---> HomoDependency
                     *   case 3. Validation && BT < NCT ---> NOT read next_version, but if next_version commit, me abort ---> HeteroDependency
                     *   case 4. Commit && BT > NCT ---> Read next version and no need to record
                     *   case 5. Commit && BT < NCT ---> Abort Directly
                     *   case 6. Abort ---> Ignore
                     */
                    TRX_STAT cur_stat; uint64_t next_trx_ct;
                    trx_table_stub_->read_status(next_ver_trx_id, cur_stat);
                    trx_table_stub_->read_ct(next_ver_trx_id, next_trx_ct);
                    if (cur_stat == TRX_STAT::VALIDATING) {
                        if (begin_time > next_trx_ct) {
                            // Optimistic read
                            ret = static_cast<MVCC*>(ret->next);
                            {
                                dep_trx_accessor accessor;
                                dep_trx_map.insert(accessor, trx_id);
                                accessor->second.homo_trx_list.emplace_back(next_ver_trx_id);
                            }   // record homo-dependency
                        } else {
                            {
                                dep_trx_accessor accessor;
                                dep_trx_map.insert(accessor, trx_id);
                                accessor->second.hetero_trx_list.emplace_back(next_ver_trx_id);
                            }   // record hetero-dependency
                        }
                    } else if (cur_stat == TRX_STAT::COMMITTED) {
                        if (begin_time > next_trx_ct) {
                            // Read next version directly
                            ret = static_cast<MVCC*>(ret->next);
                        } else {
                            // Abort
                            trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
                            ret = nullptr;
                        }
                    }
                }
            }
            break;
        }

        ret = static_cast<MVCC*>(ret->next);
    }

    return ret;
}

template<class MVCC>
decltype(MVCC::val)* MVCCList<MVCC>::AppendVersion(const uint64_t& trx_id, const uint64_t& begin_time) {
    if (head_ == nullptr) {
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

    decltype(MVCC::val) ret = (static_cast<MVCC*>(tail_))->val;

    if (tail_ != tail_previous) {
        // abort modification
        tail_previous->next = nullptr;

        pool_ptr_->Free(tail_);
        tail_ = tail_previous;
    } else {
        // tail_ == head_, only one version in the list
        // occurs only in "Add" functions, like AddVertex, AddVP...
        tail_->val = MVCC::EMPTY_VALUE;
        tail_->Commit(tail_previous, 0);
    }

    return ret;
}
