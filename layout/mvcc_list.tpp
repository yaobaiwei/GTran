/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
         Modified by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

template<class MVCC>
MVCC* MVCCList<MVCC>::GetHead() {
    return head_;
}

template<class MVCC>
MVCC* MVCCList<MVCC>::GetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time, const bool& read_only) {
    if (tail_->GetTransactionID() == trx_id)
        return tail_;

    MVCC* ret = head_;

    while (true) {
        // if suitable, break
        if (ret->GetTransactionID() == trx_id || begin_time < ret->GetEndTime()) {
            // Check whether there is next version
            if (ret->next != nullptr) {
                uint64_t next_ver_trx_id = (static_cast<MVCC*>(ret->next))->GetTransactionID();
                TrxTableStub * trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();

                if (next_ver_trx_id == 0) {   // Next version committed
                    if (!read_only) {
                        // Abort directly for non-read_only
                        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
                        ret = nullptr;
                    }
                } else {   // Next version NOT committed
                    /** Need to compare current_transaction_bt(BT) and next_version_tranasction_ct(NCT)
                     *   case 1. Processing ---> Ignore (Will valid in normal validation for non-read_only)
                     *   case 2. Validation && BT > NCT ---> Optimistic read, read next_version ---> HomoDependency
                     *   case 3. Validation && BT < NCT ---> Read current_version,
                     *                                          For non-read_only, if next_version commit, me abort ---> HeteroDependency
                     *                                          For read_only, no dependency
                     *   case 4. Commit && BT > NCT ---> Read next version and no need to record
                     *   case 5. Commit && BT < NCT ---> For non-read_only, Abort Directly
                     *                                   For read_only, Read current_version
                     *   case 6. Abort ---> Ignore
                     */
                    TRX_STAT cur_stat; uint64_t next_trx_ct;
                    trx_table_stub_->read_ct(next_ver_trx_id, cur_stat, next_trx_ct);
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
                            if (!read_only) {
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
                            if (!read_only) {
                                // Abort
                                trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
                                ret = nullptr;
                            }
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
        MVCC* head_mvcc = mem_pool_->Get();

        head_mvcc->Init(trx_id, begin_time);
        head_mvcc->next = nullptr;

        head_ = head_mvcc;
        tail_ = head_mvcc;
        tail_last_ = head_mvcc;

        return &head_mvcc->val;
    }

    // TODO(entityless): [Blocking] if trx_id == tail_->GetTransactionID(), replace the version
    if (tail_->GetBeginTime() > MVCC::MAX_TIME) {
        // the tail is uncommited
        if (tail_->GetTransactionID() == trx_id) {
            // in the same transaction, the original value will be overwritten
            if (tail_->NeedGC())
                tail_->InTransactionGC();

            return &tail_->val;
        } else {
            return nullptr;
        }
    }

    MVCC* mvcc = mem_pool_->Get();

    mvcc->Init(trx_id, begin_time);
    mvcc->next = nullptr;

    tail_->next = mvcc;
    tail_last_buffer_ = tail_last_;
    tail_last_ = tail_;
    tail_ = mvcc;

    return &mvcc->val;
}

template<class MVCC>
decltype(MVCC::val)* MVCCList<MVCC>::AppendInitialVersion() {
    // load data from HDFS
    MVCC* initial_mvcc = mem_pool_->Get();

    initial_mvcc->Init(MVCC::MIN_TIME, MVCC::MAX_TIME);
    initial_mvcc->next = nullptr;

    head_ = initial_mvcc;
    tail_ = initial_mvcc;
    tail_last_ = initial_mvcc;

    return &initial_mvcc->val;
}

template<class MVCC>
void MVCCList<MVCC>::CommitVersion(const uint64_t& trx_id, const uint64_t& commit_time) {
    assert(tail_->GetTransactionID() == trx_id);

    tail_->Commit(tail_last_, commit_time);
}

template<class MVCC>
decltype(MVCC::val) MVCCList<MVCC>::AbortVersion(const uint64_t& trx_id) {
    assert(tail_->GetTransactionID() == trx_id);

    decltype(MVCC::val) ret = (static_cast<MVCC*>(tail_))->val;

    if (tail_ != tail_last_) {
        // abort modification
        tail_last_->next = nullptr;

        mem_pool_->Free(tail_);
        tail_ = tail_last_;
        tail_last_ = tail_last_buffer_;
    } else {
        // tail_ == head_, only one version in the list
        // occurs only in "Add" functions, like AddVertex, AddVP...
        tail_->val = MVCC::EMPTY_VALUE;
        tail_->Commit(tail_last_, 0);
    }

    return ret;
}
