/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
         Modified by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

template<class Item>
MVCCList<Item>::MVCCList() {
    pthread_spin_init(&lock_, 0);
}

template<class Item>
Item* MVCCList<Item>::GetHead() {
    return head_;
}

template<class Item>
bool MVCCList<Item>::GetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time,
                                       const bool& read_only, MVCC_PTR& ret) {
    // return false for abort

    pthread_spin_lock(&lock_);
    Item* head_snapshot = head_;
    Item* tail_snapshot = tail_;
    Item* pre_tail_snapshot = pre_tail_;
    // tmp_pre_tail_ is not nullptr only when the tail is uncommitted
    Item* tmp_pre_tail_snapshot = tmp_pre_tail_;
    pthread_spin_unlock(&lock_);

    if (tail_snapshot->GetTransactionID() == trx_id) {
        // in the same trx
        ret = tail_snapshot;
        return true;
    }

    if (tmp_pre_tail_snapshot != nullptr) {
        // process finished, not committed/aborted
        tail_snapshot = pre_tail_snapshot;
    }

    // the whole MVCCList is not visible to the current trx
    if (begin_time < head_snapshot->GetBeginTime()) {
        ret = nullptr;
        return true;
    }

    ret = head_snapshot;

    while (true) {
        // if visible, break
        if (begin_time < ret->GetEndTime()) {
            // Check whether there is next version
            if (ret->next != nullptr) {
                uint64_t next_ver_trx_id = (static_cast<Item*>(ret->next))->GetTransactionID();
                TrxTableStub * trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();

                if (next_ver_trx_id == 0) {   // Next version committed
                    if (!read_only) {
                        // Abort directly for non-read_only
                        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
                        ret = nullptr;
                        return false;
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
                            ret = static_cast<Item*>(ret->next);
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
                            ret = static_cast<Item*>(ret->next);
                        } else {
                            if (!read_only) {
                                // Abort
                                trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
                                ret = nullptr;
                                return false;
                            }
                        }
                    }
                }
            }
            break;
        }

        if (ret == tail_snapshot) {
            // if the last version is still not visible, system error occurs.
            assert("no visible version, system error" != "");
        }

        ret = static_cast<Item*>(ret->next);
    }

    return true;
}

template<class Item>
decltype(Item::val)* MVCCList<Item>::AppendVersion(const uint64_t& trx_id, const uint64_t& begin_time) {
    SimpleSpinLockGuard lock_guard(&lock_);

    if (head_ == nullptr) {
        Item* head_mvcc = mem_pool_->Get();

        head_mvcc->Init(trx_id, begin_time);
        head_mvcc->next = nullptr;

        head_ = head_mvcc;
        tail_ = head_mvcc;
        pre_tail_ = head_mvcc;
        tmp_pre_tail_ = head_mvcc;

        return &head_mvcc->val;
    }

    if (tail_->GetBeginTime() > Item::MAX_TIME) {
        // the tail is uncommitted
        if (tail_->GetTransactionID() == trx_id) {
            // in the same transaction, the original value will be overwritten
            if (tail_->NeedGC())
                tail_->InTransactionGC();

            return &tail_->val;
        } else {
            return nullptr;
        }
    }

    Item* new_version = mem_pool_->Get();

    new_version->Init(trx_id, begin_time);
    new_version->next = nullptr;

    tail_->next = new_version;
    tmp_pre_tail_ = pre_tail_;
    pre_tail_ = tail_;
    tail_ = new_version;

    return &new_version->val;
}

template<class Item>
decltype(Item::val)* MVCCList<Item>::AppendInitialVersion() {
    // load data from HDFS
    Item* initial_mvcc = mem_pool_->Get();

    initial_mvcc->Init(Item::MIN_TIME, Item::MAX_TIME);
    initial_mvcc->next = nullptr;

    head_ = initial_mvcc;
    tail_ = initial_mvcc;
    pre_tail_ = initial_mvcc;

    return &initial_mvcc->val;
}

template<class Item>
void MVCCList<Item>::CommitVersion(const uint64_t& trx_id, const uint64_t& commit_time) {
    SimpleSpinLockGuard lock_guard(&lock_);
    assert(tail_->GetTransactionID() == trx_id);

    tail_->Commit(pre_tail_, commit_time);
    tmp_pre_tail_ = nullptr;
}

template<class Item>
decltype(Item::val) MVCCList<Item>::AbortVersion(const uint64_t& trx_id) {
    SimpleSpinLockGuard lock_guard(&lock_);
    assert(tail_->GetTransactionID() == trx_id);

    decltype(Item::val) ret = (static_cast<Item*>(tail_))->val;

    if (tail_ != pre_tail_) {
        // abort modification
        pre_tail_->next = nullptr;

        mem_pool_->Free(tail_);
        tail_ = pre_tail_;
        pre_tail_ = tmp_pre_tail_;
    } else {
        // tail_ == head_, only one version in the list
        // occurs only in "Add" functions, like AddVertex, AddVP...
        tail_->val = Item::EMPTY_VALUE;
        tail_->Commit(pre_tail_, 0);
    }

    tmp_pre_tail_ = nullptr;
    return ret;
}

template<class Item>
void MVCCList<Item>::SelfGarbageCollect() {
    while (head_ != nullptr) {
        if (head_->NeedGC())
            head_->InTransactionGC();
        Item* tmp_next = head_->next;
        mem_pool_->Free(head_);
        head_ = tmp_next;
    }
}
