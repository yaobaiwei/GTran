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

// return false if abort
template<class Item>
bool MVCCList<Item>::GetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time,
                                       const bool& read_only, MVCCItem_PTR& ret) {
    // get a snapshot of 4 pointers of the MVCCList in critical region
    pthread_spin_lock(&lock_);
    Item* head_snapshot = head_;
    Item* tail_snapshot = tail_;
    Item* pre_tail_snapshot = pre_tail_;
    Item* tmp_pre_tail_snapshot = tmp_pre_tail_;
    pthread_spin_unlock(&lock_);

    // the MVCCList is empty
    if (head_snapshot == nullptr) {
        ret = nullptr;
        return true;
    }

    if (tail_snapshot->GetTransactionID() == trx_id) {
        // In the same trx, the uncommitted tail is visible.
        ret = tail_snapshot;
        return true;
    }

    // tmp_pre_tail_ is not nullptr only when the tail_ is uncommitted.
    if (tmp_pre_tail_snapshot != nullptr) {
        tail_snapshot = pre_tail_snapshot;
    }

    // The whole MVCCList is not visible to the current trx
    if (begin_time < head_snapshot->GetBeginTime()) {
        ret = nullptr;
        return true;
    }

    // Begin the iteration from the head of MVCCList
    ret = head_snapshot;

    while (true) {
        // If visible, break
        if (begin_time < ret->GetEndTime()) {
            // Check whether there is next version
            if (ret->next != nullptr) {
                uint64_t next_ver_trx_id = (static_cast<Item*>(ret->next))->GetTransactionID();
                TrxTableStub * trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();

                if (next_ver_trx_id == 0) {  // Next version committed
                    if (!read_only) {
                        // Abort directly for non-read_only (read set has been modified)
                        ret = nullptr;
                        return false;
                    }
                } else {  // Next version NOT committed
                    /* Need to compare current_transaction_bt(BT) and next_version_tranasction_ct(NCT)
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
            // If the last version is still not visible, system error occurs.
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
        Item* head_mvcc = mem_pool_->Get(TidMapper::GetInstance()->GetTidUnique());

        head_mvcc->Init(trx_id, begin_time);
        head_mvcc->next = nullptr;

        head_ = head_mvcc;
        tail_ = head_mvcc;
        pre_tail_ = head_mvcc;
        tmp_pre_tail_ = head_mvcc;

        return &head_mvcc->val;
    }

    if (tail_->GetBeginTime() > Item::MAX_TIME) {
        // The tail is uncommitted
        if (tail_->GetTransactionID() == trx_id) {
            /* In the same transaction, the original value will be overwritten,
             * and the MVCCList will not be extended.
             */
            if (tail_->NeedGC())
                tail_->ValueGC();

            return &tail_->val;
        } else {
            // write-write conflict occurs, abort
            return nullptr;
        }
    }

    Item* new_version = mem_pool_->Get(TidMapper::GetInstance()->GetTidUnique());

    new_version->Init(trx_id, begin_time);
    new_version->next = nullptr;

    // Append a new version
    tail_->next = new_version;
    // Stores the original pre_tail, will be used in AbortVersion.
    tmp_pre_tail_ = pre_tail_;

    // extend the MVCCList
    pre_tail_ = tail_;
    tail_ = new_version;

    return &new_version->val;
}

template<class Item>
decltype(Item::val)* MVCCList<Item>::AppendInitialVersion() {
    // Loading data from HDFS, do not need to lock the list
    Item* initial_mvcc = mem_pool_->Get(TidMapper::GetInstance()->GetTidUnique());

    initial_mvcc->Init(Item::MIN_TIME, Item::MAX_TIME);
    initial_mvcc->next = nullptr;

    head_ = initial_mvcc;
    tail_ = initial_mvcc;
    pre_tail_ = initial_mvcc;

    return &initial_mvcc->val;
}

/* During commit stage, not only the tail_ need to be modified.
 * The end_time of the pre_tail_ need to be modified,
 * and it won't be visible to transaction with timestamp >= commit_time after Commit.
 */
template<class Item>
void MVCCList<Item>::CommitVersion(const uint64_t& trx_id, const uint64_t& commit_time) {
    SimpleSpinLockGuard lock_guard(&lock_);
    assert(tail_->GetTransactionID() == trx_id);

    tail_->Commit(pre_tail_, commit_time);
    tmp_pre_tail_ = nullptr;
}

// Remove the tail_ added during processing stage.
template<class Item>
void MVCCList<Item>::AbortVersion(const uint64_t& trx_id) {
    SimpleSpinLockGuard lock_guard(&lock_);
    assert(tail_->GetTransactionID() == trx_id);

    if (tail_->NeedGC())
        tail_->ValueGC();

    mem_pool_->Free(tail_, TidMapper::GetInstance()->GetTidUnique());

    if (tail_ != pre_tail_) {
        // More than one version in MVCCList
        pre_tail_->next = nullptr;

        tail_ = pre_tail_;
        pre_tail_ = tmp_pre_tail_;
    } else {
        // Only one version in MVCCList
        tail_ = head_ = pre_tail_ = nullptr;
    }

    tmp_pre_tail_ = nullptr;
}

// Clear the MVCCList
template<class Item>
void MVCCList<Item>::SelfGarbageCollect() {
    SimpleSpinLockGuard lock_guard(&lock_);
    while (head_ != nullptr) {
        if (head_->NeedGC())
            head_->ValueGC();
        Item* tmp_next = head_->next;
        mem_pool_->Free(head_, TidMapper::GetInstance()->GetTidUnique());
        head_ = tmp_next;
    }

    tail_ = pre_tail_ = tmp_pre_tail_ = nullptr;
}
