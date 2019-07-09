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
pair<bool, bool> MVCCList<Item>::TryPreReadUncommittedTail(const uint64_t& trx_id, const uint64_t& begin_time,
                                                           const bool& read_only) {
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

    uint64_t tail_trx_id = tail_->GetTransactionID();
    assert(tail_trx_id != 0);

    TrxTableStub * trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();

    TRX_STAT cur_stat;
    uint64_t tail_trx_ct;
    trx_table_stub_->read_ct(tail_trx_id, cur_stat, tail_trx_ct);
    if (cur_stat == TRX_STAT::VALIDATING) {
        if (begin_time > tail_trx_ct) {
            // Optimistic read
            {
                dep_trx_accessor accessor;
                dep_trx_map.insert(accessor, trx_id);
                accessor->second.homo_trx_list.emplace(tail_trx_id);
            }   // record homo-dependency
            return make_pair(true, true);
        } else {
            if (!read_only) {
                dep_trx_accessor accessor;
                dep_trx_map.insert(accessor, trx_id);
                accessor->second.hetero_trx_list.emplace(tail_trx_id);
            }   // record hetero-dependency
        }
    } else if (cur_stat == TRX_STAT::COMMITTED) {
        if (begin_time > tail_trx_ct) {
            return make_pair(true, true);  // Read uncommited version directly
        } else {
            if (!read_only) {
                return make_pair(false, false);  // Abort
            }
        }
    }

    return make_pair(true, false);
}

// ReturnValue
//  first: false if abort
//  second: false if no version visible
template<class Item>
pair<bool, bool> MVCCList<Item>::GetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time,
                                       const bool& read_only, ValueType& ret) {
    Item *iterate_head, *iterate_tail;

    {
        SimpleSpinLockGuard lock_guard(&lock_);

        // The MVCCList is empty
        if (head_ == nullptr) {
            return make_pair(true, false);
        }

        // The tail_ is commited
        if (tail_->GetTransactionID() == 0) {
            // No visible version
            if (head_->GetBeginTime() > begin_time) {
                return make_pair(true, false);
            } else {  // At least one visible between head_ and tail_
                // For non-readonly transaction, if the visible version is not tail_, abort directly
                if (!read_only) {
                    if (tail_->GetBeginTime() > begin_time) {
                        return make_pair(false, false);
                    }
                    ret = tail_->val;
                    return make_pair(true, true);
                }
            }
            iterate_tail = tail_;
        } else {  // The tail is uncommited
            // The tail_ is created by the same transaction
            if (tail_->GetTransactionID() == trx_id) {
                ret = tail_->val;
                return make_pair(true, true);
            } else if (head_ == tail_) {  // Only one version in the whole mvcc_list, and it is uncommited
                pair<bool, bool> preread_visible = TryPreReadUncommittedTail(trx_id, begin_time, read_only);
                if (!preread_visible.first)
                    return make_pair(false, false);

                if (preread_visible.second)
                    ret = tail_->val;

                // Early return, since no commited version in the MVCCList
                return preread_visible;
            } else {  // At least one committed version in MVCCList; pre_tail is committed, and tail_ is uncommitted.
                // The whole mvcc_list is not visible
                if (head_->GetBeginTime() > begin_time) {
                    return make_pair(true, false);
                } else if (pre_tail_->GetBeginTime() <= begin_time) {  // pre_tail_ is visible to me
                    pair<bool, bool> preread_visible = TryPreReadUncommittedTail(trx_id, begin_time, read_only);

                    if (!preread_visible.first)
                        return make_pair(false, false);

                    if (preread_visible.second)
                        ret = tail_->val;
                    else
                        ret = pre_tail_->val;
                    return make_pair(true, true);
                } else {  // There is a visible version in [head_, pre_tail_)
                    if (!read_only)
                        return make_pair(false, false);
                }
            }
            iterate_tail = pre_tail_;
        }
        iterate_head = head_;
    }

    // Begin the iteration from the head of MVCCList
    Item* version = iterate_head;

    while (true) {
        // If visible, break
        if (begin_time < version->GetEndTime())
            break;

        assert(version != iterate_tail);

        version = static_cast<Item*>(version->next);
    }

    ret = version->val;
    return make_pair(true, true);
}

template<class Item>
decltype(Item::val)* MVCCList<Item>::AppendVersion(const uint64_t& trx_id, const uint64_t& begin_time,
                                                   decltype(Item::val)* old_val_header, bool* old_val_exists) {
    SimpleSpinLockGuard lock_guard(&lock_);

    if (head_ == nullptr) {
        Item* head_mvcc = mem_pool_->Get(TidMapper::GetInstance()->GetTidUnique());

        head_mvcc->Init(trx_id, begin_time);
        head_mvcc->next = nullptr;

        head_ = head_mvcc;
        tail_ = head_mvcc;
        pre_tail_ = head_mvcc;
        tmp_pre_tail_ = head_mvcc;

        if (old_val_exists != nullptr) {
            *old_val_exists = false;
        }

        return &head_mvcc->val;
    }

    // The tail is uncommitted
    if (tail_->GetBeginTime() > Item::MAX_TIME) {
        if (tail_->GetTransactionID() == trx_id) {
            /* In the same transaction, the original value will be overwritten,
             * and the MVCCList will not be extended.
             */
            if (tail_->NeedGC())
                tail_->ValueGC();

            return &tail_->val;
        } else {  // write-write conflict occurs, abort
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

    // Get Old Value for Index Update
    if (old_val_header != nullptr) {
        old_val_header[0] = pre_tail_->val;
    }

    // extend the MVCCList
    pre_tail_ = tail_;
    tail_ = new_version;

    return &new_version->val;
}

// Loading data from HDFS, do not need to lock the list
template<class Item>
decltype(Item::val)* MVCCList<Item>::AppendInitialVersion() {
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

    // More than one version in MVCCList
    if (tail_ != pre_tail_) {
        pre_tail_->next = nullptr;

        tail_ = pre_tail_;
        pre_tail_ = tmp_pre_tail_;
    } else {  // Only one version in MVCCList
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
