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

// Only for serializable isolation level
// Retuen value:
//  first: false if abort
//  second: false if do not read the uncommitted tail
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

    if (!config_->global_enable_opt_preread) { return make_pair(false, false); }

    uint64_t tail_trx_id = tail_->GetTransactionID();
    CHECK(tail_trx_id != 0);

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

    // Ignore
    return make_pair(true, false);
}

// Retuen value:
//  first: false if abort
//  second: false if no version visible
template<class Item>
pair<bool, bool> MVCCList<Item>::GetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time,
                                                   const bool& read_only, ValueType& ret) {
    if (config_->isolation_level == ISOLATION_LEVEL::SERIALIZABLE)
        return SerializableLevelGetVisibleVersion(trx_id, begin_time, read_only, ret);
    else
        return make_pair(true, SnapshotLevelGetVisibleVersion(trx_id, begin_time, ret));
}

// Retuen value:
//  first: false if abort
//  second: false if no version visible
template<class Item>
pair<bool, bool> MVCCList<Item>::SerializableLevelGetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time,
                                                                    const bool& read_only, ValueType& ret) {
    SimpleSpinLockGuard lock_guard(&lock_);

    // The MVCCList is empty
    if (head_ == nullptr) {
        return make_pair(true, false);
    }

    // One special case: the uncommitted tail is created by the same transaction
    if (tail_->GetTransactionID() == trx_id) {
        ret = tail_->val;
        return make_pair(true, true);
    }

    // head_ == tail_, uncommited
    if (head_->GetTransactionID() != 0) {
        pair<bool, bool> preread_visible = TryPreReadUncommittedTail(trx_id, begin_time, read_only);

        // Abort
        if (!preread_visible.first)
            return make_pair(false, false);

        if (preread_visible.second)
            ret = head_->val;

        return preread_visible;
    }

    // Head is committed, and its begin_time > trx.begin_time
    if (head_->GetBeginTime() > begin_time) {
        if (read_only)
            return make_pair(true, false);
        else
            return make_pair(false, false);
    }

    // locate a version that trx.begin_time is within [version.begin_time, version.end_time)
    Item* version = head_;
    while (true) {
        // If visible, break
        if (begin_time < version->GetEndTime())
            break;

        CHECK(version != tail_);

        version = static_cast<Item*>(version->GetNext());
    }

    if (version->NextIsUncommitted()) {
        pair<bool, bool> preread_visible = TryPreReadUncommittedTail(trx_id, begin_time, read_only);
        if (!preread_visible.first)
            return make_pair(false, false);
        if (preread_visible.second) {
            ret = tail_->val;
            return make_pair(true, true);
        } else {
            ret = version->val;
            return make_pair(true, true);
        }
    }

    // Non-readonly, and visible_version->next is committed
    if (!read_only && version->GetNext() != nullptr)
        return make_pair(false, false);

    ret = version->val;
    return make_pair(true, true);
}


// Retuen value::  false if no version visible
template<class Item>
bool MVCCList<Item>::SnapshotLevelGetVisibleVersion(const uint64_t& trx_id, const uint64_t& begin_time,
                                                    ValueType& ret) {
    SimpleSpinLockGuard lock_guard(&lock_);

    // The MVCCList is empty
    if (head_ == nullptr) {
        return false;
    }

    // One special case: the uncommitted tail is created by the same transaction
    if (tail_->GetTransactionID() == trx_id) {
        ret = tail_->val;
        return true;
    }

    if (head_->GetTransactionID() != 0 || head_->GetBeginTime() > begin_time)
        return false;

    // locate a version that trx.begin_time is within [version.begin_time, version.end_time)
    Item* version = head_;
    while (true) {
        // If visible, break
        if (begin_time < version->GetEndTime())
            break;

        CHECK(version != tail_);

        version = static_cast<Item*>(version->GetNext());
    }

    ret = version->val;
    return true;
}

template<class Item>
decltype(Item::val)* MVCCList<Item>::AppendVersion(const uint64_t& trx_id, const uint64_t& begin_time,
                                                   decltype(Item::val)* old_val_header, bool* old_val_exists) {
    SimpleSpinLockGuard lock_guard(&lock_);

    if (head_ == nullptr) {
        Item* head_mvcc = mem_pool_->Get(TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));

        head_mvcc->Init(trx_id, begin_time);

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

    Item* new_version = mem_pool_->Get(TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));

    new_version->Init(trx_id, begin_time);

    // Append a new version
    tail_->AppendNextVersion(new_version);
    // Stores the original pre_tail, will be used in AbortVersion.
    tmp_pre_tail_ = pre_tail_;

    // Get Old Value for Index Update
    if (old_val_header != nullptr) {
        old_val_header[0] = tail_->val;
    }

    // extend the MVCCList
    pre_tail_ = tail_;
    tail_ = new_version;

    return &new_version->val;
}

// Loading data from HDFS, do not need to lock the list
template<class Item>
decltype(Item::val)* MVCCList<Item>::AppendInitialVersion() {
    Item* initial_mvcc = mem_pool_->Get(TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));

    initial_mvcc->Init(Item::MIN_TIME, Item::MAX_TIME);

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
    CHECK(tail_->GetTransactionID() == trx_id);

    tail_->Commit(pre_tail_, commit_time);
    tmp_pre_tail_ = nullptr;
}

// Remove the tail_ added during processing stage.
template<class Item>
void MVCCList<Item>::AbortVersion(const uint64_t& trx_id) {
    SimpleSpinLockGuard lock_guard(&lock_);
    CHECK(tail_->GetTransactionID() == trx_id);

    if (tail_->NeedGC())
        tail_->ValueGC();

    mem_pool_->Free(tail_, TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));

    // More than one version in MVCCList
    if (tail_ != pre_tail_) {
        pre_tail_->AbortNextVersion();

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
        Item* tmp_next = head_->GetNext();
        mem_pool_->Free(head_, TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));
        head_ = tmp_next;
    }

    tail_ = pre_tail_ = tmp_pre_tail_ = nullptr;
}
