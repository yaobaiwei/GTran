/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
 *          Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
 */

#include "core/transaction_status_table.hpp"

uint64_t TrxIDHash(uint64_t trx_id) {
    // Because the lowest QIB_BITS bits are 0.
    // To avoid frequent hash collision in TrxTable.
    return trx_id >> QID_BITS;
}

TransactionStatusTable::TransactionStatusTable() {
    config_ = Config::GetInstance();

    // release version
    buffer_ = config_ -> trx_table;
    buffer_sz_ = config_ -> trx_table_sz;
    trx_num_total_buckets_ = config_ -> trx_num_total_buckets;
    trx_num_main_buckets_ = config_ -> trx_num_main_buckets;
    trx_num_indirect_buckets_ = config_ -> trx_num_indirect_buckets;
    trx_num_slots_ = config_ -> trx_num_slots;

    table_ = (TidStatus*) buffer_;
    last_ext_ = 0;
}

bool TransactionStatusTable::query_ct(uint64_t trx_id, uint64_t& ct) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    TidStatus * p = nullptr;
    if (find_trx(trx_id, &p)) {
        ct = p->ct;
        return true;
    }
    return false;
}

bool TransactionStatusTable::query_status(uint64_t trx_id, TRX_STAT & status) {
    CHECK(IS_VALID_TRX_ID(trx_id));
    TidStatus * p = nullptr;

    if (find_trx(trx_id, &p)) {
        status = p -> getState();
        return true;
    }
    return false;
}

bool TransactionStatusTable::register_ct(uint64_t trx_id, uint64_t ct) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    TidStatus * p;
    if (find_trx(trx_id, &p)) {
        p->enterCommitTime(ct);
        return true;
    }
    return false;
}

bool TransactionStatusTable::find_trx(uint64_t trx_id, TidStatus** p) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    uint64_t bucket_id = TrxIDHash(trx_id) % trx_num_main_buckets_;

    while (true) {
        for (int i = 0; i < ASSOCIATIVITY_; ++i) {
            uint64_t slot_id = bucket_id * ASSOCIATIVITY_ + i;

            if (i < ASSOCIATIVITY_ - 1) {
                if (table_[slot_id].trx_id == trx_id) {  // found it
                    *p = table_ + slot_id;
                    return true;
                }
            } else {
                if (table_[slot_id].trx_id == 0) {
                    return false;
                } else {
                    bucket_id = table_[slot_id].trx_id;
                    break;
                }
            }
        }
    }

    return false;
}

bool TransactionStatusTable::insert_single_trx(const uint64_t& trx_id, const uint64_t& bt, const bool& readonly) {
    // insert into btct_table
    uint64_t bucket_id = TrxIDHash(trx_id) % trx_num_main_buckets_;

    uint64_t slot_id = bucket_id * ASSOCIATIVITY_;  // used as cursor to traverse the target bucket

    while (slot_id < trx_num_slots_) {
        for (int i = 0; i < ASSOCIATIVITY_ -1; ++i, ++slot_id) {
            if (table_[slot_id].trx_id == trx_id) {
                // already exists
                CHECK(false) << "Transaction Status Table Error: already exists";
            }

            if (table_[slot_id].isEmpty() || table_[slot_id].isErased()) {
                table_[slot_id].enterProcessState(trx_id);

                // For non-readonly transactions, record them to GC list when they are finished.
                // in here, we only code for ro case:
                if (readonly)
                    record_ro_trx_with_bt(table_ + slot_id, bt);

                // printf("[Trx Table] insert_single_trx, slot %d found; offset is %d, %s\n", slot_id, slot_id * sizeof(TidStatus), table_[slot_id].DebugString().c_str());
                goto done;
            }
        }

        // whether the bucket_ext (indirect-header region) is used
        if (!table_[slot_id].trx_id == 0) {
            slot_id = table_[slot_id].trx_id * ASSOCIATIVITY_;
            continue;
        }

        // allocate a new bucket
        if (last_ext_ >= trx_num_indirect_buckets_) {
            cerr << "Transaction Status Table Error: out of indirect-header region." << endl;
            CHECK(false);
        }
        table_[slot_id].trx_id = trx_num_main_buckets_ + last_ext_;
        ++last_ext_;
        slot_id = table_[slot_id].trx_id * ASSOCIATIVITY_;
        // printf("[Trx Table] insert_single_trx, slot %d found; offset is %d, %s\n", slot_id, slot_id * sizeof(TidStatus), table_[slot_id].DebugString().c_str());
        table_[slot_id].enterProcessState(trx_id);
        goto done;
    }

done:
    CHECK(slot_id < trx_num_slots_) << "slot is not enough";
    CHECK(table_[slot_id].trx_id == trx_id);

    return true;
}

bool TransactionStatusTable::modify_status(uint64_t trx_id, TRX_STAT new_status, const uint64_t& ct) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    if (!register_ct(trx_id, ct))
        return false;

    return modify_status(trx_id, new_status);
}

bool TransactionStatusTable::modify_status(uint64_t trx_id, TRX_STAT new_status) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    TRX_STAT old_status;

    TidStatus * p = nullptr;
    bool found = find_trx(trx_id, &p);

    if (found) {
        CHECK(p != nullptr);

        old_status = p->getState();

        switch (new_status) {
            case TRX_STAT::VALIDATING:
                p -> enterValidationState();
                DLOG(INFO) << "[TRX] modify status: " << p->DebugString() << endl;
                return true;
            case TRX_STAT::ABORT:
                p -> enterAbortState();
                DLOG(INFO) << "[TRX] modify status: " << p->DebugString() << endl;
                return true;
            case TRX_STAT::COMMITTED:
                p -> enterCommitState();
                DLOG(INFO) << "[TRX] modify status: " << p->DebugString() << endl;
                return true;
            default:
                cerr << "Transaction Status Table Error: modify_status" << endl;
                CHECK(false);
                break;
        }

    } else {
        return false;
    }

    return false;
}

// called by GC thread
void TransactionStatusTable::erase_trx_via_min_bt(uint64_t global_min_bt, vector<uint64_t> *non_readonly_trx_ids) {
    TsPtrNode *head_snapshot, *tail_snapshot;

    // erase readonly trxs
    head_snapshot = ro_trxs_head_;
    tail_snapshot = ro_trxs_tail_;
    if (head_snapshot != nullptr && tail_snapshot != nullptr &&
        head_snapshot != tail_snapshot && head_snapshot->next != tail_snapshot) {
        // Lock-free. Only perform erasure when len(list) > 2
        ro_trxs_head_ = erase(head_snapshot, tail_snapshot, global_min_bt);
    }

    // erase non-readonly trxs
    head_snapshot = nro_trxs_head_;
    tail_snapshot = nro_trxs_tail_;
    if (head_snapshot != nullptr && tail_snapshot != nullptr &&
        head_snapshot != tail_snapshot && head_snapshot->next != tail_snapshot) {
        nro_trxs_head_ = erase(head_snapshot, tail_snapshot, global_min_bt, non_readonly_trx_ids);
    }
}

void TransactionStatusTable::record_ro_trx_with_bt(TidStatus* ptr, uint64_t bt) {
    TsPtrNode* new_tail = new TsPtrNode;
    new_tail->ts = bt;
    new_tail->ptr = ptr;
    // Record pair<BeginTime, TidStatus*>.

    if (ro_trxs_tail_ == nullptr) {
        // The list is empty
        ro_trxs_head_ = ro_trxs_tail_ = new_tail;
    } else {
        // Append to the tail
        ro_trxs_tail_->next = new_tail;
        ro_trxs_tail_ = new_tail;
    }
}

void TransactionStatusTable::record_nro_trx_with_et(uint64_t trx_id, uint64_t endtime) {
    TsPtrNode* new_tail = new TsPtrNode;
    new_tail->ts = endtime;
    find_trx(trx_id, &new_tail->ptr);
    // Record pair<FinishTime, TidStatus*>.

    if (nro_trxs_tail_ == nullptr) {
        // The list is empty
        nro_trxs_head_ = nro_trxs_tail_ = new_tail;
    } else {
        // Append to the tail
        nro_trxs_tail_->next = new_tail;
        nro_trxs_tail_ = new_tail;
    }
}

//This function will be called by erase_trx_via_min_bt()
TsPtrNode* TransactionStatusTable::erase(TsPtrNode* head, TsPtrNode* tail, uint64_t global_min_bt, vector<uint64_t> *non_readonly_trx_ids) {
    /*  TsPtrNode:
            ts: the erasable timestamp threshold
            ptr: pointer to a slot in the table_
            next: pointer to the next TsPtrNode

        From the parameters, a linked-list of TsPtrNode (with timestamp in ascending order) is given (head -> ... -> tail).
        When non_readonly_trx_ids == nullptr, this is a linked-list of readonly transactions.
    */

    while (true) {
        // To ensure the safety of lockless implementation, when len(list) <= 2, do not perform erasure.
        if (head->next == tail)
            break;
        // Only perform erasure if the trx_id in the slot will not requested anymore (TsPtrNode::ts < global_min_bt)
        if (head->ts >= global_min_bt)
            break;

        // record erased trx id if needed
        if (non_readonly_trx_ids != nullptr)
            non_readonly_trx_ids->push_back(head->ptr->trx_id);

        // Mark the slot as erased, therefore the slot can be reused.
        head->ptr->markErased();

        // shorten the linked-list after erasure
        TsPtrNode* ori_head = head;
        head = head->next;
        delete ori_head;
    }
    return head;
}
