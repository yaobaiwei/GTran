/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#include "core/transactions_table.hpp"

TrxGlobalCoordinator* TrxGlobalCoordinator::trx_coordinator = nullptr;

TrxGlobalCoordinator::TrxGlobalCoordinator() {
    next_time_ = 1;
}

TrxGlobalCoordinator* TrxGlobalCoordinator::GetInstance() {
    if (trx_coordinator == nullptr)
        trx_coordinator = new TrxGlobalCoordinator();
    return trx_coordinator;
}

bool TrxGlobalCoordinator::allocate_bt(uint64_t& bt) {
    bt = next_time_++;
    return true;
}

/* delete with check existance */
bool TrxGlobalCoordinator::allocate_ct(uint64_t& ct) {
    ct = next_time_++;
    return true;
}

TransactionTable::TransactionTable() {
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

bool TransactionTable::query_bt(uint64_t trx_id, uint64_t& bt) {
    // CHECK(bt.find(trx_id) != bt.endl())
    CHECK(IS_VALID_TRX_ID(trx_id));

    bt_table_const_accessor ac;
    bt_table_.find(ac, trx_id);
    bt = ac->second;
    return true;
}

bool TransactionTable::query_ct(uint64_t trx_id, uint64_t& ct) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    TidStatus * p = nullptr;
    if (find_trx(trx_id, &p)) {
        ct = p->ct;
        return true;
    }
    return false;
}

bool TransactionTable::query_status(uint64_t trx_id, TRX_STAT & status) {
    CHECK(IS_VALID_TRX_ID(trx_id));
    TidStatus * p = nullptr;
    bool found = find_trx(trx_id, &p);
    if (!found) {
        return false;
    } else {
        status = p -> getState();
        return true;
    }
}


bool TransactionTable::register_bt(uint64_t trx_id, uint64_t bt) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    bt_table_accessor ac;
    bt_table_.insert(ac, trx_id);
    ac->second = bt;
    return true;
}

bool TransactionTable::deregister_bt(uint64_t trx_id) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    bt_table_accessor ac;
    bt_table_.find(ac, trx_id);
    bt_table_.erase(ac);
    return true;
}

bool TransactionTable::register_ct(uint64_t trx_id, uint64_t ct) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    TidStatus * p;
    if (find_trx(trx_id, &p)) {
        p->enterCommitTime(ct);
        return true;
    }
    return false;
}

bool TransactionTable::find_trx(uint64_t trx_id, TidStatus** p) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    uint64_t bucket_id = trx_id % trx_num_main_buckets_;

    while (true) {
        for (int i = 0; i < ASSOCIATIVITY_; ++i) {
            uint64_t slot_id = bucket_id * ASSOCIATIVITY_ + i;

            if (i < ASSOCIATIVITY_ - 1) {
                if (table_[slot_id].trx_id == trx_id) {  // found it
                    *p = table_ + slot_id;
                    return true;
                }
            } else {
                if (table_[slot_id].isEmpty()) {
                    CHECK(false);
                    return false;  // not found
                } else {
                    bucket_id = table_[slot_id].trx_id;
                    break;
                }
            }
        }
    }

    CHECK(false);
    return false;
}

bool TransactionTable::insert_single_trx(const uint64_t& trx_id, const uint64_t& bt) {
    // insert into btct_table
    if (!register_bt(trx_id, bt))
        return false;

    uint64_t bucket_id = trx_id % trx_num_main_buckets_;

    printf("[Trx Table] bucket %d found\n", bucket_id);
    uint64_t slot_id = bucket_id * ASSOCIATIVITY_;  // used as cursor to traverse the target bucket

    while (slot_id < trx_num_slots_) {
        for (int i = 0; i < ASSOCIATIVITY_ -1; ++i, ++slot_id) {
            if (table_[slot_id].trx_id == trx_id) {
                // already exists
                cerr << "Transaction Status Table Error: already exists" << endl;
                CHECK(false);
            }

            if (table_[slot_id].isEmpty()) {
                printf("[Trx Table] slot %d found; offset is %d\n", slot_id, slot_id * sizeof(TidStatus) );
                table_[slot_id].enterProcessState(trx_id);
                printf("Inserted %s\n", table_[slot_id].DebugString().c_str());
                goto done;
            }
        }

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
        table_[slot_id].enterProcessState(trx_id);
    }

done:
    CHECK(slot_id < trx_num_slots_) << "slot is not enough";
    CHECK(table_[slot_id].trx_id == trx_id);

    return true;
}

bool TransactionTable::modify_status(uint64_t trx_id, TRX_STAT new_status, const uint64_t& ct, bool is_read_only) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    if (!register_ct(trx_id, ct))
        return false;

    return modify_status(trx_id, new_status);
}

bool TransactionTable::modify_status(uint64_t trx_id, TRX_STAT new_status) {
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

