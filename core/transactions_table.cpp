/* Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#include "core/transactions_table.hpp"

Transactions* Transactions::t_table_ = nullptr;

Transactions::Transactions() {
    config_ = Config::GetInstance();

    // release version
    buffer_ = config_ -> trx_table;
    buffer_sz_ = config_ -> trx_table_sz;
    num_total_buckets_ = config_ -> num_total_buckets;
    num_main_buckets_ = config_ -> num_main_buckets;
    num_indirect_buckets_ = config_ -> num_indirect_buckets;
    num_slots_ = config_ -> num_slots;

    // for test
    // {
    //     buffer_sz_ = 67108864;
    //     buffer_ = new char[buffer_sz_];
    //     memset(buffer_, 0, buffer_sz_);
    //     num_total_buckets_ = 1048576;
    //     num_main_buckets_ = 838860;
    //     num_indirect_buckets_ = 209716;
    //     num_slots_ = 8388608;
    // }

    table_ = (TidStatus*) buffer_;
    last_ext_ = 0;
    next_time_ = 1;
    next_trx_id_ = next_time_;
}

Transactions* Transactions::GetInstance() {
    if (t_table_ == nullptr)
        t_table_ = new Transactions();
    return t_table_;
}

bool Transactions::query_bt(uint64_t trx_id, uint64_t& bt) {
    // CHECK(bt.find(trx_id) != bt.endl())
    CHECK(is_valid_trx_id(trx_id));

    btct_table_const_accessor ac;
    btct_table_.find(ac, trx_id);
    bt = (ac->second).first;
    return true;
}

bool Transactions::query_ct(uint64_t trx_id, uint64_t& ct) {
    // CHECK(bt.find(trx_id) != bt.endl())
    CHECK(is_valid_trx_id(trx_id));
    btct_table_const_accessor ac;
    btct_table_.find(ac, trx_id);
    ct = (ac->second).second;
    return true;
}

bool Transactions::insert_single_trx(uint64_t& trx_id, uint64_t& bt) {
    // trx_id is inner representation now
    if (!allocate_trx_id(trx_id))
        return false;

    if (!allocate_bt(bt))
        return false;

    // insert into btct_table
    if (!register_bt(trx_id, bt))
        return false;

    uint64_t bucket_id = trx_id % num_main_buckets_;

    printf("[Trx Table] bucket %d found\n", bucket_id);
    uint64_t slot_id = bucket_id * ASSOCIATIVITY_;  // used as cursor to traverse the target bucket

    while (slot_id < num_slots_) {
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
        if (last_ext_ >= num_indirect_buckets_) {
            cerr << "Transaction Status Table Error: out of indirect-header region." << endl;
            CHECK(false);
        }
        table_[slot_id].trx_id = num_main_buckets_ + last_ext_;
        ++last_ext_;
        slot_id = table_[slot_id].trx_id * ASSOCIATIVITY_;
        table_[slot_id].enterProcessState(trx_id);
    }

done:
    CHECK(slot_id < num_slots_) << "slot is not enough";
    CHECK(table_[slot_id].trx_id == trx_id);

    return true;
}

bool Transactions::modify_status(uint64_t trx_id, TRX_STAT new_status) {
    CHECK(is_valid_trx_id(trx_id));

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

bool Transactions::modify_status(uint64_t trx_id, TRX_STAT new_status, uint64_t& ct) {
    CHECK(is_valid_trx_id(trx_id));

    allocate_ct(ct);
    if (!register_ct(trx_id, ct)) {
        return false;
    }

    return modify_status(trx_id, new_status);
}

bool Transactions::print_single_item(uint64_t trx_id) {
    CHECK(is_valid_trx_id(trx_id));

    TidStatus * p = nullptr;

    bool found = find_trx(trx_id, &p);

    uint64_t bt, ct;
    query_bt(trx_id, bt);
    query_ct(trx_id, ct);

    if (found) {
        CHECK(p != nullptr);
        printf("[TRX Table] print_single_item: trx_id=%llx; P=%d; V=%d; C=%d; A=%d; occupied=%d; bt=%ld; ct=%ld\n", (long long)trx_id, p->P, p->V, p->C, p->A, p->occupied, bt, ct);
    } else {
        printf("[TRX Table] Not Found");
        return false;
    }
}

bool Transactions::delete_single_item(uint64_t trx_id) {
    CHECK(is_valid_trx_id(trx_id));
    bool op_succ_1 = deregister_btct(trx_id);

    TidStatus * p = nullptr;
    bool found = find_trx(trx_id, &p);

    if (found) {
        CHECK(p != nullptr);
        p -> setEmpty();
        DLOG(INFO) << "[TRX] delete_single_item: " << p->DebugString() << endl;
        return true;
    } else {
        return false;
    }
}

// private functions part:

uint64_t Transactions::next_trx_id() {
    return 0x8000000000000000 | next_trx_id_;
}

bool Transactions::allocate_trx_id(uint64_t& trx_id) {
    // no need to check, since you need to write this trx_id variable in this function

    trx_id = next_trx_id();
    next_trx_id_ ++;
    printf("[Trx Table] allocated trx_id = %llx\n", (long long) trx_id);
    // DLOG(INFO) << "[Trx Table] allocated trx_id = " << (int)trx_id << std::endl;
    return true;
}

bool Transactions::allocate_bt(uint64_t& bt) {
    bt = next_time_ ++;
    return true;
}

/* delete with check existance */
bool Transactions::allocate_ct(uint64_t& ct) {
    ct = next_time_ ++;
    return true;
}

bool Transactions::find_trx(uint64_t trx_id, TidStatus** p) {
    CHECK(is_valid_trx_id(trx_id));

    uint64_t bucket_id = trx_id % num_main_buckets_;
    // printf("[TRX] find_trx bucket_id %d, table_ = %llx\n, trx_id = %llx", bucket_id, table_, trx_id);
    while (true) {
        for (int i = 0; i < ASSOCIATIVITY_; ++i) {
            uint64_t slot_id = bucket_id * ASSOCIATIVITY_ + i;

            if (i < ASSOCIATIVITY_ - 1) {
                // printf("[TRX Table] find_trx: trx_id=%llx; P=%d; V=%d; C=%d; A=%d; occupied=%d\n", (long long)(table_[slot_id].trx_id), table_[slot_id].P, table_[slot_id].V, table_[slot_id].C, table_[slot_id].A, table_[slot_id].occupied);

                if (table_[slot_id].trx_id == trx_id) {  // found it
                    *p = table_ + slot_id;
                    return true;
                }
            } else {
                if (table_[slot_id].isEmpty()) {
                    return false;  // operation failed because not found
                } else {
                    bucket_id = table_[slot_id].trx_id;
                    break;  // break for loop
                }
            }
        }
    }

    return false;
}

bool Transactions::register_ct(uint64_t trx_id, uint64_t ct) {
    CHECK(is_valid_trx_id(trx_id) && is_valid_time(ct));

    btct_table_accessor ac;
    btct_table_.insert(ac, trx_id);
    ac->second.second = ct;
    return true;
}

bool Transactions::deregister_btct(uint64_t trx_id) {
    CHECK(is_valid_trx_id(trx_id));

    btct_table_accessor ac;
    btct_table_.find(ac, trx_id);
    btct_table_.erase(ac);
    return true;
}

bool Transactions::register_bt(uint64_t trx_id, uint64_t bt) {
    CHECK(is_valid_trx_id(trx_id) && is_valid_time(bt));

    btct_table_accessor ac;
    btct_table_.insert(ac, trx_id);
    ac->second.first = bt;
    return true;
}

