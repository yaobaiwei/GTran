/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#include "core/RCT.hpp"

RCTable * RCTable::rct_ = nullptr;

RCTable::RCTable() {
     /* Init b+tree_ */
    bpt_config_.order = 7;
    bpt_config_.entries = 10;

    tree_ = bplus_tree_init(bpt_config_.order, bpt_config_.entries);
    pthread_spin_init(&lock_, 0);

    if (tree_ == nullptr) {
        std::cerr << "RCT Error: Init Failure!" << std::endl;
        CHECK(false);
    }
}

// outer trx id => inner representation
long long RCTable::trxid2llint(uint64_t trx_id) { // to long long
    CHECK(IS_VALID_TRX_ID(trx_id)) << "[RCTable] trxid2int: invalid trx_id";  // make sure will not lose infomation due to being capped
    return ((trx_id << 1) >> 1);
}

// inner representation => outer trx id
uint64_t RCTable::llint2trxid(long long trx_id) {
    CHECK_GE(trx_id, 0) << "[RCTable] trx_id should >= 0";
    return trx_id | 0x8000000000000000;
}

bool RCTable::insert_trx(uint64_t ct, uint64_t trx_id) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    long long inner_trx_id = trxid2llint(trx_id);
    
    DLOG(INFO) << "[RCT] insert_trx: " << ct << " trx_id " << inner_trx_id << " inserted" << endl;

    SimpleSpinLockGuard lock_guard(&lock_);
    bplus_tree_put(tree_, ct, inner_trx_id);
    return true;
}

bool RCTable::query_trx(uint64_t bt, uint64_t ct, std::set<uint64_t>& trx_ids) {
    CHECK_EQ(trx_ids.size(), 0) << "[RCTable] trx_ids should be empty";
    
    std::set<long long> values;
    pthread_spin_lock(&lock_);
    bplus_tree_get_range(tree_, bt, ct, values);
    pthread_spin_unlock(&lock_);
    for(auto it = values.begin(); it!=values.end(); ++ it){
        trx_ids.insert(llint2trxid(*it));
    }

    return true;
}

bool RCTable::delete_transaction(uint64_t ct) {
    bplus_tree_put(tree_, ct, 0);
    return true;
}

