/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#include "core/RCT.hpp"

void RCTable::insert_trx(uint64_t ct, uint64_t trx_id) {
    CHECK(IS_VALID_TRX_ID(trx_id));
    WriterLockGuard lock_guard(lock_);
    rct_map_.insert(std::make_pair(ct, trx_id));
}

void RCTable::query_trx(uint64_t bt, uint64_t ct, std::set<uint64_t>& trx_ids) const {   
    CHECK_EQ(trx_ids.size(), 0) << "[RCTable] trx_ids should be empty";
    if (ct <= bt)
        return;
    ReaderLockGuard lock_guard(lock_);

    auto lower = rct_map_.lower_bound(bt);
    auto upper = rct_map_.upper_bound(ct);

    while (lower != upper) {
        trx_ids.insert(lower->second);
        lower++;
    }
}

void RCTable::erase_trxs(uint64_t min_bt) {
    WriterLockGuard lock_guard(lock_);

    if (min_bt == 0)
        return;

    auto lower = rct_map_.lower_bound(0);
    auto upper = rct_map_.upper_bound(min_bt - 1);

    // dbg
    auto lower_dbg = rct_map_.lower_bound(0);
    while (lower_dbg != upper) {
        printf("[RCTable::erase_trxs], erased %lu %lu\n", lower_dbg->first, lower_dbg->second);
        lower_dbg++;
    }

    rct_map_.erase(lower, upper);
}
