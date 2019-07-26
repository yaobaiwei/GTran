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
