// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "core/RCT.hpp"

void RCTable::insert_trx(uint64_t ct, uint64_t trx_id) {
    CHECK(IS_VALID_TRX_ID(trx_id));
    WriterLockGuard lock_guard(lock_);
    rct_map_.insert(std::make_pair(ct, trx_id));
}

void RCTable::query_trx(uint64_t bt, uint64_t ct, std::vector<uint64_t>& trx_ids) const {
    CHECK_EQ(trx_ids.size(), 0) << "[RCTable] trx_ids should be empty";
    if (ct <= bt)
        return;
    ReaderLockGuard lock_guard(lock_);

    auto lower = rct_map_.lower_bound(bt);
    auto upper = rct_map_.upper_bound(ct);

    while (lower != upper) {
        trx_ids.emplace_back(lower->second);
        lower++;
    }
}

void RCTable::erase_trxs(uint64_t min_bt) {
    WriterLockGuard lock_guard(lock_);

    if (min_bt == 0)
        return;

    auto lower = rct_map_.lower_bound(0);
    auto upper = rct_map_.upper_bound(min_bt - 1);

    rct_map_.erase(lower, upper);
}
