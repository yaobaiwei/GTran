// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
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

#pragma once

#include <inttypes.h>
#include <stdint.h>
#include <iostream>
#include <set>
#include <map>
#include "bplustreelib/bplustree.hpp"
#include "core/common.hpp"
#include "glog/logging.h"
#include "utils/write_prior_rwlock.hpp"

class GCProducer;
class GCConsumer;

class RCTable {
 private:
    std::map<uint64_t, uint64_t> rct_map_;

    mutable WritePriorRWLock lock_;

    RCTable() {}
    RCTable(const RCTable&);  // not to def
    RCTable& operator=(const RCTable&);  // not to def
    ~RCTable() {}

 public:
    static RCTable* GetInstance() {
        static RCTable instance;
        return &instance;
    }

    void insert_trx(uint64_t ct, uint64_t trx_id);

    void query_trx(uint64_t bt, uint64_t ct, std::vector<uint64_t>& trx_ids) const;

    // Erase all transactions with CT < min-bt
    void erase_trxs(uint64_t min_bt);

    friend class GCProducer;
    friend class GCConsumer;
};
