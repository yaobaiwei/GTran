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

#ifndef EXPERT_TERMINATE_EXPERT_HPP_
#define EXPERT_TERMINATE_EXPERT_HPP_

#include <unistd.h>

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/type.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/exec_plan.hpp"
#include "core/message.hpp"
#include "core/factory.hpp"
#include "expert/abstract_expert.hpp"
#include "layout/pmt_rct_table.hpp"
#include "layout/index_store.hpp"
#include "layout/data_storage.hpp"
#include "utils/tool.hpp"
#include "utils/mymath.hpp"

#include "glog/logging.h"

class TerminateExpert : public AbstractExpert {
 public:
    TerminateExpert(int id,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity,
            map<EXPERT_T, unique_ptr<AbstractExpert>> * experts,
            tbb::concurrent_hash_map<uint64_t, QueryPlan> * msg_logic_table) :
        AbstractExpert(id, core_affinity),
        mailbox_(mailbox),
        experts_(experts),
        msg_logic_table_(msg_logic_table),
        type_(EXPERT_T::TERMINATE) {
        config_ = Config::GetInstance();
        index_store_ = IndexStore::GetInstance();
        prepare_clean_expert_set();
    }

    void process(const QueryPlan & qplan, Message & msg);

 private:
    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config * config_;

    // Index Store
    IndexStore * index_store_;

    // Expert Set
    set<EXPERT_T> need_clean_expert_set_;

    // Trx-QueryPlan-map
    typedef tbb::concurrent_hash_map<uint64_t, QueryPlan> trx_experts_hashmap;
    trx_experts_hashmap* msg_logic_table_;

    // Expert Pointer
    map<EXPERT_T, unique_ptr<AbstractExpert>>* experts_;

    void prepare_clean_expert_set();
};

#endif  // EXPERT_TERMINATE_EXPERT_HPP_
