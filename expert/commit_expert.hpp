/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef EXPERT_COMMIT_EXPERT_HPP_
#define EXPERT_COMMIT_EXPERT_HPP_

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

class CommitExpert : public AbstractExpert {
 public:
    CommitExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity,
            map<EXPERT_T, unique_ptr<AbstractExpert>> * experts,
            tbb::concurrent_hash_map<uint64_t, QueryPlan> * msg_logic_table) :
        AbstractExpert(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        experts_(experts),
        msg_logic_table_(msg_logic_table),
        type_(EXPERT_T::COMMIT) {
        config_ = Config::GetInstance();
        index_store_ = IndexStore::GetInstance();
        prepare_clean_expert_set();
    }

    void process(const QueryPlan & qplan, Message & msg);

 private:
    // number of thread
    int num_thread_;

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

#endif  // EXPERT_COMMIT_EXPERT_HPP_
