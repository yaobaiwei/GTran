/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef ACTOR_COMMIT_ACTOR_HPP_
#define ACTOR_COMMIT_ACTOR_HPP_

#include <unistd.h>

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "base/type.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/exec_plan.hpp"
#include "core/message.hpp"
#include "core/factory.hpp"
#include "core/index_store.hpp"
#include "layout/pmt_rct_table.hpp"
#include "layout/data_storage.hpp"
#include "utils/tool.hpp"
#include "utils/mymath.hpp"

#include "glog/logging.h"

class CommitActor : public AbstractActor {
 public:
    CommitActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity,
            map<ACTOR_T, unique_ptr<AbstractActor>> * actors,
            tbb::concurrent_hash_map<uint64_t, QueryPlan> * msg_logic_table) :
        AbstractActor(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        actors_(actors),
        msg_logic_table_(msg_logic_table),
        type_(ACTOR_T::COMMIT) {
        config_ = Config::GetInstance();
        prepare_clean_actor_set();
    }

    void process(const QueryPlan & qplan, Message & msg);

 private:
    // number of thread
    int num_thread_;

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config * config_;

    // Actor Set
    set<ACTOR_T> need_clean_actor_set_;

    // Trx-QueryPlan-map
    typedef tbb::concurrent_hash_map<uint64_t, QueryPlan> trx_actors_hashmap;
    trx_actors_hashmap* msg_logic_table_;

    // Actor Pointer
    map<ACTOR_T, unique_ptr<AbstractActor>>* actors_;

    void prepare_clean_actor_set();
};

#endif  // ACTOR_COMMIT_ACTOR_HPP_
