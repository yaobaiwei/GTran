/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/
#ifndef ACTOR_STATUS_ACTOR_HPP_
#define ACTOR_STATUS_ACTOR_HPP_

#include <string>
#include <vector>
#include <algorithm>

#include "actor/abstract_actor.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "utils/config.hpp"
#include "utils/tool.hpp"

class StatusActor : public AbstractActor {
 public:
    StatusActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        AbstractActor(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(ACTOR_T::CONFIG) {
        config_ = Config::GetInstance();
    }

    void process(const QueryPlan & qplan, Message & msg);

 private:
    // Number of Threads
    int num_thread_;

    // Actor type
    ACTOR_T type_;

    // Config
    Config * config_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
};

#endif  // ACTOR_STATUS_ACTOR_HPP_
