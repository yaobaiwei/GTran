/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/
#ifndef EXPERT_STATUS_EXPERT_HPP_
#define EXPERT_STATUS_EXPERT_HPP_

#include <string>
#include <vector>
#include <algorithm>

#include "base/type.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "expert/abstract_expert.hpp"
#include "utils/config.hpp"
#include "utils/tool.hpp"

class StatusExpert : public AbstractExpert {
 public:
    StatusExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        AbstractExpert(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(EXPERT_T::CONFIG) {
        config_ = Config::GetInstance();
    }

    void process(const QueryPlan & qplan, Message & msg);

 private:
    // Number of Threads
    int num_thread_;

    // Expert type
    EXPERT_T type_;

    // Config
    Config * config_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
};

#endif  // EXPERT_STATUS_EXPERT_HPP_
