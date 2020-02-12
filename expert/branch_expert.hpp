/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#pragma once

#include <vector>

#include "expert/abstract_expert.hpp"

// Branch Expert
// Copy incoming data to sub branches
class BranchExpert : public AbstractExpert{
 public:
    BranchExpert(int id,
            int num_thread,
            AbstractMailbox* mailbox,
            CoreAffinity* core_affinity) :
        AbstractExpert(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox) {}

    void process(const QueryPlan & qplan,  Message & msg) {
        int tid = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);

        if (msg.meta.msg_type == MSG_T::SPAWN) {
            vector<int> step_vec;
            get_steps(qplan.experts[msg.meta.step], step_vec);

            vector<Message> msg_vec;
            msg.CreateBranchedMsg(qplan.experts, step_vec, num_thread_, core_affinity_, msg_vec);

            for (auto& m : msg_vec) {
                mailbox_->Send(tid, m);
            }
        } else {
            cout << "Unexpected msg type in branch expert." << endl;
            exit(-1);
        }
    }

 private:
    int num_thread_;
    AbstractMailbox* mailbox_;

    void get_steps(const Expert_Object & expert, vector<int>& steps) {
        vector<value_t> params = expert.params;
        CHECK(params.size() >= 1);
        for (int i = 0; i < params.size(); i++) {
            steps.push_back(Tool::value_t2int(params[i]));
        }
    }
};
