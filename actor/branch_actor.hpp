/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#pragma once

#include <vector>
#include "actor/abstract_actor.hpp"

// Branch Actor
// Copy incoming data to sub branches
class BranchActor : public AbstractActor{
 public:
    BranchActor(int id,
            int num_thread,
            AbstractMailbox* mailbox,
            CoreAffinity* core_affinity) :
        AbstractActor(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox) {}

    void process(const QueryPlan & qplan,  Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        if (msg.meta.msg_type == MSG_T::SPAWN) {
            vector<int> step_vec;
            get_steps(qplan.actors[msg.meta.step], step_vec);

            vector<Message> msg_vec;
            msg.CreateBranchedMsg(qplan.actors, step_vec, num_thread_, core_affinity_, msg_vec);

            for (auto& m : msg_vec) {
                mailbox_->Send(tid, m);
            }
        } else {
            cout << "Unexpected msg type in branch actor." << endl;
            exit(-1);
        }
    }

 private:
    int num_thread_;
    AbstractMailbox* mailbox_;

    void get_steps(const Actor_Object & actor, vector<int>& steps) {
        vector<value_t> params = actor.params;
        CHECK(params.size() >= 1);
        for (int i = 0; i < params.size(); i++) {
            steps.push_back(Tool::value_t2int(params[i]));
        }
    }
};
