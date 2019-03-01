/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huan (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <vector>
#include "actor/abstract_actor.hpp"

// Branch Actor
// Copy incoming data to sub branches
class RepeatActor : public AbstractActor {
 public:
    RepeatActor(int id,
            DataStore* data_store,
            int num_thread,
            AbstractMailbox* mailbox,
            CoreAffinity* core_affinity) :
        AbstractActor(id, data_store, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox) {}

    void process(const vector<Actor_Object> & actors,  Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        if (msg.meta.msg_type == MSG_T::SPAWN) {
            vector<int> step_vec;
            get_steps(actors[msg.meta.step], step_vec);

            vector<Message> msg_vec;
            msg.CreateBranchedMsg(actors, step_vec, num_thread_, data_store_, core_affinity_, msg_vec);

            for (auto& m : msg_vec) {
                mailbox_->Send(tid, m);
            }
        } else {
            cout << "Unexpected msg type in repeat actor." << endl;
            exit(-1);
        }
    }

 private:
    int num_thread_;
    AbstractMailbox* mailbox_;

    void get_steps(const Actor_Object & actor, vector<int>& steps) {
        vector<value_t> params = actor.params;
        assert(params.size() >= 1);
        for (int i = 0; i < params.size(); i++) {
            steps.push_back(Tool::value_t2int(params[i]));
        }
    }
};
