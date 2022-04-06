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
