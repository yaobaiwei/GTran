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

#ifndef EXPERT_AS_EXPERT_HPP_
#define EXPERT_AS_EXPERT_HPP_

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "expert/abstract_expert.hpp"
#include "utils/tool.hpp"

class AsExpert : public AbstractExpert {
 public:
    AsExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        AbstractExpert(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(EXPERT_T::AS) {}

    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = qplan.experts[m.step];

        // Get Params
        int label_step_key = Tool::value_t2int(expert_obj.params.at(0));

        // record history_t
        RecordHistory(label_step_key, msg.data);

        // Create Message
        vector<Message> msg_vec;
        msg.CreateNextMsg(qplan.experts, msg.data, num_thread_, core_affinity_, msg_vec);

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
     }

 private:
    // Number of Threads
    int num_thread_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // write into side effect
    void RecordHistory(int label_step_key, vector<pair<history_t, vector<value_t>>> & data) {
        vector<pair<history_t, vector<value_t>>> newData;
        map<value_t, int> value_pos;
        int cnt;

        for (auto & data_pair : data) {
            value_pos.clear();
            cnt = 0;
            for (auto & elem : data_pair.second) {
                history_t his;
                vector<value_t> newValue;

                // Check whether elem is already in newData
                map<value_t, int>::iterator itr = value_pos.find(elem);
                if (itr == value_pos.end()) {
                    // move previous history to new one
                    his = data_pair.first;

                    // push back current label key
                    his.emplace_back(label_step_key, elem);

                    newValue.push_back(elem);
                    newData.emplace_back(his, newValue);

                    // Insert into map
                    value_pos.insert(pair<value_t, int>(elem, cnt++));
                } else {
                    // append elem to certain history
                    int pos = value_pos.at(elem);

                    if (pos >= value_pos.size()) {
                        cout << "Position larger than size : " << pos << " & " << value_pos.size() << endl;
                    } else {
                        newData.at(pos).second.push_back(elem);
                    }
                }
            }
        }
        data.swap(newData);
    }
};

#endif  // EXPERT_AS_EXPERT_HPP_
