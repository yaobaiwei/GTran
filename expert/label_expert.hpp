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

#ifndef EXPERT_LABEL_EXPERT_HPP_
#define EXPERT_LABEL_EXPERT_HPP_

#include <string>
#include <utility>
#include <vector>

#include "base/type.hpp"
#include "base/predicate.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "expert/abstract_expert.hpp"
#include "utils/tool.hpp"

class LabelExpert : public AbstractExpert {
 public:
    LabelExpert(int id,
            int machine_id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        AbstractExpert(id, core_affinity),
        machine_id_(machine_id),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(EXPERT_T::LABEL) {
        config_ = Config::GetInstance();
    }

    // Label:
    //         Output all labels of input
    // Parmas:
    //         inType
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = qplan.experts[m.step];

        // Get Params
        Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params.at(0));

        bool read_success = true;
        switch (inType) {
          case Element_T::VERTEX:
            read_success = VertexLabel(qplan, msg.data);
            break;
          case Element_T::EDGE:
            read_success = EdgeLabel(qplan, msg.data);
            break;
          default:
            cout << "Wrong in type"  << endl;
        }

        // Create Message
        vector<Message> msg_vec;
        if (read_success) {
            msg.CreateNextMsg(qplan.experts, msg.data, num_thread_, core_affinity_, msg_vec);
        } else {
            string abort_info = "Abort with [Processing][LabelExpert::process]";
            msg.CreateAbortMsg(qplan.experts, msg_vec, abort_info);
        }

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
     }

 private:
    // Number of Threads
    int num_thread_;
    int machine_id_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config* config_;

    bool VertexLabel(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> newData;
            for (auto & elem : data_pair.second) {
                vid_t v_id(Tool::value_t2int(elem));

                label_t label;
                READ_STAT read_status = data_storage_->GetVL(v_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, label);
                if (read_status == READ_STAT::ABORT) {
                    return false;
                } else if (read_status == READ_STAT::NOTFOUND) {
                    continue;
                }
                string keyStr;
                data_storage_->GetNameFromIndex(Index_T::V_LABEL, label, keyStr);

                value_t val;
                Tool::str2str(keyStr, val);
                newData.push_back(val);
            }

            data_pair.second.swap(newData);
        }
        return true;
    }

    bool EdgeLabel(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> newData;
            for (auto & elem : data_pair.second) {
                eid_t e_id;
                uint2eid_t(Tool::value_t2uint64_t(elem), e_id);

                label_t label;
                READ_STAT read_status = data_storage_->GetEL(e_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, label);
                if (read_status == READ_STAT::ABORT) {
                    return false;
                } else if (read_status == READ_STAT::NOTFOUND) {
                    continue;
                }
                string keyStr;
                data_storage_->GetNameFromIndex(Index_T::E_LABEL, label, keyStr);

                value_t val;
                Tool::str2str(keyStr, val);
                newData.push_back(val);
            }
            data_pair.second.swap(newData);
        }
        return true;
    }
};

#endif  // EXPERT_LABEL_EXPERT_HPP_
