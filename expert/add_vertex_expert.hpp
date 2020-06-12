// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
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

#ifndef EXPERT_ADD_VERTEX_EXPERT_HPP_
#define EXPERT_ADD_VERTEX_EXPERT_HPP_

#include <string>
#include <vector>

#include "base/core_affinity.hpp"
#include "base/type.hpp"
#include "core/message.hpp"
#include "expert/abstract_expert.hpp"
#include "layout/data_storage.hpp"
#include "layout/index_store.hpp"
#include "layout/pmt_rct_table.hpp"

class AddVertexExpert : public AbstractExpert {
 public:
    AddVertexExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity) :
        AbstractExpert(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(EXPERT_T::ADDV) {
        config_ = Config::GetInstance();
        pmt_rct_table_ = PrimitiveRCTTable::GetInstance();
        index_store_ = IndexStore::GetInstance();
    }

    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);
        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = qplan.experts[m.step];
        vector<uint64_t> update_data;

        // Get Params
        int lid = static_cast<int>(Tool::value_t2int(expert_obj.params.at(0)));
        process_add_vertex(qplan, lid, msg.data, update_data);

        // Insert Updates Information into RCT Table
        pmt_rct_table_->InsertRecentActionSet(Primitive_T::IV, qplan.trxid, update_data);

        // Insert update data to topo index
        index_store_->InsertToUpdateBuffer(qplan.trxid, update_data, ID_T::VID, true);

        vector<Message> msg_vec;
        msg.CreateNextMsg(qplan.experts, msg.data, num_thread_, core_affinity_, msg_vec);
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
    Config* config_;

    // RCT Table
    PrimitiveRCTTable * pmt_rct_table_;

    // Index Store
    IndexStore * index_store_;

    void process_add_vertex(const QueryPlan & qplan, int label_id, vector<pair<history_t, vector<value_t>>> & data, vector<uint64_t> & update_data) {
        for (auto & pair : data) {
            vector<value_t> newData;
            // The number of vertices to be added is the number of result from last step.
            //  If addV is the first step, the default value should be one.

            // Record w_set
            PushToRWRecord(qplan.trxid, pair.second.size(), false);
            for (auto & vertex : pair.second) {
                vid_t new_v_id = data_storage_->ProcessAddV(label_id, qplan.trxid, qplan.st);
                value_t new_val;
                Tool::str2int(to_string(new_v_id.value()), new_val);
                newData.emplace_back(new_val);

                uint64_t rct_insert_val = static_cast<uint64_t>(vid_t2uint(new_v_id));
                update_data.emplace_back(move(rct_insert_val));
            }
            pair.second.swap(newData);
        }
    }
};

#endif  // EXPERT_ADD_VERTEX_EXPERT_HPP_
