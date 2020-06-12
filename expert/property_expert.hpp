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

#ifndef EXPERT_PROPERTY_EXPERT_HPP_
#define EXPERT_PROPERTY_EXPERT_HPP_

#include <string>
#include <vector>

#include "base/core_affinity.hpp"
#include "base/type.hpp"
#include "core/message.hpp"
#include "core/factory.hpp"
#include "core/id_mapper.hpp"
#include "expert/abstract_expert.hpp"
#include "layout/data_storage.hpp"
#include "layout/index_store.hpp"
#include "layout/pmt_rct_table.hpp"
#include "utils/tool.hpp"

class PropertyExpert : public AbstractExpert {
 public:
    PropertyExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity) :
        AbstractExpert(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(EXPERT_T::PROPERTY) {
        config_ = Config::GetInstance();
        pmt_rct_table_ = PrimitiveRCTTable::GetInstance();
        trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();
        index_store_ = IndexStore::GetInstance();
    }

    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);
        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = qplan.experts[m.step];

        // Prepare for Update Data (RCT and Index)
        vector<pair<uint64_t, value_t>> update_data;  // pair<vpid, old_value>
        Primitive_T pmt_type;

        // Get Params
        Element_T elem_type = static_cast<Element_T>(Tool::value_t2int(expert_obj.params.at(0)));
        int pid = static_cast<int>(Tool::value_t2int(expert_obj.params.at(1)));
        value_t new_val = expert_obj.params.at(2);

        bool index_updatable = index_store_->IsIndexEnabled(elem_type, pid);

        PROCESS_STAT process_stat = PROCESS_STAT::SUCCESS;
        switch (elem_type) {
          case Element_T::VERTEX:
            pmt_type = Primitive_T::MVP;
            process_stat = processVertexProperty(qplan, msg.data, pid, new_val, update_data);
            break;
          case Element_T::EDGE:
            pmt_type = Primitive_T::MEP;
            process_stat = processEdgeProperty(qplan, msg.data, pid, new_val, update_data);
            break;
          default:
            process_stat = PROCESS_STAT::ABORT;
            cout << "[Error] Unexpected Element Type in PropertyExpert" << endl;
        }

        // Create Message
        vector<Message> msg_vec;
        if (process_stat == PROCESS_STAT::SUCCESS) {
            // Insert Updates Information into RCT Table if success
            vector<uint64_t> ids;
            vector<value_t> values;
            transform(begin(update_data), end(update_data),
                      back_inserter(ids),
                      [](auto const& pair){ return pair.first; });
            transform(begin(update_data), end(update_data),
                      back_inserter(values),
                      [](auto const& pair){ return pair.second; });

            pmt_rct_table_->InsertRecentActionSet(pmt_type, qplan.trxid, ids);

            // Insert Update data to Index Buffer
            if (index_updatable) {
                if (elem_type == Element_T::VERTEX) {
                    index_store_->InsertToUpdateBuffer(qplan.trxid, ids, ID_T::VPID, true, &new_val, &values);
                } else if (elem_type == Element_T::EDGE) {
                    index_store_->InsertToUpdateBuffer(qplan.trxid, ids, ID_T::EPID, true, &new_val, &values);
                }
            }

            msg.CreateNextMsg(qplan.experts, msg.data, num_thread_, core_affinity_, msg_vec);
        } else {
            string abort_info = "Abort with [Processing][PropertyExpert::process]" + abort_reason_map[process_stat];
            msg.CreateAbortMsg(qplan.experts, msg_vec, abort_info);
        }

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

 private:
    // Node Info
    int num_thread_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config* config_;

    // TrxTableStub
    TrxTableStub * trx_table_stub_;

    // RCT Table
    PrimitiveRCTTable * pmt_rct_table_;

    // Index Store
    IndexStore * index_store_;

    PROCESS_STAT processVertexProperty(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data,
            int propertyId, value_t new_val, vector<pair<uint64_t, value_t>> & update_data) {
        PROCESS_STAT process_stat;
        for (auto & pair : data) {
            PushToRWRecord(qplan.trxid, pair.second.size(), false);
            for (auto & val : pair.second) {
                vpid_t vpid(Tool::value_t2int(val), propertyId);
                value_t old_val = value_t();

                process_stat = data_storage_->ProcessModifyVP(vpid, new_val, old_val, qplan.trxid, qplan.st);
                if (process_stat != PROCESS_STAT::SUCCESS) {
                    return process_stat;
                }
                update_data.emplace_back(vpid_t2uint(vpid), old_val);
            }
        }
        return PROCESS_STAT::SUCCESS;
    }

    PROCESS_STAT processEdgeProperty(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data,
            int propertyId, value_t new_val, vector<pair<uint64_t, value_t>> & update_data) {
        PROCESS_STAT process_stat;
        for (auto & pair : data) {
            PushToRWRecord(qplan.trxid, pair.second.size(), false);
            for (auto & val : pair.second) {
                eid_t eid;
                uint2eid_t(Tool::value_t2uint64_t(val), eid);
                epid_t epid(eid, propertyId);
                value_t old_val = value_t();

                process_stat = data_storage_->ProcessModifyEP(epid, new_val, old_val, qplan.trxid, qplan.st);
                if (process_stat != PROCESS_STAT::SUCCESS) {
                    return process_stat;
                }
                update_data.emplace_back(epid_t2uint(epid), old_val);
            }
        }
        return PROCESS_STAT::SUCCESS;
    }
};

#endif  // EXPERT_PROPERTY_EXPERT_HPP_
