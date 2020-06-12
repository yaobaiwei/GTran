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

#ifndef EXPERT_HAS_LABEL_EXPERT_HPP_
#define EXPERT_HAS_LABEL_EXPERT_HPP_

#include <string>
#include <utility>
#include <vector>

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "expert/abstract_expert.hpp"
#include "expert/expert_validation_object.hpp"
#include "utils/tool.hpp"

class HasLabelExpert : public AbstractExpert {
 public:
    HasLabelExpert(int id,
            int machine_id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        AbstractExpert(id, core_affinity),
        machine_id_(machine_id),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(EXPERT_T::HASLABEL) {
        config_ = Config::GetInstance();
    }

    // HasLabel:
    //  Pass if any label_key matches
    // Parmas:
    //  inType
    //  vector<value_t> value_t.type = int
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = qplan.experts[m.step];

        // Get Params
        CHECK(expert_obj.params.size() > 1);
        Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params.at(0));

        if (qplan.trx_type != TRX_READONLY && config_->isolation_level == ISOLATION_LEVEL::SERIALIZABLE) {
            // Record Input Set
            for (auto & data_pair : msg.data) {
                v_obj.RecordInputSetValueT(qplan.trxid, expert_obj.index, inType, data_pair.second, m.step == 1 ? true : false);
            }
        }

        vector<label_t> lid_list;
        for (int pos = 1; pos < expert_obj.params.size(); pos++) {
            label_t lid = static_cast<label_t>(Tool::value_t2int(expert_obj.params.at(pos)));
            lid_list.emplace_back(lid);
        }

        bool read_success = true;
        switch (inType) {
          case Element_T::VERTEX:
            VertexHasLabel(qplan, lid_list, msg.data, read_success);
            break;
          case Element_T::EDGE:
            EdgeHasLabel(qplan, lid_list, msg.data, read_success);
            break;
          default:
            cout << "Wrong in type"  << endl;
        }
        // Create Message
        vector<Message> msg_vec;
        if (read_success) {
            msg.CreateNextMsg(qplan.experts, msg.data, num_thread_, core_affinity_, msg_vec);
        } else {
            string abort_info = "Abort with [Processing][HasLabelExpert::process]";
            msg.CreateAbortMsg(qplan.experts, msg_vec, abort_info);
        }

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

    bool valid(uint64_t TrxID, vector<Expert_Object*> & expert_list, const vector<rct_extract_data_t> & check_set) {
        for (auto & expert_obj : expert_list) {
            CHECK(expert_obj->expert_type == EXPERT_T::HASLABEL);
            vector<uint64_t> local_check_set;

            // Analysis params
            Element_T inType = (Element_T) Tool::value_t2int(expert_obj->params.at(0));

            // Compare check_set and parameters
            for (auto & val : check_set) {
                if (get<1>(val) == 0 && get<2>(val) == inType) {
                    local_check_set.emplace_back(get<0>(val));
                }
            }

            if (local_check_set.size() != 0) {
                if(!v_obj.Validate(TrxID, expert_obj->index, local_check_set)) {
                    return false;
                }
            }
        }
        return true;
    }

    void clean_trx_data(uint64_t TrxID) { v_obj.DeleteInputSet(TrxID); }

 private:
    // Number of Threads
    int num_thread_;
    int machine_id_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config* config_;

    // Validation Store
    ExpertValidationObject v_obj;

    void VertexHasLabel(const QueryPlan & qplan, vector<label_t> lid_list, vector<pair<history_t, vector<value_t>>> & data, bool & read_success) {
        auto checkFunction = [&](value_t & value) {
            if (!read_success) { return false; }
            vid_t v_id(Tool::value_t2int(value));

            label_t label;
            READ_STAT read_status = data_storage_->GetVL(v_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, label);
            if (read_status == READ_STAT::ABORT) {
                read_success = false;
                return false;
            } else if (read_status == READ_STAT::NOTFOUND) {
                return true;  // Erase
            }

            for (auto & lid : lid_list) {
                if (lid == label) {
                    return false;
                }
            }
            return true;
        };

        for (auto & data_pair : data) {
            data_pair.second.erase(remove_if(data_pair.second.begin(), data_pair.second.end(), checkFunction), data_pair.second.end());
        }
    }

    void EdgeHasLabel(const QueryPlan & qplan, vector<label_t> lid_list, vector<pair<history_t, vector<value_t>>> & data, bool & read_success) {
        auto checkFunction = [&](value_t & value) {
            if (!read_success) { return false; }
            eid_t e_id;
            uint2eid_t(Tool::value_t2uint64_t(value), e_id);

            label_t label;
            READ_STAT read_status = data_storage_->GetEL(e_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, label);
            if (read_status == READ_STAT::ABORT) {
                read_success = false;
                return false;
            } else if (read_status == READ_STAT::NOTFOUND) {
                return true;  // Erase
            }

            for (auto & lid : lid_list) {
                if (lid == label) {
                    return false;
                }
            }
            return true;
        };

        for (auto & data_pair : data) {
            data_pair.second.erase(remove_if(data_pair.second.begin(), data_pair.second.end(), checkFunction), data_pair.second.end());
        }
    }
};

#endif  // EXPERT_HAS_LABEL_EXPERT_HPP_
