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

#ifndef EXPERT_HAS_EXPERT_HPP_
#define EXPERT_HAS_EXPERT_HPP_

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "expert/abstract_expert.hpp"
#include "expert/expert_validation_object.hpp"
#include "layout/index_store.hpp"
#include "utils/tool.hpp"

class HasExpert : public AbstractExpert {
 public:
    HasExpert(int id,
            int machine_id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        AbstractExpert(id, core_affinity),
        machine_id_(machine_id),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(EXPERT_T::HAS) {
        config_ = Config::GetInstance();
    }

    // Has:
    // inType
    // [ key:  int
    //   is_indexed: bool
    //   pred: Predicate_T
    //   pred_param: value_t]
    // Has(params) :
    //     -> key = pid; pred = ANY; pred_params = value_t(one) : has(key)
    //     -> key = pid; pred = EQ; pred_params = value_t(one) : has(key, value)
    //     -> key = pid; pred = <others>; pred_params = value_t(one/two) : has(key, predicate)
    // HasValue(params) : values -> [key = -1; pred = EQ; pred_params = string(value)]
    // HasNot(params) : key -> [key = pid; pred = NONE; pred_params = -1]
    // HasKey(params) : keys -> [key = pid; pred = ANY; pred_params = -1]
    //
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = qplan.experts[m.step];

        // store all predicate
        vector<pair<int, PredicateValue>> pred_chain;

        // Get Params
        CHECK(expert_obj.params.size() > 0 && (expert_obj.params.size() - 1) % 3 == 0);  // make sure input format
        Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params.at(0));
        int numParamsGroup = (expert_obj.params.size() - 1) / 3;  // number of groups of params

        if (qplan.trx_type != TRX_READONLY && config_->isolation_level == ISOLATION_LEVEL::SERIALIZABLE) {
            // Record Input Set
            for (auto & data_pair : msg.data) {
                v_obj.RecordInputSetValueT(qplan.trxid, expert_obj.index, inType, data_pair.second, m.step == 1 ? true : false);
            }
        }

        // Create predicate chain for this query
        for (int i = 0; i < numParamsGroup; i++) {
            int pos = i * 3 + 1;
            // Get predicate params
            int pid = Tool::value_t2int(expert_obj.params.at(pos));
            Predicate_T pred_type = (Predicate_T) Tool::value_t2int(expert_obj.params.at(pos + 1));
            vector<value_t> pred_params;
            Tool::value_t2vec(expert_obj.params.at(pos + 2), pred_params);
            pred_chain.emplace_back(pid, PredicateValue(pred_type, pred_params));
        }

        bool read_success = true;
        switch (inType) {
          case Element_T::VERTEX:
            EvaluateVertex(qplan, msg.data, pred_chain, read_success);
            break;
          case Element_T::EDGE:
            EvaluateEdge(qplan, msg.data, pred_chain, read_success);
            break;
          default:
            cout << "Wrong inType" << endl;
        }

        // Create Message
        vector<Message> msg_vec;
        if (read_success) {
            msg.CreateNextMsg(qplan.experts, msg.data, num_thread_, core_affinity_, msg_vec);
        } else {
            string abort_info = "Abort with [Processing][HasExpert::process]";
            msg.CreateAbortMsg(qplan.experts, msg_vec, abort_info);
        }

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

    bool valid(uint64_t TrxID, vector<Expert_Object*> & expert_list, const vector<rct_extract_data_t> & check_set) {
        for (auto & expert_obj : expert_list) {
            CHECK(expert_obj->expert_type == EXPERT_T::HAS);
            vector<uint64_t> local_check_set;

            // Analysis params
            set<int> plist;
            Element_T inType = (Element_T) Tool::value_t2int(expert_obj->params.at(0));
            int numParamsGroup = (expert_obj->params.size() - 1) / 3;  // number of groups of params
            for (int i = 0; i < numParamsGroup; i++) {
                int pos = i * 3 + 1;
                // Get predicate params
                plist.emplace(Tool::value_t2int(expert_obj->params.at(pos)));
            }

            // Compare check_set and parameters
            for (auto & val : check_set) {
                if (plist.find(get<1>(val)) != plist.end() && get<2>(val) == inType) {
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

    void EvaluateVertex(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data,
            const vector<pair<int, PredicateValue>> & pred_chain, bool & read_success) {
        auto checkFunction = [&](value_t& value){
            if (!read_success) { return false; }
            vid_t v_id(Tool::value_t2int(value));
            vector<pair<label_t, value_t>> vp_kv_pair_list;
            READ_STAT read_status = data_storage_->GetAllVP(v_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, vp_kv_pair_list);
            if (read_status == READ_STAT::ABORT) {
                read_success = false;
                return false;
            } else if (read_status == READ_STAT::NOTFOUND) {
                return true;  // Erase
            }

            for (auto & pred_pair : pred_chain) {
                int pid = pred_pair.first;
                PredicateValue pred = pred_pair.second;

                if (pid == -1) {
                    int counter = vp_kv_pair_list.size();
                    for (auto & pair : vp_kv_pair_list) {
                        if (!Evaluate(pred, &(pair.second))) {
                            counter--;
                        }
                    }

                    // Cannot match all properties, erase
                    if (counter == 0) {
                        return true;
                    }
                } else {
                    // Check whether key exists for this vtx
                    int counter = vp_kv_pair_list.size();
                    value_t val;
                    for (auto pair : vp_kv_pair_list) {
                        if (pid == pair.first) {
                            val = pair.second;
                            break;
                        } else {
                            counter--;
                        }
                    }

                    if (counter == 0) {
                        if (pred.pred_type == Predicate_T::NONE)
                            continue;
                        return true;
                    }

                    if (pred.pred_type == Predicate_T::ANY)
                        continue;

                    // Erase when doesnt match
                    if (!Evaluate(pred, &val)) {
                        return true;
                    }
                }
            }

            return false;
        };

        for (auto & data_pair : data) {
            data_pair.second.erase(remove_if(data_pair.second.begin(), data_pair.second.end(), checkFunction), data_pair.second.end());
        }
    }

    void EvaluateEdge(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data,
            const vector<pair<int, PredicateValue>> & pred_chain, bool & read_success) {
        auto checkFunction = [&](value_t& value){
            if (!read_success) { return false; }
            eid_t e_id;
            uint2eid_t(Tool::value_t2uint64_t(value), e_id);
            vector<pair<label_t, value_t>> ep_kv_pair_list;
            READ_STAT read_status = data_storage_->GetAllEP(e_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, ep_kv_pair_list);
            if (read_status == READ_STAT::ABORT) {
                read_success = false;
                return false;
            } else if (read_status == READ_STAT::NOTFOUND) {
                return true;  // Erase
            }

            for (auto & pred_pair : pred_chain) {
                int pid = pred_pair.first;
                PredicateValue pred = pred_pair.second;

                if (pid == -1) {
                    int counter = ep_kv_pair_list.size();
                    for (auto & pair : ep_kv_pair_list) {
                        if (!Evaluate(pred, &(pair.second))) {
                            counter--;
                        }
                    }

                    // Cannot match all properties, erase
                    if (counter == 0) {
                        return true;
                    }
                } else {
                    // Check whether key exists for this vtx
                    int counter = ep_kv_pair_list.size();
                    value_t val;
                    for (auto pair : ep_kv_pair_list) {
                        if (pid == pair.first) {
                            val = pair.second;
                            break;
                        } else {
                            counter--;
                        }
                    }

                    if (counter == 0) {
                        if (pred.pred_type == Predicate_T::NONE)
                            continue;
                        return true;
                    }

                    if (pred.pred_type == Predicate_T::ANY)
                        continue;

                    // Erase when doesnt match
                    if (!Evaluate(pred, &val)) {
                        return true;
                    }
                }
            }

            return false;
        };

        for (auto & data_pair : data) {
            data_pair.second.erase(remove_if(data_pair.second.begin(), data_pair.second.end(), checkFunction), data_pair.second.end());
        }
    }
};

#endif  // EXPERT_HAS_EXPERT_HPP_
