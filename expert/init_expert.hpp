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

#ifndef EXPERT_INIT_EXPERT_HPP_
#define EXPERT_INIT_EXPERT_HPP_

#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include "glog/logging.h"

#include "base/node.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/result_collector.hpp"
#include "expert/abstract_expert.hpp"
#include "expert/expert_validation_object.hpp"
#include "layout/index_store.hpp"
#include "utils/tool.hpp"
#include "utils/timer.hpp"

class InitExpert : public AbstractExpert {
 public:
    InitExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity,
            int num_nodes) :
        AbstractExpert(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        num_nodes_(num_nodes),
        type_(EXPERT_T::INIT),
        is_ready_(false) {
        config_ = Config::GetInstance();
        index_store_ = IndexStore::GetInstance();
    }

    virtual ~InitExpert() {}

    // TODO(nick): Need to support dynamic V/E cache
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);

        Expert_Object expert_obj = qplan.experts[msg.meta.step];
        CHECK(expert_obj.params.size() >= 2);
        bool with_input = Tool::value_t2int(expert_obj.params[1]);

        bool next_count = qplan.experts[msg.meta.step + 1].expert_type == EXPERT_T::COUNT;

        vector<pair<history_t, vector<value_t>>> init_data;
        if (with_input) {
            InitWithInput(tid, qplan.experts, init_data, msg, next_count);
        } else if (expert_obj.params.size() == 2) {
            InitWithoutIndex(tid, qplan, init_data, msg, next_count);
        } else {
            InitWithIndex(tid, qplan.experts, init_data, msg, next_count);
            if (qplan.trx_type != TRX_READONLY && config_->isolation_level == ISOLATION_LEVEL::SERIALIZABLE) {
                Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params.at(0));
                v_obj.RecordInputSetValueT(qplan.trxid, 0, inType, init_data.at(0).second, false);
            }
        }

        PushToRWRecord(qplan.trxid, init_data.size(), true);

        vector<Message> msg_vec;
        msg.CreateNextMsg(qplan.experts, init_data, num_thread_, core_affinity_, msg_vec);
        for (auto & msg_ : msg_vec) {
            mailbox_->Send(tid, msg_);
        }
    }

    bool valid(uint64_t TrxID, vector<Expert_Object*> & expert_list, const vector<rct_extract_data_t> & check_set) {
        // For InitExpert, input set also need to be validated.
        // However, it's unnecessary to record input set since there are only 2 situations:
        // 1) g.V() --> All;
        // 2) g.V(A) --> No Need to Validate;
        // 3) g.V().has() with index --> Check data from Index;
        for (auto & expert_obj : expert_list) {
            CHECK(expert_obj->expert_type == EXPERT_T::INIT);
            vector<uint64_t> local_check_set;

            // Analysis params
            CHECK(expert_obj->params.size() >= 2);
            Element_T inType = (Element_T)Tool::value_t2int(expert_obj->params.at(0));
            bool with_input = Tool::value_t2int(expert_obj->params[1]);

            if (!with_input) {
                if (expert_obj->params.size() == 2) {
                    // Scenario 1)
                    for (auto & val : check_set) {
                        if (get<2>(val) == inType) {
                            return false;  // Once find one --> Abort
                        }
                    }
                } else {
                    // Scenario 3)
                    set<int> plist;
                    int numParamsGroup = (expert_obj->params.size() - 2) / 3;

                    for (int i = 0; i < numParamsGroup; i++) {
                        int pos = i * 3 + 2;
                        // Get predicate params
                        plist.emplace(Tool::value_t2int(expert_obj->params.at(pos)));
                    }

                    for (auto & val : check_set) {
                        if (plist.find(get<1>(val)) != plist.end() && get<2>(val) == inType) {
                            local_check_set.emplace_back(get<0>(val));
                        }
                    }

                    if (local_check_set.size() != 0) {
                        if (!v_obj.Validate(TrxID, 0, local_check_set)) {
                            return false; 
                        }
                    }
                }
            }
        }
        return true;
    }

 private:
    // Number of threads
    int num_thread_;
    int num_nodes_;
    bool is_ready_;

    Config * config_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // Pointer of index store
    IndexStore * index_store_;

    // Validation Store
    ExpertValidationObject v_obj;

    /* =============OLAP Impl=============== */
    // Ensure only one thread ever runs the expert
    // std::mutex thread_mutex_;

    // Cached data
    // vector<Message> vtx_msgs;
    // vector<Message> edge_msgs;

    // msg for count expert
    // vector<Message> vtx_count_msgs;
    // vector<Message> edge_count_msgs;
    /* =============OLAP Impl=============== */

    void InitData(const QueryPlan& qplan, Element_T inType, vector<pair<history_t, vector<value_t>>> & init_data, bool & next_count) {
        // convert id to msg
        Meta m;
        m.step = 1;
        m.msg_path = to_string(num_nodes_);

        uint64_t start_t, end_t;
        start_t = timer::get_usec();
        if (inType == Element_T::VERTEX) {
            InitVtxData(m, qplan, init_data, next_count);
            end_t = timer::get_usec();
            // cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for initV_Msg in init_expert" << endl;
        } else {
            InitEdgeData(m, qplan, init_data, next_count);
            end_t = timer::get_usec();
            // cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for initE_Msg in init_expert" << endl;
        }
    }

    void InitVtxData(const Meta& m, const QueryPlan& qplan, vector<pair<history_t, vector<value_t>>> & init_data, bool & next_count) {
        vector<vid_t> vid_list;
        uint64_t start_time = timer::get_usec();
        if (config_->global_enable_indexing) {
            index_store_->ReadVtxTopoIndex(qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, vid_list);
        } else {
            data_storage_->GetAllVertices(qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, vid_list);
        }
        uint64_t end_time = timer::get_usec();
        // cout << "[Timer] " << (end_time - start_time) << " us for GetAllVertices()" << endl;
        uint64_t count = vid_list.size();

        // vector<pair<history_t, vector<value_t>>> data;
        init_data.clear();
        init_data.emplace_back(history_t(), vector<value_t>());
        init_data[0].second.reserve(count);

        if (next_count) {
            value_t v; 
            Tool::str2int(to_string(count), v);
            init_data[0].second.emplace_back(v);
        } else {
            for (auto& vid : vid_list) {
                value_t v;
                Tool::str2int(to_string(vid.value()), v);
                init_data[0].second.emplace_back(v);
            }
        }
        vector<vid_t>().swap(vid_list);
    }

    void InitEdgeData(const Meta& m, const QueryPlan& qplan, vector<pair<history_t, vector<value_t>>>& init_data, bool & next_count) {
        vector<eid_t> eid_list;
        uint64_t start_time = timer::get_usec();
        if (config_->global_enable_indexing) {
            index_store_->ReadEdgeTopoIndex(qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, eid_list);
        } else {
            data_storage_->GetAllEdges(qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, eid_list);
        }
        uint64_t end_time = timer::get_usec();
        // cout << "[Timer] " << (end_time - start_time) << " us for GetAllEdges()" << endl;
        uint64_t count = eid_list.size();

        // vector<pair<history_t, vector<value_t>>> data;
        init_data.clear();
        init_data.emplace_back(history_t(), vector<value_t>());
        init_data[0].second.reserve(count);

        if (next_count) {
            value_t v; 
            Tool::str2int(to_string(count), v);
            init_data[0].second.emplace_back(v);
        } else {
            for (auto& eid : eid_list) {
                value_t v;
                Tool::str2uint64_t(to_string(eid.value()), v);
                init_data[0].second.push_back(v);
            }
        }
        vector<eid_t>().swap(eid_list);
    }

    void InitWithInput(int tid, const vector<Expert_Object> & expert_objs, vector<pair<history_t, vector<value_t>>>& init_data, Message & msg, bool& next_count) {
        Meta m = msg.meta;
        const Expert_Object& expert_obj = expert_objs[m.step];
        msg.max_data_size = config_->max_data_size;

        init_data.emplace_back(history_t(), vector<value_t>());
        // Get V/E from expert params
        init_data[0].second.insert(init_data[0].second.end(),
                                 make_move_iterator(expert_obj.params.begin() + 2),
                                 make_move_iterator(expert_obj.params.end()));

        if (next_count) {
            int size = init_data[0].second.size();
            init_data[0].second.clear();

            value_t v;
            Tool::str2int(to_string(size), v);
            init_data[0].second.emplace_back(v);
        }
    }

    void InitWithIndex(int tid, const vector<Expert_Object> & expert_objs, vector<pair<history_t, vector<value_t>>>& init_data, Message & msg, bool& next_count) {
        Meta m = msg.meta;
        const Expert_Object& expert_obj = expert_objs[m.step];

        // store all predicate
        vector<pair<int, PredicateValue>> pred_chain;

        // Get Params
        CHECK((expert_obj.params.size() - 2) % 3 == 0);  // make sure input format
        Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params.at(0));
        int numParamsGroup = (expert_obj.params.size() - 2) / 3;  // number of groups of params

        // Create predicate chain for this query
        for (int i = 0; i < numParamsGroup; i++) {
            int pos = i * 3 + 2;
            // Get predicate params
            int pid = Tool::value_t2int(expert_obj.params.at(pos));
            Predicate_T pred_type = (Predicate_T) Tool::value_t2int(expert_obj.params.at(pos + 1));
            vector<value_t> pred_params;
            Tool::value_t2vec(expert_obj.params.at(pos + 2), pred_params);
            pred_chain.emplace_back(pid, PredicateValue(pred_type, pred_params));
        }

        msg.max_data_size = config_->max_data_size;
        msg.data.clear();
        msg.data.emplace_back(history_t(), vector<value_t>());
        init_data.clear();
        init_data.emplace_back(history_t(), vector<value_t>());
        index_store_->ReadPropIndex(inType, pred_chain, init_data[0].second);

        if (next_count) {
            int size = init_data[0].second.size();
            init_data[0].second.clear();

            value_t v;
            Tool::str2int(to_string(size), v);
            init_data[0].second.emplace_back(v);
        }
    }

    void InitWithoutIndex(int tid, const QueryPlan& qplan, vector<pair<history_t, vector<value_t>>> & init_data, Message & msg, bool & next_count) {
        Meta m = msg.meta;
        const Expert_Object& expert_obj = qplan.experts[m.step];

        // Get init element type
        Element_T inType = (Element_T)Tool::value_t2int(expert_obj.params.at(0));

        // No need to lock since init is for each transaction rather than whole system.
        InitData(qplan, inType, init_data, next_count);

        m.step++;
        // update meta
        m.msg_type = MSG_T::SPAWN;
        if (qplan.experts[m.step].IsBarrier()) {
            m.msg_type = MSG_T::BARRIER;
            m.recver_nid = m.parent_nid;
        }
    }
};

#endif  // EXPERT_INIT_EXPERT_HPP_
