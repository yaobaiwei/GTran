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

#ifndef EXPERT_INDEX_EXPERT_HPP_
#define EXPERT_INDEX_EXPERT_HPP_

#include <algorithm>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "expert/abstract_expert.hpp"
#include "layout/index_store.hpp"
#include "utils/tool.hpp"

class IndexExpert : public AbstractExpert {
 public:
    IndexExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        AbstractExpert(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(EXPERT_T::HAS) {
        index_store_ = IndexStore::GetInstance(); 
        }

    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = qplan.experts[m.step];

        // Get Params
        CHECK(expert_obj.params.size() == 2);  // make sure input format
        Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params[0]);
        int pid = Tool::value_t2int(expert_obj.params[1]);

        bool enabled = false;
        switch (inType) {
          case Element_T::VERTEX:
            enabled = BuildIndexVtx(qplan, pid);
            break;
          case Element_T::EDGE:
            enabled = BuildIndexEdge(qplan, pid);
            break;
          default:
            cout << "Wrong inType" << endl;
            return;
        }

        string ena = (enabled? "enabled":"disabled");
        string s = "Index is " + ena + " in node" + to_string(m.recver_nid);
        value_t v;
        Tool::str2str(s, v);
        msg.data.emplace_back(history_t(), vector<value_t>{v});
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

    IndexStore * index_store_;

    map<int, bool> vtx_enabled_map;
    map<int, bool> edge_enabled_map;

    bool BuildIndexVtx(const QueryPlan & qplan, int pid) {
        if (vtx_enabled_map.find(pid) != vtx_enabled_map.end()) {
            return index_store_->SetIndexMapEnable(Element_T::VERTEX, pid, true);
        }
        vector<vid_t> vid_list;
        data_storage_->GetAllVertices(qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, vid_list);

        map<value_t, vector<uint64_t>> index_map;
        vector<uint64_t> no_key_vec;

        for (auto& vid : vid_list) {
            uint64_t vtx = vid_t2uint(vid);
            value_t val;
            if (pid == 0) {
                label_t label;
                data_storage_->GetVL(vid, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, label);
                Tool::str2int(to_string(label), val);
                index_map[val].push_back(move(vtx));
            } else {
                vpid_t vp_id(vid, pid);
                bool found = data_storage_->GetVPByPKey(vp_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, val)
                             == READ_STAT::SUCCESS;
                if (!found) {
                    no_key_vec.push_back(move(vtx));
                } else {
                    index_map[val].push_back(move(vtx));
                }
            }
        }

        index_store_->SetIndexMap(Element_T::VERTEX, pid, index_map, no_key_vec);
        vtx_enabled_map[pid] = true;

        return index_store_->SetIndexMapEnable(Element_T::VERTEX, pid);
    }

    bool BuildIndexEdge(const QueryPlan & qplan, int pid) {
        if (edge_enabled_map.find(pid) != edge_enabled_map.end()) {
            return index_store_->SetIndexMapEnable(Element_T::EDGE, pid, true);
        }
        vector<eid_t> eid_list;
        data_storage_->GetAllEdges(qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, eid_list);

        map<value_t, vector<uint64_t>> index_map;
        vector<uint64_t> no_key_vec;

        for (auto& eid : eid_list) {
            uint64_t edge = eid_t2uint(eid);
            value_t val;

            if (pid == 0) {
                label_t label;
                data_storage_->GetEL(eid, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, label);
                Tool::str2int(to_string(label), val);
                index_map[val].push_back(move(edge));
            } else {
                epid_t ep_id(eid, pid);
                bool found = data_storage_->GetEPByPKey(ep_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, val)
                             == READ_STAT::SUCCESS;
                if (!found) {
                    no_key_vec.push_back(move(edge));
                } else {
                    index_map[val].push_back(move(edge));
                }
            }
        }

        index_store_->SetIndexMap(Element_T::EDGE, pid, index_map, no_key_vec);
        edge_enabled_map[pid] = true;

        return index_store_->SetIndexMapEnable(Element_T::EDGE, pid);
    }
};

#endif  // EXPERT_INDEX_EXPERT_HPP_
