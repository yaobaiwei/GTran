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
#include <sstream>
#include <functional>
#include <string>
#include <utility>

#include "base/core_affinity.hpp"
#include "base/node.hpp"
#include "base/serialization.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "core/exec_plan.hpp"
#include "core/id_mapper.hpp"
#include "expert/expert_object.hpp"

#define TEN_MB 1048576

struct Branch_Info{
    // parent route
    int node_id;
    int thread_id;
    // indicate the branch order
    int index;
    // history key of parent
    int key;
    // msg id of parent, unique on each node
    uint64_t msg_id;
    // msg path of parent
    string msg_path;
};

ibinstream& operator<<(ibinstream& m, const Branch_Info& info);

obinstream& operator>>(obinstream& m, Branch_Info& info);

struct Meta {
    // query
    uint64_t qid;
    int step;
    uint8_t query_count_in_trx;

    // route
    int recver_nid;
    int recver_tid;

    // parent route
    int parent_nid;
    int parent_tid;

    // type
    MSG_T msg_type;

    // Msg disptching path
    string msg_path;

    // branch info
    vector<Branch_Info> branch_infos;

    // Query Plan
    QueryPlan qplan;

    std::string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const Meta& meta);

obinstream& operator>>(obinstream& m, Meta& meta);

typedef vector<pair<int, value_t>> history_t;

struct HistoryTHash {
    // To limit the upper bound of cost for hashing
    constexpr static int max_hash_len = 4;

    size_t operator() (const history_t& his) const{
        uint64_t hash_tmp = mymath::hash_u64(his.size());

        int hash_loop_len = (max_hash_len > his.size()) ? his.size() : max_hash_len;

        // Only use the former elements to compute the hash value
        for (int i = 0; i < hash_loop_len; i++)
            hash_tmp = mymath::hash_u64(hash_tmp + ValueTHash()(his[i].second) + his[i].first);

        return hash_tmp;
    }
};

bool operator==(const history_t& l, const history_t& r);

class Message {
    // Node node_;// = Node::StaticInstance();

 public:
    Meta meta;

    std::vector<pair<history_t, vector<value_t>>> data;

    // size of data
    size_t data_size;
    // maximum size of data
    size_t max_data_size;

    Message() : data_size(sizeof(size_t)), max_data_size(TEN_MB) {}
    explicit Message(const Meta& m) : Message() {
        meta = m;
    }

    // move data from srouce into msg
    bool InsertData(pair<history_t, vector<value_t>>& pair);
    void InsertData(vector<pair<history_t, vector<value_t>>>& vec);

    // create init msg
    static void CreateInitMsg(uint64_t qid, uint8_t query_count_in_trx, int parent_node, int nodes_num, int recv_tid,
                              QueryPlan& qplan, vector<Message>& vec);

    // broadcast msg to all nodes
    void CreateBroadcastMsg(MSG_T type, int nodes_num, vector<Message>& vec);

    // Processing stage abort
    // Send to end expert directly
    void CreateAbortMsg(const vector<Expert_Object>& experts, vector<Message> & vec, string abort_info = "");

    // experts:  experts chain for current message
    // data:    new data processed by expert_type
    // vec:     messages to be send
    // mapper:  function that maps value_t to particular machine, default NULL
    void CreateNextMsg(const vector<Expert_Object>& experts, vector<pair<history_t, vector<value_t>>>& data,
                    int num_thread, CoreAffinity* core_affinity, vector<Message>& vec);

    // experts:  experts chain for current message
    // stpes:   branching steps
    // vec:     messages to be send
    void CreateBranchedMsg(const vector<Expert_Object>& experts, vector<int>& steps, int num_thread,
                        CoreAffinity* core_affinity, vector<Message>& vec);

    // experts:  experts chain for current message
    // stpes:   branching steps
    // msg_id:  assigned by expert to indicate parent msg
    // vec:     messages to be send
    void CreateBranchedMsgWithHisLabel(const vector<Expert_Object>& experts, vector<int>& steps, uint64_t msg_id,
                            int num_thread, CoreAffinity* core_affinity, vector<Message>& vec);

    // create Feed msg
    // Feed data to all node with tid = parent_tid
    void CreateFeedMsg(int key, int nodes_num, vector<value_t>& data, vector<Message>& vec);

    std::string DebugString() const;

 private:
    // dispatch input data to different node
    void DispatchData(Meta& m, const vector<Expert_Object>& experts, vector<pair<history_t, vector<value_t>>>& data,
                    int num_thread, CoreAffinity * core_affinity, vector<Message>& vec);
    // update route to next expert
    bool UpdateRoute(Meta& m, const vector<Expert_Object>& experts);
    // update route to barrier or labelled branch experts for msg collection
    bool UpdateCollectionRoute(Meta& m, const vector<Expert_Object>& experts);
    // get the node where vertex or edge is stored
    static int GetNodeId(const value_t & v, SimpleIdMapper * id_mapper, bool consider_both_edge = false);
    // Redistribute params of experts according to data locality
    static void AssignParamsByLocality(vector<QueryPlan>& qplans);

    // Construct EdgeID for AddE
    static void ConstructEdge(vector<pair<history_t, vector<value_t>>> & data, const Expert_Object & expert_obj);
    static void GenerateEdgeWithData(vector<pair<history_t, vector<value_t>>> & data, AddEdgeMethodType method_type, int step_param, bool isFrom);
    static void GenerateEdgeWithHistory(vector<pair<history_t, vector<value_t>>> & data, int step_param, int other_end_vid, bool isFrom);
    static void GenerateEdgeWithBothHistory(vector<pair<history_t, vector<value_t>>> & data, int from_label, int to_label);
};

ibinstream& operator<<(ibinstream& m, const Message& msg);

obinstream& operator>>(obinstream& m, Message& msg);

size_t MemSize(const int& i);
size_t MemSize(const char& c);
size_t MemSize(const value_t& data);

template<class T1, class T2>
size_t MemSize(const pair<T1, T2>& p);

template<class T>
size_t MemSize(const vector<T>& data);

#include "message.tpp"
