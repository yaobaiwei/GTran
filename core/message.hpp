/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
         Modified by Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/


#pragma once

#include <vector>
#include <sstream>
#include <functional>
#include <string>
#include <utility>

#include "actor/actor_object.hpp"
#include "base/core_affinity.hpp"
#include "base/node.hpp"
#include "base/serialization.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "core/exec_plan.hpp"
#include "core/id_mapper.hpp"

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
    static void CreateInitMsg(uint64_t qid, int parent_node, int nodes_num, int recv_tid,
                              QueryPlan& qplan, vector<Message>& vec);

    // broadcast msg to all nodes
    void CreateBroadcastMsg(MSG_T type, int nodes_num, vector<Message>& vec);

    // Processing stage abort
    // Send to end actor directly
    void CreateAbortMsg(const vector<Actor_Object>& actors, vector<Message> & vec);

    // actors:  actors chain for current message
    // data:    new data processed by actor_type
    // vec:     messages to be send
    // mapper:  function that maps value_t to particular machine, default NULL
    void CreateNextMsg(const vector<Actor_Object>& actors, vector<pair<history_t, vector<value_t>>>& data,
                    int num_thread, CoreAffinity* core_affinity, vector<Message>& vec, bool considerDstVtx = false);

    // actors:  actors chain for current message
    // stpes:   branching steps
    // vec:     messages to be send
    void CreateBranchedMsg(const vector<Actor_Object>& actors, vector<int>& steps, int num_thread,
                        CoreAffinity* core_affinity, vector<Message>& vec);

    // actors:  actors chain for current message
    // stpes:   branching steps
    // msg_id:  assigned by actor to indicate parent msg
    // vec:     messages to be send
    void CreateBranchedMsgWithHisLabel(const vector<Actor_Object>& actors, vector<int>& steps, uint64_t msg_id,
                            int num_thread, CoreAffinity* core_affinity, vector<Message>& vec);

    // create Feed msg
    // Feed data to all node with tid = parent_tid
    void CreateFeedMsg(int key, int nodes_num, vector<value_t>& data, vector<Message>& vec);

    std::string DebugString() const;

 private:
    // dispatch input data to different node
    void dispatch_data(Meta& m, const vector<Actor_Object>& actors, vector<pair<history_t, vector<value_t>>>& data,
                    int num_thread, CoreAffinity * core_affinity, vector<Message>& vec, bool considerDstVtx = false);
    // update route to next actor
    bool update_route(Meta& m, const vector<Actor_Object>& actors);
    // update route to barrier or labelled branch actors for msg collection
    bool update_collection_route(Meta& m, const vector<Actor_Object>& actors);
    // get the node where vertex or edge is stored
    static int get_node_id(const value_t & v, SimpleIdMapper * id_mapper, bool considerDstVtx = false);
    // Redistribute params of actors according to data locality
    static void AssignParamsByLocality(vector<QueryPlan>& qplans);
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
