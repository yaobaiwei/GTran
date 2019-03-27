/* Copyright 2019 Husky Data Lab, CUHK

Authors: Nick Fang (jcfang6@cse.cuhk.edu.hk)
         Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef ACTOR_INIT_ACTOR_HPP_
#define ACTOR_INIT_ACTOR_HPP_

#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include "glog/logging.h"

#include "actor/abstract_actor.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/result_collector.hpp"
#include "core/index_store.hpp"
#include "base/node.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "storage/layout.hpp"
#include "utils/tool.hpp"
#include "utils/timer.hpp"


#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"

class InitActor : public AbstractActor {
 public:
    InitActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity,
            IndexStore * index_store,
            int num_nodes) :
        AbstractActor(id, core_affinity),
        index_store_(index_store),
        num_thread_(num_thread),
        mailbox_(mailbox),
        num_nodes_(num_nodes),
        type_(ACTOR_T::INIT),
        is_ready_(false) {
        config_ = Config::GetInstance();

        // read snapshot here
        // write snapshot @ InitData

        MPISnapshot* snapshot = MPISnapshot::GetInstance();

        int snapshot_read_cnt = 0;

        snapshot_read_cnt +=
            ((snapshot->ReadData("init_actor_vtx_msg", vtx_msgs, ReadMailboxMsgImpl)) ? 1 : 0);
        snapshot_read_cnt +=
            ((snapshot->ReadData("init_actor_edge_msg", edge_msgs, ReadMailboxMsgImpl)) ? 1 : 0);
        snapshot_read_cnt +=
            ((snapshot->ReadData("init_actor_vtx_count_msg", vtx_count_msgs, ReadMailboxMsgImpl)) ? 1 : 0);
        snapshot_read_cnt +=
            ((snapshot->ReadData("init_actor_edge_count_msg", edge_count_msgs, ReadMailboxMsgImpl)) ? 1 : 0);

        if (snapshot_read_cnt == 4) {
            is_ready_ = true;
        } else {
            // atomic, all fail
            vtx_msgs.resize(0);
            edge_msgs.resize(0);
            vtx_count_msgs.resize(0);
            edge_count_msgs.resize(0);
        }
    }

    virtual ~InitActor() {}

    // TODO(nick): Need to support dynamic V/E cache
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        Actor_Object actor_obj = qplan.actors[msg.meta.step];
        assert(actor_obj.params.size() >= 2);
        bool with_input = Tool::value_t2int(actor_obj.params[1]);
        if (with_input) {
            InitWithInput(tid, qplan.actors, msg);
        } else if (actor_obj.params.size() == 2) {
            InitWithoutIndex(tid, qplan.actors, msg);
        } else {
            InitWithIndex(tid, qplan.actors, msg);
        }
    }

 private:
    // Number of threads
    int num_thread_;
    int num_nodes_;
    bool is_ready_;

    Config * config_;

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // Pointer of index store
    IndexStore * index_store_;

    // Ensure only one thread ever runs the actor
    std::mutex thread_mutex_;

    // Cached data
    vector<Message> vtx_msgs;
    vector<Message> edge_msgs;

    // msg for count actor
    vector<Message> vtx_count_msgs;
    vector<Message> edge_count_msgs;

    void InitData() {
        if (is_ready_) {
            return;
        }
        MPISnapshot* snapshot = MPISnapshot::GetInstance();

        // convert id to msg
        Meta m;
        m.step = 1;
        m.msg_path = to_string(num_nodes_);

        uint64_t start_t = timer::get_usec();

        InitVtxData(m);
        snapshot->WriteData("init_actor_vtx_msg", vtx_msgs, WriteMailboxMsgImpl);
        snapshot->WriteData("init_actor_vtx_count_msg", vtx_count_msgs, WriteMailboxMsgImpl);
        uint64_t end_t = timer::get_usec();
        cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for initV_Msg in init_actor" << endl;

        start_t = timer::get_usec();
        InitEdgeData(m);
        snapshot->WriteData("init_actor_edge_msg", edge_msgs, WriteMailboxMsgImpl);
        snapshot->WriteData("init_actor_edge_count_msg", edge_count_msgs, WriteMailboxMsgImpl);
        end_t = timer::get_usec();
        cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for initE_Msg in init_actor" << endl;
    }

    void InitVtxData(const Meta& m) {
        vector<vid_t> vid_list;
        // TODO(Nick): Replace with new caching strategy
        data_storage_->GetAllVertices(0, 0, true, vid_list);
        uint64_t count = vid_list.size();

        vector<pair<history_t, vector<value_t>>> data;
        data.emplace_back(history_t(), vector<value_t>());
        data[0].second.reserve(count);

        for (auto& vid : vid_list) {
            value_t v;
            Tool::str2int(to_string(vid.value()), v);
            data[0].second.push_back(v);
        }
        vector<vid_t>().swap(vid_list);


        vtx_msgs.clear();
        do {
            Message msg(m);
            msg.max_data_size = config_->max_data_size;
            msg.InsertData(data);
            vtx_msgs.push_back(move(msg));
        } while ((data.size() != 0));

        string num = "\t" + to_string(vtx_msgs.size());
        for (auto & msg_ : vtx_msgs) {
            msg_.meta.msg_path += num;
        }

        Message vtx_count_msg(m);
        vtx_count_msg.max_data_size = config_->max_data_size;
        value_t v;
        Tool::str2int(to_string(count), v);
        vtx_count_msg.data.emplace_back(history_t(), vector<value_t>{v});
        vtx_count_msgs.push_back(move(vtx_count_msg));
    }

    void InitEdgeData(const Meta& m) {
        vector<eid_t> eid_list;
        data_storage_->GetAllEdges(0, 0, true, eid_list);
        uint64_t count = eid_list.size();

        vector<pair<history_t, vector<value_t>>> data;
        data.emplace_back(history_t(), vector<value_t>());
        data[0].second.reserve(count);
        for (auto& eid : eid_list) {
            value_t v;
            Tool::str2uint64_t(to_string(eid.value()), v);
            data[0].second.push_back(v);
        }
        vector<eid_t>().swap(eid_list);

        edge_msgs.clear();
        do {
            Message msg(m);
            msg.max_data_size = config_->max_data_size;
            msg.InsertData(data);
            edge_msgs.push_back(move(msg));
        } while ((data.size() != 0));

        string num = "\t" + to_string(edge_msgs.size());
        for (auto & msg_ : edge_msgs) {
            msg_.meta.msg_path += num;
        }

        Message edge_count_msg(m);
        edge_count_msg.max_data_size = config_->max_data_size;
        value_t v;
        Tool::str2int(to_string(count), v);
        edge_count_msg.data.emplace_back(history_t(), vector<value_t>{v});
        edge_count_msgs.push_back(move(edge_count_msg));
    }

    void InitWithInput(int tid, const vector<Actor_Object> & actor_objs, Message & msg) {
        Meta m = msg.meta;
        const Actor_Object& actor_obj = actor_objs[m.step];

        msg.max_data_size = config_->max_data_size;
        msg.data.clear();
        msg.data.emplace_back(history_t(), vector<value_t>());

        // Get V/E from actor params
        msg.data[0].second.insert(msg.data[0].second.end(),
                                  make_move_iterator(actor_obj.params.begin() + 2),
                                  make_move_iterator(actor_obj.params.end()));

        vector<Message> vec;
        msg.CreateNextMsg(actor_objs, msg.data, num_thread_, core_affinity_, vec);

        // Send Message
        for (auto& msg_ : vec) {
            mailbox_->Send(tid, msg_);
        }
    }

    void InitWithIndex(int tid, const vector<Actor_Object> & actor_objs, Message & msg) {
        Meta m = msg.meta;
        const Actor_Object& actor_obj = actor_objs[m.step];

        // store all predicate
        vector<pair<int, PredicateValue>> pred_chain;

        // Get Params
        assert((actor_obj.params.size() - 2) % 3 == 0);  // make sure input format
        Element_T inType = (Element_T) Tool::value_t2int(actor_obj.params.at(0));
        int numParamsGroup = (actor_obj.params.size() - 2) / 3;  // number of groups of params

        // Create predicate chain for this query
        for (int i = 0; i < numParamsGroup; i++) {
            int pos = i * 3 + 2;
            // Get predicate params
            int pid = Tool::value_t2int(actor_obj.params.at(pos));
            Predicate_T pred_type = (Predicate_T) Tool::value_t2int(actor_obj.params.at(pos + 1));
            vector<value_t> pred_params;
            Tool::value_t2vec(actor_obj.params.at(pos + 2), pred_params);
            pred_chain.emplace_back(pid, PredicateValue(pred_type, pred_params));
        }

        msg.max_data_size = config_->max_data_size;
        msg.data.clear();
        msg.data.emplace_back(history_t(), vector<value_t>());
        index_store_->GetElements(inType, pred_chain, msg.data[0].second);

        vector<Message> vec;
        msg.CreateNextMsg(actor_objs, msg.data, num_thread_, core_affinity_, vec);

        // Send Message
        for (auto& msg_ : vec) {
            mailbox_->Send(tid, msg_);
        }
    }

    void InitWithoutIndex(int tid, const vector<Actor_Object> & actor_objs, Message & msg) {
        if (!is_ready_) {
            if (thread_mutex_.try_lock()) {
                InitData();
                is_ready_ = true;
                thread_mutex_.unlock();
            } else {
                // wait until InitMsg finished
                while (!thread_mutex_.try_lock()) {}
                thread_mutex_.unlock();
            }
        }
        Meta m = msg.meta;
        const Actor_Object& actor_obj = actor_objs[m.step];

        // Get init element type
        Element_T inType = (Element_T)Tool::value_t2int(actor_obj.params.at(0));
        vector<Message>* msg_vec;

        if (actor_objs[m.step + 1].actor_type == ACTOR_T::COUNT) {
            if (inType == Element_T::VERTEX) {
                msg_vec = &vtx_count_msgs;
            } else if (inType == Element_T::EDGE) {
                msg_vec = &edge_count_msgs;
            }
        } else {
            if (inType == Element_T::VERTEX) {
                msg_vec = &vtx_msgs;
            } else if (inType == Element_T::EDGE) {
                msg_vec = &edge_msgs;
            }
        }

        m.step++;
        // update meta
        m.msg_type = MSG_T::SPAWN;
        if (actor_objs[m.step].IsBarrier()) {
            m.msg_type = MSG_T::BARRIER;
            m.recver_nid = m.parent_nid;
        }

        thread_mutex_.lock();
        // Send Message
        for (auto& msg : *msg_vec) {
            // Update routing infos
            msg.meta.qid = m.qid;
            msg.meta.msg_type = m.msg_type;
            msg.meta.recver_nid = m.recver_nid;
            msg.meta.recver_tid = core_affinity_->GetThreadIdForActor(actor_objs[m.step].actor_type);
            msg.meta.parent_nid = m.parent_nid;
            msg.meta.parent_tid = m.parent_tid;

            mailbox_->Send(tid, msg);
        }
        thread_mutex_.unlock();
    }
};

#endif  // ACTOR_INIT_ACTOR_HPP_
