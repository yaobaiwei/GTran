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
#include "storage/data_store.hpp"
#include "utils/tool.hpp"
#include "utils/timer.hpp"


#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"

class InitActor : public AbstractActor {
 public:
    InitActor(int id,
            DataStore* data_store,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity,
            IndexStore * index_store,
            int num_nodes) :
        AbstractActor(id, data_store, core_affinity),
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
            ((snapshot->ReadData("init_actor_vtx_data", vtx_data, ReadMailboxDataImpl)) ? 1 : 0);
        snapshot_read_cnt +=
            ((snapshot->ReadData("init_actor_edge_data", edge_data, ReadMailboxDataImpl)) ? 1 : 0);
        snapshot_read_cnt +=
            ((snapshot->ReadData("init_actor_vtx_data_count", vtx_data_count, ReadMailboxDataImpl)) ? 1 : 0);
        snapshot_read_cnt +=
            ((snapshot->ReadData("init_actor_edge_data_count", edge_data_count, ReadMailboxDataImpl)) ? 1 : 0);

        if (snapshot_read_cnt == 4) {
            is_ready_ = true;
        } else {
            // atomic, all fail
            vtx_data.resize(0);
            edge_data.resize(0);
            vtx_data_count.resize(0);
            edge_data_count.resize(0);
        }
    }

    virtual ~InitActor() {}

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
    vector<AbstractMailbox::mailbox_data_t> vtx_data;
    vector<AbstractMailbox::mailbox_data_t> edge_data;

    // msg for count actor
    vector<AbstractMailbox::mailbox_data_t> vtx_data_count;
    vector<AbstractMailbox::mailbox_data_t> edge_data_count;

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
        snapshot->WriteData("init_actor_vtx_data", vtx_data, WriteMailboxDataImpl);
        snapshot->WriteData("init_actor_vtx_data_count", vtx_data_count, WriteMailboxDataImpl);
        uint64_t end_t = timer::get_usec();
        cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for initV_Msg in init_actor" << endl;

        start_t = timer::get_usec();
        InitEdgeData(m);
        snapshot->WriteData("init_actor_edge_data", edge_data, WriteMailboxDataImpl);
        snapshot->WriteData("init_actor_edge_data_count", edge_data_count, WriteMailboxDataImpl);
        end_t = timer::get_usec();
        cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for initE_Msg in init_actor" << endl;
    }

    void InitVtxData(const Meta& m) {
        vector<vid_t> vid_list;
        data_store_->GetAllVertices(vid_list);
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

        vector<Message> vtx_msgs;
        do {
            Message msg(m);
            msg.max_data_size = config_->max_data_size;
            msg.InsertData(data);
            vtx_msgs.push_back(move(msg));
        } while ((data.size() != 0));

        string num = "\t" + to_string(vtx_msgs.size());
        for (auto & msg_ : vtx_msgs) {
            msg_.meta.msg_path += num;
            AbstractMailbox::mailbox_data_t data;
            data.stream << msg_;
            vtx_data.push_back(move(data));
        }

        Message count_msg(m);
        count_msg.max_data_size = config_->max_data_size;
        value_t v;
        Tool::str2int(to_string(count), v);
        count_msg.data.emplace_back(history_t(), vector<value_t>{v});
        AbstractMailbox::mailbox_data_t msg_data;
        msg_data.stream << count_msg;
        vtx_data_count.push_back(move(msg_data));
    }

    void InitEdgeData(const Meta& m) {
        vector<eid_t> eid_list;
        data_store_->GetAllEdges(eid_list);
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

        vector<Message> edge_msgs;
        do {
            Message msg(m);
            msg.max_data_size = config_->max_data_size;
            msg.InsertData(data);
            edge_msgs.push_back(move(msg));
        } while ((data.size() != 0));

        string num = "\t" + to_string(edge_msgs.size());
        for (auto & msg_ : edge_msgs) {
            msg_.meta.msg_path += num;
            AbstractMailbox::mailbox_data_t data;
            data.stream << msg_;
            edge_data.push_back(move(data));
        }

        Message count_msg(m);
        count_msg.max_data_size = config_->max_data_size;
        value_t v;
        Tool::str2int(to_string(count), v);
        count_msg.data.emplace_back(history_t(), vector<value_t>{v});
        AbstractMailbox::mailbox_data_t msg_data;
        msg_data.stream << count_msg;
        edge_data_count.push_back(move(msg_data));
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
        msg.CreateNextMsg(actor_objs, msg.data, num_thread_, data_store_, core_affinity_, vec);

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
        msg.CreateNextMsg(actor_objs, msg.data, num_thread_, data_store_, core_affinity_, vec);

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
        vector<AbstractMailbox::mailbox_data_t>* data_vec;

        if (actor_objs[m.step + 1].actor_type == ACTOR_T::COUNT) {
            if (inType == Element_T::VERTEX) {
                data_vec = &vtx_data_count;
            } else if (inType == Element_T::EDGE) {
                data_vec = &edge_data_count;
            }
        } else {
            if (inType == Element_T::VERTEX) {
                data_vec = &vtx_data;
            } else if (inType == Element_T::EDGE) {
                data_vec = &edge_data;
            }
        }

        // update meta
        m.step++;
        m.msg_type = MSG_T::SPAWN;
        if (actor_objs[m.step].IsBarrier()) {
            m.msg_type = MSG_T::BARRIER;
            m.recver_nid = m.parent_nid;
        }

        thread_mutex_.lock();
        // Send Message
        for (auto& data : *data_vec) {
            m.recver_tid = core_affinity_->GetThreadIdForActor(actor_objs[m.step].actor_type);
            update_route(data.stream, m);
            data.dst_nid = m.recver_nid;
            data.dst_tid = m.recver_tid;

            mailbox_->Send(tid, data);
        }
        thread_mutex_.unlock();
    }
};

#endif  // ACTOR_INIT_ACTOR_HPP_
