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
#include "base/node.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "layout/index_store.hpp"
#include "utils/tool.hpp"
#include "utils/timer.hpp"

class InitActor : public AbstractActor {
 public:
    InitActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity,
            int num_nodes) :
        AbstractActor(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        num_nodes_(num_nodes),
        type_(ACTOR_T::INIT),
        is_ready_(false) {
        config_ = Config::GetInstance();
        index_store_ = IndexStore::GetInstance();
    }

    virtual ~InitActor() {}

    // TODO(nick): Need to support dynamic V/E cache
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        Actor_Object actor_obj = qplan.actors[msg.meta.step];
        assert(actor_obj.params.size() >= 2);
        bool with_input = Tool::value_t2int(actor_obj.params[1]);

        vector<pair<history_t, vector<value_t>>> init_data;
        if (with_input) {
            InitWithInput(tid, qplan.actors, init_data, msg);
        } else {
            InitWithoutIndex(tid, qplan, init_data, msg);
        }

        vector<Message> msg_vec;
        msg.CreateNextMsg(qplan.actors, init_data, num_thread_, core_affinity_, msg_vec);
        for (auto & msg_ : msg_vec) {
            mailbox_->Send(tid, msg_);
        }

        // if (with_input) {
        //     InitWithInput(tid, qplan.actors, msg);
        // } else if (actor_obj.params.size() == 2) {
        //     InitWithoutIndex(tid, qplan.actors, msg);
        // } else {
        //     InitWithIndex(tid, qplan.actors, msg);
        // }
    }

    bool valid(uint64_t TrxID, vector<Actor_Object*> & actor_list, const vector<rct_extract_data_t> & check_set) {
        // For InitActor, input set also need to be validated.
        // However, it's unnecessary to record input set since there are only 2 situations:
        // 1) g.V() --> All;
        // 2) g.V(A) --> No Need to Validate;
        for (auto & actor_obj : actor_list) {
            assert(actor_obj->actor_type == ACTOR_T::INIT);
            vector<uint64_t> local_check_set;

            // Analysis params
            assert(actor_obj->params.size() >= 2);
            Element_T inType = (Element_T)Tool::value_t2int(actor_obj->params.at(0));
            bool with_input = Tool::value_t2int(actor_obj->params[1]);

            if (!with_input) {
                for (auto & val : check_set) {
                    if (get<2>(val) == inType) {
                        return false;  // Once find one --> Abort
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

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // Pointer of index store
    IndexStore * index_store_;

    /* =============OLAP Impl=============== */
    // Ensure only one thread ever runs the actor
    // std::mutex thread_mutex_;

    // Cached data
    // vector<Message> vtx_msgs;
    // vector<Message> edge_msgs;

    // msg for count actor
    // vector<Message> vtx_count_msgs;
    // vector<Message> edge_count_msgs;
    /* =============OLAP Impl=============== */

    void InitData(const QueryPlan& qplan, Element_T inType, vector<pair<history_t, vector<value_t>>> & init_data) {
        // convert id to msg
        Meta m;
        m.step = 1;
        m.msg_path = to_string(num_nodes_);

        uint64_t start_t, end_t;
        start_t = timer::get_usec();
        if (inType == Element_T::VERTEX) {
            InitVtxData(m, qplan, init_data);
            end_t = timer::get_usec();
            cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for initV_Msg in init_actor" << endl;
        } else {
            InitEdgeData(m, qplan, init_data);
            end_t = timer::get_usec();
            cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for initE_Msg in init_actor" << endl;
        }
    }

    void InitVtxData(const Meta& m, const QueryPlan& qplan, vector<pair<history_t, vector<value_t>>> & init_data) {
        vector<vid_t> vid_list;
        uint64_t start_time = timer::get_usec();
        // data_storage_->GetAllVertices(qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, vid_list);
        index_store_->ReadVtxTopoIndex(qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, vid_list);
        uint64_t end_time = timer::get_usec();
        cout << "[Timer] " << (end_time - start_time) << " us for GetAllVertices()" << endl;
        uint64_t count = vid_list.size();

        // vector<pair<history_t, vector<value_t>>> data;
        init_data.clear();
        init_data.emplace_back(history_t(), vector<value_t>());
        init_data[0].second.reserve(count);

        for (auto& vid : vid_list) {
            value_t v;
            Tool::str2int(to_string(vid.value()), v);
            init_data[0].second.push_back(v);
        }
        vector<vid_t>().swap(vid_list);
    }

    void InitEdgeData(const Meta& m, const QueryPlan& qplan, vector<pair<history_t, vector<value_t>>>& init_data) {
        vector<eid_t> eid_list;
        uint64_t start_time = timer::get_usec();
        // data_storage_->GetAllEdges(qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, eid_list);
        index_store_->ReadEdgeTopoIndex(qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, eid_list);
        uint64_t end_time = timer::get_usec();
        cout << "[Timer] " << (end_time - start_time) << " us for GetAllEdges()" << endl;
        uint64_t count = eid_list.size();

        // vector<pair<history_t, vector<value_t>>> data;
        init_data.clear();
        init_data.emplace_back(history_t(), vector<value_t>());
        init_data[0].second.reserve(count);
        for (auto& eid : eid_list) {
            value_t v;
            Tool::str2uint64_t(to_string(eid.value()), v);
            init_data[0].second.push_back(v);
        }
        vector<eid_t>().swap(eid_list);
    }

    void InitWithInput(int tid, const vector<Actor_Object> & actor_objs, vector<pair<history_t, vector<value_t>>>& init_data, Message & msg) {
        Meta m = msg.meta;
        const Actor_Object& actor_obj = actor_objs[m.step];
        msg.max_data_size = config_->max_data_size;

        init_data.emplace_back(history_t(), vector<value_t>());
        // Get V/E from actor params
        init_data[0].second.insert(init_data[0].second.end(),
                                 make_move_iterator(actor_obj.params.begin() + 2),
                                 make_move_iterator(actor_obj.params.end()));
    }

    void InitWithIndex(int tid, const vector<Actor_Object> & actor_objs, Message & msg) {
        // Meta m = msg.meta;
        // const Actor_Object& actor_obj = actor_objs[m.step];

        // // store all predicate
        // vector<pair<int, PredicateValue>> pred_chain;

        // // Get Params
        // assert((actor_obj.params.size() - 2) % 3 == 0);  // make sure input format
        // Element_T inType = (Element_T) Tool::value_t2int(actor_obj.params.at(0));
        // int numParamsGroup = (actor_obj.params.size() - 2) / 3;  // number of groups of params

        // // Create predicate chain for this query
        // for (int i = 0; i < numParamsGroup; i++) {
        //     int pos = i * 3 + 2;
        //     // Get predicate params
        //     int pid = Tool::value_t2int(actor_obj.params.at(pos));
        //     Predicate_T pred_type = (Predicate_T) Tool::value_t2int(actor_obj.params.at(pos + 1));
        //     vector<value_t> pred_params;
        //     Tool::value_t2vec(actor_obj.params.at(pos + 2), pred_params);
        //     pred_chain.emplace_back(pid, PredicateValue(pred_type, pred_params));
        // }

        // msg.max_data_size = config_->max_data_size;
        // msg.data.clear();
        // msg.data.emplace_back(history_t(), vector<value_t>());
        // index_store_->GetElements(inType, pred_chain, msg.data[0].second);

        // vector<Message> vec;
        // msg.CreateNextMsg(actor_objs, msg.data, num_thread_, core_affinity_, vec);

        // // Send Message
        // for (auto& msg_ : vec) {
        //     mailbox_->Send(tid, msg_);
        // }
    }

    void InitWithoutIndex(int tid, const QueryPlan& qplan, vector<pair<history_t, vector<value_t>>> & init_data, Message & msg) {
        Meta m = msg.meta;
        const Actor_Object& actor_obj = qplan.actors[m.step];

        // Get init element type
        Element_T inType = (Element_T)Tool::value_t2int(actor_obj.params.at(0));

        // No need to lock since init is for each transaction rather than whole system.
        InitData(qplan, inType, init_data);

        m.step++;
        // update meta
        m.msg_type = MSG_T::SPAWN;
        if (qplan.actors[m.step].IsBarrier()) {
            m.msg_type = MSG_T::BARRIER;
            m.recver_nid = m.parent_nid;
        }
    }
};

#endif  // ACTOR_INIT_ACTOR_HPP_
