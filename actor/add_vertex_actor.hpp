/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)

*/

#ifndef ACTOR_ADD_VERTEX_ACTOR_HPP_
#define ACTOR_ADD_VERTEX_ACTOR_HPP_

#include <string>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "base/core_affinity.hpp"
#include "base/type.hpp"
#include "core/message.hpp"
#include "layout/data_storage.hpp"

class AddVertexActor : public AbstractActor {
 public:
    AddVertexActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity) :
        AbstractActor(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(ACTOR_T::ADDV) {
        config_ = Config::GetInstance();
    }

    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();
        // Get Actor_Object
        Meta & m = msg.meta;
        Actor_Object actor_obj = qplan.actors[m.step];

        // Get Params
        int lid = static_cast<int>(Tool::value_t2int(actor_obj.params.at(0)));
        process_add_vertex(qplan, lid, msg.data);

        vector<Message> msg_vec;
        msg.CreateNextMsg(qplan.actors, msg.data, num_thread_, core_affinity_, msg_vec);
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

 private:
    // Number of Threads
    int num_thread_;

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config* config_;

    void process_add_vertex(const QueryPlan & qplan, int label_id, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & pair : data) {
            vector<value_t> newData;
            // The number of vertices will be added is the number of result from last step.
            //  If addV is the first step, the default value should be one.
            //  TODO(nick) : support g.addV()
            for (auto & vertex : pair.second) {
                vid_t new_v_id = data_storage_->ProcessAddV(label_id, qplan.trxid, qplan.st);
                value_t new_val;
                Tool::str2int(to_string(new_v_id.value()), new_val);
                newData.emplace_back(new_val);
            }
            pair.second.swap(newData);
        }
    }
};

#endif  // ACTOR_ADD_VERTEX_ACTOR_HPP_
