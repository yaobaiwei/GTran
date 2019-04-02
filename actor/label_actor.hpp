/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef ACTOR_LABEL_ACTOR_HPP_
#define ACTOR_LABEL_ACTOR_HPP_

#include <string>
#include <utility>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "utils/tool.hpp"

class LabelActor : public AbstractActor {
 public:
    LabelActor(int id,
            int machine_id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        AbstractActor(id, core_affinity),
        machine_id_(machine_id),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(ACTOR_T::LABEL) {
        config_ = Config::GetInstance();
    }

    // Label:
    //         Output all labels of input
    // Parmas:
    //         inType
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Actor_Object
        Meta & m = msg.meta;
        Actor_Object actor_obj = qplan.actors[m.step];

        // Get Params
        Element_T inType = (Element_T) Tool::value_t2int(actor_obj.params.at(0));

        switch (inType) {
          case Element_T::VERTEX:
            VertexLabel(qplan, msg.data);
            break;
          case Element_T::EDGE:
            EdgeLabel(qplan, msg.data);
            break;
          default:
                cout << "Wrong in type"  << endl;
        }

        // Create Message
        vector<Message> msg_vec;
        msg.CreateNextMsg(qplan.actors, msg.data, num_thread_, core_affinity_, msg_vec);

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
     }

 private:
    // Number of Threads
    int num_thread_;
    int machine_id_;

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config* config_;

    void VertexLabel(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> newData;
            for (auto & elem : data_pair.second) {
                vid_t v_id(Tool::value_t2int(elem));

                label_t label;
                data_storage_->GetVL(v_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, label);
                string keyStr;
                data_storage_->GetNameFromIndex(Index_T::V_LABEL, label, keyStr);

                value_t val;
                Tool::str2str(keyStr, val);
                newData.push_back(val);
            }

            data_pair.second.swap(newData);
        }
    }

    void EdgeLabel(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> newData;
            for (auto & elem : data_pair.second) {
                eid_t e_id;
                uint2eid_t(Tool::value_t2uint64_t(elem), e_id);

                label_t label;
                data_storage_->GetEL(e_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, label);
                string keyStr;
                data_storage_->GetNameFromIndex(Index_T::E_LABEL, label, keyStr);

                value_t val;
                Tool::str2str(keyStr, val);
                newData.push_back(val);
            }
            data_pair.second.swap(newData);
        }
    }
};

#endif  // ACTOR_LABEL_ACTOR_HPP_
