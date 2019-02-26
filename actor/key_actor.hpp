/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef ACTOR_KEY_ACTOR_HPP_
#define ACTOR_KEY_ACTOR_HPP_

#include <string>
#include <utility>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "actor/actor_validation_object.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"

class KeyActor : public AbstractActor {
 public:
    KeyActor(int id,
            DataStore* data_store,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity):
        AbstractActor(id, data_store, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(ACTOR_T::KEY) {}

    // Key:
    //  Output all keys of properties of input
    // Parmas:
    //  inType
    void process(const vector<Actor_Object> & actor_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Actor_Object
        Meta & m = msg.meta;
        Actor_Object actor_obj = actor_objs[m.step];

        // Get Params
        Element_T inType = (Element_T) Tool::value_t2int(actor_obj.params.at(0));

        // Record Input Set
        // TODO(Aaronchangji)
        //  : Get trxID from message
        //  : step_number is actually index_number for same step in transaction
        for (auto & data_pair : msg.data) {
            // v_obj.RecordInputSetValueT(trxID, step_num, inType, data_pair.second, step_num == 1 ? true : false);
            v_obj.RecordInputSetValueT(m.qid, m.step, inType, data_pair.second, m.step == 1 ? true : false);
        }

        switch(inType) {
            case Element_T::VERTEX:
                VertexKeys(tid, msg.data);
                break;
            case Element_T::EDGE:
                EdgeKeys(tid, msg.data);
                break;
            default:
                cout << "Wrong in type"  << endl;
        }

        // Create Message
        vector<Message> msg_vec;
        msg.CreateNextMsg(actor_objs, msg.data, num_thread_, data_store_, core_affinity_, msg_vec);

        // Send Message
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

    // Validation Store
    ActorValidationObject v_obj;

    void VertexKeys(int tid, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> newData;
            for (auto & elem : data_pair.second) {
                vid_t v_id(Tool::value_t2int(elem));

                Vertex* vtx = data_store_->GetVertex(v_id);
                for (auto & pkey : vtx->vp_list) {
                    string keyStr;
                    data_store_->GetNameFromIndex(Index_T::V_PROPERTY, pkey, keyStr);

                    value_t val;
                    Tool::str2str(keyStr, val);
                    newData.push_back(val);
                }
            }
            data_pair.second.swap(newData);
        }
    }

    void EdgeKeys(int tid, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> newData;
            for (auto & elem : data_pair.second) {
                eid_t e_id;
                uint2eid_t(Tool::value_t2uint64_t(elem), e_id);

                Edge* edge = data_store_->GetEdge(e_id);
                for (auto & pkey : edge->ep_list) {
                    string keyStr;
                    data_store_->GetNameFromIndex(Index_T::E_PROPERTY, pkey, keyStr);

                    value_t val;
                    Tool::str2str(keyStr, val);
                    newData.push_back(val);
                }
            }
            data_pair.second.swap(newData);
        }
    }
};

#endif  // ACTOR_KEY_ACTOR_HPP_
