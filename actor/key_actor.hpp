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
#include "utils/tool.hpp"

class KeyActor : public AbstractActor {
 public:
    KeyActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity):
        AbstractActor(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(ACTOR_T::KEY) {}

    // Key:
    //  Output all keys of properties of input
    // Parmas:
    //  inType
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Actor_Object
        Meta & m = msg.meta;
        Actor_Object actor_obj = qplan.actors[m.step];

        // Get Params
        Element_T inType = (Element_T) Tool::value_t2int(actor_obj.params.at(0));

        // Record Input Set
        for (auto & data_pair : msg.data) {
            v_obj.RecordInputSetValueT(qplan.trxid, actor_obj.index, inType, data_pair.second, m.step == 1 ? true : false);
        }

        switch (inType) {
          case Element_T::VERTEX:
            VertexKeys(qplan, msg.data);
            break;
          case Element_T::EDGE:
            EdgeKeys(qplan, msg.data);
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

    bool valid(uint64_t TrxID, vector<Actor_Object*> & actor_list, const vector<rct_extract_data_t> & check_set) {
        for (auto & actor_obj : actor_list) {
            assert(actor_obj->actor_type == ACTOR_T::KEY);
            vector<uint64_t> local_check_set;

            // Analysis params
            Element_T inType = (Element_T) Tool::value_t2int(actor_obj->params.at(0));

            // Compare check_set and parameters
            for (auto & val : check_set) {
                if (get<2>(val) == inType) {
                    local_check_set.emplace_back(get<0>(val));
                }
            }

            if (local_check_set.size() != 0) {
                if (!v_obj.Validate(TrxID, actor_obj->index, local_check_set)) {
                    return false;
                }
            }
        }
        return true;
    }

    void clean_input_set(uint64_t TrxID) { v_obj.DeleteInputSet(TrxID); }

 private:
    // Number of Threads
    int num_thread_;

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // Validation Store
    ActorValidationObject v_obj;

    void VertexKeys(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> newData;
            for (auto & elem : data_pair.second) {
                vid_t v_id(Tool::value_t2int(elem));

                vector<vpid_t> vp_list;
                data_storage_->GetVPidList(v_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, vp_list);
                for (auto & pkey : vp_list) {
                    string keyStr;
                    data_storage_->GetNameFromIndex(Index_T::V_PROPERTY, static_cast<label_t>(pkey.pid), keyStr);

                    value_t val;
                    Tool::str2str(keyStr, val);
                    newData.push_back(val);
                }
            }
            data_pair.second.swap(newData);
        }
    }

    void EdgeKeys(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> newData;
            for (auto & elem : data_pair.second) {
                eid_t e_id;
                uint2eid_t(Tool::value_t2uint64_t(elem), e_id);

                vector<epid_t> ep_list;
                data_storage_->GetEPidList(e_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, ep_list);
                for (auto & pkey : ep_list) {
                    string keyStr;
                    data_storage_->GetNameFromIndex(Index_T::E_PROPERTY, static_cast<label_t>(pkey.pid), keyStr);

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
