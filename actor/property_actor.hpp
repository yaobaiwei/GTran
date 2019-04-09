/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)

*/

#ifndef ACTOR_PROPERTY_ACTOR_HPP_
#define ACTOR_PROPERTY_ACTOR_HPP_

#include <string>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "base/core_affinity.hpp"
#include "base/type.hpp"
#include "core/message.hpp"
#include "core/factory.hpp"
#include "core/id_mapper.hpp"
#include "layout/data_storage.hpp"
#include "utils/tool.hpp"

class PropertyActor : public AbstractActor {
 public:
    PropertyActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity) :
        AbstractActor(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(ACTOR_T::PROPERTY) {
        config_ = Config::GetInstance();
        trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();
    }

    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();
        // Get Actor_Object
        Meta & m = msg.meta;
        Actor_Object actor_obj = qplan.actors[m.step];

        // Get Params
        Element_T elem_type = static_cast<Element_T>(Tool::value_t2int(actor_obj.params.at(0)));
        int pid = static_cast<int>(Tool::value_t2int(actor_obj.params.at(1)));
        value_t new_val = actor_obj.params.at(2);

        bool success = true;
        switch(elem_type) {
          case Element_T::VERTEX:
            success = processVertexProperty(qplan, msg.data, pid, new_val);
            break;
          case Element_T::EDGE:
            success = processEdgeProperty(qplan, msg.data, pid, new_val);
            break;
          default:
            success = false;
            cout << "[Error] Unexpected Element Type in PropertyActor" << endl;
        }

        // Create Message
        vector<Message> msg_vec;
        if (success) {
            msg.CreateNextMsg(qplan.actors, msg.data, num_thread_, core_affinity_, msg_vec);
        } else {
            msg.CreateAbortMsg(qplan.actors, msg_vec);
        }

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }

    }

 private:
    // Node Info 
    int num_thread_;

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config* config_;

    // TrxTableStub
    TrxTableStub * trx_table_stub_;

    bool processVertexProperty(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data, int propertyId, value_t new_val) {
        for (auto & pair : data) {
            for (auto & val : pair.second) {
                vpid_t vpid(Tool::value_t2int(val), propertyId);
                if (!data_storage_->ProcessModifyVP(vpid, new_val, qplan.trxid, qplan.st)) {
                    return false;
                }
            }
        }
        return true;
    }

    bool processEdgeProperty(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data, int propertyId, value_t new_val) {
        for (auto & pair : data) {
            for (auto & val : pair.second) {
                eid_t eid;
                uint2eid_t(Tool::value_t2uint64_t(val), eid);
                epid_t epid(eid, propertyId);
                if (!data_storage_->ProcessModifyEP(epid, new_val, qplan.trxid, qplan.st)) {
                    return false;
                }
            }
        }
        return true;
    }
};

#endif  // ACTOR_PROPERTY_ACTOR_HPP_
