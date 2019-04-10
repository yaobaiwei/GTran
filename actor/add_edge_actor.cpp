/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)

*/

#include "actor/add_edge_actor.hpp"

void AddEdgeActor::process(const QueryPlan & qplan, Message & msg) {
    int tid = TidMapper::GetInstance()->GetTid();
    // Get Actor_Object
    Meta & m = msg.meta;
    Actor_Object actor_obj = qplan.actors[m.step];

    // Get Params
    int lid = static_cast<int>(Tool::value_t2int(actor_obj.params.at(0)));
    bool success = true;
    for (auto & pair : msg.data) {
        // TODO(nick) : support g.addE()
        vector<value_t>::iterator itr = pair.second.begin();
        do {
            eid_t e_id;
            uint2eid_t(Tool::value_t2uint64_t(*itr), e_id);
            bool is_src_v_local = false, is_dst_v_local = false;

            // outV
            if (id_mapper_->GetMachineIdForVertex(e_id.out_v) == machine_id_) {
                is_src_v_local = true;
                if (!data_storage_->ProcessAddE(e_id, lid, true, qplan.trxid, qplan.st)) {
                    success = false;
                }
            }

            // inV
            if (id_mapper_->GetMachineIdForVertex(e_id.in_v) == machine_id_) {
                is_dst_v_local = true;
                if (!data_storage_->ProcessAddE(e_id, lid, false, qplan.trxid, qplan.st)) {
                    success = false;
                }
            }

            if (is_dst_v_local && !is_src_v_local) {
                // erase this eid since it's duplicated for src_v machine and dst_v machine
                itr = pair.second.erase(itr);
            } else {
                itr++;
            } 
        } while (itr != pair.second.end());
    }

    vector<Message> msg_vec;
    if (success) {
        msg.CreateNextMsg(qplan.actors, msg.data, num_thread_, core_affinity_, msg_vec);
    } else {
        msg.CreateAbortMsg(qplan.actors, msg_vec);
    }
    for (auto& msg : msg_vec) {
        mailbox_->Send(tid, msg);
    }
}
