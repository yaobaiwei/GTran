/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)

*/

#include "actor/drop_actor.hpp"

void DropActor::process(const QueryPlan & qplan, Message & msg) {
    int tid = TidMapper::GetInstance()->GetTid();

    // Get Actor_Object
    Meta & m = msg.meta;
    Actor_Object actor_obj = qplan.actors[m.step];

    // Get Params
    Element_T elem_type = static_cast<Element_T>(Tool::value_t2int(actor_obj.params.at(0)));
    bool isProperty = static_cast<bool>(Tool::value_t2int(actor_obj.params.at(1)));

    bool success = processDrop(qplan, msg.data, elem_type, isProperty);

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

bool DropActor::processDrop(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data, Element_T elem_type, bool isProperty) {
    vector<value_t> newData;  // For dropV, store connected edge
    for (auto & pair : data) {
        for (auto & val : pair.second) {
            if (elem_type == Element_T::VERTEX) {
                if (isProperty) {
                    CHECK(val.type == PropKeyValueType);
                    vpid_t vpid;
                    uint2vpid_t(Tool::value_t2uint64_t(val), vpid);
                    if (!data_storage_->ProcessDropVP(vpid, qplan.trxid, qplan.st)) {
                        return false;
                    }
                } else {
                    vid_t v_id(Tool::value_t2int(val));
                    vector<eid_t> in_eids; vector<eid_t> out_eids;
                    // Drop V
                    if (!data_storage_->ProcessDropV(v_id, qplan.trxid, qplan.st, in_eids, out_eids)) {
                        return false; 
                    }

                    if (in_eids.size() == 0 && out_eids.size() == 0) {
                        // there is no edge to drop
                        continue;
                    }

                    for (auto & eid : in_eids) {
                        value_t tmp_v;
                        Tool::uint64_t2value_t(eid.value(), tmp_v);
                        newData.emplace_back(move(tmp_v));
                    }
                    for (auto & eid : out_eids) {
                        value_t tmp_v;
                        Tool::uint64_t2value_t(eid.value(), tmp_v);
                        newData.emplace_back(move(tmp_v));
                    }
                }
            } else if (elem_type == Element_T::EDGE) {
                if (isProperty) {
                    CHECK(val.type == PropKeyValueType);
                    epid_t epid;
                    uint2epid_t(Tool::value_t2uint64_t(val), epid);
                    if (!data_storage_->ProcessDropEP(epid, qplan.trxid, qplan.st)) {
                        return false;
                    }
                } else {
                    eid_t e_id;
                    uint2eid_t(Tool::value_t2uint64_t(val), e_id);
                    // outE
                    if (id_mapper_->GetMachineIdForVertex(e_id.out_v) == machine_id_) {
                        if (!data_storage_->ProcessDropE(e_id, true, qplan.trxid, qplan.st)) {
                            return false; 
                        }
                    }

                    // inE
                    if (id_mapper_->GetMachineIdForVertex(e_id.in_v) == machine_id_) {
                        if (!data_storage_->ProcessDropE(e_id, false, qplan.trxid, qplan.st)) {
                            return false; 
                        }
                    }
                }
            } else {
                cout << "[Error] Unexpected element type in DropActor" << endl;
                return false;
            }
        }
    }
    data.clear();
    data.emplace_back(history_t(), newData);
    return true;
}
