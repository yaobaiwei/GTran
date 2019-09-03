/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)

*/

#include "actor/drop_actor.hpp"

void DropActor::process(const QueryPlan & qplan, Message & msg) {
    int tid = TidMapper::GetInstance()->GetTid();

    // Get Actor_Object
    Meta & m = msg.meta;
    Actor_Object actor_obj = qplan.actors[m.step];

    // Prepare for Update Data (RCT and Index)
    vector<uint64_t> update_ids;
    vector<value_t> update_vals;
    Primitive_T pmt_type;

    // Get Params
    Element_T elem_type = static_cast<Element_T>(Tool::value_t2int(actor_obj.params.at(0)));
    bool isProperty = static_cast<bool>(Tool::value_t2int(actor_obj.params.at(1)));

    PROCESS_STAT process_stat = processDrop(qplan, msg.data, elem_type, isProperty, update_ids, update_vals, pmt_type);

    // Create Message
    vector<Message> msg_vec;
    if (process_stat == PROCESS_STAT::SUCCESS) {
        // Insert Updates Information into RCT Table if success
        pmt_rct_table_->InsertRecentActionSet(pmt_type, qplan.trxid, update_ids);

        // Insert Update data to topo index (Currently only topo)
        if (!isProperty && elem_type == Element_T::VERTEX) {
            index_store_->InsertToUpdateBuffer(qplan.trxid, update_ids, ID_T::VID, false);
        } else if (!isProperty && elem_type == Element_T::EDGE) {
            index_store_->InsertToUpdateBuffer(qplan.trxid, update_ids, ID_T::EID, false);
        } else if (isProperty && elem_type == Element_T::VERTEX) {
            index_store_->InsertToUpdateBuffer(qplan.trxid, update_ids, ID_T::VPID, false, NULL, &update_vals);
        } else if (isProperty && elem_type == Element_T::EDGE) {
            index_store_->InsertToUpdateBuffer(qplan.trxid, update_ids, ID_T::EPID, false, NULL, &update_vals);
        } else {
            cout << "[Drop Actor] Unexpected type combination" << endl;
            assert(false);
        }

        msg.CreateNextMsg(qplan.actors, msg.data, num_thread_, core_affinity_, msg_vec);
    } else {
        string abort_info = "Abort with [Processing][DropActor::process]" + abort_reason_map[process_stat];
        msg.CreateAbortMsg(qplan.actors, msg_vec, abort_info);
    }

    // Send Message
    for (auto& msg : msg_vec) {
        mailbox_->Send(tid, msg);
    }
}

PROCESS_STAT DropActor::processDrop(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data, Element_T elem_type,
        bool isProperty, vector<uint64_t> & update_ids, vector<value_t> & update_vals, Primitive_T& pmt_type) {
    vector<value_t> newData;  // For dropV, store connected edge
    PROCESS_STAT process_stat;
    for (auto & pair : data) {
        PushToRWRecord(qplan.trxid, pair.second.size(), false);
        for (auto & val : pair.second) {
            if (elem_type == Element_T::VERTEX) {
                if (isProperty) {
                    CHECK(val.type == PropKeyValueType);
                    uint64_t vpid_uint64 = Tool::value_t2uint64_t(val);
                    update_ids.emplace_back(vpid_uint64);
                    pmt_type = Primitive_T::DVP;

                    vpid_t vpid;
                    uint2vpid_t(vpid_uint64, vpid);
                    value_t old_val;
                    process_stat = data_storage_->ProcessDropVP(vpid, qplan.trxid, qplan.st, old_val);
                    if (process_stat != PROCESS_STAT::SUCCESS) {
                        return process_stat;
                    }
                    update_vals.emplace_back(old_val);
                } else {
                    int vid_int = Tool::value_t2int(val);
                    update_ids.emplace_back(static_cast<uint64_t>(vid_int));
                    pmt_type = Primitive_T::DV;

                    vid_t v_id(vid_int);
                    vector<eid_t> in_eids; vector<eid_t> out_eids;
                    // Drop V
                    process_stat = data_storage_->ProcessDropV(v_id, qplan.trxid, qplan.st, in_eids, out_eids);
                    if (process_stat != PROCESS_STAT::SUCCESS) {
                        return process_stat; 
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
                    uint64_t epid_uint64 = Tool::value_t2uint64_t(val);
                    update_ids.emplace_back(epid_uint64);
                    pmt_type = Primitive_T::DEP;

                    epid_t epid;
                    uint2epid_t(epid_uint64, epid);
                    value_t old_val;
                    process_stat = data_storage_->ProcessDropEP(epid, qplan.trxid, qplan.st, old_val);
                    if (process_stat != PROCESS_STAT::SUCCESS) {
                        return process_stat;
                    }
                    update_vals.emplace_back(old_val);
                } else {
                    uint64_t eid_uint64 = Tool::value_t2uint64_t(val);
                    update_ids.emplace_back(eid_uint64);
                    pmt_type = Primitive_T::DE;

                    eid_t e_id;
                    uint2eid_t(eid_uint64, e_id);
                    // outE
                    if (id_mapper_->GetMachineIdForVertex(e_id.out_v) == machine_id_) {
                        process_stat = data_storage_->ProcessDropE(e_id, true, qplan.trxid, qplan.st);
                        if (process_stat != PROCESS_STAT::SUCCESS) {
                            return process_stat; 
                        }
                    }

                    // inE
                    if (id_mapper_->GetMachineIdForVertex(e_id.in_v) == machine_id_) {
                        process_stat = data_storage_->ProcessDropE(e_id, false, qplan.trxid, qplan.st);
                        if (process_stat != PROCESS_STAT::SUCCESS) {
                            return process_stat; 
                        }
                    }
                }
            } else {
                cout << "[Error] Unexpected element type in DropActor" << endl;
                CHECK(false);
            }
        }
    }
    data.clear();
    data.emplace_back(history_t(), newData);
    return PROCESS_STAT::SUCCESS;
}
