/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)

*/

#include "expert/add_edge_expert.hpp"

void AddEdgeExpert::process(const QueryPlan & qplan, Message & msg) {
    int tid = TidMapper::GetInstance()->GetTid();
    // Get Expert_Object
    Meta & m = msg.meta;
    Expert_Object expert_obj = qplan.experts[m.step];
    vector<uint64_t> update_data;

    // Get Params
    int lid = static_cast<int>(Tool::value_t2int(expert_obj.params.at(0)));
    bool success = true;
    PROCESS_STAT process_stat;

    string abort_info;
    for (auto & pair : msg.data) {
        // Record Write Set
        PushToRWRecord(qplan.trxid, pair.second.size(), false);

        vector<value_t>::iterator itr = pair.second.begin();
        do {
            eid_t e_id;
            uint64_t eid_uint64 = Tool::value_t2uint64_t(*itr);
            uint2eid_t(eid_uint64, e_id);

            bool is_src_v_local = false, is_dst_v_local = false;

            // outV
            if (id_mapper_->GetMachineIdForVertex(e_id.out_v) == machine_id_) {
                update_data.emplace_back(eid_uint64);
                is_src_v_local = true;
                process_stat = data_storage_->ProcessAddE(e_id, lid, true, qplan.trxid, qplan.st);
                if (process_stat != PROCESS_STAT::SUCCESS) {
                    success = false;
                    abort_info = "Abort with [Processing][DataStorage::ProcessAddE<OutE>(" +
                                         to_string(e_id.out_v) + "->" + to_string(e_id.in_v) + ")]" +
                                         abort_reason_map[process_stat];
                }
            }

            // inV
            if (id_mapper_->GetMachineIdForVertex(e_id.in_v) == machine_id_) {
                is_dst_v_local = true;
                process_stat = data_storage_->ProcessAddE(e_id, lid, false, qplan.trxid, qplan.st);
                if (process_stat != PROCESS_STAT::SUCCESS) {
                    success = false;
                    abort_info = "Abort with [Processing][DataStorage::ProcessAddE<InE>(" +
                                         to_string(e_id.out_v) + "->" + to_string(e_id.in_v) + ")]" +
                                         abort_reason_map[process_stat];
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
        // Insert Updates Information into RCT Table if success
        pmt_rct_table_->InsertRecentActionSet(Primitive_T::IE, qplan.trxid, update_data);

        // Insert Update data to Index Buffer
        index_store_->InsertToUpdateBuffer(qplan.trxid, update_data, ID_T::EID, true);

        msg.CreateNextMsg(qplan.experts, msg.data, num_thread_, core_affinity_, msg_vec);
    } else {
        msg.CreateAbortMsg(qplan.experts, msg_vec, abort_info);
    }
    for (auto& msg : msg_vec) {
        mailbox_->Send(tid, msg);
    }
}
