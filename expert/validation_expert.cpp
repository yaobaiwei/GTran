// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "expert/validation_expert.hpp"
void ValidationExpert::process(const QueryPlan & qplan, Message & msg) {
    int tid = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);

    // Move Update Data of Transaction from IndexBuffer to IndexRegion
    uint64_t self_ct; TRX_STAT stat;
    trx_table_stub_->read_ct(qplan.trxid, stat, self_ct);
    index_store_->MoveTopoBufferToRegion(qplan.trxid, self_ct);

    // SNAPSHOT ISOLATION directly skip validation phase
    if (config_->isolation_level == ISOLATION_LEVEL::SNAPSHOT) {
        // Create Message
        vector<Message> msg_vec;
        msg.CreateNextMsg(qplan.experts, msg.data, num_thread_, core_affinity_, msg_vec);

        // Send Message
        for (auto& msg : msg_vec) {
            msg.meta.msg_type = MSG_T::COMMIT;
            mailbox_->Send(tid, msg);
        }

        return;
    }

    // ===================Abstract====================//
    /**
     * 0. Valid Dependency Read, including, (Details in layout/mvcc_list.tpp::TryPreReadUncommittedTail())
     *      a.Validate Homogeneous PreRead [PreRead V-State Trx with CT < BT]
     *      b.Validate Heterogeneous PreRead [PreRead V-State Trx with CT > BT]
     * 1. Normal Validation Step
     *      1.0. Get RCTList from parameters;
     *      1.1. Process Current Trx to get set of steps;
     *      1.2. Get RCT Content with TrxList (step1) in local;
     *      1.3. Merge prepared Primitive2Step map and set of steps (step2) to get pmt2step_map for cur_trx;
     *      1.4. Combine pmt2step_map (step4) and RCT Content (step3) into step2content map;
     *      1.5. Iterate setp2content map to invoke valid() in each expert
     *      1.6. Complete validation for optimistic validation
     * 2. Complete last validation for Homogeneous pre-read (part of it has been done in step 0)
     */

    bool isAbort = false;

    // -------------------Step 0----------------------//
    set<uint64_t> homo_dep_read;
    set<uint64_t> hetero_dep_read;
    data_storage_->GetDepReadTrxList(qplan.trxid, homo_dep_read, hetero_dep_read);

    if (qplan.trx_type == TRX_READONLY) {
        // Read-Only Trx only need to check HomoPreRead
        valid_optimistic_read(homo_dep_read, isAbort);
        if (isAbort) {
            // Abort
            trx_table_stub_->update_status(qplan.trxid, TRX_STAT::ABORT);
        }
    } else {
        if (!valid_dependency_read(qplan.trxid, homo_dep_read, hetero_dep_read)) {
            // Abort
            trx_table_stub_->update_status(qplan.trxid, TRX_STAT::ABORT);
            isAbort = true;
        }
    }

    // -----------------Step 1------------------------//
    if (!isAbort) {
        isAbort = validate(qplan, msg);
    }

    // -----------------Step 2------------------------//
    if (!isAbort && homo_dep_read.size() != 0) {
        valid_optimistic_read(homo_dep_read, isAbort);
    }

    // Create Message
    vector<Message> msg_vec;
    msg.CreateNextMsg(qplan.experts, msg.data, num_thread_, core_affinity_, msg_vec);

    // Send Message
    for (auto& msg : msg_vec) {
        msg.meta.msg_type = isAbort ? MSG_T::ABORT : MSG_T::COMMIT;
        mailbox_->Send(tid, msg);
    }
}

bool ValidationExpert::validate(const QueryPlan & qplan, Message & msg) {
    // Get info of transaction
    Meta & m = msg.meta;
    uint64_t cur_trxID = qplan.trxid;
    uint64_t cur_qid = m.qid;
    // Get number of queries in this transcation (Except validation itself)
    uint64_t num_queries = cur_qid & _8LFLAG;
    bool isAbort = false;

    // ===================Step 1.0======================//
    vector<uint64_t> trxIDList;
    Expert_Object valid_expert_obj = qplan.experts[m.step];
    for (auto val : valid_expert_obj.params) {
        trxIDList.emplace_back(Tool::value_t2uint64_t(val));
    }

    // ===================Step 1.1======================//
    set<vstep_t> trx_step_sets;
    step2aobj_map_t_ step_aobj_map;
    process_trx(num_queries, cur_qid, trx_step_sets, step_aobj_map);

    // ===================Step 1.2======================//
    // map<Primitive -> map<TrxID -> vector<RCTValue>>>
    unordered_map<int, unordered_map<uint64_t, vector<rct_extract_data_t>>> rct_content_map;
    if (!get_recent_action_set(trxIDList, rct_content_map)) {
        // There is not RCT data, no need to validate further, commit
        return false;
    }

    // ===================Step 1.3======================//
    unordered_map<int, vector<vstep_t>> curPrimitiveStepMap;
    for (int i = 0; i < static_cast<int>(Primitive_T::COUNT); i++) {
        set<vstep_t> pre_vstep_set = primitiveStepMap_[i];
        vector<vstep_t> intersection_vector(trx_step_sets.size() + pre_vstep_set.size());
        vector<vstep_t>::iterator itr = set_intersection(trx_step_sets.begin(), trx_step_sets.end(),
                                                       pre_vstep_set.begin(), pre_vstep_set.end(),
                                                       intersection_vector.begin());
        intersection_vector.resize(itr - intersection_vector.begin());
        curPrimitiveStepMap[i] = intersection_vector;
    }

    // ===================Step 1.4======================//
    step2TrxRct_map_t_ check_step_map;
    for (int i = 0; i < static_cast<int>(Primitive_T::COUNT); i++) {
        if (rct_content_map.find(i) == rct_content_map.end()) { continue; }

        for (auto & vstep : curPrimitiveStepMap.at(i)) {
            // Check step existence in check_step_map
            if (check_step_map.find(vstep) == check_step_map.end()) {
                check_step_map.emplace(vstep, rct_content_map.at(i));
            } else {
                for (auto & pair : rct_content_map.at(i)) {
                    unordered_map<uint64_t, vector<rct_extract_data_t>>::iterator itr = check_step_map.at(vstep).find(pair.first);
                    if (itr != check_step_map.at(vstep).end()) {
                        check_step_map.at(vstep).at(pair.first).insert(check_step_map.at(vstep).at(pair.first).end(), pair.second.begin(), pair.second.end());
                    } else {
                        check_step_map.at(vstep).emplace(pair.first, pair.second);
                    }
                }
            }
        }
    }

    // ===================Step 1.5===================//
    vector<uint64_t> optimistic_validation_trx;
    isAbort = do_step_validation(cur_trxID, check_step_map, optimistic_validation_trx, step_aobj_map);

    // ===================Step 1.6===================//
    // Optimistic Validation
    if (!isAbort && optimistic_validation_trx.size() != 0) {
        valid_optimistic_validation(optimistic_validation_trx, isAbort);
    }

    return isAbort;
}

void ValidationExpert::prepare_primitive_list() {
    // Prepare Validation Expert Set
    needValidateExpertSet_.emplace(EXPERT_T::TRAVERSAL);
    needValidateExpertSet_.emplace(EXPERT_T::VALUES);
    needValidateExpertSet_.emplace(EXPERT_T::PROPERTIES);
    needValidateExpertSet_.emplace(EXPERT_T::KEY);
    needValidateExpertSet_.emplace(EXPERT_T::HASLABEL);
    needValidateExpertSet_.emplace(EXPERT_T::HAS);
    needValidateExpertSet_.emplace(EXPERT_T::PROJECT);
    needValidateExpertSet_.emplace(EXPERT_T::INIT);  // For g.V().count(), etc.

    // Prepare Validation PrimitiveToStep map
    // IV & DV & IE & DE
    set<vstep_t> step_set;
    step_set.emplace(EXPERT_T::HASLABEL, Step_T::HASLABEL, 1);  // First step
    step_set.emplace(EXPERT_T::HAS, Step_T::HASNOT, 1);  // First step
    step_set.emplace(EXPERT_T::PROJECT, 1);  // First step
    step_set.emplace(EXPERT_T::INIT);
    primitiveStepMap_[static_cast<int>(Primitive_T::IV)] = step_set;
    primitiveStepMap_[static_cast<int>(Primitive_T::DV)] = step_set;
    primitiveStepMap_[static_cast<int>(Primitive_T::IE)] = step_set;
    primitiveStepMap_[static_cast<int>(Primitive_T::DE)] = step_set;
    // I/M/D VP
    step_set.clear();
    step_set.emplace(EXPERT_T::VALUES);
    step_set.emplace(EXPERT_T::PROPERTIES);
    step_set.emplace(EXPERT_T::HAS, Step_T::HASVALUE, 0);
    step_set.emplace(EXPERT_T::HAS, Step_T::HAS, 0);
    step_set.emplace(EXPERT_T::PROJECT, 0);
    primitiveStepMap_[static_cast<int>(Primitive_T::MVP)] = step_set;
    primitiveStepMap_[static_cast<int>(Primitive_T::MEP)] = step_set;

    step_set.emplace(EXPERT_T::KEY);
    step_set.emplace(EXPERT_T::HAS, Step_T::HASNOT, 0);
    step_set.emplace(EXPERT_T::HAS, Step_T::HASKEY, 0);
    primitiveStepMap_[static_cast<int>(Primitive_T::IVP)] = step_set;
    primitiveStepMap_[static_cast<int>(Primitive_T::DVP)] = step_set;
    step_set.emplace(EXPERT_T::TRAVERSAL);
    primitiveStepMap_[static_cast<int>(Primitive_T::IEP)] = step_set;
    primitiveStepMap_[static_cast<int>(Primitive_T::DEP)] = step_set;
}

// False --> Abort; True --> Continue
bool ValidationExpert::valid_dependency_read(uint64_t trxID, set<uint64_t> & homo_dep_read, set<uint64_t> & hetero_dep_read) {
    // Homo PreRead
    set<uint64_t>::iterator itr = homo_dep_read.begin();
    for ( ; itr != homo_dep_read.end(); ) {
        // Abort --> Abort
        TRX_STAT stat;
        trx_table_stub_->read_status(*itr, stat);
        if (stat == TRX_STAT::ABORT) {
            return false;
        } else if (stat == TRX_STAT::COMMITTED) {
            itr = homo_dep_read.erase(itr);
            continue;
        }
        itr++;
    }

    // Hetero PreRead
    itr = hetero_dep_read.begin();
    for ( ; itr != hetero_dep_read.end(); ) {
        // Commit --> Abort
        TRX_STAT stat;
        trx_table_stub_->read_status(*itr, stat);
        if (stat == TRX_STAT::COMMITTED) {
            return false;
        } else if (stat == TRX_STAT::ABORT) {
            itr = hetero_dep_read.erase(itr);
            continue;
        }
        itr++;
    }

    return true;
}

void ValidationExpert::process_trx(int num_queries, uint64_t cur_qid, set<vstep_t> & trx_step_sets, step2aobj_map_t_& step_aobj_map) {
    for (uint64_t query_index = 0; query_index < num_queries; query_index++) {
        // Generate qid for each query
        uint64_t _qid = ((cur_qid >> 8) << 8) | query_index;
        const_accessor c_ac;
        if (!msg_logic_table_->find(c_ac, _qid)) {
            cout << "Not found QueryPlan for query " << _qid << endl;
            continue;
        }

        int step_counter = 0;
        for (auto & cur_expert_obj : c_ac->second.experts) {
            vstep_t vstep;
            step_counter++;
            if (needValidateExpertSet_.find(cur_expert_obj.expert_type) != needValidateExpertSet_.end()) {
                // Analyse the step type, only for has expert
                if (cur_expert_obj.expert_type == EXPERT_T::HAS) {
                    get_vstep_for_has(&cur_expert_obj, step_counter, trx_step_sets, step_aobj_map);
                } else {
                    get_vstep(&cur_expert_obj, step_counter, trx_step_sets, step_aobj_map);
                }
            }
        }
    }
}

// True -> Get something; False -> No data got
bool ValidationExpert::get_recent_action_set(const vector<uint64_t> & trxIDList,
        unordered_map<int, unordered_map<uint64_t, vector<rct_extract_data_t>>> & rct_map) {
    bool dataGot = false;
    for (int p = 0; p < static_cast<int>(Primitive_T::COUNT); p++) {
        unordered_map<uint64_t, vector<rct_extract_data_t>> trx_rct_map;
        pmt_rct_table_->GetRecentActionSet((Primitive_T)p, trxIDList, trx_rct_map);
        if (trx_rct_map.size() != 0) {
            dataGot = true;
            rct_map.emplace(p, trx_rct_map);
        }
    }
    return dataGot;
}

void ValidationExpert::get_vstep(Expert_Object * cur_expert_obj, int step_num,
        set<vstep_t> & step_sets, step2aobj_map_t_ & step_aobj_map) {
    vstep_t vstep;
    switch (cur_expert_obj->expert_type) {
      case EXPERT_T::HASLABEL:
        vstep = vstep_t(EXPERT_T::HASLABEL, Step_T::HASLABEL, step_num == 1 ? 1 : 0);
        break;
      case EXPERT_T::PROJECT:
        vstep = vstep_t(EXPERT_T::PROJECT, step_num == 1 ? 1 : 0);
        break;
      default:
        vstep = vstep_t((EXPERT_T)cur_expert_obj->expert_type);
    }
    step_sets.emplace(vstep);
    insert_step_aobj_map(step_aobj_map, vstep, cur_expert_obj);
}

void ValidationExpert::get_vstep_for_has(Expert_Object * cur_expert_obj, int step_num,
        set<vstep_t> & step_sets, step2aobj_map_t_ & step_aobj_map) {
    Step_T has_step_type;
    // Get Params
    CHECK(cur_expert_obj->params.size() > 0 && (cur_expert_obj->params.size() - 1) % 3 == 0);
    Element_T inType = (Element_T) Tool::value_t2int(cur_expert_obj->params.at(0));
    int numParamsGroup = (cur_expert_obj->params.size() - 1) / 3;

    for (int i = 0; i < numParamsGroup; i++) {
        int pos = i * 3 + 1;

        int pid = Tool::value_t2int(cur_expert_obj->params.at(pos));
        Predicate_T pred_type = (Predicate_T) Tool::value_t2int(cur_expert_obj->params.at(pos + 1));
        vector<value_t> pred_params;
        Tool::value_t2vec(cur_expert_obj->params.at(pos + 2), pred_params);

        if (pred_type == Predicate_T::ANY) {  // HasKey
            has_step_type = Step_T::HASKEY;
        } else if (pred_type == Predicate_T::NONE) {  // HasNot
            has_step_type = Step_T::HASNOT;
        } else {
            if (pid == -1) {  // HasValue
                has_step_type = Step_T::HASVALUE;
            } else {  // Has(key, value)
                has_step_type = Step_T::HAS;
            }
        }

        vstep_t vstep(EXPERT_T::HAS, has_step_type, step_num == 1 ? 1 : 0);
        step_sets.emplace(vstep);
        insert_step_aobj_map(step_aobj_map, vstep, cur_expert_obj);
    }
}

bool ValidationExpert::do_step_validation(uint64_t cur_trxID, step2TrxRct_map_t_ & check_step_map,
        vector<uint64_t> & optimistic_validation_trx, step2aobj_map_t_ & step_aobj_map) {
    for (auto & each_step : check_step_map) {
        // For each step, check status
        TRX_STAT trx_stat;
        trx_table_stub_->read_status(cur_trxID, trx_stat);
        if (trx_stat == TRX_STAT::ABORT) {
            return true;
        }

        for (auto & each_rct_trx : each_step.second) {
            // Get related Expert Object and invoke valid()
            if (!experts_->at(static_cast<EXPERT_T>(each_step.first.expert_type))->
                    valid(cur_trxID, step_aobj_map.at(each_step.first), each_rct_trx.second)) {
                // Conflict, if no opt_validation, abort
                if (!config_->global_enable_opt_validation) {
                    trx_table_stub_->update_status(cur_trxID, TRX_STAT::ABORT);
                    return true;
                }

                // Failed, there is conflict, check rct_trx status
                TRX_STAT cur_stat;
                trx_table_stub_->read_status(each_rct_trx.first, cur_stat);
                if (cur_stat == TRX_STAT::VALIDATING) {
                    // Optimistic Validation
                    optimistic_validation_trx.emplace_back(each_rct_trx.first);
                    continue;
                } else if (cur_stat == TRX_STAT::ABORT) {
                    continue;
                } else {
                    // Abort
                    trx_table_stub_->update_status(cur_trxID, TRX_STAT::ABORT);
                    return true;
                }
            }
        }
    }
    return false;
}

void ValidationExpert::valid_optimistic_validation(vector<uint64_t> & optimistic_validation_trx, bool & isAbort) {
    int opt_valid_counter = 0;
    while (true) {
        vector<uint64_t>::iterator itr = optimistic_validation_trx.begin();
        while (itr != optimistic_validation_trx.end()) {
            TRX_STAT cur_stat;
            trx_table_stub_->read_status(*itr, cur_stat);
            switch (cur_stat) {
              case TRX_STAT::VALIDATING:
                itr++; break;
              case TRX_STAT::ABORT:
                itr = optimistic_validation_trx.erase(itr); break;
              case TRX_STAT::COMMITTED:
                isAbort = true; return;
              default :
                isAbort = true;
                cout << "[Error] Unexpected Transaction Status during Validation" << endl;
                return;
            }
        }

        if (!optimistic_validation_trx.size() == 0) {
            // Sleep for a while
            this_thread::sleep_for(chrono::microseconds(OPT_VALID_SLEEP_TIME_));
        } else { return; }

        opt_valid_counter++;
        if (opt_valid_counter >= OPT_VALID_TIMEOUT_) {
            isAbort = true; return;
        }
    }
}

void ValidationExpert::valid_optimistic_read(set<uint64_t> & homo_dep_read, bool & isAbort) {
    int opt_read_counter = 0;
    while (true) {
        set<uint64_t>::iterator itr = homo_dep_read.begin();
        while (itr != homo_dep_read.end()) {
            TRX_STAT cur_stat;
            trx_table_stub_->read_status(*itr, cur_stat);
            switch (cur_stat) {
              case TRX_STAT::VALIDATING:
                itr++; break;
              case TRX_STAT::ABORT:
                isAbort = true; return;
              case TRX_STAT::COMMITTED:
                itr = homo_dep_read.erase(itr); break;
              default :
                isAbort = true;
                cout << "[Error] Unexpected Transaction Status during Validation" << endl;
                return;
            }
        }

        if (!homo_dep_read.size() == 0) {
            // Sleep for a while
            this_thread::sleep_for(chrono::microseconds(OPT_VALID_SLEEP_TIME_));
        } else { return; }

        opt_read_counter++;
        if (opt_read_counter >= OPT_VALID_TIMEOUT_) {
            isAbort = true; return;
        }
    }
}

void ValidationExpert::insert_step_aobj_map(step2aobj_map_t_ & step_aobj_map, const vstep_t & vstep, Expert_Object * cur_expert_obj) {
    if (step_aobj_map.find(vstep) != step_aobj_map.end()) {
        step_aobj_map.at(vstep).emplace_back(cur_expert_obj);
    } else {
        vector<Expert_Object*> tmp_index_vec;
        tmp_index_vec.emplace_back(cur_expert_obj);
        step_aobj_map.emplace(vstep, tmp_index_vec);
    }
}

// Test for pmt_rct_table_->InsertRCT
void ValidationExpert::test_insert_rct(uint64_t trxID, vector<uint64_t> & values, vector<int> & p_vec) {
    for (auto & p : p_vec) {
        pmt_rct_table_->InsertRecentActionSet((Primitive_T)p, trxID, values);
    }
}
