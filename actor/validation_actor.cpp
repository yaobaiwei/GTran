/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#include "actor/validation_actor.hpp"
void ValidationActor::process(const QueryPlan & qplan, Message & msg) {
    int tid = TidMapper::GetInstance()->GetTid();

    bool isAbort = validate(qplan, msg);

    // Create Message
    vector<Message> msg_vec;
    msg.CreateNextMsg(qplan.actors, msg.data, num_thread_, core_affinity_, msg_vec);

    // Send Message
    for (auto& msg : msg_vec) {
        msg.meta.msg_type = isAbort ? MSG_T::ABORT : MSG_T::COMMIT;
        mailbox_->Send(tid, msg);
    }
}

bool ValidationActor::validate(const QueryPlan & qplan, Message & msg) {
    // Get info of transaction
    Meta & m = msg.meta;
    uint64_t cur_trxID = qplan.trxid;
    uint64_t cur_qid = m.qid;
    // Get number of queries in this transcation (Except validation itself)
    uint64_t num_queries = cur_qid & _8LFLAG;

    // ===================Abstract====================//
    /**
     * 0. Valid dependency read;
     * 1. Get RCTList from parameters;
     * 2. Process Current Trx to get : a. set of steps; b. step to index map; c. step to parameters map;
     * 3. Get RCT Content with TrxList (step1) in local;
     * 4. Merge prepared Primitive2Step map and set of steps (step2) to get pmt2step_map for cur_trx;
     * 5. Combine pmt2step_map (step4) and RCT Content (step3) into step2content map;
     * 6. Iterate setp2content map to invoke valid() in each actor
     * 7. Complete last validation for optimistic validation
     * 8. Complete last validation for optimistic pre-read
     */

    bool isAbort = false;
    // ===================Step 0======================//
    vector<uint64_t> homo_dep_read;
    vector<uint64_t> hetero_dep_read;
    data_storage_->GetDepReadTrxList(cur_trxID, homo_dep_read, hetero_dep_read);
    if (qplan.trx_type == TRX_READONLY) {
        valid_optimistic_read(homo_dep_read, isAbort);
        if (isAbort) {
            // Abort
            trx_table_stub_->update_status(cur_trxID, TRX_STAT::ABORT);
        }
        return isAbort;
    } else {
        if (!valid_dependency_read(cur_trxID, homo_dep_read, hetero_dep_read)) {
            // Abort
            trx_table_stub_->update_status(cur_trxID, TRX_STAT::ABORT);
            return true;
        }
    }

    // ===================Step 1======================//
    vector<uint64_t> trxIDList;
    Actor_Object valid_actor_obj = qplan.actors[m.step];
    for (auto val : valid_actor_obj.params) {
        trxIDList.emplace_back(Tool::value_t2uint64_t(val));
    }

    // ===================Step 2======================//
    set<vstep_t> trx_step_sets;
    step2aobj_map_t_ step_aobj_map;
    process_trx(num_queries, cur_qid, trx_step_sets, step_aobj_map);

    // ===================Step 3======================//
    // map<Primitive -> map<TrxID -> vector<RCTValue>>>
    unordered_map<int, unordered_map<uint64_t, vector<rct_extract_data_t>>> rct_content_map;
    get_recent_action_set(trxIDList, rct_content_map);

    // ===================Step 4======================//
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

    // ===================Step 5======================//
    step2TrxRct_map_t_ check_step_map;
    for (int i = 0; i < static_cast<int>(Primitive_T::COUNT); i++) {
        for (auto & vs : curPrimitiveStepMap.at(i)) {
            check_step_map.emplace(vs, rct_content_map.at(i));
        }
    }

    // ===================Step 6===================//
    vector<uint64_t> optimistic_validation_trx;
    isAbort = do_step_validation(cur_trxID, check_step_map, optimistic_validation_trx, step_aobj_map);

    // ===================Step 7===================//
    // Optimistic Validation
    if (!isAbort && optimistic_validation_trx.size() != 0) {
        valid_optimistic_validation(optimistic_validation_trx, isAbort);
    }

    // ===================Step 8===================//
    if (!isAbort && homo_dep_read.size() != 0) {
        valid_optimistic_read(homo_dep_read, isAbort);
    }

    return isAbort;
}

void ValidationActor::prepare_primitive_list() {
    // Prepare Validation Actor Set
    needValidateActorSet_.emplace(ACTOR_T::TRAVERSAL);
    needValidateActorSet_.emplace(ACTOR_T::VALUES);
    needValidateActorSet_.emplace(ACTOR_T::PROPERTIES);
    needValidateActorSet_.emplace(ACTOR_T::KEY);
    needValidateActorSet_.emplace(ACTOR_T::HASLABEL);
    needValidateActorSet_.emplace(ACTOR_T::HAS);
    needValidateActorSet_.emplace(ACTOR_T::PROJECT);

    // Prepare Validation PrimitiveToStep map
    // IV & DV & IE & DE
    set<vstep_t> step_set;
    step_set.emplace(ACTOR_T::HASLABEL, Step_T::HASLABEL, 1);  // First step
    step_set.emplace(ACTOR_T::HAS, Step_T::HASNOT, 1);  // First step
    step_set.emplace(ACTOR_T::PROJECT, 1);  // First step
    primitiveStepMap_[static_cast<int>(Primitive_T::IV)] = step_set;
    primitiveStepMap_[static_cast<int>(Primitive_T::DV)] = step_set;
    primitiveStepMap_[static_cast<int>(Primitive_T::IE)] = step_set;
    primitiveStepMap_[static_cast<int>(Primitive_T::DE)] = step_set;
    // I/M/D VP
    step_set.clear();
    step_set.emplace(ACTOR_T::VALUES);
    step_set.emplace(ACTOR_T::PROPERTIES);
    step_set.emplace(ACTOR_T::HAS, Step_T::HASVALUE, 0);
    step_set.emplace(ACTOR_T::HAS, Step_T::HAS, 0);
    step_set.emplace(ACTOR_T::PROJECT, 0);
    primitiveStepMap_[static_cast<int>(Primitive_T::MVP)] = step_set;
    primitiveStepMap_[static_cast<int>(Primitive_T::MEP)] = step_set;

    step_set.emplace(ACTOR_T::KEY);
    step_set.emplace(ACTOR_T::HAS, Step_T::HASNOT, 0);
    step_set.emplace(ACTOR_T::HAS, Step_T::HASKEY, 0);
    primitiveStepMap_[static_cast<int>(Primitive_T::IVP)] = step_set;
    primitiveStepMap_[static_cast<int>(Primitive_T::DVP)] = step_set;
    step_set.emplace(ACTOR_T::TRAVERSAL);
    primitiveStepMap_[static_cast<int>(Primitive_T::IEP)] = step_set;
    primitiveStepMap_[static_cast<int>(Primitive_T::DEP)] = step_set;
}

// False --> Abort; True --> Continue
bool ValidationActor::valid_dependency_read(uint64_t trxID, vector<uint64_t> & homo_dep_read, vector<uint64_t> & hetero_dep_read) {
    vector<uint64_t>::iterator itr = homo_dep_read.begin();
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

    itr = hetero_dep_read.begin();
    for ( ; itr != hetero_dep_read.end(); ) {
        // Commit --> Abort
        TRX_STAT stat;
        trx_table_stub_->read_status(*itr, stat);
        if (stat == TRX_STAT::COMMITTED) {
            return false;
        } else if (stat == TRX_STAT::ABORT) {
            itr = homo_dep_read.erase(itr);
            continue;
        }
        itr++;
    }

    return true;
}

void ValidationActor::process_trx(int num_queries, uint64_t cur_qid, set<vstep_t> & trx_step_sets, step2aobj_map_t_& step_aobj_map) {
    for (uint64_t query_index = 0; query_index < num_queries; query_index++) {
        // Generate qid for each query
        uint64_t _qid = ((cur_qid >> 8) << 8) | query_index;
        const_accessor c_ac;
        if (!msg_logic_table_->find(c_ac, _qid)) {
            cout << "Not found QueryPlan for query " << _qid << endl;
            continue;
        }

        int step_counter = 0;
        for (auto & cur_actor_obj : c_ac->second.actors) {
            vstep_t vstep;
            step_counter++;
            if (needValidateActorSet_.find(cur_actor_obj.actor_type) != needValidateActorSet_.end()) {
                // Analyse the step type, only for has actor
                if (cur_actor_obj.actor_type == ACTOR_T::HAS) {
                    get_vstep_for_has(&cur_actor_obj, step_counter, trx_step_sets, step_aobj_map);
                } else {
                    get_vstep(&cur_actor_obj, step_counter, trx_step_sets, step_aobj_map);
                }
            }
        }
    }
}

void ValidationActor::get_recent_action_set(const vector<uint64_t> & trxIDList,
        unordered_map<int, unordered_map<uint64_t, vector<rct_extract_data_t>>> & rct_map) {
    for (int p = 0; p < static_cast<int>(Primitive_T::COUNT); p++) {
        unordered_map<uint64_t, vector<rct_extract_data_t>> trx_rct_map;
        pmt_rct_table_->GetRecentActionSet((Primitive_T)p, trxIDList, trx_rct_map);
        rct_map[p] = move(trx_rct_map);
    }
}

void ValidationActor::get_vstep(Actor_Object * cur_actor_obj, int step_num,
        set<vstep_t> & step_sets, step2aobj_map_t_ & step_aobj_map) {
    vstep_t vstep;
    switch (cur_actor_obj->actor_type) {
      case ACTOR_T::HASLABEL:
        vstep = vstep_t(ACTOR_T::HASLABEL, Step_T::HASLABEL, step_num == 1 ? 1 : 0);
        break;
      case ACTOR_T::PROJECT:
        vstep = vstep_t(ACTOR_T::PROJECT, step_num == 1 ? 1 : 0);
        break;
      default:
        vstep = vstep_t((ACTOR_T)cur_actor_obj->actor_type);
    }
    step_sets.emplace(vstep);
    insert_step_aobj_map(step_aobj_map, vstep, cur_actor_obj);
}

void ValidationActor::get_vstep_for_has(Actor_Object * cur_actor_obj, int step_num,
        set<vstep_t> & step_sets, step2aobj_map_t_ & step_aobj_map) {
    Step_T has_step_type;
    // Get Params
    assert(cur_actor_obj->params.size() > 0 && (cur_actor_obj->params.size() - 1) % 3 == 0);
    Element_T inType = (Element_T) Tool::value_t2int(cur_actor_obj->params.at(0));
    int numParamsGroup = (cur_actor_obj->params.size() - 1) / 3;

    for (int i = 0; i < numParamsGroup; i++) {
        int pos = i * 3 + 1;

        int pid = Tool::value_t2int(cur_actor_obj->params.at(pos));
        Predicate_T pred_type = (Predicate_T) Tool::value_t2int(cur_actor_obj->params.at(pos + 1));
        vector<value_t> pred_params;
        Tool::value_t2vec(cur_actor_obj->params.at(pos + 2), pred_params);

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

        vstep_t vstep(ACTOR_T::HAS, has_step_type, step_num == 1 ? 1 : 0);
        step_sets.emplace(vstep);
        insert_step_aobj_map(step_aobj_map, vstep, cur_actor_obj);
    }
}

bool ValidationActor::do_step_validation(uint64_t cur_trxID, step2TrxRct_map_t_ & check_step_map,
        vector<uint64_t> & optimistic_validation_trx, step2aobj_map_t_ & step_aobj_map) {
    for (auto & each_step : check_step_map) {
        // For each step, check status
        TRX_STAT trx_stat;
        trx_table_stub_->read_status(cur_trxID, trx_stat);
        if (trx_stat == TRX_STAT::ABORT) {
            return true;
        }

        for (auto & each_rct_trx : each_step.second) {
            // Get related Actor Object and invoke valid()
            if (!actors_->at(static_cast<ACTOR_T>(each_step.first.actor_type))->
                    valid(cur_trxID, step_aobj_map.at(each_step.first), each_rct_trx.second)) {
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

void ValidationActor::valid_optimistic_validation(vector<uint64_t> & optimistic_validation_trx, bool & isAbort) {
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
            usleep(OPT_VALID_SLEEP_TIME_);
        } else { return; }

        opt_valid_counter++;
        if (opt_valid_counter >= OPT_VALID_TIMEOUT_) {
            isAbort = true; return;
        }
    }
}

void ValidationActor::valid_optimistic_read(vector<uint64_t> & homo_dep_read, bool & isAbort) {
    int opt_read_counter = 0;
    while (true) {
        vector<uint64_t>::iterator itr = homo_dep_read.begin();
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
            usleep(OPT_VALID_SLEEP_TIME_);
        } else { return; }

        opt_read_counter++;
        if (opt_read_counter >= OPT_VALID_TIMEOUT_) {
            isAbort = true; return;
        }
    }
}

void ValidationActor::insert_step_aobj_map(step2aobj_map_t_ & step_aobj_map, const vstep_t & vstep, Actor_Object * cur_actor_obj) {
    if (step_aobj_map.find(vstep) != step_aobj_map.end()) {
        step_aobj_map.at(vstep).emplace_back(cur_actor_obj);
    } else {
        vector<Actor_Object*> tmp_index_vec;
        tmp_index_vec.emplace_back(cur_actor_obj);
        step_aobj_map.emplace(vstep, tmp_index_vec);
    }
}

// Test for pmt_rct_table_->InsertRCT
void ValidationActor::test_insert_rct(uint64_t trxID, vector<uint64_t> & values, vector<int> & p_vec) {
    for (auto & p : p_vec) {
        pmt_rct_table_->InsertRecentActionSet((Primitive_T)p, trxID, values);
    }
}
