/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/
#include <utility>
#include "core/exec_plan.hpp"

ibinstream& operator<<(ibinstream& m, const QueryPlan& plan) {
    m << plan.query_index;
    m << plan.actors;
    m << plan.is_process;
    m << plan.trx_type;
    m << plan.trxid;
    m << plan.st;
    return m;
}

obinstream& operator>>(obinstream& m, QueryPlan& plan) {
    m >> plan.query_index;
    m >> plan.actors;
    m >> plan.is_process;
    m >> plan.trx_type;
    m >> plan.trxid;
    m >> plan.st;
    return m;
}

void TrxPlan::SetST(uint64_t st) {
    st_ = st;
}

void TrxPlan::RegPlaceHolder(uint8_t src_index, uint8_t dst_index, int actor_index, int param_index) {
    // Record the position of placeholder
    // When src_index is finished, results will be inserted into recorded positions.
    place_holder_[src_index].emplace_back(dst_index, actor_index, param_index);
    RegDependency(src_index, dst_index);
}

void TrxPlan::RegDependency(uint8_t src_index, uint8_t dst_index) {
    auto ret = topo_[src_index].insert(dst_index);

    // If not duplicated, increase count
    if (ret.second) {
        deps_count_[dst_index] ++;
    }
}

void TrxPlan::NotifyQueryFinished(int query_index) {
    if (remaining_qids_.count(query_index) == 0)
        return;

    remaining_qids_.erase(query_index);

    // All running queries has finished. Release the query of abort statement.
    if (remaining_qids_.size() == 0) {
        int index = query_plans_.size() - 1;
        deps_count_.clear();
        deps_count_[index] = 0;
    }
}

void TrxPlan::Abort() {
    if (is_abort_) {
        // Abort statement already sent
        return;
    }

    is_abort_ = true;

    // setup abort statement
    // erase validation and post_validation actor
    int index = query_plans_.size() - 1;
    QueryPlan& plan = query_plans_[index];
    plan.actors.erase(plan.actors.begin(), plan.actors.begin() + 2);

    // find all query with dep_count == 0 (already submitted)
    for (uint8_t i = 0; i < index; i++) {
        if (deps_count_[i] == 0) {
            // not finished
            if (results_.count(i) == 0) {
                // Record them in a set.
                // Query of abort statement will not be send until those queries in remaining_qids_ finished.
                remaining_qids_.emplace(i);
            }
        }
    }
}

bool TrxPlan::FillResult(int query_index, vector<value_t>& vec) {
    // Find placeholders that depend on results of query_index_
    for (position_t& pos : place_holder_[query_index]) {
        Actor_Object& actor = query_plans_[pos.query].actors[pos.actor];
        if (pos.param == -1) {
            // insert to the end of params
            pos.param = actor.params.size();
        }

        value_t result;
        switch (actor.actor_type) {
          case ACTOR_T::INIT:
            actor.params.insert(actor.params.begin() + pos.param, vec.begin(), vec.end());
            break;
          case ACTOR_T::ADDE:
            if (vec.size() == 1) {
                result = vec[0];
            } else {
                Abort();
                NotifyQueryFinished(query_index);
                return false;
            }
            actor.params[pos.param] = result;
            break;
          default:
            cout << "[Error][ExecPlan] Unexpected ActorType" << endl;
        }
    }

    for (uint8_t index : topo_[query_index]) {
        deps_count_[index] --;
    }

    // Add query header info if not parser error
    if (query_index != -1) {
        value_t v;
        string header;
        if (query_index == query_plans_.size() - 1) {
            header = "Status: ";
            header += Tool::DebugString(vec[0]);
        } else {
            header = "Query " + to_string(query_index + 1) + ": ";
            if (vec.size() == 0) {
                header += "Empty";
            }
        }
        Tool::str2str(header, v);
        results_[query_index].push_back(v);
    }

    if (is_abort_) {
        NotifyQueryFinished(query_index);
    }

    if (query_index != -1 && query_index != query_plans_.size() - 1) {
        if (vec.size() > 0) {
            results_[query_index].insert(results_[query_index].end(),
                                        make_move_iterator(vec.begin()),
                                        make_move_iterator(vec.end()));
        }
    }

    // check if commit statement or parser error
    if (query_index == query_plans_.size() - 1 || query_index == -1) {
        is_end_ = true;
    }
    return true;
}

// return false if transaction end
bool TrxPlan::NextQueries(vector<QueryPlan>& plans) {
    if (is_end_) {
        // End of transaction
        return false;
    }

    for (auto itr = deps_count_.begin(); itr != deps_count_.end();) {
        // Send out queries whose dependency count = 0
        if(itr->second == 0) {
            // Set transaction info
            QueryPlan& plan = query_plans_[itr->first];
            plan.query_index = itr->first;
            plan.trxid = trxid;
            plan.st = st_;
            plan.trx_type = trx_type_;
            plans.push_back(move(plan));

            // erase to reduce search space
            itr = deps_count_.erase(itr);
        } else {
            itr++;
        }
    }
    return true;
}

void TrxPlan::GetResult(vector<value_t>& vec) {
    // Append query results in increasing order
    // Transaction status (aborted/committed) is handled by commit actor
    for (auto itr = results_.begin(); itr != results_.end(); itr ++) {
        vec.insert(vec.end(), make_move_iterator(itr->second.begin()), make_move_iterator(itr->second.end()));
    }
}
