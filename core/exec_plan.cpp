/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/
#include <utility>
#include "core/exec_plan.hpp"

ibinstream& operator<<(ibinstream& m, const QueryPlan& plan) {
    m << plan.actors;
    m << plan.trx_type;
    m << plan.trxid;
    m << plan.st;
    return m;
}

obinstream& operator>>(obinstream& m, QueryPlan& plan) {
    m >> plan.actors;
    m >> plan.trx_type;
    m >> plan.trxid;
    m >> plan.st;
    return m;
}

void TrxPlan::RegPlaceHolder(int src_index, int dst_index, int actor_index, int param_index) {
    // Record the position of placeholder
    // When src_index is finished, results will be inserted into recorded positions.
    dependents_[src_index].emplace_back(dst_index, actor_index, param_index);
}

void TrxPlan::FillResult(vector<value_t>& vec) {
    // Find placeholders that depend on results of query_index_
    for (auto& pos : dependents_[query_index_]) {
        Actor_Object& actor = query_plans_[pos.query].actors[pos.actor];
        if (pos.param == -1) {
            // insert to the end of params
            pos.param = actor.params.size();
        }
        switch (actor.actor_type) {
          case ACTOR_T::INIT:
          case ACTOR_T::ADDE:
            actor.params.insert(actor.params.begin() + pos.param, vec.begin(), vec.end());
            break;
          default:
            value_t result;
            if (vec.size() == 1) {
                result = vec[0];
            } else {
                Tool::vec2value_t(vec, result);
            }
            actor.params[pos.param] = result;
            break;
        }
    }

    // Append result set
    value_t v;
    string header = "Query " + to_string(query_index_ + 1) + ": ";
    Tool::str2str(header, v);
    results.push_back(v);
    results.insert(results.end(), make_move_iterator(vec.begin()), make_move_iterator(vec.end()));
}

int TrxPlan::NextQuery(QueryPlan& plan) {
    query_index_++;
    if (query_index_ == query_plans_.size()) {
        return -1;
    }
    plan.actors = move(query_plans_[query_index_].actors);
    plan.trxid = trxid;
    plan.st = st_;
    plan.trx_type = trx_type_;
    return query_index_;
}
