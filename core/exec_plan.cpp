#include "core/exec_plan.hpp"

ibinstream& operator<<(ibinstream& m, const QueryPlan& plan)
{
    m << plan.actors;
    m << plan.trx_type;
    m << plan.trxid;
    m << plan.st;
    return m;
}

obinstream& operator>>(obinstream& m, QueryPlan& plan)
{
    m >> plan.actors;
    m >> plan.trx_type;
    m >> plan.trxid;
    m >> plan.st;
    return m;
}

void TrxPlan::RegPlaceHolder(int src_index, int dst_index, int actor_index, int param_index){
    dependents_[src_index].emplace_back(dst_index, actor_index, param_index);
}

void TrxPlan::FillPlaceHolder(vector<value_t>& results){
    for(auto& pos : dependents_[query_index_]){
        Actor_Object& actor = query_plans_[pos.query].actors[pos.actor];
        if(actor.actor_type == ACTOR_T::INIT){
            // TODO: Better move eids/vids to message data instead of actor params
            actor.params.insert(actor.params.end(),results.begin(), results.end());
        }else{
            value_t result;
            if(results.size() == 1){
                result = results[0];
            }else{
                Tool::vec2value_t(results, result);
            }
            actor.params[pos.param] = result;
        }
    }
}

bool TrxPlan::NextQuery(QueryPlan& plan){
    query_index_ ++;
    if(query_index_ == query_plans_.size()){
        return false;
    }
    plan.actors = move(query_plans_[query_index_].actors);
    plan.trxid = trxid_;
    plan.st = st_;
    plan.trx_type = trx_type_;
    return true;
}

inline bool isTrxReadyOnly(char trx_type){
    return trx_type == TRX_READONLY;
}
inline bool isTrxUpdate(char trx_type){
    return trx_type & TRX_UPDATE != 0;
}
inline bool isTrxAdd(char trx_type){
    return trx_type & TRX_ADD != 0;
}
inline bool isTrxDelete(char trx_type){
    return trx_type & TRX_DELETE != 0;
}
