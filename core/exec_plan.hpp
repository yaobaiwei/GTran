/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/
#pragma once
#include <string>
#include <map>
#include <vector>

#include "actor/actor_object.hpp"
#include "base/serialization.hpp"
#include "base/type.hpp"
#include "utils/timer.hpp"

// Execution plan for query
class QueryPlan {
 public:
    QueryPlan() {}

    // Query info
    vector<Actor_Object> actors;

    // Transaction info
    uint64_t trxid;
    char trx_type;
    uint64_t st;
};

ibinstream& operator<<(ibinstream& m, const QueryPlan& plan);

obinstream& operator>>(obinstream& m, QueryPlan& plan);

class Parser;

#define TRX_READONLY 0
#define TRX_UPDATE   1
#define TRX_ADD      2
#define TRX_DELETE   4

// Execution plan for transaction
class TrxPlan {
 public:
    TrxPlan() {}
    TrxPlan(uint64_t trxid_, uint64_t st, string client_host_) : trxid(trxid_), st_(st), client_host(client_host_) {
        trx_type_ = TRX_READONLY;
        query_index_ = -1;
        start_time = timer::get_usec();
    }

    // Register place holder
    void RegPlaceHolder(int src_index, int dst_index, int actor_index, int param_index);

    // Fill in placeholder and trx result after query done
    void FillResult(vector<value_t>& vec);

    // Get exection plan, false if finished
    bool NextQuery(QueryPlan& plan);

    uint64_t trxid;

    string client_host;

    // physical time
    uint64_t start_time;

    // Results from all queries
    vector<value_t> results;

 private:
    // Locate the position of place holder
    struct position_t {
        int query;
        int actor;
        int param;
        position_t(int q, int a, int p) : query(q), actor(a), param(p){}
    };

    uint64_t st_;
    char trx_type_;

    // indicate current query line
    int query_index_;

    // Info of all queries
    vector<QueryPlan> query_plans_;

    // Place holder
    map<int, vector<position_t>> dependents_;

    friend Parser;
};

inline bool isTrxReadyOnly(char trx_type);
inline bool isTrxUpdate(char trx_type);
inline bool isTrxAdd(char trx_type);
inline bool isTrxDelete(char trx_type);
