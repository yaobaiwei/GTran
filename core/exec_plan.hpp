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
    uint8_t query_index;
    vector<Actor_Object> actors;

    // Transaction info
    uint64_t trxid;
    uint8_t trx_type;
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
        start_time = timer::get_usec();
        is_abort_ = false;
        is_end_ = false;
    }

    // Register place holder, dst_index depends on src_index
    void RegPlaceHolder(uint8_t src_index, uint8_t dst_index, int actor_index, int param_index);

    // Register dependency, dst_index depends on src_index
    void RegDependency(uint8_t src_index, uint8_t dst_index);

    // Fill in placeholder and trx result after query done
    // Return false if the transaction is aborted due to placeholder error
    bool FillResult(int query_index, vector<value_t>& vec);

    void Abort();

    // Get result of queries after transaction finished
    void GetResult(vector<value_t>& vec);

    // Get exection plan, return false if finished
    bool NextQueries(vector<QueryPlan>& plans);

    uint64_t trxid;

    string client_host;

    // physical time
    uint64_t start_time;

 private:
    // Locate the position of place holder
    struct position_t {
        int query;
        int actor;
        int param;
        position_t(int q, int a, int p) : query(q), actor(a), param(p){}
    };

    uint64_t st_;
    uint8_t trx_type_;
    uint8_t received_;

    bool is_abort_;
    bool is_end_;

    // Info of all queries
    vector<QueryPlan> query_plans_;

    // Number of dependency of each query
    map<uint8_t, uint8_t> deps_count_;

    // Record children of query
    // If b,c depends on a, then a -> set(b, c)
    // When a finished, decrease the deps_count of b and c
    map<uint8_t, set<uint8_t>> topo_;
    map<uint8_t, vector<position_t>> place_holder_;

    // Query index to query result
    map<int, vector<value_t>> results_;

    friend Parser;
};

inline bool isTrxReadOnly(uint8_t trx_type) { return trx_type == TRX_READONLY; }
inline bool isTrxUpdate(uint8_t trx_type) { return trx_type & TRX_UPDATE != 0; }
inline bool isTrxAdd(uint8_t trx_type) { return trx_type & TRX_ADD != 0; }
inline bool isTrxDelete(uint8_t trx_type) { return trx_type & TRX_DELETE != 0; }
