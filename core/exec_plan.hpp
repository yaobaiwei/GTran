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

#pragma once
#include <string>
#include <map>
#include <unordered_set>
#include <vector>

#include "base/serialization.hpp"
#include "base/type.hpp"
#include "expert/expert_object.hpp"
#include "utils/timer.hpp"

// Execution plan for query
class QueryPlan {
 public:
    QueryPlan() : is_process(true){}

    // Query info
    uint8_t query_index;
    vector<Expert_Object> experts;
    bool is_process;  // True if query is in process phase

    // Transaction info
    uint64_t trxid;
    uint8_t trx_type;
    uint64_t st;
};

ibinstream& operator<<(ibinstream& m, const QueryPlan& plan);

obinstream& operator>>(obinstream& m, QueryPlan& plan);

#define TRX_READONLY 0
#define TRX_UPDATE   1
#define TRX_ADD      2
#define TRX_DELETE   4

// Execution plan for transaction
class TrxPlan {
 public:
    TrxPlan() {}
    TrxPlan(uint64_t trxid_, string client_host_) : trxid(trxid_), client_host(client_host_) {
        trx_type_ = TRX_READONLY;
        start_time = timer::get_usec();
        is_abort_ = false;
        is_end_ = false;
    }

    // This is needed since when parsing is finished and TrxPlan is created,
    // the begin time of the transaction is unknown.
    void SetST(uint64_t st);

    // Register place holder, dst_index depends on src_index
    void RegPlaceHolder(uint8_t src_index, uint8_t dst_index, int expert_index, int param_index);

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

    uint64_t GetStartTime() const {return st_;}
    uint8_t GetQueryCount() const {return query_plans_.size();}
    uint8_t GetTrxType() const {return trx_type_;}
    bool isAbort() { return is_abort_; }

 private:
    // Locate the position of place holder
    struct position_t {
        int query;
        int expert;
        int param;
        position_t(int q, int a, int p) : query(q), expert(a), param(p){}
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

    friend class ParserObject;
};

inline bool isTrxReadOnly(uint8_t trx_type) { return trx_type == TRX_READONLY; }
inline bool isTrxUpdate(uint8_t trx_type) { return trx_type & TRX_UPDATE != 0; }
inline bool isTrxAdd(uint8_t trx_type) { return trx_type & TRX_ADD != 0; }
inline bool isTrxDelete(uint8_t trx_type) { return trx_type & TRX_DELETE != 0; }
