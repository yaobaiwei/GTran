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
#include <vector>
#include <map>
#include <utility>

#include "base/type.hpp"
#include "core/exec_plan.hpp"
#include "layout/data_storage.hpp"
#include "layout/index_store.hpp"
#include "utils/tool.hpp"
#include "utils/hdfs_core.hpp"
#include "utils/config.hpp"

class Parser {
 private:
/*-----------------Global members for all transactions--------------------------
------------------------------------------------------------------------------*/

    Config * config;
    IndexStore * index_store;
    string_index * indexes;

    vector<string> vpks, vlks, epks, elks;

    string vpks_str, vlks_str, epks_str, elks_str;

 public:
    // Parse query string
    bool Parse(const string& trx_input, TrxPlan& vec, string& error_msg);

    Parser(IndexStore* index_store_): index_store(index_store_) {
        config = Config::GetInstance();
    }

    int GetPid(Element_T type, string& property);

    // load property and label mapping
    void LoadMapping(DataStorage * data_storage);

    friend class ParserObject;
};


/* One ParserObject is created specifically for the parsing of one transaction.
 * ParserObject contains necessary local members for parsing one transaction or one query.
 * It will only be created in Parser::Parse().
 */
class ParserObject {
 private:
    enum IO_T { EDGE, VERTEX, VP, EP, INT, DOUBLE, CHAR, STRING, COLLECTION };
    static const char *IOType[];
    // str to enum
    static const map<string, Step_T> str2step;         // step type
    static const map<string, Predicate_T> str2pred;    // predicate type

    static const int index_ratio = 3;

    // Used to access global members for all transactions.
    Parser* parser_;

/*-----------------local members for one transaction----------------------------
------------------------------------------------------------------------------*/

    // Name of query variable to <query index, type>
    map<string, pair<int, IO_T>> place_holder;

    // Unique index of each expert object in transaction
    int expert_index;

    // Current line
    uint8_t line_index;

    // Topological level of last update query
    int last_update;

    // next side effect key
    uint8_t side_effect_key;

    TrxPlan* trx_plan;

/*-----------------local members for one line in trx----------------------------
------------------------------------------------------------------------------*/
    bool is_read_only_;

    // In/out data type
    IO_T io_type_;

    // first step index in sub query
    int first_in_sub_;

    // record step index of expert which should send data to non-local nodes
    int dispatch_step_;

    // disable index when (count of given predicate > min_count_ * ratio)
    vector<uint64_t> index_count_;
    uint64_t min_count_;

    // str to id
    map<string, int> str2ls_;  // label step
    map<string, int> str2se_;  // side-effect

    // id to enm
    map<int, IO_T> ls2type_;  // label step output type

    // tmp experts store
    vector<Expert_Object> experts_;


/*---------------------------------Functions------------------------------------
------------------------------------------------------------------------------*/

    // IO type checking
    bool IsNumber();
    bool IsValue(uint8_t& type);
    bool IsElement();
    bool IsElement(Element_T& type);
    IO_T Value2IO(uint8_t type);

    // check the type of last expert
    bool CheckLastExpert(EXPERT_T type);

    // check if parameter is query
    bool CheckIfQuery(const string& param);

    // Get priority for re-ordering
    int GetStepPriority(Step_T type);

    // splitting parameters
    void SplitParam(string& param, vector<string>& params);
    void SplitPredicate(string& param, Predicate_T& pred_type, vector<string>& params);

    void ClearTrx();
    void ClearQuery();

    void AppendExpert(Expert_Object& expert);
    void RemoveLastExpert();

    void RegPlaceHolder(const string& var, int step, int param_index, IO_T type);

    // Parse each line of transaction
    bool ParseLine(const string& query, vector<Expert_Object>& vec, string& error_msg);

    // Parse build index
    void ParseIndex(const string& param);

    // Parse set config
    void ParseSetConfig(const string& param);

    // Parse display runtime status of the system
    void ParseDisplayStatus(const string& param);

    // Parse query or sub-query
    void ParseQuery(const string& query);

    // extract steps and corresponding params from query string
    void GetSteps(const string& query, vector<pair<Step_T, string>>& tokens);

    // Re-ordering Optimization
    void ReOrderSteps(vector<pair<Step_T, string>>& tokens);

    // mapping steps to experts
    void ParseSteps(const vector<pair<Step_T, string>>& tokens);

    // mapping string to label key or property key
    bool ParseKeyId(string key, bool isLabel, int& id, uint8_t* type = NULL);

    // error message for unexpected key
    string ExpectedKey(bool isLabel);

    // Parse sub-query for branching experts
    void ParseSub(const vector<string>& params, int current_step, bool checkType);

    // Parse predicate
    void ParsePredicate(string& param, uint8_t type, Expert_Object& expert, bool toKey);

    // Parse experts
    void ParseInit(const string& line, string& var_name, string& query);
    void ParseAddE(const vector<string>& params);
    void ParseFromTo(const vector<string>& params, Step_T type);
    void ParseAddV(const vector<string>& params);
    void ParseAggregate(const vector<string>& params);
    void ParseAs(const vector<string>& params);
    void ParseBranch(const vector<string>& params);
    void ParseBranchFilter(const vector<string>& params, Step_T type);
    void ParseCap(const vector<string>& params);
    void ParseCount(const vector<string>& params);
    void ParseDedup(const vector<string>& params);
    void ParseDrop(const vector<string>& params);
    void ParseGroup(const vector<string>& params, Step_T type);
    void ParseHas(const vector<string>& params, Step_T type);
    void ParseHasLabel(const vector<string>& params);
    void ParseIs(const vector<string>& params);
    void ParseKey(const vector<string>& params);
    void ParseLabel(const vector<string>& params);
    void ParseMath(const vector<string>& params, Step_T type);
    void ParseOrder(const vector<string>& params);
    void ParseProject(Element_T type, int key_id, int value_id);
    void ParseProperties(const vector<string>& params);
    void ParseProperty(const vector<string>& params);
    void ParseRange(const vector<string>& params, Step_T type);
    void ParseCoin(const vector<string>& params);
    void ParseRepeat(const vector<string>& params);
    void ParseSelect(const vector<string>& params);
    void ParseTraversal(const vector<string>& params, Step_T type);
    void ParseValues(const vector<string>& params);
    void ParseWhere(const vector<string>& params);

    // Add commit statement including validation & finish (Commit or Abort)
    void AddCommitStatement(TrxPlan& vec);

    ParserObject(Parser* parser) : parser_(parser) {}

    bool Parse(const string& trx_input, TrxPlan& vec, string& error_msg);

    // parsing exception
    struct ParserException {
        string message;

        explicit ParserException(const std::string &message) : message(message) {}
        explicit ParserException(const char *message) : message(message) {}
    };

    friend class Parser;
};
