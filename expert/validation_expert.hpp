// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
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

#ifndef EXPERT_VALIDATION_EXPERT_HPP_
#define EXPERT_VALIDATION_EXPERT_HPP_

#include <unistd.h>
#include <tbb/concurrent_hash_map.h>

#include <algorithm>
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/type.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/exec_plan.hpp"
#include "core/message.hpp"
#include "core/factory.hpp"
#include "expert/abstract_expert.hpp"
#include "layout/index_store.hpp"
#include "layout/pmt_rct_table.hpp"
#include "utils/tool.hpp"
#include "utils/mymath.hpp"

#include "glog/logging.h"

class ValidationExpert : public AbstractExpert {
 public:
    ValidationExpert(int id,
            int machine_id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity,
            map<EXPERT_T, unique_ptr<AbstractExpert>> * experts,
            tbb::concurrent_hash_map<uint64_t, QueryPlan> * msg_logic_table) :
        AbstractExpert(id, core_affinity),
        machine_id_(machine_id),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(EXPERT_T::VALIDATION),
        experts_(experts),
        msg_logic_table_(msg_logic_table) {
        config_ = Config::GetInstance();
        pmt_rct_table_ = PrimitiveRCTTable::GetInstance();
        trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();
        index_store_ = IndexStore::GetInstance();
        prepare_primitive_list();
    }

    void process(const QueryPlan & qplan, Message & msg);

 private:
    // Number of Threads
    int num_thread_;
    int machine_id_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config * config_;
    // API for worker to communicate with master
    TrxTableStub * trx_table_stub_;

    // Index Store
    IndexStore * index_store_;

    // Qid-Expert-map and Trx-vector<Expert>-map
    map<EXPERT_T, unique_ptr<AbstractExpert>>* experts_;
    typedef tbb::concurrent_hash_map<uint64_t, QueryPlan> trx_experts_hashmap;
    typedef tbb::concurrent_hash_map<uint64_t, QueryPlan>::const_accessor const_accessor;
    trx_experts_hashmap* msg_logic_table_;

    // Primitive -> RCT Table
    PrimitiveRCTTable * pmt_rct_table_;

    // TIMEOUT
    static const uint64_t OPT_VALID_TIMEOUT_ = 1;
    static const uint64_t OPT_VALID_SLEEP_TIME_ = 100;

    enum { EXPERT_TYPE_BITS = 15 };
    enum { ONLY_FIRST_STEP_BITS = 1 };
    enum { STEP_TYPE_BITS = 32 - EXPERT_TYPE_BITS - ONLY_FIRST_STEP_BITS };
    // validation_step_t
    struct vstep_t {
        uint32_t expert_type : EXPERT_TYPE_BITS;
        uint32_t step_type : STEP_TYPE_BITS;
        uint32_t only_first : ONLY_FIRST_STEP_BITS;  // 0 for first or not first, 1 for only first

        vstep_t() : expert_type(0), step_type(0), only_first(0) {}

        vstep_t(EXPERT_T expert_type_) : expert_type(static_cast<int>(expert_type_)), step_type(0), only_first(0) {
            CHECK(expert_type == static_cast<int>(expert_type_));
        }

        vstep_t(EXPERT_T expert_type_, int only_first_) :
            expert_type(static_cast<int>(expert_type_)), step_type(0), only_first(only_first_) {
            CHECK((expert_type == static_cast<int>(expert_type_)) && (only_first == only_first_));
        }

        vstep_t(EXPERT_T expert_type_, Step_T step_type_, int only_first_)
            : expert_type(static_cast<int>(expert_type_)), step_type(static_cast<int>(step_type_)), only_first(only_first_) {
            CHECK((expert_type == static_cast<int>(expert_type_)) && (step_type == static_cast<int>(step_type_)) && (only_first == only_first_));
        }

        bool operator == (const vstep_t & right) const {
            return (right.expert_type == expert_type && right.step_type == step_type && right.only_first == only_first);
        }

        bool operator < (const vstep_t & right) const {
            if (expert_type < right.expert_type) {
                return true;
            } else if (expert_type > right.expert_type) {
                return false;
            } else {
                if (step_type < right.step_type) {
                    return true;
                } else if (step_type > right.step_type) {
                    return false;
                } else {
                    if (only_first < right.only_first) {
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        }

        void DebugPrint() {
            cout << "Expert : {" << ExpertType[expert_type] << "}; Step : {" << step_type << "}; IsOnlyFirstStep : {" << only_first << "}"<< endl;
        }
    };

    struct KeyHasher {
        std::size_t operator()(const vstep_t& key) const {
            int k1 = static_cast<int>(key.expert_type);
            int k2 = static_cast<int>(key.step_type);
            int k3 = static_cast<int>(key.only_first);
            size_t seed = 0;
            mymath::hash_combine(seed, k1);
            mymath::hash_combine(seed, k2);
            mymath::hash_combine(seed, k3);
            return seed;
        }
    };

    // Set for necessarily validate expert
    // Access Only
    set<EXPERT_T> needValidateExpertSet_;
    unordered_map<int, set<vstep_t>> primitiveStepMap_;

    // step_rct_map
    typedef unordered_map<vstep_t, unordered_map<uint64_t, vector<rct_extract_data_t>>, KeyHasher> step2TrxRct_map_t_;
    typedef unordered_map<vstep_t, vector<Expert_Object*>, KeyHasher> step2aobj_map_t_;

    // Validate transaction
    // return: isAbort
    bool validate(const QueryPlan & qplan, Message & msg);

    void prepare_primitive_list();

    bool valid_dependency_read(uint64_t trxID, set<uint64_t> & homo_dep_read, set<uint64_t> & hetero_dep_read);

    void process_trx(int num_queries, uint64_t cur_qid, set<vstep_t> & trx_step_sets, step2aobj_map_t_ & step_aobj_map);

    bool get_recent_action_set(const vector<uint64_t> & trxIDList, unordered_map<int, unordered_map<uint64_t, vector<rct_extract_data_t>>> & rct_map);

    void get_vstep(Expert_Object * cur_expert_obj, int step_num, set<vstep_t> & step_sets, step2aobj_map_t_ & step_aobj_map);
    void get_vstep_for_has(Expert_Object * cur_expert_obj, int step_num, set<vstep_t> & step_sets, step2aobj_map_t_ & step_aobj_map);

    bool do_step_validation(uint64_t cur_trxID, step2TrxRct_map_t_ & check_step_map, vector<uint64_t> & optimistic_validation_trx, step2aobj_map_t_ & step_aobj_map);
    void valid_optimistic_validation(vector<uint64_t> & optimistic_validation_trx, bool & isAbort);
    void valid_optimistic_read(set<uint64_t> & homo_dep_read, bool & isAbort);

    void insert_step_aobj_map(step2aobj_map_t_ & step_aobj_map, const vstep_t & vstep, Expert_Object * obj);
    // Test for InsertRCT
    void test_insert_rct(uint64_t trxID, vector<uint64_t> & values, vector<int> & p_vec);
};

#endif  // EXPERT_VALIDATION_EXPERT_HPP_
