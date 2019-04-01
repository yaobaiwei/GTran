/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef ACTOR_VALIDATION_ACTOR_HPP_
#define ACTOR_VALIDATION_ACTOR_HPP_

#include <unistd.h>
#include <tbb/concurrent_hash_map.h>

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "base/type.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/exec_plan.hpp"
#include "core/message.hpp"
#include "core/factory.hpp"
#include "core/index_store.hpp"
#include "layout/pmt_rct_table.hpp"
#include "utils/tool.hpp"
#include "utils/mymath.hpp"

#include "glog/logging.h"

class ValidationActor : public AbstractActor {
 public:
    ValidationActor(int id,
            int machine_id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity,
            PrimitiveRCTTable * pmt_rct_table,
            map<ACTOR_T, unique_ptr<AbstractActor>> * actors,
            tbb::concurrent_hash_map<uint64_t, QueryPlan> * msg_logic_table) :
        AbstractActor(id, core_affinity),
        machine_id_(machine_id),
        num_thread_(num_thread),
        mailbox_(mailbox),
        pmt_rct_table_(pmt_rct_table),
        type_(ACTOR_T::VALIDATION),
        actors_(actors),
        msg_logic_table_(msg_logic_table) {
        config_ = Config::GetInstance();
        trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();
        prepare_primitive_list();
    }

    void process(const QueryPlan & qplan, Message & msg);

 private:
    // Number of Threads
    int num_thread_;
    int machine_id_;

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config * config_;
    // API for worker to communicate with master
    TrxTableStub * trx_table_stub_;

    // Qid-Actor-map and Trx-vector<Actor>-map
    map<ACTOR_T, unique_ptr<AbstractActor>>* actors_;
    typedef tbb::concurrent_hash_map<uint64_t, QueryPlan> trx_actors_hashmap;
    typedef tbb::concurrent_hash_map<uint64_t, QueryPlan>::const_accessor const_accessor;
    trx_actors_hashmap* msg_logic_table_;

    // Primitive -> RCT Table
    PrimitiveRCTTable * pmt_rct_table_;

    // TIMEOUT
    static const uint64_t OPT_VALID_TIMEOUT_ = 5;
    static const uint64_t OPT_VALID_SLEEP_TIME_ = 1000;

    enum { ACTOR_TYPE_BITS = 15 };
    enum { ONLY_FIRST_STEP_BITS = 1 };
    enum { STEP_TYPE_BITS = 32 - ACTOR_TYPE_BITS - ONLY_FIRST_STEP_BITS };
    // validation_step_t
    struct vstep_t {
        uint32_t actor_type : ACTOR_TYPE_BITS;
        uint32_t step_type : STEP_TYPE_BITS;
        uint32_t only_first : ONLY_FIRST_STEP_BITS;  // 0 for first or not first, 1 for only first

        vstep_t() : actor_type(0), step_type(0), only_first(0) {}

        vstep_t(ACTOR_T actor_type_) : actor_type(static_cast<int>(actor_type_)), step_type(0), only_first(0) {
            assert(actor_type == static_cast<int>(actor_type_));
        }

        vstep_t(ACTOR_T actor_type_, int only_first_) :
            actor_type(static_cast<int>(actor_type_)), step_type(0), only_first(only_first_) {
            assert((actor_type == static_cast<int>(actor_type_)) && (only_first == only_first_));
        }

        vstep_t(ACTOR_T actor_type_, Step_T step_type_, int only_first_)
            : actor_type(static_cast<int>(actor_type_)), step_type(static_cast<int>(step_type_)), only_first(only_first_) {
            assert((actor_type == static_cast<int>(actor_type_)) && (step_type == static_cast<int>(step_type_)) && (only_first == only_first_));
        }

        bool operator == (const vstep_t & vk1) const {
            return (vk1.actor_type == actor_type && vk1.step_type == step_type && vk1.only_first == only_first);
        }

        bool operator < (const vstep_t & vk1) const {
            return step_type < vk1.step_type;
        }

        void DebugPrint() {
            cout << "Actor : {" << actor_type << "}; Step : {" << step_type << "}; IsOnlyFirstStep : {" << only_first << "}"<< endl;
        }
    };

    struct KeyHasher {
        std::size_t operator()(const vstep_t& key) const {
            int k1 = static_cast<int>(key.actor_type);
            int k2 = static_cast<int>(key.step_type);
            int k3 = static_cast<int>(key.only_first);
            size_t seed = 0;
            mymath::hash_combine(seed, k1);
            mymath::hash_combine(seed, k2);
            mymath::hash_combine(seed, k3);
            return seed;
        }
    };

    // Set for necessarily validate actor
    // Access Only
    set<ACTOR_T> needValidateActorSet_;
    unordered_map<int, set<vstep_t>> primitiveStepMap_;

    // step_rct_map
    typedef unordered_map<vstep_t, unordered_map<uint64_t, vector<rct_extract_data_t>>, KeyHasher> step2TrxRct_map_t_;
    typedef unordered_map<vstep_t, vector<Actor_Object*>, KeyHasher> step2aobj_map_t_;

    // Validate transaction
    // return: isAbort
    bool validate(const QueryPlan & qplan, Message & msg);

    void prepare_primitive_list();

    bool valid_dependency_read(uint64_t trxID, vector<uint64_t> & homo_dep_read, vector<uint64_t> & hetero_dep_read);

    void process_trx(int num_queries, int cur_qid, set<vstep_t> & trx_step_sets, step2aobj_map_t_ & step_aobj_map);

    void get_recent_action_set(const vector<uint64_t> & trxIDList, unordered_map<int, unordered_map<uint64_t, vector<rct_extract_data_t>>> & rct_map);

    void get_vstep(Actor_Object * cur_actor_obj, int step_num, set<vstep_t> & step_sets, step2aobj_map_t_ & step_aobj_map);
    void get_vstep_for_has(Actor_Object * cur_actor_obj, int step_num, set<vstep_t> & step_sets, step2aobj_map_t_ & step_aobj_map);

    bool do_step_validation(int cur_trxID, step2TrxRct_map_t_ & check_step_map, vector<uint64_t> & optimistic_validation_trx, step2aobj_map_t_ & step_aobj_map);
    void valid_optimistic_validation(vector<uint64_t> & optimistic_validation_trx, bool & isAbort);
    void valid_optimistic_read(vector<uint64_t> & homo_dep_read, bool & isAbort);

    void insert_step_aobj_map(step2aobj_map_t_ & step_aobj_map, const vstep_t & vstep, Actor_Object * obj);
    // Test for InsertRCT
    void test_insert_rct(uint64_t trxID, vector<uint64_t> & values, vector<int> & p_vec);
};

#endif  // ACTOR_VALIDATION_ACTOR_HPP_
