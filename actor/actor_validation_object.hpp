/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef ACTOR_ACTOR_VALIDATION_OBJECT_HPP_
#define ACTOR_ACTOR_VALIDATION_OBJECT_HPP_

#include <tbb/concurrent_hash_map.h>

#include <string>
#include <vector>
#include <type_traits>

#include "base/type.hpp"
#include "utils/tool.hpp"

class ActorValidationObject {
 public :
    ActorValidationObject() {}

    void RecordInputSetValueT(uint64_t TransactionID, int step_num, Element_T data_type, const vector<value_t> & input_set, bool recordALL);
    void RecordInputSet(uint64_t TransactionID, int step_num, const vector<uint64_t> & input_set, bool recordALL);

    // Return Value :
    //     True --> no conflict
    //     False --> conflict, do dependency check
    bool Validate(uint64_t TransactionID, int step_num, const vector<uint64_t> & check_set);

    void DeleteInputSet(uint64_t TransactionID);

 private:
    // TransactionID -> <step>
    // <TrxID, step> -> <value>
    //
    struct validation_record_key_t {
        uint64_t trxID;
        int step_num;

        validation_record_key_t() : trxID(0), step_num(0) {}
        validation_record_key_t(uint64_t _trxID, int _step_num) : trxID(_trxID), step_num(_step_num) {}

        bool operator==(const validation_record_key_t & key) const {
            if ((trxID == key.trxID) && (step_num == key.step_num)) {
                return true;
            }
            return false;
        }

        void DebugString() {
            cout << "Transaction : " << trxID << " Step : " << step_num << endl;
        }
    };

    struct validation_record_hash_compare {
        static size_t hash(const validation_record_key_t& key) {
            uint64_t k1 = (uint64_t)key.trxID;
            int k2 = static_cast<int>(key.step_num);
            size_t seed = 0;
            mymath::hash_combine(seed, k1);
            mymath::hash_combine(seed, k2);
            return seed;
        }

        static bool equal(const validation_record_key_t& key1, const validation_record_key_t& key2) {
            return (key1 == key2);
        }
    };

    struct validation_record_val_t {
        // If isAll = true; data will be empty;
        vector<uint64_t> data;
        bool isAll;

        validation_record_val_t() {}

        void DebugString() {
            cout << (isAll ? "True" : "False") << endl;
            cout << "Data size : " << data.size() << endl;
            if (!isAll) {
                for (auto & item : data) {
                    cout << item << endl;
                }
            }
        }
    };

    tbb::concurrent_hash_map<uint64_t, vector<int>> trx_to_step_table;
    typedef tbb::concurrent_hash_map<uint64_t, vector<int>>::accessor trx2step_accessor;
    typedef tbb::concurrent_hash_map<uint64_t, vector<int>>::const_accessor trx2step_const_accessor;

    tbb::concurrent_hash_map<validation_record_key_t, validation_record_val_t, validation_record_hash_compare> validation_data;
    typedef tbb::concurrent_hash_map<validation_record_key_t, validation_record_val_t, validation_record_hash_compare>::accessor data_accessor;
    typedef tbb::concurrent_hash_map<validation_record_key_t, validation_record_val_t, validation_record_hash_compare>::const_accessor data_const_accessor;
};

#endif  // ACTOR_ACTOR_VALIDATION_OBJECT_HPP_
