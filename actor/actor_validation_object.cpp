/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#include <tbb/concurrent_hash_map.h>

#include <string>
#include <vector>
#include <type_traits>

#include "base/type.hpp"
#include "utils/tool.hpp"

#include "actor/actor_validation_object.hpp"

void ActorValidationObject::RecordInputSetValueT(uint64_t TransactionID, int step_num, Element_T data_type, const vector<value_t> & input_set, bool recordALL) {
    vector<uint64_t> transformedInput;
    switch (data_type) {
      case Element_T::VERTEX:
        for (auto & item : input_set) {
            transformedInput.emplace_back(Tool::value_t2int(item));
        }
        break;
      case Element_T::EDGE:
        for (auto & item : input_set) {
            transformedInput.emplace_back(Tool::value_t2uint64_t(item));
        }
        break;
      default:
        cout << "wrong input data type" << endl;
        return;
    }
    RecordInputSet(TransactionID, step_num, transformedInput, recordALL);
}

void ActorValidationObject::RecordInputSet(uint64_t TransactionID, int step_num, const vector<uint64_t> & input_set, bool recordALL) {
    // Get TrxStepKey
    validation_record_key_t key(TransactionID, step_num);

    // Insert Trx2Step map
    {
        trx2step_accessor tac;
        trx_to_step_table.insert(tac, TransactionID);
        tac->second.emplace_back(step_num);
    }

    // Insert data
    data_accessor dac;
    validation_data.insert(dac, key);
    dac->second.isAll = recordALL;
    if (!recordALL)
        dac->second.data.insert(dac->second.data.end(), input_set.begin(), input_set.end());
}

// Return Value :
//     True --> no conflict
//     False --> conflict, do dependency check
bool ActorValidationObject::Validate(uint64_t TransactionID, const vector<int> & step_num_list, const vector<uint64_t> & check_set) {
    // TODO(Aaron) : Check Transaction Status on Master
    for (auto & step_num : step_num_list) {
        if (!do_validation(TransactionID, step_num, check_set)) {
            // TODO(Aaron) : Update Transaction Status on Master
            return false;
        }
    }
    return true;
}

void ActorValidationObject::DeleteInputSet(uint64_t TransactionID) {
    // Actually if delete, there is definitely no other access and insert.
    trx2step_const_accessor tcac;
    if (trx_to_step_table.find(tcac, TransactionID)) {
        for (auto & step : tcac->second) {
            validation_record_key_t key(TransactionID, step);
            validation_data.erase(key);
        }

        trx_to_step_table.erase(TransactionID);
    }
}

bool ActorValidationObject::do_validation(uint64_t TransactionID, int step_num, const vector<uint64_t> & recv_set) {
    validation_record_key_t key(TransactionID, step_num);
    for (auto & item : recv_set) {
        uint64_t inv = item >> VID_BITS;
        uint64_t outv = item - (inv << VID_BITS);

        {
            data_const_accessor daca;
            if (!validation_data.find(daca, key)) {
                return true;  // Did not find input_set --> validation success
            }

            if (daca->second.isAll) {
                return false;  // Definitely in input_set
            }

            // Once found match, conflict
            for (auto & record_item : daca->second.data) {
                if (item == record_item || inv == record_item || outv == record_item) {
                    return false;
                }
            }
        }
    }
    return true;
}

