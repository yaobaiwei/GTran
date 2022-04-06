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

#include <tbb/concurrent_hash_map.h>

#include <string>
#include <vector>
#include <type_traits>

#include "base/type.hpp"
#include "expert/expert_validation_object.hpp"
#include "utils/tool.hpp"


void ExpertValidationObject::RecordInputSetValueT(uint64_t TransactionID, int step_num, Element_T data_type, const vector<value_t> & input_set, bool recordALL) {
    vector<uint64_t> transformedInput;
    if (!recordALL) {  // Unnecessary to transfer when recording all
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
    }
    RecordInputSet(TransactionID, step_num, transformedInput, recordALL);
}

void ExpertValidationObject::RecordInputSet(uint64_t TransactionID, int step_num, const vector<uint64_t> & input_set, bool recordALL) {
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
bool ExpertValidationObject::Validate(uint64_t TransactionID, int step_num, const vector<uint64_t> & check_set) {
    validation_record_key_t key(TransactionID, step_num);
    for (auto & item : check_set) {
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

void ExpertValidationObject::DeleteInputSet(uint64_t TransactionID) {
    // Actually if delete, there is definitely no other access and insert.
    trx2step_const_accessor tcac;
    if (trx_to_step_table.find(tcac, TransactionID)) {
        for (auto & step : tcac->second) {
            validation_record_key_t key(TransactionID, step);
            validation_data.erase(key);
        }
        trx_to_step_table.erase(tcac);
    }
}
