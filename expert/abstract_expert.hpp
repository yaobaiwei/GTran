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

#ifndef EXPERT_ABSTRACT_EXPERT_HPP_
#define EXPERT_ABSTRACT_EXPERT_HPP_

#include <string>
#include <vector>
#include <thread>
#include <chrono>

#include "base/core_affinity.hpp"
#include "core/message.hpp"
#include "layout/data_storage.hpp"
#include "utils/tid_pool_manager.hpp"

class AbstractExpert {
 public:
    AbstractExpert(int id,
            CoreAffinity* core_affinity):
        id_(id),
        core_affinity_(core_affinity) {
        // instance initialized in worker.hpp
        data_storage_ = DataStorage::GetInstance();
    }

    virtual ~AbstractExpert() {}
    const int GetExpertId() { return id_; }
    virtual void process(const QueryPlan & qplan, Message & msg) = 0;
    virtual bool valid(uint64_t TrxID, vector<Expert_Object*> & step_index_list,
                       const vector<rct_extract_data_t> & check_set) {}
    virtual void clean_trx_data(uint64_t TrxID) {}

 protected:
    // Data Storage
    DataStorage* data_storage_;

    // Core affinity
    CoreAffinity* core_affinity_;

 private:
    // Expert ID
    int id_;
};

#endif  // EXPERT_ABSTRACT_EXPERT_HPP_
