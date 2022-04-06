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

#include <atomic>

#include "base/type.hpp"
#include "glog/logging.h"
#include "tbb/concurrent_hash_map.h"

class TidPoolManager {
 private:
    explicit TidPoolManager(const TidPoolManager&);
    TidPoolManager& operator=(const TidPoolManager&);
    TidPoolManager() {}
    ~TidPoolManager() {}

    typedef tbb::concurrent_hash_map<pthread_t, int> TidMap;
    typedef tbb::concurrent_hash_map<pthread_t, int>::accessor TidAccessor;
    typedef tbb::concurrent_hash_map<pthread_t, int>::const_accessor TidConstAccessor;

    struct TidManager {
        TidMap tid_map;
        atomic_int tid_to_assign;

        TidManager() {
            tid_to_assign = 0;
        }

        void Register(int tid = -1) {
            TidAccessor accessor;
            int _tid_to_assign;
            if (tid == -1)
                _tid_to_assign = tid_to_assign++;  // auto-increment thread id (currently for using containers)
            else
                _tid_to_assign = tid;  // assigned thread id (currently for using RDMA)
            tid_map.insert(accessor, pthread_self());
            accessor->second = _tid_to_assign;
        }

        int GetTid() {
            TidConstAccessor accessor;
            CHECK(tid_map.find(accessor, pthread_self()));
            return accessor->second;
        }
    };

    TidManager tid_managers_[(int)TID_TYPE::COUNT];

 public:
    static TidPoolManager* GetInstance() {
        static TidPoolManager single_instance;
        return &single_instance;
    }

    void Register(TID_TYPE type, int tid = -1) {
        tid_managers_[(int)type].Register(tid);
    }

    int GetTid(TID_TYPE type) {
        return tid_managers_[(int)type].GetTid();
    }
};
