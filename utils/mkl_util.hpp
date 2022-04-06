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

#include <mkl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>

#include "tbb/concurrent_hash_map.h"

class MKLUtil {
 private:
    explicit MKLUtil(const MKLUtil&);
    MKLUtil& operator=(const MKLUtil&);
    explicit MKLUtil();
    ~MKLUtil();

    static tbb::concurrent_hash_map<pthread_t, MKLUtil*> mkl_util_instance_map_;
    typedef tbb::concurrent_hash_map<pthread_t, MKLUtil*>::accessor InstanceMapAccessor;

    // random number generator stream of MKL
    VSLStreamStatePtr rng_stream_;

 public:
    static MKLUtil* GetInstance() {
        InstanceMapAccessor accessor;

        // one MKLUtil instance per thread
        bool is_new = mkl_util_instance_map_.insert(accessor, pthread_self());
        if (is_new) {
            accessor->second = new MKLUtil();
        }

        return accessor->second;
    }

    template<class T>
    bool UniformRNG(T* output, int len, T min, T max) {
        int status = vsRngUniform(VSL_RNG_METHOD_UNIFORM_STD, rng_stream_, len, output, min, max);
        return status == VSL_STATUS_OK;
    }
};
