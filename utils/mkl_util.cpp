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

#include "mkl_util.hpp"

tbb::concurrent_hash_map<pthread_t, MKLUtil*> MKLUtil::mkl_util_instance_map_;

static __inline__ unsigned long long GetClockCycle() {
    unsigned hi, lo;
    __asm__ volatile("rdtsc":"=a"(lo), "=d"(hi));
    return ((unsigned long long)lo)|(((unsigned long long)hi) << 32);
}

MKLUtil::MKLUtil() {
    int rand_seed = GetClockCycle();

    vslNewStream(&rng_stream_, VSL_BRNG_SFMT19937, rand_seed);
}

MKLUtil::~MKLUtil() {
    vslDeleteStream(&rng_stream_);
}
