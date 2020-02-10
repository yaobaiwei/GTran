/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

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
