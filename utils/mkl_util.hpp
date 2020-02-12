/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

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
