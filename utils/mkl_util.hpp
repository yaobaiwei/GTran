/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <termios.h>
#include <unistd.h>
#include <stdio.h>
#include <memory.h>
#include <signal.h>
#include <mkl.h>
#include <cstdio>
#include <string>
#include <cstdlib>
#include <iostream>
#include <list>

#include "tid_mapper.hpp"

namespace std {
// one instance for one tid
// before using this, the thread must have registed in TidMapper
class MKLUtil {
 private:
    explicit MKLUtil(const MKLUtil&);  // not to def
    MKLUtil& operator=(const MKLUtil&);  // not to def
    ~MKLUtil() {
        printf("MKLUtil::~MKLUtil()\n");
    }
    explicit MKLUtil(int tid);

    static tbb::concurrent_hash_map<int, MKLUtil*> instance_map_;
    typedef tbb::concurrent_hash_map<int, MKLUtil*>::accessor InstanceMapAccessor;

    // get instance from the map with key tid
    static MKLUtil* GetInstanceActual(int tid) {
        InstanceMapAccessor accessor;
        bool is_new = instance_map_.insert(accessor, tid);
        if (is_new) {
            accessor->second = new MKLUtil(tid);
        }

        return accessor->second;
    }

    int tid_;

    // RNG related
    VSLStreamStatePtr rng_stream_;

 public:
    // currently, assuming that each tid has only one instance of MKLUtil.
    // maybe in the future, a "thread instance" will be able to use a pool of threads.
    // if you wants to use the main thread to do computation with this class, you need to register tid for it
    static MKLUtil* GetInstance() {
        auto tid = TidMapper::GetInstance()->GetTidUnique();

        return GetInstanceActual(tid);
    }

    // Interface for testing
    void Test();

    bool UniformRNGI4(int* dst, int len, int min, int max);
    bool UniformRNGF4(float* dst, int len, float min, float max);
    bool UniformRNGF8(double* dst, int len, double min, double max);
};
}  // namespace std
