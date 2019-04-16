/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once


#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <pthread.h>
#include <string.h>

#include <atomic>
#include <iostream>

#include "tbb/concurrent_hash_map.h"

namespace std {
class TidMapper {
 private:
    explicit TidMapper(const TidMapper&);
    TidMapper& operator=(const TidMapper&);
    ~TidMapper() {}

    typedef tbb::concurrent_hash_map<pthread_t, int>::accessor TidAccessor;
    tbb::concurrent_hash_map<pthread_t, int> manual_tid_map_;
    tbb::concurrent_hash_map<pthread_t, int> unique_tid_map_;

    atomic_int thread_count_;

 public:
    static TidMapper* GetInstance() {
        static TidMapper thread_mapper_single_instance;
        return &thread_mapper_single_instance;
    }

    void Register(int tid);
    int GetTid();
    int GetTidUnique();

 private:
    TidMapper();
};
}  // namespace std
