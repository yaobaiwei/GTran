/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include <utility>
#include "tid_mapper.hpp"

using std::TidMapper;

TidMapper::TidMapper() {
    thread_count_ = 0;
}

void TidMapper::Register(int tid) {
    TidAccessor unique_accessor, manual_accessor;
    int unique_tid = thread_count_++;
    unique_tid_map_.insert(unique_accessor, pthread_self());
    manual_tid_map_.insert(manual_accessor, pthread_self());

    unique_accessor->second = unique_tid;
    manual_accessor->second = tid;
}

int TidMapper::GetTid() {
    TidAccessor manual_accessor;
    assert(manual_tid_map_.find(manual_accessor, pthread_self()));
    return manual_accessor->second;
}

int TidMapper::GetTidUnique() {
    TidAccessor unique_accessor;
    assert(unique_tid_map_.find(unique_accessor, pthread_self()));
    return unique_accessor->second;
}
