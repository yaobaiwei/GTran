/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)

*/

#ifndef ACTOR_ABSTRACT_ACTOR_HPP_
#define ACTOR_ABSTRACT_ACTOR_HPP_

#include <string>
#include <vector>
#include <thread>
#include <chrono>

#include "base/core_affinity.hpp"
#include "core/message.hpp"
#include "storage/data_store.hpp"
#include "utils/tid_mapper.hpp"

class AbstractActor {
 public:
    AbstractActor(int id,
            DataStore* data_store,
            CoreAffinity* core_affinity):
        id_(id),
        data_store_(data_store),
        core_affinity_(core_affinity) {}

    virtual ~AbstractActor() {}
    const int GetActorId() { return id_; }
    virtual void process(const QueryPlan & qplan, Message & msg) = 0;
    virtual bool valid(uint64_t TrxID, const vector<Actor_Object> & step_index_list, const vector<rct_read_data_t> & check_set) {}

 protected:
    // Data Store
    DataStore* data_store_;

    // Core affinity
    CoreAffinity* core_affinity_;

 private:
    // Actor ID
    int id_;
};

#endif  // ACTOR_ABSTRACT_ACTOR_HPP_
