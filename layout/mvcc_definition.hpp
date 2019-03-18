/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdio>

#include "layout/mvcc_value_store.hpp"
#include "storage/layout.hpp"

struct AbstractMVCC {
 private:
    uint64_t begin_time;
    uint64_t end_time;

 protected:
    AbstractMVCC* next;

    // TODO(entityless): Test these functions if they're right

    // if begin_time field is not a transaction id, return 0. else, return trx_id starts from 1 | 0x8000000000000000.
    uint64_t GetTransactionID() const {return (begin_time >> 63) * begin_time;}
    uint64_t GetEndTime() const {return GetTransactionID() > 0 ? MIN_TIME : end_time;}
    uint64_t GetBeginTime() const {return begin_time;}

    uint64_t Init(const uint64_t& _trx_id, const uint64_t& _begin_time) {
        begin_time = _trx_id;
        end_time = _begin_time;  // notice that this is not commit time
    }

    uint64_t Commit(AbstractMVCC* last, const uint64_t& commit_time) {
        end_time = MAX_TIME;
        begin_time = commit_time;

        if (last != nullptr && last != this) {
            last->end_time = begin_time;
        }
    }

    static constexpr uint64_t MIN_TIME = 0;
    static constexpr uint64_t MAX_TIME = 0x7FFFFFFFFFFFFFFF;
};

struct PropertyMVCC : public AbstractMVCC {
 private:
    ValueHeader val;
    void SetValue(const ValueHeader& _val) {val = _val;}
 public:
    ValueHeader GetValue() {return val;}

    template<class MVCC> friend class MVCCList;
};

struct TopologyMVCC : public AbstractMVCC {
 private:
    bool val;  // whether the edge exists or not
    void SetValue(const bool& _val) {val = _val;}
 public:
    bool GetValue() {return val;}

    template<class MVCC> friend class MVCCList;
};
