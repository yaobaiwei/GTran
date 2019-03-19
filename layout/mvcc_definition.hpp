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
 public:
    ValueHeader GetValue() {return val;}

    static constexpr ValueHeader EMPTY_VALUE = ValueHeader(0, 0);

    template<class MVCC> friend class MVCCList;
};

struct VertexMVCC : public AbstractMVCC {
 private:
    bool val;  // whether the edge exists or not
 public:
    bool GetValue() {return val;}

    static constexpr bool EMPTY_VALUE = false;

    template<class MVCC> friend class MVCCList;
};

struct EdgePropertyRow;
template <class T>
class PropertyRowList;

struct EdgeItem {
    label_t label;  // if 0, then the edge is deleted
    PropertyRowList<EdgePropertyRow>* ep_row_list;  // for in_e, this is always nullptr

    bool Exist() {return label != 0;}
    EdgeItem() {}

    constexpr EdgeItem(label_t _label, PropertyRowList<EdgePropertyRow>* _ep_row_list) :
    label(_label), ep_row_list(_ep_row_list) {}
};

struct EdgeMVCC : public AbstractMVCC {
 private:
    EdgeItem val;
 public:
    EdgeItem GetValue() {return val;}

    static constexpr EdgeItem EMPTY_VALUE = EdgeItem(0, nullptr);

    template<class MVCC> friend class MVCCList;
};
