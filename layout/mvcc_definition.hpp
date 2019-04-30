/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdio>

#include "layout/mvcc_value_store.hpp"
#include "layout/layout_type.hpp"
#include "utils/tid_mapper.hpp"

struct AbstractMVCCItem {
 private:
    uint64_t begin_time;
    uint64_t end_time;

 protected:
    AbstractMVCCItem* next;

    // if begin_time field is not a transaction id, return 0. else, return trx_id starts from 1 | 0x8000000000000000.
    uint64_t GetTransactionID() const {return (begin_time >> 63) * begin_time;}
    uint64_t GetEndTime() const {return GetTransactionID() > 0 ? MIN_TIME : end_time;}
    uint64_t GetBeginTime() const {return begin_time;}

    uint64_t Init(const uint64_t& _trx_id, const uint64_t& _begin_time) {
        begin_time = _trx_id;
        end_time = _begin_time;  // notice that this is not commit time
    }

    uint64_t Commit(AbstractMVCCItem* last, const uint64_t& commit_time) {
        end_time = MAX_TIME;
        begin_time = commit_time;

        if (last != nullptr && last != this) {
            last->end_time = begin_time;
        }
    }

    static constexpr uint64_t MIN_TIME = 0;
    static constexpr uint64_t MAX_TIME = 0x7FFFFFFFFFFFFFFF;

 public:
    AbstractMVCCItem* GetNext() const {return next;}

    friend class GCExecutor;
};

struct PropertyMVCCItem : public AbstractMVCCItem {
 protected:
    ValueHeader val;

 public:
    ValueHeader GetValue() const {return val;}

    static constexpr ValueHeader EMPTY_VALUE = ValueHeader(0, 0);

    bool NeedGC() {return !val.IsEmpty();}

    template<class MVCC> friend class MVCCList;
};

struct VPropertyMVCCItem : public PropertyMVCCItem {
 public:
    void ValueGC();
    static void SetGlobalValueStore(MVCCValueStore* ptr) {value_store = ptr;}
 private:
    static MVCCValueStore* value_store;
};

struct EPropertyMVCCItem : public PropertyMVCCItem {
 public:
    void ValueGC();
    static void SetGlobalValueStore(MVCCValueStore* ptr) {value_store = ptr;}
 private:
    static MVCCValueStore* value_store;
};

struct VertexMVCCItem : public AbstractMVCCItem {
 private:
    bool val;  // whether the edge exists or not
 public:
    bool GetValue() const {return val;}

    static constexpr bool EMPTY_VALUE = false;

    bool NeedGC() const {return false;}
    void ValueGC();

    template<class MVCC> friend class MVCCList;
};

struct EdgeMVCCItem : public AbstractMVCCItem {
 private:
    EdgeItem val;
 public:
    EdgeItem GetValue() const {return val;}

    static constexpr EdgeItem EMPTY_VALUE = EdgeItem(0, nullptr);

    bool NeedGC() const {return val.ep_row_list != nullptr;}
    void ValueGC();

    template<class MVCC> friend class MVCCList;
};
/** For each type of MVCC:
 *   Member:
 *      ValueType val: the specific version value
 *      ValueType EMPTY_VALUE: constexpr, an empty ValueType
 *   Interfaces (all public):
 *      ValueType GetValue(): get val
 *      bool NeedGC(): identify if recycling this MVCC needs to invokes GC for val
 *      void ValueGC(): called when the version is overwritten in the same transaction, and NeedGC()
 */
