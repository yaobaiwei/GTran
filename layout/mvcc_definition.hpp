/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdio>

#include "core/common.hpp"
#include "layout/mvcc_value_store.hpp"
#include "layout/layout_type.hpp"
#include "utils/tid_mapper.hpp"

class GCProducer;
class GCConsumer;

struct AbstractMVCCItem {
 private:
    /* In our system, a timestamp's highest bit is always 0,
     *      while a trx_id's highest bit is always 1.
     * If this MVCCItem is commited, it will be visible to a transaction with timestamp
     *      in range of [begin_time, end_time).
     * If this MVCCItem is uncommited (inserted in processing stage), the begin_time field
     *      stores the trx_id (with 1 as the highest bit, always > MAX_TIME), and the
     *      end_time field stores the begin timestamp of the transaction that inserts this MVCCItem.
     */
    uint64_t begin_time;
    uint64_t end_time;
    AbstractMVCCItem* next;
    /* MVCCList is not the friend class of AbstractMVCCItem, which means that
     * variable begin_time and end_time cannot be directly modified.
     */

 protected:
    // if begin_time field is not a transaction id, return 0. else, return trx_id starts from 1 | TRX_ID_MASK.
    uint64_t GetTransactionID() const {return IS_VALID_TRX_ID(begin_time) ? begin_time : 0;}
    uint64_t GetEndTime() const {return GetTransactionID() > 0 ? MIN_TIME : end_time;}
    uint64_t GetBeginTime() const {return begin_time;}
    bool NextIsUncommitted() const {return IS_VALID_TRX_ID(end_time);}

    /* Init the begin_time and end_time after creating MVCCItem.
     *   Case 1. Called by AppendInitialVersion:
     *      This happens only during loading data; begin_time will be
     *      set to MIN_TIME, and end_time will be set to MAX_TIME.
     *   Case 2. Called by AppendVersion:
     *      This happens during processing stage; _trx_id is the transaction
     *      id, and _begin_time is the begin timestamp of the transaction.
     */
    uint64_t Init(const uint64_t& _trx_id, const uint64_t& _begin_time) {
        begin_time = _trx_id;
        end_time = _begin_time;  // notice that this is not commit time
        next = nullptr;
    }

    void AppendNextVersion(AbstractMVCCItem* new_version) {
        next = new_version;
        end_time = new_version->begin_time;  // TrxID of the new_version
    }

    void AbortNextVersion() {
        next = nullptr;
        end_time = MAX_TIME;
    }

    /* During commit stage, not only the current MVCCItem need to be modified.
     * The end_time of the previous MVCCItem of the current MVCCItem need to be modified,
     * and it won't be visible to transaction with timestamp >= commit_time after Commit.
     */
    uint64_t Commit(AbstractMVCCItem* previous_item, const uint64_t& commit_time) {
        end_time = MAX_TIME;
        begin_time = commit_time;

        if (previous_item != nullptr && previous_item != this) {
            previous_item->end_time = begin_time;
        }
    }

    static constexpr uint64_t MIN_TIME = 0;
    static constexpr uint64_t MAX_TIME = MAX_TIMESTAMP;

 public:
    AbstractMVCCItem* GetNext() const {return next;}

    friend class GCProducer;
    friend class GCConsumer;
};

/* Each type of MVCC must provide members and interfaces below:
 *   Members:
 *      ValueType val: the value of a specific version
 *   Interfaces (all public):
 *      ValueType GetValue(): get a copy of val
 *      bool NeedGC():  identify if recycling this MVCC needs to invokes GC for val
 *      void ValueGC(): invokes GC for val
 */

struct PropertyMVCCItem : public AbstractMVCCItem {
 protected:
    // ValueHeader is the metadata of a inserted value in MVCCValueStore
    ValueHeader val;

 public:
    ValueHeader GetValue() const {return val;}

    bool NeedGC() {return !val.IsEmpty();}

    template<class MVCC> friend class MVCCList;
};

// MVCCItem for VP and EP is seperated, as their value_store pointer is different.
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
    bool val;
 public:
    bool GetValue() const {return val;}

    bool NeedGC() const {return false;}
    void ValueGC();

    template<class MVCC> friend class MVCCList;
};

struct EdgeMVCCItem : public AbstractMVCCItem {
 private:
    EdgeVersion val;
 public:
    EdgeVersion GetValue() const {return val;}

    bool NeedGC() const {return val.ep_row_list != nullptr;}
    void ValueGC();

    template<class MVCC> friend class MVCCList;
};
