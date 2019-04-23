/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
         Modified by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/
#pragma once
#include <limits.h>
#include <mkl_vsl.h>
#include <string>
#include <map>
#include <set>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <utility>

#include "actor/abstract_actor.hpp"
#include "actor/actor_validation_object.hpp"
#include "core/factory.hpp"
#include "core/result_collector.hpp"
#include "utils/tool.hpp"

#include "utils/mkl_util.hpp"

namespace BarrierData {
struct barrier_data_base {
    map<string, int> path_counter;
};
}  // namespace BarrierData

// Base class for barrier actors
template<class T = BarrierData::barrier_data_base>
class BarrierActorBase :  public AbstractActor {
    static_assert(std::is_base_of<BarrierData::barrier_data_base, T>::value,
                "T must derive from barrier_data_base");
    using BarrierDataTable = tbb::concurrent_hash_map<mkey_t, T, MkeyHashCompare>;
    using TrxTable = tbb::concurrent_hash_map<uint64_t, set<mkey_t>>;

 public:
    BarrierActorBase(int id, CoreAffinity* core_affinity, AbstractMailbox * mailbox)
        : AbstractActor(id, core_affinity), mailbox_(mailbox) {}

    void process(const QueryPlan & qplan, Message & msg);

    void clean_input_set(uint64_t TrxID) override;

 protected:
    virtual void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            typename BarrierDataTable::accessor& ac,
            bool isReady) = 0;

    // get labelled branch key if in branch
    static int get_branch_key(Meta & m);

    // get history value by given key
    static bool get_history_value(history_t& his, int history_key, value_t& val, bool erase_his = false);

    static int get_branch_value(history_t& his, int branch_key, bool erase_his = true);

    static inline bool is_next_barrier(const vector<Actor_Object>& actors, int step) {
        int next = actors[step].next_actor;
        return next < actors.size() && actors[next].IsBarrier();
    }

    // Mailbox
    AbstractMailbox * mailbox_;

 private:
    // concurrent_hash_map, storing data for barrier processing
    BarrierDataTable data_table_;

    // Trxid to set of mkey_t
    // Record not earsed mkey_t in data_table_
    TrxTable trx_table_;

    // Check if msg all collected
    static bool IsReady(typename BarrierDataTable::accessor& ac, Meta& m, string end_path);

    // get msg info
    // key : mkey_t, identifier of msg
    // end_path: identifier of msg collection completed
    static void GetMsgInfo(Message& msg, mkey_t &key, string &end_path);
};

namespace BarrierData {
struct end_data : barrier_data_base{
    vector<value_t> result;
};
}  // namespace BarrierData

class EndActor : public BarrierActorBase<BarrierData::end_data> {
 public:
    EndActor(int id,
            int num_nodes,
            ResultCollector * rc,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierActorBase<BarrierData::end_data>(id, core_affinity, mailbox),
        num_nodes_(num_nodes),
        rc_(rc) {}

 private:
    ResultCollector * rc_;
    int num_nodes_;

    void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            BarrierDataTable::accessor& ac,
            bool isReady);
};

namespace BarrierData {
struct agg_data : barrier_data_base {
    vector<value_t> agg_data;
    vector<pair<history_t, vector<value_t>>> msg_data;
};
}  // namespace BarrierData

class AggregateActor : public BarrierActorBase<BarrierData::agg_data> {
 public:
    AggregateActor(int id,
            int num_nodes,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierActorBase<BarrierData::agg_data>(id, core_affinity, mailbox),
        num_nodes_(num_nodes),
        num_thread_(num_thread) {}

 private:
    int num_nodes_;
    int num_thread_;

    void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            BarrierDataTable::accessor& ac,
            bool isReady);
};

class CapActor : public BarrierActorBase<> {
 public:
    CapActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierActorBase<>(id, core_affinity, mailbox),
        num_thread_(num_thread) {}

 private:
    int num_thread_;

    void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            BarrierDataTable::accessor& ac,
            bool isReady);
};

namespace BarrierData {
struct count_data : barrier_data_base {
    // int: assigned branch value by labelled branch step
    // pair:
    //  history_t: histroy of data
    //  int:       record num of incoming data
    unordered_map<int, pair<history_t, int>> counter_map;
};
}  // namespace BarrierData

class CountActor : public BarrierActorBase<BarrierData::count_data> {
 public:
    CountActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierActorBase<BarrierData::count_data>(id, core_affinity, mailbox),
        num_thread_(num_thread) {}

 private:
    int num_thread_;

    void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            BarrierDataTable::accessor& ac,
            bool isReady);
};

namespace BarrierData {
struct dedup_data : barrier_data_base {
    // int: assigned branch value by labelled branch step
    // vec: filtered data
    unordered_map<int, vector<pair<history_t, vector<value_t>>>> data_map;
    unordered_map<int, set<history_t>> dedup_his_map;    // for dedup by history
    unordered_map<int, set<value_t>> dedup_val_map;        // for dedup by value
};
}  // namespace BarrierData

class DedupActor : public BarrierActorBase<BarrierData::dedup_data> {
 public:
    DedupActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierActorBase<BarrierData::dedup_data>(id, core_affinity, mailbox),
        num_thread_(num_thread) {}

 private:
    int num_thread_;

    void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            BarrierDataTable::accessor& ac,
            bool isReady);
};

namespace BarrierData {
struct group_data : barrier_data_base {
    // int: assigned branch value by labelled branch step
    // pair:
    //        history_t:                 histroy of data
    //        map<string,value_t>:    record key and values of grouped data
    unordered_map<int, pair<history_t, map<string, vector<value_t>>>> data_map;
};
}  // namespace BarrierData

class GroupActor : public BarrierActorBase<BarrierData::group_data> {
 public:
    GroupActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierActorBase<BarrierData::group_data>(id, core_affinity, mailbox),
        num_thread_(num_thread) {}

 private:
    int num_thread_;

    void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            BarrierDataTable::accessor& ac,
            bool isReady);
};

namespace BarrierData {
struct order_data : barrier_data_base {
    // int: assigned branch value by labelled branch step
    // pair:
    //  history_t:                            histroy of data
    //  map<value_t, multiset<value_t>>:
    //      value_t:                          key for ordering
    //      multiset<value_t>:                store real data

    // for order with mapping
    unordered_map<int, pair<history_t, map<value_t, multiset<value_t>>>> data_map;
    // for order without mapping
    unordered_map<int, pair<history_t, multiset<value_t>>> data_set;
};
}  // namespace BarrierData

class OrderActor : public BarrierActorBase<BarrierData::order_data> {
 public:
    OrderActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierActorBase<BarrierData::order_data>(id, core_affinity, mailbox),
        num_thread_(num_thread) {}

 private:
    int num_thread_;

    void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            BarrierDataTable::accessor& ac,
            bool isReady);
};

namespace BarrierData {
struct post_validation_data : barrier_data_base {
    // Record abort, avoid sending multiple abort msg
    bool isAbort;
    post_validation_data() : isAbort(false) {}
};
}  // namespace BarrierData

class PostValidationActor : public BarrierActorBase<BarrierData::post_validation_data> {
 public:
    PostValidationActor(int id,
            int num_nodes,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierActorBase<BarrierData::post_validation_data>(id, core_affinity, mailbox),
        num_nodes_(num_nodes) {
        trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();
    }

 private:
    int num_nodes_;
    TrxTableStub * trx_table_stub_;

    void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            BarrierDataTable::accessor& ac,
            bool isReady);
};

namespace BarrierData {
struct range_data : barrier_data_base{
    // int: assigned branch value by labelled branch step
    // pair:
    //        int: counter, record num of incoming data
    //        vec: record data in given range
    unordered_map<int, pair<int, vector<pair<history_t, vector<value_t>>>>> counter_map;
};
}  // namespace BarrierData

class RangeActor : public BarrierActorBase<BarrierData::range_data> {
 public:
    RangeActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierActorBase<BarrierData::range_data>(id, core_affinity, mailbox),
        num_thread_(num_thread) {}

 private:
    int num_thread_;

    void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            BarrierDataTable::accessor& ac,
            bool isReady);
};

class CoinActor : public BarrierActorBase<BarrierData::range_data> {
 public:
    CoinActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierActorBase<BarrierData::range_data>(id, core_affinity, mailbox),
        num_thread_(num_thread) {}

 private:
    int num_thread_;

    void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            BarrierDataTable::accessor& ac,
            bool isReady);
};

namespace BarrierData {
struct math_meta_t {
    int count;
    value_t value;
    history_t history;
};

struct math_data : barrier_data_base {
    // int: assigned branch value by labelled branch step
    // math_data_t: meta data for math
    unordered_map<int, math_meta_t> data_map;
};
}  // namespace BarrierData

class MathActor : public BarrierActorBase<BarrierData::math_data> {
 public:
    MathActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierActorBase<BarrierData::math_data>(id, core_affinity, mailbox),
        num_thread_(num_thread) {}

 private:
    int num_thread_;

    void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            BarrierDataTable::accessor& ac,
            bool isReady);

    static void sum(BarrierData::math_meta_t& data, value_t& v);

    static void max(BarrierData::math_meta_t& data, value_t& v);

    static void min(BarrierData::math_meta_t& data, value_t& v);

    static void to_double(BarrierData::math_meta_t& data, bool isMean);
};

#include "barrier_actor.tpp"
