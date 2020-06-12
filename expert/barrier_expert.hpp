// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

#include "core/factory.hpp"
#include "core/result_collector.hpp"
#include "expert/abstract_expert.hpp"
#include "expert/expert_validation_object.hpp"
#include "utils/tool.hpp"

#include "utils/mkl_util.hpp"

namespace BarrierData {
struct barrier_data_base {
    map<string, int> path_counter;
};
}  // namespace BarrierData

// Base class for barrier experts
template<class T = BarrierData::barrier_data_base>
class BarrierExpertBase :  public AbstractExpert {
    static_assert(std::is_base_of<BarrierData::barrier_data_base, T>::value,
                "T must derive from barrier_data_base");
    using BarrierDataTable = tbb::concurrent_hash_map<mkey_t, T, MkeyHashCompare>;
    using TrxTable = tbb::concurrent_hash_map<uint64_t, set<mkey_t>>;

 public:
    BarrierExpertBase(int id, CoreAffinity* core_affinity, AbstractMailbox * mailbox)
        : AbstractExpert(id, core_affinity), mailbox_(mailbox) {}

    void process(const QueryPlan & qplan, Message & msg);

    void clean_trx_data(uint64_t TrxID) override;

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

    static inline bool is_next_barrier(const vector<Expert_Object>& experts, int step) {
        int next = experts[step].next_expert;
        return next < experts.size() && experts[next].IsBarrier();
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
    bool is_abort = false;
};
}  // namespace BarrierData

class EndExpert : public BarrierExpertBase<BarrierData::end_data> {
 public:
    EndExpert(int id,
            int num_nodes,
            ResultCollector * rc,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierExpertBase<BarrierData::end_data>(id, core_affinity, mailbox),
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

class AggregateExpert : public BarrierExpertBase<BarrierData::agg_data> {
 public:
    AggregateExpert(int id,
            int num_nodes,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierExpertBase<BarrierData::agg_data>(id, core_affinity, mailbox),
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

class CapExpert : public BarrierExpertBase<> {
 public:
    CapExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierExpertBase<>(id, core_affinity, mailbox),
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

class CountExpert : public BarrierExpertBase<BarrierData::count_data> {
 public:
    CountExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierExpertBase<BarrierData::count_data>(id, core_affinity, mailbox),
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
    unordered_map<int, unordered_set<history_t, HistoryTHash>> dedup_his_map;    // for dedup by history
    unordered_map<int, unordered_set<value_t, ValueTHash>> dedup_val_map;        // for dedup by value
};
}  // namespace BarrierData

class DedupExpert : public BarrierExpertBase<BarrierData::dedup_data> {
 public:
    DedupExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierExpertBase<BarrierData::dedup_data>(id, core_affinity, mailbox),
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

class GroupExpert : public BarrierExpertBase<BarrierData::group_data> {
 public:
    GroupExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierExpertBase<BarrierData::group_data>(id, core_affinity, mailbox),
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

class OrderExpert : public BarrierExpertBase<BarrierData::order_data> {
 public:
    OrderExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierExpertBase<BarrierData::order_data>(id, core_affinity, mailbox),
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

class PostValidationExpert : public BarrierExpertBase<BarrierData::post_validation_data> {
 public:
    PostValidationExpert(int id,
            int num_nodes,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierExpertBase<BarrierData::post_validation_data>(id, core_affinity, mailbox),
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

class RangeExpert : public BarrierExpertBase<BarrierData::range_data> {
 public:
    RangeExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierExpertBase<BarrierData::range_data>(id, core_affinity, mailbox),
        num_thread_(num_thread) {}

 private:
    int num_thread_;

    void do_work(int tid,
            const QueryPlan& qplan,
            Message & msg,
            BarrierDataTable::accessor& ac,
            bool isReady);
};

class CoinExpert : public BarrierExpertBase<BarrierData::range_data> {
 public:
    CoinExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierExpertBase<BarrierData::range_data>(id, core_affinity, mailbox),
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

class MathExpert : public BarrierExpertBase<BarrierData::math_data> {
 public:
    MathExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        BarrierExpertBase<BarrierData::math_data>(id, core_affinity, mailbox),
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

#include "barrier_expert.tpp"
