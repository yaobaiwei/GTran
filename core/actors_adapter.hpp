/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
         Modified by Nick Fang (jcfang6@cse.cuhk.edu.hk)
                     Changji Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef ACTORS_ADAPTER_HPP_
#define ACTORS_ADAPTER_HPP_

#include <omp.h>
#include <tbb/concurrent_hash_map.h>
#include <map>
#include <vector>
#include <atomic>
#include <thread>
#include <utility>
#include <memory>

#include "actor/abstract_actor.hpp"
#include "actor/as_actor.hpp"
#include "actor/barrier_actor.hpp"
#include "actor/branch_actor.hpp"
#include "actor/commit_actor.hpp"
#include "actor/config_actor.hpp"
#include "actor/drop_actor.hpp"
#include "actor/has_actor.hpp"
#include "actor/has_label_actor.hpp"
#include "actor/init_actor.hpp"
#include "actor/index_actor.hpp"
#include "actor/is_actor.hpp"
#include "actor/key_actor.hpp"
#include "actor/label_actor.hpp"
#include "actor/labelled_branch_actor.hpp"
#include "actor/project_actor.hpp"
#include "actor/properties_actor.hpp"
#include "actor/select_actor.hpp"
#include "actor/traversal_actor.hpp"
#include "actor/values_actor.hpp"
#include "actor/validation_actor.hpp"
#include "actor/where_actor.hpp"
#include "actor/repeat_actor.hpp"

#include "base/node.hpp"
#include "base/type.hpp"
#include "base/core_affinity.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/factory.hpp"
#include "core/result_collector.hpp"
#include "core/index_store.hpp"
#include "layout/data_storage.hpp"
#include "layout/pmt_rct_table.hpp"
#include "utils/config.hpp"
#include "utils/timer.hpp"
#include "utils/tid_mapper.hpp"

using namespace std;

class ActorAdapter {
 public:
    ActorAdapter(Node & node,
            ResultCollector * rc,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity,
            IndexStore * index_store,
            PrimitiveRCTTable * pmt_rct_table) :
        node_(node),
        rc_(rc),
        mailbox_(mailbox),
        core_affinity_(core_affinity),
        index_store_(index_store),
        pmt_rct_table_(pmt_rct_table) {
        config_ = Config::GetInstance();
        num_thread_ = config_->global_num_threads;
        times_.resize(num_thread_, 0);
    }

    void Init() {
        int id = 0;
        actors_[ACTOR_T::AGGREGATE] = unique_ptr<AbstractActor>(new AggregateActor(id ++, node_.get_local_size(), num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::AS] = unique_ptr<AbstractActor>(new AsActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::BRANCH] = unique_ptr<AbstractActor>(new BranchActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::BRANCHFILTER] = unique_ptr<AbstractActor>(new BranchFilterActor(id ++, num_thread_, mailbox_, core_affinity_, &id_allocator_));
        actors_[ACTOR_T::CAP] = unique_ptr<AbstractActor>(new CapActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::COMMIT] = unique_ptr<AbstractActor>(new CommitActor(id ++, num_thread_, mailbox_, core_affinity_, &actors_));
        actors_[ACTOR_T::CONFIG] = unique_ptr<AbstractActor>(new ConfigActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::COUNT] = unique_ptr<AbstractActor>(new CountActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::DROP] = unique_ptr<AbstractActor>(new DropActor(id ++, num_thread_, node_.get_local_rank(), mailbox_, core_affinity_));
        actors_[ACTOR_T::DEDUP] = unique_ptr<AbstractActor>(new DedupActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::END] = unique_ptr<AbstractActor>(new EndActor(id ++, node_.get_local_size(), rc_, mailbox_, core_affinity_));
        actors_[ACTOR_T::GROUP] = unique_ptr<AbstractActor>(new GroupActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::HAS] = unique_ptr<AbstractActor>(new HasActor(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::HASLABEL] = unique_ptr<AbstractActor>(new HasLabelActor(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::INIT] = unique_ptr<AbstractActor>(new InitActor(id ++, num_thread_, mailbox_, core_affinity_, index_store_, node_.get_local_size()));
        actors_[ACTOR_T::INDEX] = unique_ptr<AbstractActor>(new IndexActor(id ++, num_thread_, mailbox_, core_affinity_, index_store_));
        actors_[ACTOR_T::IS] = unique_ptr<AbstractActor>(new IsActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::KEY] = unique_ptr<AbstractActor>(new KeyActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::LABEL] = unique_ptr<AbstractActor>(new LabelActor(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::MATH] = unique_ptr<AbstractActor>(new MathActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::ORDER] = unique_ptr<AbstractActor>(new OrderActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::POSTVALIDATION] = unique_ptr<AbstractActor>(new PostValidationActor(id ++, node_.get_local_size(), mailbox_, core_affinity_));
        actors_[ACTOR_T::PROJECT] = unique_ptr<AbstractActor>(new ProjectActor(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::PROPERTIES] = unique_ptr<AbstractActor>(new PropertiesActor(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::RANGE] = unique_ptr<AbstractActor>(new RangeActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::COIN] = unique_ptr<AbstractActor>(new CoinActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::REPEAT] = unique_ptr<AbstractActor>(new RepeatActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::SELECT] = unique_ptr<AbstractActor>(new SelectActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::TRAVERSAL] = unique_ptr<AbstractActor>(new TraversalActor(id ++, num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::VALIDATION] = unique_ptr<AbstractActor>(new ValidationActor(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_, pmt_rct_table_, &actors_, &msg_logic_table_));
        actors_[ACTOR_T::VALUES] = unique_ptr<AbstractActor>(new ValuesActor(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        actors_[ACTOR_T::WHERE] = unique_ptr<AbstractActor>(new WhereActor(id ++, num_thread_, mailbox_, core_affinity_));
    }

    void Start() {
        Init();
        TidMapper* tmp_tid_mapper_ptr = TidMapper::GetInstance();  // in case of initial in parallel region
        trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();

        for (int i = 0; i < num_thread_; ++i)
            thread_pool_.emplace_back(&ActorAdapter::ThreadExecutor, this, i);
    }

    void Stop() {
      for (auto &thread : thread_pool_)
        thread.join();
    }

    void execute(int tid, Message & msg) {
        Meta & m = msg.meta;
        if (m.msg_type == MSG_T::INIT) {
            // acquire write lock for insert
            accessor ac;
            msg_logic_table_.insert(ac, m.qid);
            ac->second = move(m.qplan);
        } else if (m.msg_type == MSG_T::FEED) {
            assert(msg.data.size() == 1);
            agg_t agg_key(m.qid, m.step);
            data_storage_->InsertAggData(agg_key, msg.data[0].second);

            return;
        } else if (m.msg_type == MSG_T::EXIT) {
            const_accessor ac;
            msg_logic_table_.find(ac, m.qid);

            // erase aggregate result
            int i = 0;
            for (auto& act : ac->second.actors) {
                if (act.actor_type == ACTOR_T::AGGREGATE) {
                    agg_t agg_key(m.qid, i);
                    data_storage_->DeleteAggData(agg_key);
                }
                i++;
            }

            // earse only after query with qid is done
            // TODO : Erase table after all transaction is done;
            //      Do not erase currently
            // msg_logic_table_.erase(ac);

            return;
        }

        const_accessor ac;
        // qid not found
        if (!msg_logic_table_.find(ac, m.qid)) {
            // throw msg to the same thread as init msg
            msg.meta.recver_tid = msg.meta.parent_tid;
            mailbox_->Send(tid, msg);
            return;
        }

        // Check status of transaction itself
        //  if abort, return directly
        //  TODO :  Most fine-grained check, need to consider trade-off
        if (ac->second.trx_type != TRX_READONLY && m.msg_type != MSG_T::COMMIT && m.msg_type != MSG_T::ABORT) {
            TRX_STAT status;
            trx_table_stub_->read_status(ac->second.trxid, status);
            if (status == TRX_STAT::ABORT) {
                return;
            }
        }

        int current_step;
        do {
            current_step = msg.meta.step;
            ACTOR_T next_actor = ac->second.actors[current_step].actor_type;
            actors_[next_actor]->process(ac->second, msg);
        }while(current_step != msg.meta.step);  // process next actor directly if step is modified
    }

    void ThreadExecutor(int tid) {
        TidMapper::GetInstance()->Register(tid);
        // bind thread to core
        if (config_->global_enable_core_binding) {
            core_affinity_->BindToCore(tid);
        }

        vector<int> steal_list;
        core_affinity_->GetStealList(tid, steal_list);

        while (true) {
            mailbox_->Sweep(tid);

            Message recv_msg;
            bool success = mailbox_->TryRecv(tid, recv_msg);
            times_[tid] = timer::get_usec();
            if (success) {
                execute(tid, recv_msg);
                times_[tid] = timer::get_usec();
            } else {
                if (!config_->global_enable_workstealing)
                    continue;

                if (steal_list.size() == 0) {  // num_thread_ < 6
                    success = mailbox_->TryRecv((tid + 1) % num_thread_, recv_msg);
                    if (success) {
                        execute(tid, recv_msg);
                    }
                } else {  // num_thread_ >= 6
                    for (auto itr = steal_list.begin(); itr != steal_list.end(); itr++) {
                        if (times_[tid] < times_[*itr] + STEALTIMEOUT)
                                continue;

                        success = mailbox_->TryRecv(*itr, recv_msg);
                        if (success) {
                            execute(tid, recv_msg);
                            break;
                        }
                    }
                }
                times_[tid] = timer::get_usec();
            }
        }
    }

 private:
    AbstractMailbox * mailbox_;
    ResultCollector * rc_;
    DataStorage * data_storage_;
    IndexStore * index_store_;
    Config * config_;
    CoreAffinity * core_affinity_;
    msg_id_alloc id_allocator_;
    Node node_;
    // Validation
    PrimitiveRCTTable * pmt_rct_table_;
    TrxTableStub * trx_table_stub_;

    // Actors pool <actor_type, [actors]>
    map<ACTOR_T, unique_ptr<AbstractActor>> actors_;

    // global map to record the vec<actor_obj> of query
    // avoid repeatedly transfer vec<actor_obj> for message
    tbb::concurrent_hash_map<uint64_t, QueryPlan> msg_logic_table_;
    typedef tbb::concurrent_hash_map<uint64_t, QueryPlan>::accessor accessor;
    typedef tbb::concurrent_hash_map<uint64_t, QueryPlan>::const_accessor const_accessor;
    // Thread pool
    vector<thread> thread_pool_;

    // clocks
    vector<uint64_t> times_;
    int num_thread_;

    // 5 more timers for total, recv , send, serialization, create msg
    static const int timer_offset = 5;

    static const uint64_t STEALTIMEOUT = 1000;
};


#endif /* ACTORS_ADAPTER_HPP_ */
