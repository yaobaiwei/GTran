/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
         Modified by Nick Fang (jcfang6@cse.cuhk.edu.hk)
                     Changji Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef EXPERTS_ADAPTER_HPP_
#define EXPERTS_ADAPTER_HPP_

#include <omp.h>
#include <tbb/concurrent_hash_map.h>
#include <map>
#include <vector>
#include <atomic>
#include <thread>
#include <utility>
#include <memory>

#include "expert/abstract_expert.hpp"
#include "expert/add_edge_expert.hpp"
#include "expert/add_vertex_expert.hpp"
#include "expert/as_expert.hpp"
#include "expert/barrier_expert.hpp"
#include "expert/branch_expert.hpp"
#include "expert/config_expert.hpp"
#include "expert/drop_expert.hpp"
#include "expert/has_expert.hpp"
#include "expert/has_label_expert.hpp"
#include "expert/init_expert.hpp"
#include "expert/index_expert.hpp"
#include "expert/is_expert.hpp"
#include "expert/key_expert.hpp"
#include "expert/label_expert.hpp"
#include "expert/labelled_branch_expert.hpp"
#include "expert/project_expert.hpp"
#include "expert/properties_expert.hpp"
#include "expert/property_expert.hpp"
#include "expert/select_expert.hpp"
#include "expert/status_expert.hpp"
#include "expert/terminate_expert.hpp"
#include "expert/traversal_expert.hpp"
#include "expert/values_expert.hpp"
#include "expert/validation_expert.hpp"
#include "expert/where_expert.hpp"
#include "expert/repeat_expert.hpp"

#include "base/node.hpp"
#include "base/type.hpp"
#include "base/core_affinity.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/factory.hpp"
#include "core/result_collector.hpp"
#include "layout/data_storage.hpp"
#include "layout/index_store.hpp"
#include "layout/pmt_rct_table.hpp"
#include "utils/config.hpp"
#include "utils/timer.hpp"
#include "utils/tid_pool_manager.hpp"

using namespace std;

#define MSG_LOCK_NUM 4096 // The count of read-write locks used in ExpertAdapter::execute

class ExpertAdapter {
 public:
    ExpertAdapter(Node & node,
            ResultCollector * rc,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity) :
        node_(node),
        rc_(rc),
        mailbox_(mailbox),
        core_affinity_(core_affinity) {
        config_ = Config::GetInstance();
        num_thread_ = config_->global_num_threads;
        times_.resize(num_thread_, 0);
        data_storage_ = DataStorage::GetInstance();
    }

    void Init() {
        int id = 0;
        experts_[EXPERT_T::AGGREGATE] = unique_ptr<AbstractExpert>(new AggregateExpert(id ++, node_.get_local_size(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::ADDV] = unique_ptr<AbstractExpert>(new AddVertexExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::ADDE] = unique_ptr<AbstractExpert>(new AddEdgeExpert(id ++, num_thread_, node_.get_local_rank(), mailbox_, core_affinity_));
        experts_[EXPERT_T::AS] = unique_ptr<AbstractExpert>(new AsExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::BRANCH] = unique_ptr<AbstractExpert>(new BranchExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::BRANCHFILTER] = unique_ptr<AbstractExpert>(new BranchFilterExpert(id ++, num_thread_, mailbox_, core_affinity_, &id_allocator_));
        experts_[EXPERT_T::CAP] = unique_ptr<AbstractExpert>(new CapExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::CONFIG] = unique_ptr<AbstractExpert>(new ConfigExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::COUNT] = unique_ptr<AbstractExpert>(new CountExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::DROP] = unique_ptr<AbstractExpert>(new DropExpert(id ++, num_thread_, node_.get_local_rank(), mailbox_, core_affinity_));
        experts_[EXPERT_T::DEDUP] = unique_ptr<AbstractExpert>(new DedupExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::END] = unique_ptr<AbstractExpert>(new EndExpert(id ++, node_.get_local_size(), rc_, mailbox_, core_affinity_));
        experts_[EXPERT_T::GROUP] = unique_ptr<AbstractExpert>(new GroupExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::HAS] = unique_ptr<AbstractExpert>(new HasExpert(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::HASLABEL] = unique_ptr<AbstractExpert>(new HasLabelExpert(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::INIT] = unique_ptr<AbstractExpert>(new InitExpert(id ++, num_thread_, mailbox_, core_affinity_, node_.get_local_size()));
        experts_[EXPERT_T::INDEX] = unique_ptr<AbstractExpert>(new IndexExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::IS] = unique_ptr<AbstractExpert>(new IsExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::KEY] = unique_ptr<AbstractExpert>(new KeyExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::LABEL] = unique_ptr<AbstractExpert>(new LabelExpert(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::MATH] = unique_ptr<AbstractExpert>(new MathExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::ORDER] = unique_ptr<AbstractExpert>(new OrderExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::POSTVALIDATION] = unique_ptr<AbstractExpert>(new PostValidationExpert(id ++, node_.get_local_size(), mailbox_, core_affinity_));
        experts_[EXPERT_T::PROJECT] = unique_ptr<AbstractExpert>(new ProjectExpert(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::PROPERTIES] = unique_ptr<AbstractExpert>(new PropertiesExpert(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::PROPERTY] = unique_ptr<AbstractExpert>(new PropertyExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::RANGE] = unique_ptr<AbstractExpert>(new RangeExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::COIN] = unique_ptr<AbstractExpert>(new CoinExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::REPEAT] = unique_ptr<AbstractExpert>(new RepeatExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::SELECT] = unique_ptr<AbstractExpert>(new SelectExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::STATUS] = unique_ptr<AbstractExpert>(new StatusExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::TERMINATE] = unique_ptr<AbstractExpert>(new TerminateExpert(id ++, mailbox_, core_affinity_, &experts_, &msg_logic_table_));
        experts_[EXPERT_T::TRAVERSAL] = unique_ptr<AbstractExpert>(new TraversalExpert(id ++, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::VALIDATION] = unique_ptr<AbstractExpert>(new ValidationExpert(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_, &experts_, &msg_logic_table_));
        experts_[EXPERT_T::VALUES] = unique_ptr<AbstractExpert>(new ValuesExpert(id ++, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::WHERE] = unique_ptr<AbstractExpert>(new WhereExpert(id ++, num_thread_, mailbox_, core_affinity_));
    }

    void Start() {
        Init();
        trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();

        locks_ = new WritePriorRWLock[MSG_LOCK_NUM];

        for (int i = 0; i < num_thread_; ++i)
            thread_pool_.emplace_back(&ExpertAdapter::ThreadExecutor, this, i);
    }

    void Stop() {
      for (auto &thread : thread_pool_)
        thread.join();
    }

    void execute(int tid, Message & msg) {
        Meta & m = msg.meta;

        bool acquire_writer_lock = false, check_trx_status = false;
        if (m.msg_type == MSG_T::INIT && m.qplan.experts[0].expert_type == EXPERT_T::TERMINATE) {
            acquire_writer_lock = true;
        }

        uint64_t trx_id = m.qid & _56HFLAG;
        uint64_t lock_id = (trx_id >> QID_BITS) % MSG_LOCK_NUM;
        uint8_t query_index = m.qid - trx_id;
        CHECK(m.query_count_in_trx > 1);

        if (query_index != m.query_count_in_trx - 1 && m.msg_type != MSG_T::ABORT && m.msg_type != MSG_T::TERMINATE) {
            // Do not need to check if:
            //      1. The last query (validation / commit / abort)
            //      2. Not an abort / terminate msg
            check_trx_status = true;
        }

        RWLockGuard rw_lock_guard(locks_[lock_id], acquire_writer_lock);

        if (check_trx_status) {
            TRX_STAT status;
            trx_table_stub_->read_status(trx_id, status);
            if (status == TRX_STAT::ABORT) {
                CHECK(msg.meta.msg_type != MSG_T::TERMINATE);
                msg.meta.msg_type = MSG_T::TERMINATE;
                msg.meta.recver_nid = msg.meta.parent_nid;
                msg.meta.recver_tid = msg.meta.parent_tid;
                msg.data.clear();
                value_t v;
                Tool::str2str("Abort with [MSG_T::TERMINATE]", v);
                msg.data.emplace_back(history_t(), vector<value_t>(1, v));
                mailbox_->Send(tid, msg);
                return;
            }
        }

        if (acquire_writer_lock) {
            while (true) {
                TRX_STAT status;
                CHECK(trx_table_stub_->read_status(trx_id, status));
                if (status == TRX_STAT::ABORT) {
                    // since the update of TrxTable is asynchronous, need to wait until the transaction is ABORT in the TrxTable
                    break;
                }
            }
        }

        if (m.msg_type == MSG_T::INIT) {
            // acquire write lock for insert
            accessor ac;
            msg_logic_table_.insert(ac, m.qid);
            ac->second = move(m.qplan);
        } else if (m.msg_type == MSG_T::FEED) {
            CHECK(msg.data.size() == 1);
            agg_t agg_key(m.qid, m.step);
            data_storage_->InsertAggData(agg_key, msg.data[0].second);

            return;
        } else if (m.msg_type == MSG_T::EXIT) {
            tbb::concurrent_hash_map<uint64_t, uint64_t>::accessor ac;
            if (exit_msg_count_table_.insert(ac, m.qid)) {
                ac->second = 0;
            }
            // collection done
            if (++ac->second == config_->global_num_workers) {
                rc_->InsertResult(m.qid, msg.data[0].second);
                // erase counter map
                exit_msg_count_table_.erase(ac);
            }
            return;
        } else if (m.msg_type == MSG_T::TERMINATE) {
            rc_->InsertAbortResult(m.qid, msg.data[0].second);
            return;
        }

        const_accessor ac;
        // qid not found
        if (!msg_logic_table_.find(ac, m.qid)) {
            // throw msg back to the mailbox
            msg.meta.recver_tid = msg.meta.parent_tid;
            mailbox_->Send(tid, msg);

            return;
        }

        int current_step;
        do {
            current_step = msg.meta.step;
            EXPERT_T next_expert = ac->second.experts[current_step].expert_type;
            experts_[next_expert]->process(ac->second, msg);
        } while (current_step != msg.meta.step);  // process next expert directly if step is modified

        // Commit expert cannot erase its own qid in process
        if (ac->second.experts[current_step].expert_type == EXPERT_T::TERMINATE) {
            msg_logic_table_.erase(ac);
        }
    }

    void ThreadExecutor(int tid) {
        TidPoolManager::GetInstance()->Register(TID_TYPE::CONTAINER);
        TidPoolManager::GetInstance()->Register(TID_TYPE::RDMA, tid);
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
    Config * config_;
    CoreAffinity * core_affinity_;
    msg_id_alloc id_allocator_;
    Node node_;
    // Validation
    TrxTableStub * trx_table_stub_;

    // Experts pool <expert_type, [experts]>
    map<EXPERT_T, unique_ptr<AbstractExpert>> experts_;

    // global map to record the vec<expert_obj> of query
    // avoid repeatedly transfer vec<expert_obj> for message
    tbb::concurrent_hash_map<uint64_t, QueryPlan> msg_logic_table_;
    typedef tbb::concurrent_hash_map<uint64_t, QueryPlan>::accessor accessor;
    typedef tbb::concurrent_hash_map<uint64_t, QueryPlan>::const_accessor const_accessor;

    tbb::concurrent_hash_map<uint64_t, uint64_t> exit_msg_count_table_;

    // Thread pool
    vector<thread> thread_pool_;

    // clocks
    vector<uint64_t> times_;
    int num_thread_;

    // locks
    WritePriorRWLock* locks_;

    // 5 more timers for total, recv , send, serialization, create msg
    static const int timer_offset = 5;

    static const uint64_t STEALTIMEOUT = 1000;
};


#endif /* EXPERTS_ADAPTER_HPP_ */
