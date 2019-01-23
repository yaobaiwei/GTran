/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
         Modified by Nick Fang (jcfang6@cse.cuhk.edu.hk)
                     Changji Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef ACTORS_ADAPTER_HPP_
#define ACTORS_ADAPTER_HPP_

#include <map>
#include <vector>
#include <atomic>
#include <thread>
#include <tbb/concurrent_hash_map.h>

#include "actor/abstract_actor.hpp"
#include "actor/as_actor.hpp"
#include "actor/barrier_actor.hpp"
#include "actor/branch_actor.hpp"
#include "actor/config_actor.hpp"
#include "actor/has_actor.hpp"
#include "actor/has_label_actor.hpp"
#include "actor/init_actor.hpp"
#include "actor/index_actor.hpp"
#include "actor/is_actor.hpp"
#include "actor/key_actor.hpp"
#include "actor/label_actor.hpp"
#include "actor/labelled_branch_actor.hpp"
#include "actor/properties_actor.hpp"
#include "actor/select_actor.hpp"
#include "actor/traversal_actor.hpp"
#include "actor/values_actor.hpp"
#include "actor/where_actor.hpp"
#include "actor/repeat_actor.hpp"

#include "base/node.hpp"
#include "base/type.hpp"
#include "base/core_affinity.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/result_collector.hpp"
#include "core/index_store.hpp"
#include "storage/data_store.hpp"
#include "utils/config.hpp"
#include "utils/timer.hpp"

#include <omp.h>

#include "utils/tid_mapper.hpp"

using namespace std;

class ActorAdapter {
public:
	ActorAdapter(Node & node, Result_Collector * rc, AbstractMailbox * mailbox, DataStore* data_store, CoreAffinity* core_affinity, IndexStore * index_store) : node_(node), rc_(rc), mailbox_(mailbox), data_store_(data_store), core_affinity_(core_affinity), index_store_(index_store) 
	{
		config_ = Config::GetInstance();
		num_thread_ = config_->global_num_threads;
		times_.resize(num_thread_, 0);
	}

	void Init(){
		int id = 0;
		actors_[ACTOR_T::AGGREGATE] = unique_ptr<AbstractActor>(new AggregateActor(id ++, data_store_, node_.get_local_size() ,num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::AS] = unique_ptr<AbstractActor>(new AsActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::BRANCH] = unique_ptr<AbstractActor>(new BranchActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::BRANCHFILTER] = unique_ptr<AbstractActor>(new BranchFilterActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_, &id_allocator_));
		actors_[ACTOR_T::CAP] = unique_ptr<AbstractActor>(new CapActor(id ++, data_store_ ,num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::CONFIG] = unique_ptr<AbstractActor>(new ConfigActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::COUNT] = unique_ptr<AbstractActor>(new CountActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::DEDUP] = unique_ptr<AbstractActor>(new DedupActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::END] = unique_ptr<AbstractActor>(new EndActor(id ++, data_store_, node_.get_local_size(), rc_, mailbox_, core_affinity_));
		actors_[ACTOR_T::GROUP] = unique_ptr<AbstractActor>(new GroupActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::HAS] = unique_ptr<AbstractActor>(new HasActor(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::HASLABEL] = unique_ptr<AbstractActor>(new HasLabelActor(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::INIT] = unique_ptr<AbstractActor>(new InitActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_, index_store_, node_.get_local_size()));
		actors_[ACTOR_T::INDEX] = unique_ptr<AbstractActor>(new IndexActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_, index_store_));
		actors_[ACTOR_T::IS] = unique_ptr<AbstractActor>(new IsActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::KEY] = unique_ptr<AbstractActor>(new KeyActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::LABEL] = unique_ptr<AbstractActor>(new LabelActor(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::MATH] = unique_ptr<AbstractActor>(new MathActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::ORDER] = unique_ptr<AbstractActor>(new OrderActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::PROPERTY] = unique_ptr<AbstractActor>(new PropertiesActor(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::RANGE] = unique_ptr<AbstractActor>(new RangeActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::COIN] = unique_ptr<AbstractActor>(new CoinActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::REPEAT] = unique_ptr<AbstractActor>(new RepeatActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::SELECT] = unique_ptr<AbstractActor>(new SelectActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::TRAVERSAL] = unique_ptr<AbstractActor>(new TraversalActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::VALUES] = unique_ptr<AbstractActor>(new ValuesActor(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
		actors_[ACTOR_T::WHERE] = unique_ptr<AbstractActor>(new WhereActor(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
		//TODO add more

		timer::init_timers((actors_.size() + timer_offset) * num_thread_);
	}

	void Start(){
		Init();
		TidMapper* tmp_tid_mapper_ptr = TidMapper::GetInstance();//in case of initial in parallel region

		for(int i = 0; i < num_thread_; ++i)
			thread_pool_.emplace_back(&ActorAdapter::ThreadExecutor, this, i);
	}

	void Stop(){
	  for (auto &thread : thread_pool_)
		thread.join();
	}

	void execute(int tid, Message & msg){
		Meta & m = msg.meta;
		if(m.msg_type == MSG_T::INIT){
			// acquire write lock for insert
			accessor ac;
			msg_logic_table_.insert(ac, m.qid);
			ac->second = move(m.actors);
		}else if(m.msg_type == MSG_T::FEED){
			assert(msg.data.size() == 1);
			agg_t agg_key(m.qid, m.step);
			data_store_->InsertAggData(agg_key, msg.data[0].second);

			return ;
		}else if(m.msg_type == MSG_T::EXIT){
			const_accessor ac;
			msg_logic_table_.find(ac, m.qid);

			// erase aggregate result
			int i = 0;
			for(auto& act : ac->second){
				if(act.actor_type == ACTOR_T::AGGREGATE){
					agg_t agg_key(m.qid, i);
					data_store_->DeleteAggData(agg_key);
				}
				i++;
			}

			// earse only after query with qid is done
			msg_logic_table_.erase(ac);

			return;
		}

		const_accessor ac;
		// qid not found
		if(! msg_logic_table_.find(ac, m.qid)){
			// throw msg to the same thread as init msg
			msg.meta.recver_tid = msg.meta.parent_tid;
			mailbox_->Send(tid, msg);
			return;
		}

		int current_step;
		do{
			current_step = msg.meta.step;
			ACTOR_T next_actor = ac->second[current_step].actor_type;
			//int offset = (actors_[next_actor]->GetActorId() + timer_offset) * num_thread_;

			//timer::start_timer(tid + offset);
			actors_[next_actor]->process(ac->second, msg);//this will modify msg??
			//timer::stop_timer(tid + offset);
		}while(current_step != msg.meta.step);	// process next actor directly if step is modified

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
			//timer::start_timer(tid + 2 * num_thread_);
			mailbox_->Sweep(tid);

	        Message recv_msg;
			//timer::start_timer(tid + 3 * num_thread_);
	        bool success = mailbox_->TryRecv(tid, recv_msg);
	        times_[tid] = timer::get_usec();
	        if(success){
				//timer::stop_timer(tid + 3 * num_thread_);
	        	execute(tid, recv_msg);
	        	times_[tid] = timer::get_usec();
				//timer::stop_timer(tid + 2 * num_thread_);
	        } else {
				//TODO work stealing
				if (!config_->global_enable_workstealing)
					continue;

				for (auto itr = steal_list.begin(); itr != steal_list.end(); itr++) {
					if(times_[tid] < times_[*itr] + STEALTIMEOUT)
							continue;

					//timer::start_timer(tid + 2 * num_thread_);
					//timer::start_timer(tid + 3 * num_thread_);
					success = mailbox_->TryRecv(*itr, recv_msg);
					if(success){
						//timer::stop_timer(tid + 3 * num_thread_);
						execute(tid, recv_msg);
						//timer::stop_timer(tid + 2 * num_thread_);
						break;
					}
				}
				times_[tid] = timer::get_usec();
			}
	    }
	};

private:
	AbstractMailbox * mailbox_;
	Result_Collector * rc_;
	DataStore * data_store_;
	IndexStore * index_store_;
	Config * config_;
	CoreAffinity * core_affinity_;
	msg_id_alloc id_allocator_;
	Node node_;

	// Actors pool <actor_type, [actors]>
	map<ACTOR_T, unique_ptr<AbstractActor>> actors_;

	//global map to record the vec<actor_obj> of query
	//avoid repeatedly transfer vec<actor_obj> for message
	tbb::concurrent_hash_map<uint64_t, vector<Actor_Object>> msg_logic_table_;
	typedef tbb::concurrent_hash_map<uint64_t, vector<Actor_Object>>::accessor accessor;
	typedef tbb::concurrent_hash_map<uint64_t, vector<Actor_Object>>::const_accessor const_accessor;
	// Thread pool
	vector<thread> thread_pool_;

	//clocks
	vector<uint64_t> times_;
	int num_thread_;

	// 5 more timers for total, recv , send, serialization, create msg
	const static int timer_offset = 5;

	const static uint64_t STEALTIMEOUT = 1000;
};


#endif /* ACTORS_ADAPTER_HPP_ */
