/*
 * actors_adapter.hpp
 *
 *  Created on: May 28, 2018
 *      Author: Hongzhi Chen
 */

#ifndef ACTORS_ADAPTER_HPP_
#define ACTORS_ADAPTER_HPP_

#include <map>
#include <vector>
#include <atomic>
#include <thread>

#include "actor/abstract_actor.hpp"
#include "actor/as_actor.hpp"
#include "actor/barrier_actor.hpp"
#include "actor/branch_actor.hpp"
#include "actor/has_actor.hpp"
#include "actor/has_label_actor.hpp"
#include "actor/init_actor.hpp"
#include "actor/is_actor.hpp"
#include "actor/key_actor.hpp"
#include "actor/label_actor.hpp"
#include "actor/labelled_branch_actor.hpp"
#include "actor/properties_actor.hpp"
#include "actor/select_actor.hpp"
#include "actor/traversal_actor.hpp"
#include "actor/values_actor.hpp"
#include "actor/where_actor.hpp"

#include "base/node.hpp"
#include "base/type.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/result_collector.hpp"
#include "storage/data_store.hpp"
#include "utils/config.hpp"
#include "utils/timer.hpp"

using namespace std;

class ActorAdapter {
public:
	ActorAdapter(Node & node, Config * config, Result_Collector * rc, AbstractMailbox * mailbox, DataStore* data_store, CoreAffinity* core_affinity) : node_(node), config_(config), num_thread_(config->global_num_threads), rc_(rc), mailbox_(mailbox), data_store_(data_store), core_affinity_(core_affinity) {
		times_.resize(num_thread_, 0);
	}

	void Init(){
		int id = 0;
		actors_[ACTOR_T::AGGREGATE] = unique_ptr<AbstractActor>(new AggregateActor(id ++, data_store_, node_.get_local_size() ,num_thread_, mailbox_));
		actors_[ACTOR_T::AS] = unique_ptr<AbstractActor>(new AsActor(id ++, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::BRANCH] = unique_ptr<AbstractActor>(new BranchActor(id ++, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::BRANCHFILTER] = unique_ptr<AbstractActor>(new BranchFilterActor(id ++, data_store_, num_thread_, mailbox_, &id_allocator_));
		actors_[ACTOR_T::CAP] = unique_ptr<AbstractActor>(new CapActor(id ++, data_store_ ,num_thread_, mailbox_));
		actors_[ACTOR_T::COUNT] = unique_ptr<AbstractActor>(new CountActor(id ++, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::DEDUP] = unique_ptr<AbstractActor>(new DedupActor(id ++, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::END] = unique_ptr<AbstractActor>(new EndActor(id ++, data_store_, node_.get_local_size(), rc_, mailbox_));
		actors_[ACTOR_T::GROUP] = unique_ptr<AbstractActor>(new GroupActor(id ++, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::HAS] = unique_ptr<AbstractActor>(new HasActor(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, config_->global_enable_caching));
		actors_[ACTOR_T::HASLABEL] = unique_ptr<AbstractActor>(new HasLabelActor(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, config_->global_enable_caching));
		actors_[ACTOR_T::INIT] = unique_ptr<AbstractActor>(new InitActor(id ++, data_store_, num_thread_, mailbox_, node_.get_local_size(), config_->max_data_size));
		actors_[ACTOR_T::IS] = unique_ptr<AbstractActor>(new IsActor(id ++, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::KEY] = unique_ptr<AbstractActor>(new KeyActor(id ++, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::LABEL] = unique_ptr<AbstractActor>(new LabelActor(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, config_->global_enable_caching));
		actors_[ACTOR_T::MATH] = unique_ptr<AbstractActor>(new MathActor(id ++, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::ORDER] = unique_ptr<AbstractActor>(new OrderActor(id ++, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::PROPERTY] = unique_ptr<AbstractActor>(new PropertiesActor(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, config_->global_enable_caching));
		actors_[ACTOR_T::RANGE] = unique_ptr<AbstractActor>(new RangeActor(id ++, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::SELECT] = unique_ptr<AbstractActor>(new SelectActor(id ++, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::TRAVERSAL] = unique_ptr<AbstractActor>(new TraversalActor(id ++, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::VALUES] = unique_ptr<AbstractActor>(new ValuesActor(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, config_->global_enable_caching));
		actors_[ACTOR_T::WHERE] = unique_ptr<AbstractActor>(new WhereActor(id ++, data_store_, num_thread_, mailbox_));
		//TODO add more

		timer::init_timers((actors_.size() + timer_offset) * num_thread_);
	}

	void Start(){
		Init();
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
			msg_logic_table_[m.qid] = move(m.actors);
		}else if(m.msg_type == MSG_T::FEED){
			assert(msg.data.size() == 1);
			agg_t agg_key(m.qid, m.step);
			data_store_->InsertAggData(agg_key, msg.data[0].second);
			return ;
		}else if(m.msg_type == MSG_T::EXIT){
			msg_logic_table_.erase(m.qid);
			// TODO: erase aggregate data

			// Print time
			timer::stop_timer(tid + 2 * num_thread_);
			double exec = 0;
			double recv = 0;
			double send = 0;
			double serialization = 0;
			double create_msg = 0;
			vector<double> actors_time(actors_.size());

			for(int i = 0; i < num_thread_; i++){
				send += timer::get_timer(i);
				serialization += timer::get_timer(i + num_thread_);
				exec += timer::get_timer(i + 2 *num_thread_);
				recv += timer::get_timer(i + 3 * num_thread_);
				create_msg += timer::get_timer(i + 4 * num_thread_);

				for(int j = 0; j < actors_.size(); j++){
					actors_time[j] += timer::get_timer(i + (timer_offset + j) * num_thread_);
				}
			}
			timer::reset_timers();

			string ofname = "timer" + to_string(m.recver_nid) + ".txt";
			std::ofstream ofs(ofname.c_str(), std::ofstream::out);
			ofs << "Exec: " << exec << "ms" << endl;

			for(auto& act : actors_){
				double t = actors_time[act.second->GetActorId()];
				if(t != 0){
					ofs << "\t" << std::left << setw(15) << string(ActorType[static_cast<int>(act.first)]) << ": " << t << "ms" << endl;
				}
			}
			ofs << endl;

			ofs << "Send: " << send << "ms" << endl;
			ofs << "Recv: " << recv << "ms" << endl;
			ofs << "Serialize: " << serialization << "ms" << endl;
			ofs << "Create Msg: " << create_msg << "ms" << endl;
			return;
		}

		auto msg_logic_table_iter = msg_logic_table_.find(m.qid);

		// qid not found
		if(msg_logic_table_iter == msg_logic_table_.end()){
			// throw msg to the same thread as init msg
			msg.meta.recver_tid = msg.meta.parent_tid;
			mailbox_->Send(tid, msg);
			return;
		}

		ACTOR_T next_actor = msg_logic_table_iter->second[m.step].actor_type;
		int offset = (actors_[next_actor]->GetActorId() + timer_offset) * num_thread_;

		timer::start_timer(tid + offset);
		actors_[next_actor]->process(tid, msg_logic_table_iter->second, msg);
		timer::stop_timer(tid + offset);
	}

	void ThreadExecutor(int tid) {
		// bind thread to core
		if (config_->global_enable_core_binding) {
			core_affinity_->BindToCore(tid);
		}

	    while (true) {
			timer::start_timer(tid + 2 * num_thread_);
			mailbox_->Sweep(tid);

	        Message recv_msg;
			timer::start_timer(tid + 3 * num_thread_);
	        bool success = mailbox_->TryRecv(tid, recv_msg);
	        times_[tid] = timer::get_usec();
	        if(success){
				timer::stop_timer(tid + 3 * num_thread_);
	        	execute(tid, recv_msg);
				mailbox_->Sweep(tid);
	        	times_[tid] = timer::get_usec();
				timer::stop_timer(tid + 2 * num_thread_);
	        } else {
				//TODO work stealing
				if (!config_->global_enable_workstealing)
					continue;

				vector<int> steal_list;
				core_affinity_->GetStealList(tid, steal_list);
				for (auto itr = steal_list.begin(); itr != steal_list.end(); itr++) {
					// if(times_[tid] < times_[*itr] + STEALTIMEOUT)
					// 		continue;

					timer::start_timer(tid + 2 * num_thread_);
					timer::start_timer(tid + 3 * num_thread_);
					success = mailbox_->TryRecv(*itr, recv_msg);
					if(success){
						timer::stop_timer(tid + 3 * num_thread_);
						execute(tid, recv_msg);
						timer::stop_timer(tid + 2 * num_thread_);
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
	Config * config_;
	CoreAffinity * core_affinity_;
	msg_id_alloc id_allocator_;
	Node node_;

	// Actors pool <actor_type, [actors]>
	map<ACTOR_T, unique_ptr<AbstractActor>> actors_;

	//global map to record the vec<actor_obj> of query
	//avoid repeatedly transfer vec<actor_obj> for message
	hash_map<uint64_t, vector<Actor_Object>> msg_logic_table_;

	// Thread pool
	vector<thread> thread_pool_;

	//clocks
	vector<uint64_t> times_;
	int num_thread_;

	// 5 more timers for total, recv , send, serialization, create msg
	const static int timer_offset = 5;

	const static uint64_t STEALTIMEOUT = 100000;
};


#endif /* ACTORS_ADAPTER_HPP_ */
