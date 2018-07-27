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
#include "actor/key_actor.hpp"
#include "actor/label_actor.hpp"
#include "actor/labelled_branch_actor.hpp"
#include "actor/properties_actor.hpp"
#include "actor/select_actor.hpp"
#include "actor/traversal_actor.hpp"
#include "actor/values_actor.hpp"

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
	ActorAdapter(Node & node, int num_thread, Result_Collector * rc, AbstractMailbox * mailbox, DataStore* data_store) : node_(node), num_thread_(num_thread), rc_(rc), mailbox_(mailbox), data_store_(data_store) {
		times_.resize(num_thread, 0);
	}

	void Init(){
		actors_[ACTOR_T::INIT] = unique_ptr<AbstractActor>(new InitActor(1, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::KEY] = unique_ptr<AbstractActor>(new KeyActor(2, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::TRAVERSAL] = unique_ptr<AbstractActor>(new TraversalActor(3, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::END] = unique_ptr<AbstractActor>(new EndActor(4, data_store_, rc_));
		actors_[ACTOR_T::COUNT] = unique_ptr<AbstractActor>(new CountActor(5, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::BRANCH] = unique_ptr<AbstractActor>(new BranchActor(6, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::BRANCHFILTER] = unique_ptr<AbstractActor>(new BranchFilterActor(7, data_store_, num_thread_, mailbox_, &id_allocator_));
		actors_[ACTOR_T::HAS] = unique_ptr<AbstractActor>(new HasActor(8, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::PROPERTY] = unique_ptr<AbstractActor>(new PropertiesActor(9, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::GROUP] = unique_ptr<AbstractActor>(new GroupActor(10, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::RANGE] = unique_ptr<AbstractActor>(new RangeActor(11, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::DEDUP] = unique_ptr<AbstractActor>(new DedupActor(12, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::HASLABEL] = unique_ptr<AbstractActor>(new HasLabelActor(13, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::VALUES] = unique_ptr<AbstractActor>(new ValuesActor(14, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::LABEL] = unique_ptr<AbstractActor>(new LabelActor(15, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::AS] = unique_ptr<AbstractActor>(new AsActor(16, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::SELECT] = unique_ptr<AbstractActor>(new SelectActor(17, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::MATH] = unique_ptr<AbstractActor>(new MathActor(18, data_store_, num_thread_, mailbox_));
		actors_[ACTOR_T::ORDER] = unique_ptr<AbstractActor>(new OrderActor(18, data_store_, num_thread_, mailbox_));
		//TODO add more
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
		}

		auto msg_logic_table_iter = msg_logic_table_.find(m.qid);

		// wait for init actor if qid not found
		if(msg_logic_table_iter == msg_logic_table_.end()){
			mailbox_->Send(tid, msg);
			return;
		}

		ACTOR_T next_actor = msg_logic_table_iter->second[m.step].actor_type;
		actors_[next_actor]->process(tid, msg_logic_table_iter->second, msg);
	}

	void ThreadExecutor(int tid) {
	    while (true) {
	        Message recv_msg;
	        bool success = mailbox_->TryRecv(tid, recv_msg);
	        times_[tid] = timer::get_usec();
	        if(success){
	        	execute(tid, recv_msg);
	        	times_[tid] = timer::get_usec();
	        }

	        //TODO work stealing
	        int steal_tid = (tid + 1) % num_thread_; //next thread
	        if(times_[tid] < times_[steal_tid] + STEALTIMEOUT)
	        	continue;

	        success = mailbox_->TryRecv(steal_tid, recv_msg);
	        if(success){
	        	execute(steal_tid, recv_msg);
	        }
        	times_[tid] = timer::get_usec();
	    }
	};

private:
	AbstractMailbox * mailbox_;
	Result_Collector * rc_;
	DataStore* data_store_;
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
	const static uint64_t STEALTIMEOUT = 100000;
};


#endif /* ACTORS_ADAPTER_HPP_ */
