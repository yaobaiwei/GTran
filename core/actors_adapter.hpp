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
#include "actor/barrier_actor.hpp"
#include "actor/branch_actor.hpp"
#include "actor/hw_actor.hpp"
#include "actor/has_actor.hpp"
#include "actor/init_actor.hpp"
#include "actor/redirect_actor.hpp"
#include "actor/traversal_actor.hpp"

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
		actors_[ACTOR_T::HW] = unique_ptr<AbstractActor>(new HwActor(0, node_, rc_, mailbox_));
		actors_[ACTOR_T::INIT] = unique_ptr<AbstractActor>(new InitActor(1, num_thread_, mailbox_, data_store_));
		actors_[ACTOR_T::REDIRECT] = unique_ptr<AbstractActor>(new RedirectActor(2, num_thread_, mailbox_, data_store_));
		actors_[ACTOR_T::TRAVERSAL] = unique_ptr<AbstractActor>(new TraversalActor(3, num_thread_, mailbox_, data_store_));
		actors_[ACTOR_T::END] = unique_ptr<AbstractActor>(new EndActor(4, rc_));
		actors_[ACTOR_T::COUNT] = unique_ptr<AbstractActor>(new CountActor(5, num_thread_, mailbox_));
		actors_[ACTOR_T::BRANCH] = unique_ptr<AbstractActor>(new BranchActor(6, num_thread_, mailbox_, &id_allocator_));
		actors_[ACTOR_T::BRANCHFILTER] = unique_ptr<AbstractActor>(new BranchFilterActor(7, num_thread_, mailbox_, &id_allocator_));
		actors_[ACTOR_T::HAS] = unique_ptr<AbstractActor>(new HasActor(8, num_thread_, mailbox_, data_store_));
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
			lock_guard<mutex> lk(cv_mutex);
			msg_logic_table_[m.qid] = move(m.actors);
			cv_msg_table.notify_all();
		}

		auto msg_logic_table_iter = msg_logic_table_.find(m.qid);

		// wait for init actor if qid not found
		if(msg_logic_table_iter == msg_logic_table_.end()){
			unique_lock<mutex> lk(cv_mutex);
			cv_msg_table.wait_for(lk, chrono::microseconds(INITTIMEOUT), [&](){
				msg_logic_table_iter = msg_logic_table_.find(m.qid);
				// continue when qid found
				return msg_logic_table_iter != msg_logic_table_.end();
			});

			// make sure qid was inserted before time out
			if(msg_logic_table_iter == msg_logic_table_.end()){
				cout << "QID IS : " << m.qid << endl;
				cout << "ERROR: CANNOT FIND QID IN MSG_LOGIC_TABLE!" << endl;
				exit(-1);
			}
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
	// hash_map<uint64_t, vector<Actor_Object>>::iterator msg_logic_table_iter;

	// condition lock to make sure Init actor for one qid executes first
	condition_variable cv_msg_table;
	mutex cv_mutex;

	// Thread pool
	vector<thread> thread_pool_;

	//clocks
	vector<uint64_t> times_;
	int num_thread_;
	const static uint64_t STEALTIMEOUT = 100000;
	const static uint64_t INITTIMEOUT = 1000000;
};


#endif /* ACTORS_ADAPTER_HPP_ */
