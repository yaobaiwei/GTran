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

#include "utils/config.hpp"
#include "base/type.hpp"
#include "base/node.hpp"
#include "core/abstract_mailbox.hpp"
#include "actor/abstract_actor.hpp"
#include "actor/hw_actor.hpp"
#include "core/result_collector.hpp"
#include "utils/timer.hpp"
#include "storage/data_store.hpp"

using namespace std;


class ActorAdapter {
public:
	ActorAdapter(Node & node, int num_thread, Result_Collector * rc, AbstractMailbox * mailbox, DataStore* data_store) : node_(node), num_thread_(num_thread), rc_(rc), mailbox_(mailbox), data_store_(data_store) {
		times_.resize(num_thread, 0);
	}

	void Init(){
		actors_[ACTOR_T::HW] = unique_ptr<AbstractActor>(new HwActor(0, node_, rc_, mailbox_));
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

		msg_logic_table_iter = msg_logic_table_.find(m.qid);
		if(msg_logic_table_iter == msg_logic_table_.end()){
			cout << "ERROR: CANNOT FIND QID IN MSG_LOGIC_TABLE!" << endl;
			exit(-1);
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
	        if(times_[tid] < times_[steal_tid] + TIMEOUT)
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
  Node node_;

  // Actors pool <actor_type, [actors]>
  map<ACTOR_T, unique_ptr<AbstractActor>> actors_;

  //global map to record the vec<actor_obj> of query
  //avoid repeatedly transfer vec<actor_obj> for message
  hash_map<uint64_t, vector<Actor_Object>> msg_logic_table_;
  hash_map<uint64_t, vector<Actor_Object>>::iterator msg_logic_table_iter;


  // Thread pool
  vector<thread> thread_pool_;

  //clocks
  vector<uint64_t> times_;
  int num_thread_;
  const static uint64_t TIMEOUT = 100000;

};


#endif /* ACTORS_ADAPTER_HPP_ */
