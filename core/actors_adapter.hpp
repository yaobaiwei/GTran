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

using namespace std;


class ActorAdapter {
public:
	ActorAdapter(Config* config , Node & node, AbstractMailbox * mailbox) : config_(config), node_(node), mailbox_(mailbox) { }

	void Init(){
		actors_[ACTOR_T::HW] = unique_ptr<AbstractActor>(new HwActor(0, node_, mailbox_));
		//TODO add more
	}

	void Start(){
		Init();
		for(int i = 0; i < config_->global_num_threads; ++i)
			thread_pool_.emplace_back(&ActorAdapter::ThreadExecutor, this, i);
	}

	void Stop(){
	  for (auto &thread : thread_pool_)
		thread.join();
	}

	void ThreadExecutor(int t_id) {
	    while (true) {
	        Message recv_msg = mailbox_->Recv();
	        Meta & m = recv_msg.meta;
	        ACTOR_T next_actor = m.chains[m.step++];
	        actors_[next_actor]->process(t_id, recv_msg);
	    }
	};

private:
  // global config
  Config* config_;

  // Mailbox
  AbstractMailbox * mailbox_;
  Node node_;

  // Actors pool <actor_type, [actors]>
  map<ACTOR_T, unique_ptr<AbstractActor>> actors_;

  // Actor ID counter
  atomic<int> actor_id_counter_;

  // Thread pool
  vector<thread> thread_pool_;

};


#endif /* ACTORS_ADAPTER_HPP_ */
