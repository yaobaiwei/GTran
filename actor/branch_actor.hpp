/*
 * branch_actor.hpp
 *
 *  Created on: July 13, 2018
 *      Author: Nick Fang
 */

#pragma once

#include "actor/abstract_actor.hpp"

// Branch Actor
// Copy incoming data to sub branches
class BranchActor : public AbstractActor{
public:
	BranchActor(int id, DataStore* data_store, int num_thread, AbstractMailbox* mailbox, CoreAffinity* core_affinity) : AbstractActor(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox){}
	void process(int t_id, const vector<Actor_Object> & actors,  Message & msg){

		#ifdef ACTOR_PROCESS_PRINT
		//in MT & MP model, printf is better than cout
		Node node = Node::StaticInstance();
		printf("ACTOR = %s, node = %d, tid = %d\n", "BranchActor", node.get_local_rank(), t_id);
		#ifdef ACTOR_PROCESS_SLEEP
		timespec time_sleep;
		time_sleep.tv_nsec = 500000000L;
		nanosleep(&time_sleep, NULL); 
		#endif
		#endif

		if(msg.meta.msg_type == MSG_T::SPAWN){
			vector<int> step_vec;
			get_steps(actors[msg.meta.step], step_vec);

			vector<Message> msg_vec;
			msg.CreateBranchedMsg(actors, step_vec, num_thread_, data_store_, core_affinity_, msg_vec);

			for (auto& m : msg_vec){
				mailbox_->Send(t_id, m);
			}
		}else{
			cout << "Unexpected msg type in branch actor." << endl;
			exit(-1);
		}
	}
private:
	int num_thread_;
	AbstractMailbox* mailbox_;

	void get_steps(const Actor_Object & actor, vector<int>& steps)
	{
		vector<value_t> params = actor.params;
		assert(params.size() > 1);
		for(int i = 0; i < params.size(); i++){
			steps.push_back(Tool::value_t2int(params[i]));
		}
	}
};
