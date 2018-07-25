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
	BranchActor(int id, DataStore* data_store, int num_thread, AbstractMailbox* mailbox) : AbstractActor(id, data_store), num_thread_(num_thread), mailbox_(mailbox){}
	void process(int t_id, vector<Actor_Object> & actors,  Message & msg){
		if(msg.meta.msg_type == MSG_T::SPAWN){
			vector<int> step_vec;
			get_steps(actors[msg.meta.step], step_vec);

			vector<Message> msg_vec;
			msg.CreateBranchedMsg(actors, step_vec, num_thread_, data_store_, msg_vec);

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

	void get_steps(Actor_Object & actor, vector<int>& steps)
	{
		vector<value_t> params = actor.params;
		assert(params.size() > 1);
		for(int i = 1; i < params.size(); i++){
			steps.push_back(Tool::value_t2int(params[i]));
		}
	}
};
