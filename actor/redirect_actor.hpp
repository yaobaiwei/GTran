/*
 * redirect_actor.hpp
 *
 *  Created on: July 18, 2018
 *      Author: Aaron LI 
 */
#ifndef REDIRECT_ACTOR_HPP_
#define REDIRECT_ACTOR_HPP_

#include <iostream>
#include <string>
#include "glog/logging.h"

#include "actor/abstract_actor.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/result_collector.hpp"
#include "base/type.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"

using namespace std::placeholders;

// Redirect Actor will appear before Traversal Actor
// which can make sure every item from last actor will be send to
// the machine holding the data;
class RedirectActor : public AbstractActor {
public:
	RedirectActor(int id, int num_thread, AbstractMailbox * mailbox, DataStore * datastore) : AbstractActor(id), num_thread_(num_thread), mailbox_(mailbox), datastore_(datastore), type_(ACTOR_T::REDIRECT)  {
		fp = [&](value_t & v) { return get_node_id(v); } ;
	}

	void process(int tid, vector<Actor_Object> & actor_objs, Message & msg){
		// Create Messages
		vector<Message> msg_vec;
		msg.CreateNextMsg(actor_objs, msg.data, num_thread_, msg_vec, &fp);

		// Send Message
		for (auto& msg : msg_vec) {
			mailbox_->Send(tid, msg);
		}
	}

	int get_node_id(value_t & v) {
		int type = v.type;
		if (type == 1) {
			vid_t v_id(Tool::value_t2int(v));
			return datastore_->GetMachineIdForVertex(v_id);
		} else if (type == 5) {
			eid_t e_id;
			uint2eid_t(Tool::value_t2uint64_t(v), e_id);
			return datastore_->GetMachineIdForEdge(e_id);
		} else {
			cout << "Wrong Type when getting node id" << type << endl;
			return -1;
		}
	}

private:
	// Number of threads
	int num_thread_;

	// Actor type
	ACTOR_T type_;

	// Pointer of mailbox
	AbstractMailbox * mailbox_;

	// Ensure only one thread ever runs the actor
	std::mutex thread_mutex_;

	// Data Store
	DataStore * datastore_;

	// Function pointer to assign dst 
	function<int(value_t &)> fp;
};

#endif /* REDIRECT_ACTOR_HPP_ */
