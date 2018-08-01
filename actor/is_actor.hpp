/*
 * is_actor.hpp
 *
 *  Created on: July 25, 2018
 *      Author: Aaron LI
 */
#ifndef IS_ACTOR_HPP_
#define IS_ACTOR_HPP_

#include <string>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"

class IsActor : public AbstractActor {
public:
	IsActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox) : AbstractActor(id, data_store), num_thread_(num_thread), mailbox_(mailbox), type_(ACTOR_T::IS) {}

	// [pred_T , pred_params]...
	void process(int tid, vector<Actor_Object> & actor_objs, Message & msg) {
		// Get Actor_Object
		Meta & m = msg.meta;
		Actor_Object actor_obj = actor_objs[m.step];

		// Get Params
		vector<PredicateValue> pred_chain;	

		assert(actor_obj.params.size() > 0 && (actor_obj.params.size() % 2) == 0);
		int numParamsGroup = actor_obj.params.size() / 2;

		for (int i = 0; i < numParamsGroup; i++) {
			int pos = i * 2;
			// Get predicate params
			Predicate_T pred_type = (Predicate_T) Tool::value_t2int(actor_obj.params.at(pos));
			vector<value_t> pred_params;
			Tool::value_t2vec(actor_obj.params.at(pos + 1), pred_params);

			// pred_chain.push_back(PredicateValue(pred_type, pred_params));
			pred_chain.emplace_back(pred_type, pred_params);
		}

		// Evaluate
		EvaluateData(msg.data, pred_chain);

		// Create Message
		vector<Message> msg_vec;
		msg.CreateNextMsg(actor_objs, msg.data, num_thread_, data_store_, msg_vec);

		// Send Message
		for (auto& msg : msg_vec) {
			mailbox_->Send(tid, msg);
		}
	 }

private:
	// Number of Threads
	int num_thread_;

	// Actor type
	ACTOR_T type_;

	// Pointer of mailbox
	AbstractMailbox * mailbox_;

	// Ensure only one thread ever runs the actor
	std::mutex thread_mutex_;

	void EvaluateData(vector<pair<history_t, vector<value_t>>> & data, vector<PredicateValue> & pred_chain) {
		for (auto & data_pair : data) {
			for (auto value_itr = data_pair.second.begin(); value_itr != data_pair.second.end(); ) {

				int counter = pred_chain.size();
				for (auto & pred : pred_chain) {
					if (Evaluate(pred, &(*value_itr))) {
						counter--;
					}
				}

				if (counter != 0) {
					value_itr = data_pair.second.erase(value_itr);
				} else {
					value_itr++;
				}

			}
		}
	}

};

#endif /* IS_ACTOR_HPP_ */
