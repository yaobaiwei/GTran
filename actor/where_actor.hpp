/*
 * where_actor.hpp
 *
 *  Created on: July 26, 2018
 *      Author: Aaron LI
 */
#ifndef WHERE_ACTOR_HPP_
#define WHERE_ACTOR_HPP_

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

class WhereActor : public AbstractActor {
public:
	WhereActor(int id, DataStore * data_store, int num_thread, AbstractMailbox * mailbox) : AbstractActor(id, data_store), num_thread_(num_thread), mailbox_(mailbox), type_(ACTOR_T::WHERE) {}

	// Where:
	// [	label_step_key:  int
	// 		pred: Predicate_T
	// 		pred_param: value_t]...
	//
	// 	e.g. g.V().as('a'),,,.where(neq('a'))
	// 	 	 g.V().as('a'),,,.as('b').,,,.where('a', neq('b'))
	//
	// 	Notes: Current Version does NOT support within && without
	void process(int tid, vector<Actor_Object> & actor_objs, Message & msg) {
		// Get Actor_Object
		Meta & m = msg.meta;
		Actor_Object actor_obj = actor_objs[m.step];

		// store all predicate
		vector<PredicateHistory> pred_chain;

		// Get Params
		assert(actor_obj.params.size() > 0 && ( actor_obj.params.size()  % 3 ) == 0);
		int numParamsGroup = actor_obj.params.size() / 3; // number of groups of params

		for (int i = 0; i < numParamsGroup; i++) {
			int pos = i * 3;

			// Create pred chain
			vector<int> his_labels;
			his_labels.push_back(Tool::value_t2int(actor_obj.params.at(pos)));
			Predicate_T pred_type = (Predicate_T) Tool::value_t2int(actor_obj.params.at(pos + 1));

			vector<value_t> pred_params;
			Tool::value_t2vec(actor_obj.params.at(pos + 2), pred_params);

			for (auto & params : pred_params) {
				his_labels.push_back(Tool::value_t2int(params));
			}

			pred_chain.push_back(PredicateHistory(pred_type, his_labels));
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

	void EvaluateData(vector<pair<history_t, vector<value_t>>> & data, vector<PredicateHistory> & pred_chain)
	{
		for (auto & pred : pred_chain) {

			Predicate_T pred_type = pred.pred_type;
			vector<int> step_labels = pred.history_step_labels;

			if (step_labels.size() > 2) {
				cout << "Not Support now, wait for aggregate." << endl;
				continue;
			}

			if (step_labels.at(0) == -1) { // only one history key
				// Find the value of history_label

				for (auto & data_pair : data) {
					history_t::iterator his_itr = data_pair.first.begin();
					value_t his_val;
					bool isFound = false;

					do {
						if ((*his_itr).first == step_labels.at(1)) {
							his_val = (*his_itr).second;
							isFound = true;
							break;
						}
						his_itr++;
					} while (his_itr != data_pair.first.end());

					// IF not found, try next predicate
					if (!isFound) {
						// Clear value of this history;
						data_pair.second.clear();
						continue;
					}

					PredicateValue single_pred(pred_type, his_val);

					for (auto value_itr = data_pair.second.begin(); value_itr != data_pair.second.end(); ) {
						if (!Evaluate(single_pred, &(*value_itr))) {
							// if failed
							value_itr = data_pair.second.erase(value_itr);
						} else {
							value_itr++;
						}
					}
				}
			} else {
				for (auto & data_pair : data) {
					// Find the value of two history_label and compare each other
					// if not success, erase whole history
					history_t::iterator his_itr = data_pair.first.begin();

					value_t val_first;
					value_t val_second;
					bool isFirstFound = false;
					bool isSecondFound = false;

					do {
						if ((*his_itr).first == step_labels.at(0)) {
							val_first = (*his_itr).second;
							isFirstFound = true;
						} else if ((*his_itr).first == step_labels.at(1)) {
							val_second = (*his_itr).second;
							isSecondFound = true;
						}
						his_itr++;
					} while (his_itr != data_pair.first.end());

					if (!isFirstFound || !isSecondFound) {
						data_pair.second.clear();
						continue;
					}

					if (!Evaluate(pred_type, val_first, val_second)) {
						data_pair.second.clear();
					}
				}
			}
		}
	}
};

#endif /* WHERE_ACTOR_HPP_ */
