/*
 * select_actor.hpp
 *
 *  Created on: July 24, 2018
 *      Author: Aaron LI
 */
#ifndef SELECT_ACTOR_HPP_
#define SELECT_ACTOR_HPP_

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

class SelectActor : public AbstractActor {
public:
	SelectActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox) : AbstractActor(id, data_store), num_thread_(num_thread), mailbox_(mailbox), type_(ACTOR_T::SELECT) {}

	void process(int tid, vector<Actor_Object> & actor_objs, Message & msg) {
		// Get Actor_Object
		Meta & m = msg.meta;
		Actor_Object actor_obj = actor_objs[m.step];

		assert(actor_obj.params.size() % 2 == 0);
		// Get Params
		vector<pair<int, string>> label_step_list;
		for (int i = 0; i < actor_obj.params.size(); i+=2) {
			// label_step_list.push_back(make_pair(Tool::value_t2int(actor_obj.params.at(i)), Tool::value_t2string(actor_obj.params.at(i + 1))));
			label_step_list.emplace_back(Tool::value_t2int(actor_obj.params.at(i)), Tool::value_t2string(actor_obj.params.at(i + 1)));
		}

		// sort label_step_list for quick search
		sort(label_step_list.begin(), label_step_list.end(),
			[](const pair<int, string>& l, const pair<int, string>& r){ return l.first < r.first;});

		//  Grab history_t
		if (label_step_list.size() != 1) {
			GrabHistory(label_step_list, msg.data);
		} else {
			GrabHistory(label_step_list[0].first, msg.data);
		}

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

	void GrabHistory(vector<pair<int, string>> label_step_list, vector<pair<history_t, vector<value_t>>> & data) {
		vector<value_t> result;

		for (auto & data_pair : data) {
			int value_size = data_pair.second.size();
			string res = "[";
			bool isResultEmpty = true;

			auto l_itr = label_step_list.begin();

			if (!data_pair.first.empty()) {
				vector<pair<int, value_t>>::iterator p_itr = data_pair.first.begin();

				// once there is one list ends, end search
				do {
					// cout << "label_step_list_item & history_list_item : " << *l_itr << " & " << (*p_itr).first << endl;
					if (l_itr->first == p_itr->first) {
						res += l_itr->second + ":" + Tool::DebugString(p_itr->second) + ", ";
						isResultEmpty = false;

						l_itr++;
						p_itr++;
					} else if (l_itr->first < p_itr->first) {
						l_itr++;
					} else if (l_itr->first > p_itr->first) {
						p_itr++;
					}
				} while( l_itr != label_step_list.end() && p_itr != data_pair.first.end() );
			}

			if (!isResultEmpty) {
				res.pop_back();
				res.pop_back();
			}
			res += "]";

			// cout << "Copy " << data_pair.second.size() << " times for " << res << endl;
			if (!data_pair.first.empty() && !isResultEmpty) {
				for (int i = 0; i < data_pair.second.size(); i++) {
					value_t val;
					Tool::str2str(res, val);
					result.push_back(val);
				}
			}

			data_pair.second.swap(result);
			result.clear();
		}
	}

	void GrabHistory(int label_step, vector<pair<history_t, vector<value_t>>> & data) {
		for (auto & data_pair : data) {
			vector<value_t> result;
			if (!data_pair.first.empty()) {
				vector<pair<int, value_t>>::iterator p_itr = data_pair.first.begin();
				do {
					if (label_step == (*p_itr).first) {
						for (int i = 0; i < data_pair.second.size(); i++) {
							result.push_back((*p_itr).second);
						}
						break;
					}
					p_itr++;
				} while (p_itr != data_pair.first.end());
			}

			data_pair.second.swap(result);
		}
	}

};

#endif /* SELECT_ACTOR_HPP_ */
