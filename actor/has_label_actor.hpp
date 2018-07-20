/*
 * hasLabel_actor.hpp
 *
 *  Created on: July 20, 2018
 *      Author: Aaron LI
 */
#ifndef HASLABEL_ACTOR_HPP_
#define HASLABEL_ACTOR_HPP_

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

class HasLabelActor : public AbstractActor {
public:
	HasLabelActor(int id, int num_thread, AbstractMailbox * mailbox, DataStore * datastore) : AbstractActor(id), num_thread_(num_thread), mailbox_(mailbox), datastore_(datastore), type_(ACTOR_T::HASLABEL) {}

	// HasLabel:
	// 		Pass if any label_key matches
	// Parmas:
	// 		inType
	// 		vector<value_t> value_t.type = int
	void process(int tid, vector<Actor_Object> & actor_objs, Message & msg) {
		// Get Actor_Object
		Meta & m = msg.meta;
		Actor_Object actor_obj = actor_objs[m.step];

		// Get Params
		Element_T inType = (Element_T) Tool::value_t2int(actor_obj.params.at(0));

		vector<int> lid_list;
		for (int pos = 1; pos < actor_obj.params.size(); pos++) {
			int lid = Tool::value_t2int(actor_obj.params.at(pos));
			lid_list.push_back(lid);
		}

		switch(inType) {
			case Element_T::VERTEX:
				VertexHasLabel(tid, lid_list, msg.data);
				break;
			case Element_T::EDGE:
				EdgeHasLabel(tid, lid_list, msg.data);
				break;
			default:
				cout << "Wrong in type"  << endl; 
		}

		// Create Message
		vector<Message> msg_vec;
		msg.CreateNextMsg(actor_objs, msg.data, num_thread_, msg_vec);

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

	// Node
	Node node_;

	// Pointer of Result_Collector
	Result_Collector * rc_;

	// Pointer of mailbox
	AbstractMailbox * mailbox_;

	// Ensure only one thread ever runs the actor
	std::mutex thread_mutex_;

	// DataStore
	DataStore * datastore_;

	void VertexHasLabel(int tid, vector<int> lid_list, vector<pair<history_t, vector<value_t>>> & data)
	{
		for (auto & data_pair : data) {
			for (auto value_itr = data_pair.second.begin(); value_itr != data_pair.second.end(); ) {

				vid_t v_id(Tool::value_t2int(*value_itr));

				label_t label;
				if (!datastore_->GetLabelForVertex(tid, v_id, label)) {
					cout << "Canot find label" << endl;
				}

				bool isPass = false;
				for (auto & lid : lid_list) {
					if (lid == label) {
						isPass = true;
						break;
					}
				}

				if (!isPass) {
					// if not pass, erase this item
					data_pair.second.erase(value_itr);
				} else {
					value_itr++;
				}
			}
		}
	}

	void EdgeHasLabel(int tid, vector<int> lid_list, vector<pair<history_t, vector<value_t>>> & data)
	{
		for (auto & data_pair : data) {
			for (auto value_itr = data_pair.second.begin(); value_itr != data_pair.second.end(); ) {

				eid_t e_id;
				uint2eid_t(Tool::value_t2uint64_t(*value_itr), e_id);

				label_t label;
				if (!datastore_->GetLabelForEdge(tid, e_id, label)) {
					cout << "Canot find label" << endl;
				}

				bool isPass = false;
				for (auto & lid : lid_list) {
					if (lid == label) {
						isPass = true;
						break;
					}
				}

				if (!isPass) {
					// if not pass, erase this item
					data_pair.second.erase(value_itr);
				} else {
					value_itr++;
				}
			}
		}
	}
};

#endif /* HASLABEL_ACTOR_HPP_ */
