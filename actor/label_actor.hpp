/*
 * label_actor.hpp
 *
 *  Created on: July 23, 2018
 *      Author: Aaron LI
 */
#ifndef LABEL_ACTOR_HPP_
#define LABEL_ACTOR_HPP_

#include <string>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "actor/actor_cache.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"

class LabelActor : public AbstractActor {
public:
	LabelActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox) : AbstractActor(id, data_store), num_thread_(num_thread), mailbox_(mailbox), type_(ACTOR_T::LABEL) {}

	// Label:
	// 		Output all labels of input
	// Parmas:
	// 		inType
	void process(int tid, vector<Actor_Object> & actor_objs, Message & msg) {
		// Get Actor_Object
		Meta & m = msg.meta;
		Actor_Object actor_obj = actor_objs[m.step];

		// Get Params
		Element_T inType = (Element_T) Tool::value_t2int(actor_obj.params.at(0));

		switch(inType) {
			case Element_T::VERTEX:
				VertexLabel(tid, msg.data);
				break;
			case Element_T::EDGE:
				EdgeLabel(tid, msg.data);
				break;
			default:
				cout << "Wrong in type"  << endl;
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

	// Node
	Node node_;

	// Pointer of Result_Collector
	Result_Collector * rc_;

	// Pointer of mailbox
	AbstractMailbox * mailbox_;

	// Ensure only one thread ever runs the actor
	std::mutex thread_mutex_;

	// Cache
	ActorCache cache;

	void VertexLabel(int tid, vector<pair<history_t, vector<value_t>>> & data) {
		for (auto & data_pair : data) {
			vector<value_t> newData;
			for (auto & elem : data_pair.second) {

				vid_t v_id(Tool::value_t2int(elem));

				label_t label;
				if (!cache.get_label_from_cache(v_id.value(), label)) {
					data_store_->GetLabelForVertex(tid, v_id, label);
					cache.insert_label(v_id.value(), label);
				}

				string keyStr;
				data_store_->GetNameFromIndex(Index_T::V_LABEL, label, keyStr);

				value_t val;
				Tool::str2str(keyStr, val);
				newData.push_back(val);
			}

			data_pair.second.swap(newData);
		}
	}

	void EdgeLabel(int tid, vector<pair<history_t, vector<value_t>>> & data) {
		for (auto & data_pair : data) {
			vector<value_t> newData;
			for (auto & elem : data_pair.second) {

				eid_t e_id;
				uint2eid_t(Tool::value_t2uint64_t(elem), e_id);

				label_t label;
				if (!cache.get_label_from_cache(e_id.value(), label)) {
					data_store_->GetLabelForEdge(tid, e_id, label);
					cache.insert_label(e_id.value(), label);
				}

				string keyStr;
				data_store_->GetNameFromIndex(Index_T::E_LABEL, label, keyStr);

				value_t val;
				Tool::str2str(keyStr, val);
				newData.push_back(val);
			}

			data_pair.second.swap(newData);
		}
	}
};

#endif /* LABEL_ACTOR_HPP_ */
