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
#include "actor/actor_cache.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"

class HasLabelActor : public AbstractActor {
public:

	HasLabelActor(int id, DataStore * data_store, int machine_id, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : AbstractActor(id, data_store, core_affinity), machine_id_(machine_id), num_thread_(num_thread), mailbox_(mailbox), type_(ACTOR_T::HASLABEL) 
	{
		config_ = &Config::GetInstance();
	}
	// HasLabel:
	// 		Pass if any label_key matches
	// Parmas:
	// 		inType
	// 		vector<value_t> value_t.type = int
	void process(const vector<Actor_Object> & actor_objs, Message & msg) {

		int tid = TidMapper::GetInstance().GetTid();

		// Get Actor_Object
		Meta & m = msg.meta;
		Actor_Object actor_obj = actor_objs[m.step];

		// Get Params
		assert(actor_obj.params.size() > 1);
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
		msg.CreateNextMsg(actor_objs, msg.data, num_thread_, data_store_, core_affinity_, msg_vec);

		// Send Message
		for (auto& msg : msg_vec) {
			mailbox_->Send(tid, msg);
		}
	 }

private:
	// Number of Threads
	int num_thread_;
	int machine_id_;

	// Actor type
	ACTOR_T type_;

	// Pointer of mailbox
	AbstractMailbox * mailbox_;

	// Cache
	ActorCache cache;
	Config* config_;

	void VertexHasLabel(int tid, vector<int> lid_list, vector<pair<history_t, vector<value_t>>> & data)
	{
		auto checkFunction = [&](value_t & value) {
			vid_t v_id(Tool::value_t2int(value));

			label_t label;
			if (data_store_->VPKeyIsLocal(vpid_t(v_id, 0)) || !config_->global_enable_caching) {
				data_store_->GetLabelForVertex(tid, v_id, label);
			} else {
				if (!cache.get_label_from_cache(v_id.value(), label)) {
					data_store_->GetLabelForVertex(tid, v_id, label);
					cache.insert_label(v_id.value(), label);
				}
			}

			for (auto & lid : lid_list) {
				if (lid == label) {
					return false;
				}
			}
			return true;
		};

		for (auto & data_pair : data) {
			data_pair.second.erase( remove_if(data_pair.second.begin(), data_pair.second.end(), checkFunction), data_pair.second.end() );
		}
	}

	void EdgeHasLabel(int tid, vector<int> lid_list, vector<pair<history_t, vector<value_t>>> & data)
	{
		auto checkFunction = [&](value_t & value) {
			eid_t e_id;
			uint2eid_t(Tool::value_t2uint64_t(value), e_id);

			label_t label;
			if (data_store_->EPKeyIsLocal(epid_t(e_id, 0)) || !config_->global_enable_caching) {
				data_store_->GetLabelForEdge(tid, e_id, label);
			} else {
				if (!cache.get_label_from_cache(e_id.value(), label)) {
					data_store_->GetLabelForEdge(tid, e_id, label);
					cache.insert_label(e_id.value(), label);
				}
			}

			for (auto & lid : lid_list) {
				if (lid == label) {
					return false;
				}
			}
			return true;
		};

		for (auto & data_pair : data) {
			data_pair.second.erase( remove_if(data_pair.second.begin(), data_pair.second.end(), checkFunction), data_pair.second.end() );
		}
	}
};

#endif /* HASLABEL_ACTOR_HPP_ */
