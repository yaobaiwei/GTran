/*
 * values_actor.hpp
 *
 *  Created on: July 20, 2018
 *      Author: Aaron LI
 */
#ifndef VALUES_ACTOR_HPP_
#define VALUES_ACTOR_HPP_

#include <string>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "actor/actor_cache.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"

class ValuesActor : public AbstractActor {
public:
	ValuesActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox) : AbstractActor(id, data_store), num_thread_(num_thread), mailbox_(mailbox), type_(ACTOR_T::VALUES) {}

	// inType, [key]+
	void process(int tid, vector<Actor_Object> & actor_objs, Message & msg) {
		Meta & m = msg.meta;
		Actor_Object actor_obj = actor_objs[m.step];

		Element_T inType = (Element_T)Tool::value_t2int(actor_obj.params.at(0));
		vector<int> key_list;
		for (int cnt = 1; cnt < actor_obj.params.size(); cnt++) {
			key_list.push_back(Tool::value_t2int(actor_obj.params.at(cnt)));
		}

		switch(inType) {
			case Element_T::VERTEX:
				get_properties_for_vertex(tid, key_list, msg.data);
				break;
			case Element_T::EDGE:
				get_properties_for_edge(tid, key_list, msg.data);
				break;
			default:
				cout << "Wrong in type" << endl;
		}

		vector<Message> msg_vec;
		msg.CreateNextMsg(actor_objs, msg.data, num_thread_, data_store_, msg_vec);

		// Send Message
		for (auto& msg : msg_vec) {
			mailbox_->Send(tid, msg);
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

	// Cache
	ActorCache cache;

	void get_properties_for_vertex(int tid, vector<int> & key_list, vector<pair<history_t, vector<value_t>>>& data) {
		for (auto & pair : data) {
			vector<value_t> newData;

			for (auto & value : pair.second) {
				vid_t v_id(Tool::value_t2int(value));

				if (key_list.empty()) {
					Vertex* vtx = data_store_->GetVertex(v_id);
					for (auto & pkey : vtx->vp_list) {
						vpid_t vp_id(v_id, pkey);

						value_t val;
						// Try cache
						if (!cache.get_property_from_cache(vp_id.value(), val)) {
							// not found in cache
							cout << "not found in cache" << endl;
							data_store_->GetPropertyForVertex(tid, vp_id, val);
							cache.insert_properties(vp_id.value(), val);
						}

						newData.push_back(val);
					}
				} else {
					for (auto key : key_list) {
						vpid_t vp_id(v_id, key);

						if(! data_store_->VPKeyIsExist(tid, vp_id)) {
							continue;
						}

						value_t val;
						if (!cache.get_property_from_cache(vp_id.value(), val)) {
							cout << "not found in cache" << endl;
							data_store_->GetPropertyForVertex(tid, vp_id, val);
							cache.insert_properties(vp_id.value(), val);
						}

						newData.push_back(val);
					}
				}
			}

			pair.second.swap(newData);
		}
	}

	void get_properties_for_edge(int tid, vector<int> & key_list, vector<pair<history_t, vector<value_t>>>& data) {
		for (auto & pair : data) {
			vector<value_t> newData;

			for (auto & value : pair.second) {

				eid_t e_id;
				uint2eid_t(Tool::value_t2uint64_t(value), e_id);

				if (key_list.empty()) {
					Edge* edge = data_store_->GetEdge(e_id);
					for (auto & pkey : edge->ep_list) {
						epid_t ep_id(e_id, pkey);

						value_t val;
						if (!cache.get_property_from_cache(ep_id.value(), val)) {
							// not found in cache
							cout << "not found in cache" << endl;
							data_store_->GetPropertyForEdge(tid, ep_id, val);
							cache.insert_properties(ep_id.value(), val);
						}

						newData.push_back(val);
					}
				} else {
					for (auto key : key_list) {
						epid_t ep_id(e_id, key);

						if(! data_store_->EPKeyIsExist(tid, ep_id)) {
							continue;
						}

						value_t val;
						if (!cache.get_property_from_cache(ep_id.value(), val)) {
							// not found in cache
							cout << "not found in cache" << endl;
							data_store_->GetPropertyForEdge(tid, ep_id, val);
							cache.insert_properties(ep_id.value(), val);
						}

						newData.push_back(val);
					}
				}
			}
			pair.second.swap(newData);
		}
	}
};

#endif /* VALUES_ACTOR_HPP_ */
