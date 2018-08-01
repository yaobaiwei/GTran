/*
 * has_actor.hpp
 *
 *  Created on: July 16, 2018
 *      Author: Aaron LI
 */
#ifndef HAS_ACTOR_HPP_
#define HAS_ACTOR_HPP_

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

class HasActor : public AbstractActor {
public:
	HasActor(int id, DataStore * data_store, int num_thread, AbstractMailbox * mailbox) : AbstractActor(id, data_store), num_thread_(num_thread), mailbox_(mailbox), type_(ACTOR_T::HAS) {}

	// Has:
	// inType
	// [	key:  int
	// 		pred: Predicate_T
	// 		pred_param: value_t]
	// Has(params) :
	// 	-> key = pid; pred = ANY; pred_params = value_t(one) : has(key)
	// 	-> key = pid; pred = EQ; pred_params = value_t(one) : has(key, value)
	// 	-> key = pid; pred = <others>; pred_params = value_t(one/two) : has(key, predicate)
	// HasValue(params) : values -> [key = -1; pred = EQ; pred_params = string(value)]
	// HasNot(params) : key -> [key = pid; pred = NONE; pred_params = -1]
	// HasKey(params) : keys -> [key = pid; pred = ANY; pred_params = -1]
	void process(int tid, vector<Actor_Object> & actor_objs, Message & msg) {
		// Get Actor_Object
		Meta & m = msg.meta;
		Actor_Object actor_obj = actor_objs[m.step];

		// store all predicate
		vector<pair<int, PredicateValue>> pred_chain;

		// Get Params
		assert(actor_obj.params.size() > 0 && (actor_obj.params.size() - 1) % 3 == 0); // make sure input format
		Element_T inType = (Element_T) Tool::value_t2int(actor_obj.params.at(0));
		int numParamsGroup = (actor_obj.params.size() - 1) / 3; // number of groups of params

		// Create predicate chain for this query
		for (int i = 0; i < numParamsGroup; i++) {
			int pos = i * 3 + 1;
			// Get predicate params
			int pid = Tool::value_t2int(actor_obj.params.at(pos));

			Predicate_T pred_type = (Predicate_T) Tool::value_t2int(actor_obj.params.at(pos + 1));
			vector<value_t> pred_params;
			Tool::value_t2vec(actor_obj.params.at(pos + 2), pred_params);

			cout << "[OUTER]: pred params type : " << to_string(pred_params.at(0).type) << endl;
			cout << "[OUTER]: pred params content size: " << pred_params.at(0).content.size() << endl;

			// pred_chain.push_back(pair<int, PredicateValue>(pid, PredicateValue(pred_type, pred_params)));
			pred_chain.emplace_back(pid, PredicateValue(pred_type, pred_params));
		}

		switch(inType) {
			case Element_T::VERTEX:
				EvaluateVertex(tid, msg.data, pred_chain);
				break;
			case Element_T::EDGE:
				EvaluateEdge(tid, msg.data, pred_chain);
				break;
			default:
				cout << "Wrong inType" << endl;
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

	void EvaluateVertex(int tid, vector<pair<history_t, vector<value_t>>> & data, vector<pair<int, PredicateValue>> & pred_chain)
	{
		for (auto & data_pair : data) {
			for (auto value_itr = data_pair.second.begin(); value_itr != data_pair.second.end(); ) {

				vid_t v_id(Tool::value_t2int(*value_itr));

				bool isEarsed = false;
				for (auto & pred_pair : pred_chain) {
					int pid = pred_pair.first;
					PredicateValue pred = pred_pair.second;

					cout << "Predicate value size : " << pred.values.size() << endl;
					cout << "pred params type : " << to_string(pred.values.at(0).type) << endl;
					cout << "pred params content size: " << pred.values.at(0).content.size() << endl;

					if (pid == -1) {
						Vertex* vtx = data_store_->GetVertex(v_id);
						int counter = vtx->vp_list.size();
						for (auto & pkey : vtx->vp_list) {
							vpid_t vp_id(v_id, pkey);

							value_t val;
							if (!cache.get_property_from_cache(vp_id.value(), val)) {
								data_store_->GetPropertyForVertex(tid, vp_id, val);
								cache.insert_properties(vp_id.value(), val);
							}

							if(!Evaluate(pred, &val)) {
								counter--;
							}
						}

						if (counter == 0) {
							value_itr = data_pair.second.erase(value_itr);
							isEarsed = true;
							break;
						}
					} else {

						vpid_t vp_id(v_id, pid);

						if (pred.pred_type == Predicate_T::ANY) {
							if(!data_store_->VPKeyIsExist(tid, vp_id)) {
								// erase this data and break
								value_itr = data_pair.second.erase(value_itr);
								isEarsed = true;
								break;
							}
						} else if (pred.pred_type == Predicate_T::NONE) {
							// hasNot(key)
							if(data_store_->VPKeyIsExist(tid, vp_id)) {
								// erase this data and break
								value_itr = data_pair.second.erase(value_itr);
								isEarsed = true;
								break;
							}
						} else {
							// Get Properties
							value_t val;
							if (!cache.get_property_from_cache(vp_id.value(), val)) {
								data_store_->GetPropertyForVertex(tid, vp_id, val);
								cache.insert_properties(vp_id.value(), val);
							}

							if (val.content.size() == 0) {
								// No such value or key, erase directly
								value_itr = data_pair.second.erase(value_itr);
								isEarsed = true;
								break;
							}

							// Erase when doesnt match
							if(!Evaluate(pred, &val)) {
								// cout << "Erase since cannot match" << endl;
								value_itr = data_pair.second.erase(value_itr);
								isEarsed = true;
								break;
							}
						}
					}
				}
				// If not earsed, go next itr
				if (!isEarsed) {
					value_itr++;
				}
			}
		}
	}

	void EvaluateEdge(int tid, vector<pair<history_t, vector<value_t>>> & data, vector<pair<int, PredicateValue>> & pred_chain) {
		for (auto & data_pair : data) {
			for (auto value_itr = data_pair.second.begin(); value_itr != data_pair.second.end(); ) {

				eid_t e_id;
				uint2eid_t(Tool::value_t2uint64_t(*value_itr), e_id);

				bool isEarsed = false;
				for (auto & pred_pair : pred_chain) {
					int pid = pred_pair.first;
					PredicateValue pred = pred_pair.second;

					if (pid == -1) {
						Edge* edge = data_store_->GetEdge(e_id);
						int counter = edge->ep_list.size();
						for (auto & pkey : edge->ep_list) {
							epid_t ep_id(e_id, pkey);

							value_t val;
							if (!cache.get_property_from_cache(ep_id.value(), val)) {
								data_store_->GetPropertyForEdge(tid, ep_id, val);
								cache.insert_properties(ep_id.value(), val);
							}

							if(!Evaluate(pred, &val)) {
								counter--;
							}
						}

						if (counter == 0) {
							value_itr = data_pair.second.erase(value_itr);
							isEarsed = true;
							break;
						}
					} else {

						epid_t ep_id(e_id, pid);

						if (pred.pred_type == Predicate_T::ANY) {
							if(!data_store_->EPKeyIsExist(tid, ep_id)) {
								// erase this data when NOT exist
								value_itr = data_pair.second.erase(value_itr);
								isEarsed = true;
								break;
							}
						} else if (pred.pred_type == Predicate_T::NONE) {
							// hasNot(key)
							if(data_store_->EPKeyIsExist(tid, ep_id)) {
								// erase this data when exist
								value_itr = data_pair.second.erase(value_itr);
								isEarsed = true;
								break;
							}
						} else {
							// Get Properties
							value_t val;
							if (!cache.get_property_from_cache(ep_id.value(), val)) {
								data_store_->GetPropertyForEdge(tid, ep_id, val);
								cache.insert_properties(ep_id.value(), val);
							}

							if (val.content.size() == 0) {
								// No such value or key, erase directly
								value_itr = data_pair.second.erase(value_itr);
								isEarsed = true;
								break;
							}

							// Erase when doesnt match
							if(!Evaluate(pred, &val)) {
								value_itr = data_pair.second.erase(value_itr);
								isEarsed = true;
								break;
							}
						}
					}
				}
				// If not earsed, go next itr
				if (!isEarsed) {
					value_itr++;
				}
			}
		}
	}
};

#endif /* HAS_ACTOR_HPP_ */
