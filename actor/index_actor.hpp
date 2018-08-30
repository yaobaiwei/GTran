/*
 * index_actor.hpp
 *
 *  Created on: Aug 29, 2018
 *      Author: Aaron LI, Nick Fang
 */
#ifndef INDEX_ACTOR_HPP_
#define INDEX_ACTOR_HPP_

#include <string>
#include <vector>
#include <algorithm>

#include "actor/abstract_actor.hpp"
#include "core/message.hpp"
#include "core/index_store.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"

class IndexActor : public AbstractActor {
public:
	IndexActor(int id, DataStore * data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity, IndexStore * index_store) : AbstractActor(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox), index_store_(index_store), type_(ACTOR_T::HAS) {}

	void process(int tid, vector<Actor_Object> & actor_objs, Message & msg) {
		// Get Actor_Object
		Meta & m = msg.meta;
		Actor_Object actor_obj = actor_objs[m.step];

		// Get Params
		assert(actor_obj.params.size() == 2); // make sure input format
		Element_T inType = (Element_T) Tool::value_t2int(actor_obj.params[0]);
		int pid = Tool::value_t2int(actor_obj.params[1]);


		switch(inType) {
			case Element_T::VERTEX:
				BuildIndexVtx(tid, pid);
				break;
			case Element_T::EDGE:
				BuildIndexEdge(tid, pid);
				break;
			default:
				cout << "Wrong inType" << endl;
		}

		string s = "Build index done in node" + to_string(m.recver_nid);
		value_t v;
		Tool::str2str(s, v);
		msg.data.emplace_back(history_t(), vector<value_t>{v});
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

	// Actor type
	ACTOR_T type_;

	// Pointer of mailbox
	AbstractMailbox * mailbox_;

	IndexStore * index_store_;

	void BuildIndexVtx(int tid, int pid) {
		vector<vid_t> vid_list;
		data_store_->GetAllVertices(vid_list);

		map<value_t, vector<value_t>> index_map;
		vector<value_t> no_key_vec;

		for (auto& vid : vid_list){
			value_t vtx_v;
			Tool::str2int(to_string(vid.value()), vtx_v);
			Vertex* vtx = data_store_->GetVertex(vid);

			if (pid != 0 && find(vtx->vp_list.begin(), vtx->vp_list.end(), pid) == vtx->vp_list.end()) {
				no_key_vec.push_back(move(vtx_v));
			}else{
				vpid_t vp_id(vid, pid);
				value_t val_v;
				data_store_->GetPropertyForVertex(tid, vp_id, val_v);
				index_map[val_v].push_back(move(vtx_v));
			}
		}

		index_store_->SetIndexMap(Element_T::VERTEX, pid, index_map, no_key_vec);

		// TODO: set index enable after all node done building
		index_store_->SetIndexMapEnable(Element_T::VERTEX, pid);
	}

	void BuildIndexEdge(int tid, int pid) {
		vector<eid_t> eid_list;
		data_store_->GetAllEdges(eid_list);

		map<value_t, vector<value_t>> index_map;
		vector<value_t> no_key_vec;

		for (auto& eid : eid_list){
			value_t edge_v;
			Tool::str2uint64_t(to_string(eid.value()), edge_v);
			Edge* edge = data_store_->GetEdge(eid);
			if (find(edge->ep_list.begin(), edge->ep_list.end(), pid) == edge->ep_list.end()) {
				no_key_vec.push_back(move(edge_v));
			}else{
				epid_t ep_id(eid, pid);
				value_t val_v;
				data_store_->GetPropertyForEdge(tid, ep_id, val_v);
				index_map[val_v].push_back(move(edge_v));
			}
		}

		index_store_->SetIndexMap(Element_T::EDGE, pid, index_map, no_key_vec);

		// TODO: set index enable after all node done building
		index_store_->SetIndexMapEnable(Element_T::EDGE, pid);
	}
};

#endif /* INDEX_ACTOR_HPP_ */
