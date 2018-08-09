#ifndef INIT_ACTOR_HPP_
#define INIT_ACTOR_HPP_

#include <iostream>
#include <string>
#include "glog/logging.h"

#include "actor/abstract_actor.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/result_collector.hpp"
#include "base/node.hpp"
#include "base/type.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"

using namespace std;

class InitActor : public AbstractActor {
public:
    InitActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, int nodes_num, int max_data_size) : AbstractActor(id, data_store), num_thread_(num_thread), mailbox_(mailbox), type_(ACTOR_T::INIT) {
		// copy id list from data store
		vector<vid_t> vid_list;
		vector<eid_t> eid_list;
		data_store_->GetAllEdges(eid_list);
		data_store_->GetAllVertices(vid_list);

		// convert id to msg
		Meta m;
		m.step = 1;
		m.msg_path = to_string(nodes_num);

		InitVtxMsg(m, vid_list, max_data_size);
		InitEdgeMsg(m, eid_list, max_data_size);
	}

    virtual ~InitActor(){}

    void process(int tid, vector<Actor_Object> & actor_objs, Message & msg){
        Meta & m = msg.meta;
        Actor_Object actor_obj = actor_objs[m.step];

        Element_T inType = (Element_T)Tool::value_t2int(actor_obj.params.at(0));
		vector<Message>* msg_vec;

        if (inType == Element_T::VERTEX) {
            msg_vec = &vtx_msgs;
        } else if (inType == Element_T::EDGE) {
            msg_vec = &edge_msgs;
        }

		bool isBarrier = actor_objs[m.step + 1].IsBarrier();

        // Send Message
        for (auto& msg : *msg_vec) {
			msg.meta.qid = m.qid;

			// update route
			if(isBarrier){
				msg.meta.recver_nid = m.parent_nid;
				msg.meta.recver_tid = m.parent_tid;
				msg.meta.parent_nid = m.parent_nid;
				msg.meta.parent_tid = m.parent_tid;
				msg.meta.msg_type = MSG_T::BARRIER;
			}else{
				msg.meta.recver_nid = m.recver_nid;
				msg.meta.recver_tid = (m.recver_tid ++) % num_thread_;
				msg.meta.parent_nid = m.parent_nid;
				msg.meta.parent_tid = m.parent_tid;
				msg.meta.msg_type = MSG_T::SPAWN;
			}
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

	// Messages
	vector<Message> vtx_msgs;
	vector<Message> edge_msgs;

    void InitVtxMsg(Meta& m, vector<vid_t>& vid_list, int max_data_size) {
        vector<pair<history_t, vector<value_t>>> data;
		data.emplace_back(history_t(), vector<value_t>());
        for (auto& vid : vid_list) {
            value_t v;
            Tool::str2int(to_string(vid.value()), v);
            data[0].second.push_back(v);
        }

		do{
			Message msg(m);
			msg.max_data_size = max_data_size;
			msg.InsertData(data);
			vtx_msgs.push_back(move(msg));
		}
		while((data.size() != 0));
    }

    void InitEdgeMsg(Meta& m, vector<eid_t>& eid_list, int max_data_size) {
		vector<pair<history_t, vector<value_t>>> data;
		data.emplace_back(history_t(), vector<value_t>());
        for (auto& eid : eid_list) {
            value_t v;
            Tool::str2uint64_t(to_string(eid.value()), v);
            data[0].second.push_back(v);
        }

		do{
			Message msg(m);
			msg.max_data_size = max_data_size;
			msg.InsertData(data);
			edge_msgs.push_back(move(msg));
		}
		while((data.size() != 0));
    }
};

#endif /* INIT_ACTOR_HPP_ */
