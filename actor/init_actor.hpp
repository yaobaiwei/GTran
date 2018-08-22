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
#include "utils/timer.hpp"

using namespace std;

class InitActor : public AbstractActor {
public:
    InitActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity, int num_nodes, int max_data_size) : AbstractActor(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox), num_nodes_(num_nodes), max_data_size_(max_data_size), type_(ACTOR_T::INIT), is_ready_(false) {
	}

    virtual ~InitActor(){}

    void process(int tid, vector<Actor_Object> & actor_objs, Message & msg){
		if(! is_ready_){
			if(thread_mutex_.try_lock()){
				InitMsg();
				is_ready_ = true;
				thread_mutex_.unlock();
			}else{
				// wait until InitMsg finished
				while(! thread_mutex_.try_lock());
				thread_mutex_.unlock();
			}
		}
        Meta & m = msg.meta;
        Actor_Object actor_obj = actor_objs[m.step];

        Element_T inType = (Element_T)Tool::value_t2int(actor_obj.params.at(0));
		vector<Message>* msg_vec;

        if (inType == Element_T::VERTEX) {
            msg_vec = &vtx_msgs;
        } else if (inType == Element_T::EDGE) {
            msg_vec = &edge_msgs;
        }

		MSG_T msg_type = MSG_T::SPAWN;
		int recver_nid = m.recver_nid;
		if(actor_objs[m.step + 1].IsBarrier()){
			msg_type = MSG_T::BARRIER;
			recver_nid = m.parent_nid;
		}

		thread_mutex_.lock();
        // Send Message
        for (auto& msg : *msg_vec) {
			msg.meta.qid = m.qid;

			// update route
			msg.meta.msg_type = msg_type;
			msg.meta.recver_nid = recver_nid;
			msg.meta.recver_tid = core_affinity_->GetThreadIdForActor(actor_objs[m.step+1].actor_type);
			msg.meta.parent_nid = m.parent_nid;
			msg.meta.parent_tid = m.parent_tid;

            mailbox_->Send(tid, msg);
        }
		thread_mutex_.unlock();
    }

private:
	// Number of threads
	int num_thread_;
	int num_nodes_;
	int max_data_size_;
	bool is_ready_;

	// Actor type
	ACTOR_T type_;

	// Pointer of mailbox
	AbstractMailbox * mailbox_;

	// Ensure only one thread ever runs the actor
	std::mutex thread_mutex_;

	// Messages
	vector<Message> vtx_msgs;
	vector<Message> edge_msgs;

	void InitMsg(){
		if(is_ready_){
			return;
		}
		// copy id list from data store
		uint64_t start_t = timer::get_usec();
		vector<vid_t> vid_list;
		vector<eid_t> eid_list;
		data_store_->GetAllEdges(eid_list);
		data_store_->GetAllVertices(vid_list);
		uint64_t end_t = timer::get_usec();
		cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for get_v&e in init_actor" << endl;

		// convert id to msg
		Meta m;
		m.step = 1;
		m.msg_path = to_string(num_nodes_);

		start_t = timer::get_usec();
		InitVtxMsg(m, vid_list, max_data_size_);
		end_t = timer::get_usec();
		cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for initV_Msg in init_actor" << endl;

		start_t = timer::get_usec();
		InitEdgeMsg(m, eid_list, max_data_size_);
		end_t = timer::get_usec();
		cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for initE_Msg in init_actor" << endl;
	}

    void InitVtxMsg(Meta& m, vector<vid_t>& vid_list, int max_data_size) {
		vector<pair<history_t, vector<value_t>>> data;
		data.emplace_back(history_t(), vector<value_t>());
		data[0].second.reserve(vid_list.size());
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

		string num = "\t" + to_string(vtx_msgs.size());
		for (auto & msg_ : vtx_msgs) {
			msg_.meta.msg_path += num;
		}
    }

    void InitEdgeMsg(Meta& m, vector<eid_t>& eid_list, int max_data_size) {
		vector<pair<history_t, vector<value_t>>> data;
		data.emplace_back(history_t(), vector<value_t>());
		data[0].second.reserve(eid_list.size());
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

		string num = "\t" + to_string(edge_msgs.size());
		for (auto & msg_ : edge_msgs) {
			msg_.meta.msg_path += num;
		}
    }
};

#endif /* INIT_ACTOR_HPP_ */
