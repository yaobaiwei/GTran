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
    InitActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox) : AbstractActor(id, data_store), num_thread_(num_thread), mailbox_(mailbox), type_(ACTOR_T::INIT) {
		data_store_->GetAllEdges(eid_list);
		data_store_->GetAllVertices(vid_list);
	}

    virtual ~InitActor(){}

    void process(int tid, vector<Actor_Object> & actor_objs, Message & msg){
        Meta & m = msg.meta;
        Actor_Object actor_obj = actor_objs[m.step];

        Element_T inType = (Element_T)Tool::value_t2int(actor_obj.params.at(0));

        if (inType == Element_T::VERTEX) {
            InitData(msg.data, vid_list);
        } else if (inType == Element_T::EDGE) {
            InitData(msg.data, eid_list);
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

	// v&eid_list
	vector<vid_t> vid_list;
	vector<eid_t> eid_list;

    void InitData(vector<pair<history_t, vector<value_t>>>& data, vector<vid_t> vid_list) {
        vector<value_t> newData;
        for (auto& vid : vid_list) {
            value_t v;
            Tool::str2int(to_string(vid.value()), v);
            newData.push_back(v);
        }

        data.push_back(pair<history_t, vector<value_t>>(history_t(), newData));
    }

    void InitData(vector<pair<history_t, vector<value_t>>>& data, vector<eid_t> eid_list) {
        vector<value_t> newData;
        for (auto& eid : eid_list) {
            value_t v;
            Tool::str2uint64_t(to_string(eid.value()), v);
            newData.push_back(v);
        }

        data.push_back(pair<history_t, vector<value_t>>(history_t(), newData));
    }
};

#endif /* INIT_ACTOR_HPP_ */
