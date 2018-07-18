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
    InitActor(int id, int num_thread, AbstractMailbox * mailbox, DataStore * datastore) : AbstractActor(id), num_thread_(num_thread), mailbox_(mailbox), datastore_(datastore) { type_ = ACTOR_T::INIT; }

    virtual ~InitActor(){}

    void process(int tid, vector<Actor_Object> & actor_objs, Message & msg){
        Meta & m = msg.meta;
        Actor_Object actor_obj = actor_objs[m.step];

        Element_T inType = (Element_T)Tool::value_t2int(actor_obj.params.at(0));

        vector<vid_t> vid_list;
        vector<eid_t> eid_list;

        if (inType == Element_T::VERTEX) {
            datastore_->GetAllVertices(vid_list);
            InitData(msg.data, vid_list);
        } else if (inType == Element_T::EDGE) {
            datastore_->GetAllEdges(eid_list);
            InitData(msg.data, eid_list);
        }

        vector<Message> msg_vec;
        msg.CreateNextMsg(actor_objs, msg.data, num_thread_, msg_vec);

		sleep(1);

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

    // Data Store
    DataStore * datastore_;

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

#endif /* HW_ACTOR_HPP_ */
