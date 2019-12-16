/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include "status_actor.hpp"

void StatusActor::process(const QueryPlan & qplan, Message & msg) {
    int tid = TidMapper::GetInstance()->GetTid();

    // Get Actor_Object
    Meta & m = msg.meta;
    Actor_Object actor_obj = qplan.actors[m.step];

    // Get Params
    CHECK(actor_obj.params.size() == 1);  // make sure input format
    string status_key = Tool::value_t2string(actor_obj.params[0]);

    string ret;
    if (status_key == "mem") {
        // display memory info of containers
        ret = data_storage_->GetContainerUsageString();
    } else {
        // undefined status key
        ret = "[Error] Invalid status key \"" + status_key;
        ret += "\"!\nType \"help status\" in the client console for more information.";
    }

    if (m.recver_nid == m.parent_nid) {
        value_t v;
        Tool::str2str(ret, v);
        msg.data.emplace_back(history_t(), vector<value_t>{v});
    } else {
        msg.data.emplace_back(history_t(), vector<value_t>());
    }

    // Create Message
    vector<Message> msg_vec;
    msg.CreateNextMsg(qplan.actors, msg.data, num_thread_, core_affinity_, msg_vec);

    // Send Message
    for (auto& msg : msg_vec) {
        mailbox_->Send(tid, msg);
    }
}
