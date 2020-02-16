/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include "expert/status_expert.hpp"
#include "layout/garbage_collector.hpp"

void StatusExpert::process(const QueryPlan & qplan, Message & msg) {
    int tid = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);

    // Get Expert_Object
    Meta & m = msg.meta;
    Expert_Object expert_obj = qplan.experts[m.step];

    // Get Params
    CHECK(expert_obj.params.size() == 1);  // make sure input format
    string status_key = Tool::value_t2string(expert_obj.params[0]);

    string ret;
    if (status_key == "mem") {
        // display memory info of containers
        ret = data_storage_->GetContainerUsageString();
    } else if (status_key == "gc") {
        ret = GarbageCollector::GetInstance()->GetDepGCTaskStatusStatistics();
    } else {
        // undefined status key
        ret = "[Error] Invalid status key \"" + status_key;
        ret += "\"!\nType \"help status\" in the client console for more information.";
    }

    ret = "Node " + to_string(m.recver_nid) + ":\n" + ret;

    value_t v;
    Tool::str2str(ret, v);
    msg.data.emplace_back(history_t(), vector<value_t>{v});

    // Create Message
    vector<Message> msg_vec;
    msg.CreateNextMsg(qplan.experts, msg.data, num_thread_, core_affinity_, msg_vec);

    // Send Message
    for (auto& msg : msg_vec) {
        mailbox_->Send(tid, msg);
    }
}
