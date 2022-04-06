// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
