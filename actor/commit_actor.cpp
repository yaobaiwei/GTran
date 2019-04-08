/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#include "actor/commit_actor.hpp"

void CommitActor::process(const QueryPlan & qplan, Message & msg) {
    int tid = TidMapper::GetInstance()->GetTid();

    // Get info of transaction
    Meta & m = msg.meta;

    if (m.msg_type == MSG_T::ABORT || m.msg_type == MSG_T::INIT) {
        // verification abort: MSG_T::ABORT
        // processing abort : MSG_T::INIT
        data_storage_->Abort(qplan.trxid);
        // TODO : Erase Barrier TmpData
    } else if (m.msg_type == MSG_T::COMMIT) {
        CHECK(msg.data.size() == 1);
        CHECK(msg.data.at(0).second.size() == 1);
        uint64_t ct = Tool::value_t2uint64_t(msg.data.at(0).second.at(0));
        data_storage_->Commit(qplan.trxid, ct);
    } else {
        cout << "[Error] Unexpected Message Type in Commit Actor" << endl;
        assert(false);
    }

    // Clean Dependency Read
    data_storage_->CleanDepReadTrxList(qplan.trxid);
    // Clean Input Set
    for (auto & actor_type_ : need_clean_actor_set_) {
        actors_->at(actor_type_)->clean_input_set(qplan.trxid);
    }

    // Create Message
    vector<Message> msg_vec;
    msg.data.clear();
    msg.CreateNextMsg(qplan.actors, msg.data, num_thread_, core_affinity_, msg_vec);

    // Send Message
    for (auto& msg : msg_vec) {
        mailbox_->Send(tid, msg);
    }
}

void CommitActor::prepare_clean_actor_set() {
    need_clean_actor_set_.emplace(ACTOR_T::TRAVERSAL);
    need_clean_actor_set_.emplace(ACTOR_T::VALUES);
    need_clean_actor_set_.emplace(ACTOR_T::PROPERTIES);
    need_clean_actor_set_.emplace(ACTOR_T::KEY);
    need_clean_actor_set_.emplace(ACTOR_T::HASLABEL);
    need_clean_actor_set_.emplace(ACTOR_T::HAS);
    need_clean_actor_set_.emplace(ACTOR_T::PROJECT);
}
