/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#include "actor/commit_actor.hpp"

void CommitActor::process(const QueryPlan & qplan, Message & msg) {
    int tid = TidMapper::GetInstance()->GetTid();

    // Get info of transaction
    Meta & m = msg.meta;

    value_t result;
    if (m.msg_type == MSG_T::ABORT || m.msg_type == MSG_T::INIT) {
        // verification abort: MSG_T::ABORT
        // processing abort : MSG_T::INIT
        data_storage_->Abort(qplan.trxid);
        // TODO : Erase Barrier TmpData

        Tool::str2str("Transaction aborted", result);
    } else if (m.msg_type == MSG_T::COMMIT) {
        CHECK(msg.data.size() == 1);
        CHECK(msg.data.at(0).second.size() == 1);
        uint64_t ct = Tool::value_t2uint64_t(msg.data.at(0).second.at(0));
        data_storage_->Commit(qplan.trxid, ct);
        Tool::str2str("Transaction committed", result);
    } else {
        cout << "[Error] Unexpected Message Type in Commit Actor" << endl;
        assert(false);
    }

    // Clean Dependency Read
    data_storage_->CleanDepReadTrxList(qplan.trxid);
    // Clean Transaction tmp data
    for (auto & actor_type_ : need_clean_actor_set_) {
        actors_->at(actor_type_)->clean_trx_data(qplan.trxid);
    }

    // Clean trx->QueryPlan table
    // Clean all queries in trx except current one
    uint8_t num_queries = msg.meta.qid & _8LFLAG;
    for (uint8_t query_index = 0; query_index < num_queries; query_index++) {
        msg_logic_table_->erase(qplan.trxid + query_index);
    }
    data_storage_->DeleteAggData(qplan.trxid);

    // Send exit msg to coordinator
    msg.meta.msg_type = MSG_T::EXIT;
    msg.meta.recver_nid = msg.meta.parent_nid;
    msg.meta.recver_tid = msg.meta.parent_tid;
    msg.data.clear();
    msg.data.emplace_back(history_t(), vector<value_t>{move(result)});
    mailbox_->Send(tid, msg);
}

void CommitActor::prepare_clean_actor_set() {
    // Sequential Actors, clean input set
    need_clean_actor_set_.emplace(ACTOR_T::TRAVERSAL);
    need_clean_actor_set_.emplace(ACTOR_T::VALUES);
    need_clean_actor_set_.emplace(ACTOR_T::PROPERTIES);
    need_clean_actor_set_.emplace(ACTOR_T::KEY);
    need_clean_actor_set_.emplace(ACTOR_T::HASLABEL);
    need_clean_actor_set_.emplace(ACTOR_T::HAS);
    need_clean_actor_set_.emplace(ACTOR_T::PROJECT);

    // Barrier Actors, clean BarrierDataTable
    need_clean_actor_set_.emplace(ACTOR_T::END);
    need_clean_actor_set_.emplace(ACTOR_T::AGGREGATE);
    need_clean_actor_set_.emplace(ACTOR_T::CAP);
    need_clean_actor_set_.emplace(ACTOR_T::COUNT);
    need_clean_actor_set_.emplace(ACTOR_T::DEDUP);
    need_clean_actor_set_.emplace(ACTOR_T::GROUP);
    need_clean_actor_set_.emplace(ACTOR_T::ORDER);
    need_clean_actor_set_.emplace(ACTOR_T::POSTVALIDATION);
    need_clean_actor_set_.emplace(ACTOR_T::RANGE);
    need_clean_actor_set_.emplace(ACTOR_T::COIN);
    need_clean_actor_set_.emplace(ACTOR_T::MATH);
}
