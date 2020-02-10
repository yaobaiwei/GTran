/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#include "expert/terminate_expert.hpp"

void TerminateExpert::process(const QueryPlan & qplan, Message & msg) {
    int tid = TidMapper::GetInstance()->GetTid();

    // Get info of transaction
    Meta & m = msg.meta;

    value_t result;
    if (m.msg_type == MSG_T::ABORT || m.msg_type == MSG_T::INIT) {
        // verification abort: MSG_T::ABORT
        // processing abort : MSG_T::INIT
        data_storage_->Abort(qplan.trxid);

        string abort_phase_info = m.msg_type == MSG_T::INIT ? "processing" : "validation";
        index_store_->UpdateTrxStatus(qplan.trxid, TRX_STAT::ABORT);

        Tool::str2str("Transaction aborted during " + abort_phase_info, result);
    } else if (m.msg_type == MSG_T::COMMIT) {
        CHECK_EQ(msg.data.size(), 1);
        CHECK_EQ(msg.data.at(0).second.size(), 1);
        uint64_t ct = Tool::value_t2uint64_t(msg.data.at(0).second.at(0));
        data_storage_->Commit(qplan.trxid, ct);
        index_store_->MovePropBufferToRegion(qplan.trxid, ct);
        index_store_->UpdateTrxStatus(qplan.trxid, TRX_STAT::COMMITTED);
        Tool::str2str("Transaction committed", result);
    } else {
        CHECK(false) << "[Error] Unexpected Message Type in Commit Expert\n";
    }

    // Clean Dependency Read
    data_storage_->CleanDepReadTrxList(qplan.trxid);
    // Clean Transaction tmp data
    for (auto & expert_type_ : need_clean_expert_set_) {
        experts_->at(expert_type_)->clean_trx_data(qplan.trxid);
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

void TerminateExpert::prepare_clean_expert_set() {
    // Sequential Experts, clean input set
    need_clean_expert_set_.emplace(EXPERT_T::TRAVERSAL);
    need_clean_expert_set_.emplace(EXPERT_T::VALUES);
    need_clean_expert_set_.emplace(EXPERT_T::PROPERTIES);
    need_clean_expert_set_.emplace(EXPERT_T::KEY);
    need_clean_expert_set_.emplace(EXPERT_T::HASLABEL);
    need_clean_expert_set_.emplace(EXPERT_T::HAS);
    need_clean_expert_set_.emplace(EXPERT_T::PROJECT);

    // Barrier Experts, clean BarrierDataTable
    need_clean_expert_set_.emplace(EXPERT_T::END);
    need_clean_expert_set_.emplace(EXPERT_T::AGGREGATE);
    need_clean_expert_set_.emplace(EXPERT_T::CAP);
    need_clean_expert_set_.emplace(EXPERT_T::COUNT);
    need_clean_expert_set_.emplace(EXPERT_T::DEDUP);
    need_clean_expert_set_.emplace(EXPERT_T::GROUP);
    need_clean_expert_set_.emplace(EXPERT_T::ORDER);
    need_clean_expert_set_.emplace(EXPERT_T::POSTVALIDATION);
    need_clean_expert_set_.emplace(EXPERT_T::RANGE);
    need_clean_expert_set_.emplace(EXPERT_T::COIN);
    need_clean_expert_set_.emplace(EXPERT_T::MATH);

    // Labelled Branch Experts
    need_clean_expert_set_.emplace(EXPERT_T::BRANCHFILTER);
}
