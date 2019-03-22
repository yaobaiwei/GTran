/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#include "core/trx_table_stub_zmq.hpp"

TcpTrxTableStub * TcpTrxTableStub::instance_ = nullptr;

bool TcpTrxTableStub::update_status(uint64_t trx_id, TRX_STAT new_status, std::vector<uint64_t> * trx_ids = nullptr) {
    CHECK((new_status == TRX_STAT::VALIDATING && trx_ids != nullptr) || (new_status != TRX_STAT::VALIDATING && trx_ids == nullptr)) << "[TrxTableStub] update_status: new_status should correspond to trx_ids";

    ibinstream in;
    int status_i = int(new_status);
    in << node_.get_local_rank() << trx_id << status_i;
    mailbox_ ->Send_Notify(config_->global_num_workers, in);

    if (new_status == TRX_STAT::VALIDATING) {
        obinstream out;
        mailbox_ -> Recv_Notify(out);
        out >> *trx_ids;
    }
    return true;    
}

bool TcpTrxTableStub::read_status(uint64_t trx_id, TRX_STAT& status) {
    CHECK(is_valid_trx_id(trx_id))
        << "[TcpTrxTableStub::read_status] Please provide valid trx_id";

    int t_id = TidMapper::GetInstance()->GetTid();
    ibinstream in;
    in << node_.get_local_rank() << t_id << trx_id;
    send_req(t_id, in);
    // DLOG (INFO) << "[TcpTrxTableStub::read_status] send a read_status req";

    obinstream out;
    recv_rep(t_id, out);
    // DLOG (INFO) << "[TcpTrxTableStub::read_status] recvs a read_status reply";
    int status_i;
    out >> status_i;
    status = TRX_STAT(status_i);
    return true;
}

void TcpTrxTableStub::send_req(int t_id, ibinstream& in) {
    zmq::message_t zmq_send_msg(in.size());
    memcpy(reinterpret_cast<void*>(zmq_send_msg.data()), in.get_buf(),
           in.size());
    senders_[t_id]->send(zmq_send_msg);
    return;
}

bool TcpTrxTableStub::recv_rep(int t_id, obinstream& out) {
    zmq::message_t zmq_reply_msg;
    if (receivers_[t_id]->recv(&zmq_reply_msg, 0) < 0) {
        CHECK(false) << "[TcpTrxTableStub::read_status] Worker tries to read "
                        "trx status from master failed";
    }
    char* buf = new char[zmq_reply_msg.size()];
    memcpy(buf, zmq_reply_msg.data(), zmq_reply_msg.size());
    out.assign(buf, zmq_reply_msg.size(), 0);
    return true;
}