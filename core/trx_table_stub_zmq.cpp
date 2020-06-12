// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
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

#include "core/trx_table_stub_zmq.hpp"

TcpTrxTableStub * TcpTrxTableStub::instance_ = nullptr;

bool TcpTrxTableStub::Init() {
    char addr[64] = "";
    
    //we should count on the main thread as well
    int num_channels = config_->global_num_threads + 1;
    receivers_.resize(num_channels);
    for (int i = 0; i < num_channels; ++i) {
        receivers_[i] = new zmq::socket_t(context, ZMQ_PULL);
        //the ports are matched with the trx_read_rep_sockets_ in coordinator
        snprintf(addr, sizeof(addr), "tcp://*:%d",
                node_.tcp_port + 3 + i + config_->global_num_threads);
        receivers_[i]->bind(addr);
        DLOG(INFO) << "Worker " << node_.hostname << ": " << "bind " << string(addr);
    }
    
    senders_.resize(num_channels * config_->global_num_workers);
    for (int j = 0; j < config_->global_num_workers; j++) {
        for (int i = 0; i < num_channels; ++i) {
            senders_[socket_code(j, i)] = new zmq::socket_t(context, ZMQ_PUSH);
            //the port is matched with the trx_read_recv_socket_ in coordinator
            snprintf(addr, sizeof(addr), "tcp://%s:%d", workers_[j].ibname.c_str(),
                    workers_[j].tcp_port + 4 + 2 * config_->global_num_threads);
            // for all threads sending msgs to the same worker, connect to the same port for TcpTrxTableStub::send_req
            senders_[socket_code(j, i)]->connect(addr);
            DLOG(INFO) << "Worker " << node_.hostname << ": connects to " << string(addr);
        }
    }
}

bool TcpTrxTableStub::update_status(uint64_t trx_id, TRX_STAT new_status, bool is_read_only) {
    CHECK(new_status != TRX_STAT::VALIDATING);

    int worker_id = coordinator_->GetWorkerFromTrxID(trx_id);

    if (worker_id == node_.get_local_rank()) {
        // directly append the request to the local queue
        UpdateTrxStatusReq req{node_.get_local_rank(), trx_id, new_status, is_read_only};
        pending_trx_updates_->Push(req);
    } else {
        // send the request to remote worker
        ibinstream in;
        int status_i = int(new_status);
        in << (int)(NOTIFICATION_TYPE::UPDATE_STATUS) << node_.get_local_rank() << trx_id << status_i << is_read_only;

        mailbox_->SendNotification(worker_id, in);
    }

    return true;
}

bool TcpTrxTableStub::read_status(uint64_t trx_id, TRX_STAT& status) {
    CHECK(IS_VALID_TRX_ID(trx_id)) << "[TcpTrxTableStub::read_status] Please provide valid trx_id";

    int worker_id = coordinator_->GetWorkerFromTrxID(trx_id);

    if (worker_id == node_.get_local_rank()) {
        return trx_table_->query_status(trx_id, status);
    }

    //Channel TID_TYPE::RDMA should be renamed to TID_TYPE::COMMUN
    int t_id = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);
    ibinstream in;
    in << node_.get_local_rank() << t_id << trx_id << false;

    send_req(worker_id, t_id, in);
    // DLOG (INFO) << "[TcpTrxTableStub::read_status] send a read_status req";

    obinstream out;
    recv_rep(t_id, out);
    // DLOG (INFO) << "[TcpTrxTableStub::read_status] recvs a read_status reply";
    int status_i;
    out >> status_i;
    status = TRX_STAT(status_i);
    return true;
}

bool TcpTrxTableStub::read_ct(uint64_t trx_id, TRX_STAT & status, uint64_t & ct) {
    CHECK(IS_VALID_TRX_ID(trx_id))
        << "[TcpTrxTableStub::read_status] Please provide valid trx_id";

    int worker_id = coordinator_->GetWorkerFromTrxID(trx_id);

    if (worker_id == node_.get_local_rank()) {
        bool query_status_ret = trx_table_->query_status(trx_id, status);
        bool query_ct_ret = trx_table_->query_ct(trx_id, ct);
        return query_status_ret && query_ct_ret;
    }

    //Channel TID_TYPE::RDMA should be renamed to TID_TYPE::COMMUN
    int t_id = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);
    ibinstream in;
    in << node_.get_local_rank() << t_id << trx_id << true;

    send_req(worker_id, t_id, in);
    // DLOG (INFO) << "[TcpTrxTableStub::read_ct] send a read_ct req";

    obinstream out;
    recv_rep(t_id, out);
    // DLOG (INFO) << "[TcpTrxTableStub::read_ct] recvs a read_ct reply";
    uint64_t ct_;
    int status_i;
    out >> ct_ >> status_i;
    ct = ct_;
    status = TRX_STAT(status_i);

    return true;
}

void TcpTrxTableStub::send_req(int n_id, int t_id, ibinstream& in) {
    zmq::message_t zmq_send_msg(in.size());
    memcpy(reinterpret_cast<void*>(zmq_send_msg.data()), in.get_buf(),
           in.size());
    senders_[socket_code(n_id, t_id)]->send(zmq_send_msg);
    return;
}

bool TcpTrxTableStub::recv_rep(int t_id, obinstream& out) {
    zmq::message_t zmq_reply_msg;
    if (receivers_[t_id]->recv(&zmq_reply_msg, 0) < 0) {
        CHECK(false) << "[TcpTrxTableStub::read_status] Worker tries to read "
                        "trx status failed";
    }
    char* buf = new char[zmq_reply_msg.size()];
    memcpy(buf, zmq_reply_msg.data(), zmq_reply_msg.size());
    out.assign(buf, zmq_reply_msg.size(), 0);
    return true;
}
