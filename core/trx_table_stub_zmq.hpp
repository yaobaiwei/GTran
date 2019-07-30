/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#pragma once

#include <iostream>
#include "base/node.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/buffer.hpp"
#include "core/common.hpp"
#include "core/coordinator.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/tcp_mailbox.hpp"
#include "core/trx_table_stub.hpp"
#include "glog/logging.h"
#include "tbb/concurrent_hash_map.h"
#include "utils/config.hpp"
#include "utils/tid_mapper.hpp"

class TcpTrxTableStub : public TrxTableStub {
 private:
    Node master_;
    vector<Node> workers_;

    static TcpTrxTableStub *instance_;
    vector<zmq::socket_t *> senders_;
    vector<zmq::socket_t *> receivers_;  // global_num_threads

    Coordinator* coordinator_;

    TcpTrxTableStub(AbstractMailbox *mailbox, Node &master, vector<Node> workers, ThreadSafeQueue<UpdateTrxStatusReq>* pending_trx_updates) :
                    master_(master), workers_(workers) {
        mailbox_ = mailbox;
        pending_trx_updates_ = pending_trx_updates;
        config_ = Config::GetInstance();
        node_ = Node::StaticInstance();
        coordinator_ = Coordinator::GetInstance();
        trx_table_ = TransactionTable::GetInstance();
    }

    void send_req(int n_id, int t_id, ibinstream &in);

    bool recv_rep(int t_id, obinstream &out);

    inline int socket_code(int n_id, int t_id) {
        return config_ -> global_num_threads * n_id + t_id;
    }

 public:
    static TcpTrxTableStub * GetInstance(Node & master,
                                         AbstractMailbox * mailbox,
                                         vector<Node>& workers,
                                         ThreadSafeQueue<UpdateTrxStatusReq>* pending_trx_updates) {
        assert(instance_ == nullptr);
        // << "[TcpTrxTableStub::GetInstance] duplicate initilization";
        assert(mailbox != nullptr);
        // << "[TcpTrxTableStub::GetInstance] must provide valid mailbox";
        assert(pending_trx_updates != nullptr);
        DLOG(INFO) << "[TcpTrxTableStub::GetInstance] Initialize instance_" << mailbox;

        instance_ = new TcpTrxTableStub(mailbox, master, workers, pending_trx_updates);

        assert(instance_ != nullptr);
        CHECK(instance_) << "[TcpTrxTableStub::GetInstance] Null instance_";
        return instance_;
    }

    static TcpTrxTableStub * GetInstance() {
        assert(instance_!=nullptr);
        //  << "[TcpTrxTableStub::GetInstance] must initialze instance_ first";
        DLOG(INFO) << "[TcpTrxTableStub::GetInstance] Get instance_";
        return instance_;
    }

    bool Init() override {
        string master_ibname = master_.ibname;
        char addr[64] = "";

        receivers_.resize(config_->global_num_threads);
        senders_.resize(config_->global_num_threads * config_->global_num_workers);

        for (int i = 0; i < config_->global_num_threads; ++i) {
            receivers_[i] = new zmq::socket_t(context, ZMQ_PULL);
            snprintf(addr, sizeof(addr), "tcp://*:%d",
                    node_.tcp_port + i + 3 + config_->global_num_threads);
            receivers_[i]->bind(addr);
            DLOG(INFO) << "Worker " << node_.hostname << ": " << "bind " << string(addr);
        }

        for (int j = 0; j < config_->global_num_workers; j++) {
            for (int i = 0; i < config_->global_num_threads; ++i) {
                senders_[socket_code(j, i)] = new zmq::socket_t(context, ZMQ_PUSH);
                snprintf(addr, sizeof(addr), "tcp://%s:%d", workers_[j].ibname.c_str(),
                        workers_[j].tcp_port + 3 + 2 * config_->global_num_threads);
                senders_[socket_code(j, i)]->connect(
                    addr);  // connect to the same port with update_status reqs
                DLOG(INFO) << "Worker " << node_.hostname << ": connects to " << string(addr);
            }
        }
    }

    bool update_status(uint64_t trx_id, TRX_STAT new_status, bool is_read_only = false) override;

    bool read_status(uint64_t trx_id, TRX_STAT &status) override;
    bool read_ct(uint64_t trx_id, TRX_STAT & status, uint64_t & ct) override;
};
