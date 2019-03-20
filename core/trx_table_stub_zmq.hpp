/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#pragma once

#include <tbb/concurrent_hash_map.h>
#include "base/node.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/buffer.hpp"
#include "core/common.hpp"
#include "core/rdma_mailbox.hpp"
#include <iostream>
#include "utils/config.hpp"
#include "utils/tid_mapper.hpp"
#include "core/tcp_mailbox.hpp"
#include "glog/logging.h"
#include "core/trx_table_stub.hpp"


class TcpTrxTableStub : public TrxTableStub {
 private:
    AbstractMailbox *mailbox_;
    Node node_;
    Node master_;
    Config *config_;

    static TcpTrxTableStub *instance_;
    vector<zmq::socket_t *> senders_;
    vector<zmq::socket_t *> receivers_;  // global_num_threads

    TcpTrxTableStub(AbstractMailbox * mailbox, Node & master)
        : master_(master), mailbox_(mailbox) {
        config_ = Config::GetInstance();
        node_ = Node::StaticInstance();
    }

    void send_req(int t_id, ibinstream &in);

    bool recv_rep(int t_id, obinstream &out);

 public:
    static TcpTrxTableStub * GetInstance(Node & master,
                                         AbstractMailbox * mailbox) {
        assert(instance_==nullptr) ;
        // << "[TcpTrxTableStub::GetInstance] duplicate initilization";
        assert(mailbox!=nullptr) ;
        // << "[TcpTrxTableStub::GetInstance] must provide valid mailbox";
        DLOG(INFO) << "[TcpTrxTableStub::GetInstance] Initialize instance_" << mailbox;

        instance_ = new TcpTrxTableStub(mailbox, master);

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
        string master_hostname = master_.hostname;
        char addr[64] = "";

        receivers_.resize(config_->global_num_threads);
        senders_.resize(config_->global_num_threads);

        for (int i = 0; i < config_->global_num_threads; ++i) {
            receivers_[i] = new zmq::socket_t(context, ZMQ_PULL);
            snprintf(addr, sizeof(addr), "tcp://*:%d",
                    node_.tcp_port + i + 3 + config_->global_num_threads);
            receivers_[i]->bind(addr);
            DLOG(INFO) << "Worker " << node_.hostname << ": " << "bind " << string(addr);
        }

        for (int i = 0; i < config_->global_num_threads; ++i) {
            senders_[i] = new zmq::socket_t(context, ZMQ_PUSH);
            snprintf(addr, sizeof(addr), "tcp://%s:%d", master_hostname.c_str(),
                    master_.tcp_port + 3);
            senders_[i]->connect(
                addr);  // connect to the same port with update_status reqs
            DLOG(INFO) << "Worker " << node_.hostname << ": connects to " << string(addr);
        }
    }

  bool update_status(uint64_t trx_id, TRX_STAT new_status,
                     std::vector<uint64_t> *trx_ids = nullptr) override;

  bool read_status(uint64_t trx_id, TRX_STAT &status) override;
};
