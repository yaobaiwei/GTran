/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
         Modified by Jian Zhang (jzhang@cse.cuhk.edu.hk)
*/

#ifndef MASTER_HPP_
#define MASTER_HPP_

#include <string.h>
#include <limits.h>
#include <stdlib.h>
#include <map>
#include <vector>
#include <thread>
#include <iostream>
#include <queue>

#include "base/node.hpp"
#include "base/communication.hpp"
#include "core/buffer.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/tcp_mailbox.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"
#include "utils/zmq.hpp"
#include "base/abstract_thread_safe_queue.hpp"
#include "base/thread_safe_queue.hpp"
#include "core/RCT.hpp"
#include "core/transactions_table.hpp"
#include "glog/logging.h"
#include "core/common.hpp"

struct Progress {
    uint32_t assign_tasks;
    uint32_t finish_tasks;

    uint32_t remain_tasks(){
        return assign_tasks - finish_tasks;
    }
};

class Master {
 public:
    Master(Node & node, vector<Node> & workers): node_(node), workers_(workers) {
        config_ = Config::GetInstance();
        is_end_ = false;
        client_num = 0;
    }

    ~Master() {
        delete socket_;
    }

    void Init() {
        socket_ = new zmq::socket_t(context_, ZMQ_REP);
        char addr[64];
        snprintf(addr, sizeof(addr), "tcp://*:%d", node_.tcp_port);
        socket_->bind(addr);

        if (!config_->global_use_rdma) {
            trx_read_recv_socket = new zmq::socket_t(context_, ZMQ_PULL);
            snprintf(addr, sizeof(addr), "tcp://*:%d", node_.tcp_port + 3);
            trx_read_recv_socket->bind(addr);
            DLOG(INFO) << "[Master] bind " << string(addr);
            trx_read_rep_sockets.resize(config_->global_num_threads *
                                        config_->global_num_workers);
            // connect to p+3+global_num_threads ~ p+2+2*global_num_threads
            for (int i = 0; i < config_->global_num_workers; ++i) {
                Node& r_node = GetNodeById(workers_, i + 1);

                for (int j = 0; j < config_->global_num_threads; ++j) {
                    trx_read_rep_sockets[socket_code(i, j)] =
                        new zmq::socket_t(context_, ZMQ_PUSH);
                    snprintf(
                        addr, sizeof(addr), "tcp://%s:%d",
                        workers_[i].ibname.c_str(),
                        r_node.tcp_port + j + 3 + config_->global_num_threads);
                    trx_read_rep_sockets[socket_code(i, j)]->connect(addr);
                    DLOG(INFO) << "[Master] connects to " << string(addr);
                }
            }
        }
    }

    void ProgListener() {
        while (1) {
            vector<uint32_t> prog = recv_data<vector<uint32_t>>(node_, MPI_ANY_SOURCE, true, MONITOR_CHANNEL);

            int src = prog[0];  // the slave ID
            Progress & p = progress_map_[src];
            if (prog[1] != -1) {
                p.finish_tasks = prog[1];
            } else {
                progress_map_.erase(src);
            }
            if (progress_map_.size() == 0)
                break;
        }
    }

    int ProgScheduler() {
        // find worker that remains least tasks
        uint32_t min = UINT_MAX;
        int min_index = -1;
        map<int, Progress>::iterator m_iter;
        for (m_iter = progress_map_.begin(); m_iter != progress_map_.end(); m_iter++) {
            if (m_iter->second.remain_tasks() < min) {
                min = m_iter->second.remain_tasks();
                min_index = m_iter->first;
            }
        }
        if (min_index != -1) {
            return min_index;
        }
        return rand() % (node_.get_world_size() - 1) + 1;
    }

    void ProcessREQ() {
        while (1) {
            zmq::message_t request;
            socket_->recv(&request);

            char* buf = new char[request.size()];
            memcpy(buf, request.data(), request.size());
            obinstream um(buf, request.size());

            int client_id;
            um >> client_id;
            if (client_id == -1) {  // first connection, obtain the global client ID
                client_id = ++client_num;
            }

            int target_engine_id = ProgScheduler();
            progress_map_[target_engine_id].assign_tasks++;

            uint64_t trxid, st;
            trx_p -> insert_single_trx(trxid, st);


            ibinstream m;
            m << client_id;
            m << target_engine_id;
            m << trxid;
            m << st;

            zmq::message_t msg(m.size());
            memcpy(reinterpret_cast<void*>(msg.data()), m.get_buf(), m.size());
            socket_->send(msg);
        }
    }

    //receive a msg of tran status update
    void ListenTrxTableWriteReqs() {
        int n_id;
        uint64_t trx_id;
        int status_i;
        bool is_read_only;

        while (true) {
            obinstream out;
            mailbox -> RecvNotification(out);

            TRX_STAT new_status;
            out >> n_id >> trx_id >> status_i >> is_read_only;

            UpdateTrxStatusReq req{n_id, trx_id, TRX_STAT(status_i), is_read_only};
            pending_trx_updates_.Push(req);
        }
    }

    // pop from queue and process requests of accessing the tables
    void ProcessTrxTableWriteReqs() {
        while (true) {
            // pop a req
            UpdateTrxStatusReq req;
            pending_trx_updates_.WaitAndPop(req);

            // check if P->V
            if (req.new_status == TRX_STAT::VALIDATING) {
                uint64_t bt, ct;

                // query bt
                trx_p -> query_bt(req.trx_id, bt);

                // update state and get a ct
                trx_p -> modify_status(req.trx_id, req.new_status, ct, req.is_read_only);
                //trx_p -> print_single_item(req.trx_id);

                // query RCT
                std::set<uint64_t> trx_ids;
                rct -> query_trx(bt, ct, trx_ids);

                // insert this transaction into RCT
                rct -> insert_trx(ct, req.trx_id);

                std::vector<uint64_t> trx_ids_vec(trx_ids.begin(), trx_ids.end());

                ibinstream in;
                in << trx_ids_vec;
                // only when worker send P->V, it should wait for a reply
                mailbox -> SendNotification(req.n_id, in);
            } else {
                // update state
                // worker shouldn't wait for reply since master will not notify it
                trx_p -> modify_status(req.trx_id, req.new_status);
            }
        }
    }

    // cover only TCP read
    void ListenTCPTrxReads(){
        while(true){
            ReadTrxStatusReq req;
            obinstream out;
            zmq::message_t zmq_req_msg;
            if (trx_read_recv_socket -> recv(&zmq_req_msg, 0) < 0) {
                CHECK(false) << "[Master::ListenTCPTrxReads] Master failed to recv TCP read";
            }
            // DLOG(INFO) << "[Master::ListenTCPTrxReads] recvs a read status req";
            char* buf = new char[zmq_req_msg.size()];
            memcpy(buf, zmq_req_msg.data(), zmq_req_msg.size());
            out.assign(buf, zmq_req_msg.size(), 0);

            out >> req.n_id >> req.t_id >> req.trx_id >> req.read_ct;
            pending_trx_reads_.Push(req);
        }
    }

    void ProcessTCPTrxReads() {
        while (true) {
            // pop a req
            ReadTrxStatusReq req;
            pending_trx_reads_.WaitAndPop(req);

            // DLOG(INFO) << "[Master] Processed a TCP read req: " << req.DebugString();

            ibinstream in;
            if (req.read_ct) {
                uint64_t ct_;
                TRX_STAT status;
                trx_p -> query_ct(req.trx_id, ct_);
                trx_p -> query_status(req.trx_id, status);
                int status_i = (int) status;
                in << ct_;
                in << status_i; 
                // DLOG(INFO) << "[Master::query_status] ct of " << req.trx_id << " is " << ct_;
            } else {
                TRX_STAT status;
                trx_p -> query_status(req.trx_id, status);
                int status_i = (int) status;
                in << status_i;
                // DLOG(INFO) << "[Master::query_status] status of " << req.trx_id << " is " << status_i;
            }

            zmq::message_t zmq_send_msg(in.size());
            memcpy(reinterpret_cast<void*>(zmq_send_msg.data()), in.get_buf(),
                   in.size());
            trx_read_rep_sockets[socket_code(req.n_id, req.t_id)] -> send(zmq_send_msg);
        }
    }

    void Start() {
        cout << "[Master] Start()" <<endl;
        // Register RDMA
        Buffer* buf = Buffer::GetInstance(&node_);

        if (config_->global_use_rdma)
            mailbox = new RdmaMailbox(node_, node_, buf);
        else
            mailbox = new TCPMailbox(node_, node_);
        mailbox->Init(workers_);

        trx_p = TrxGlobalCoordinator::GetInstance();
        rct = RCTable::GetInstance();

        thread listen(&Master::ProgListener, this);
        thread process(&Master::ProcessREQ, this);

        thread trx_table_write_listener(&Master::ListenTrxTableWriteReqs, this);
        thread trx_table_write_executor(&Master::ProcessTrxTableWriteReqs, this);

        thread * trx_table_tcp_read_listener, * trx_table_tcp_read_executor;
        if (!config_->global_use_rdma) {
            trx_table_tcp_read_listener = new thread(&Master::ListenTCPTrxReads, this);
            trx_table_tcp_read_executor = new thread(&Master::ProcessTCPTrxReads, this);
        }

        int end_tag = 0;
        while (end_tag < node_.get_local_size()) {
            int tag = recv_data<int>(node_, MPI_ANY_SOURCE, true, MSCOMMUN_CHANNEL);
            if (tag == DONE) {
                end_tag++;
            }
        }

        listen.join();
        process.join();
        trx_table_write_listener.join();
        trx_table_write_executor.join();
        if (!config_->global_use_rdma) {
            trx_table_tcp_read_listener->join();
            trx_table_tcp_read_executor->join();
        }
    }

 private:
    Node & node_;
    vector<Node> & workers_;
    Config * config_;
    map<int, Progress> progress_map_;
    int client_num;
    ThreadSafeQueue<UpdateTrxStatusReq> pending_trx_updates_;
    ThreadSafeQueue<ReadTrxStatusReq> pending_trx_reads_;
    TrxGlobalCoordinator * trx_p;
    RCTable * rct;
    AbstractMailbox * mailbox;

    bool is_end_;
    zmq::context_t context_;
    zmq::socket_t * socket_;

    // trx tcp
    zmq::socket_t * trx_read_recv_socket;
    vector<zmq::socket_t *> trx_read_rep_sockets;

    inline int socket_code(int n_id, int t_id) {
        return config_ -> global_num_threads * n_id + t_id;
    }
};

#endif /* MASTER_HPP_ */
