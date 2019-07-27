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
#include "core/common.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/tcp_mailbox.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"
#include "utils/zmq.hpp"
#include "base/abstract_thread_safe_queue.hpp"
#include "base/thread_safe_queue.hpp"
#include "core/transactions_table.hpp"
#include "glog/logging.h"

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

            ibinstream m;
            m << client_id;
            m << target_engine_id;

            zmq::message_t msg(m.size());
            memcpy(reinterpret_cast<void*>(msg.data()), m.get_buf(), m.size());
            socket_->send(msg);
        }
    }

    void RecvNotification() {
        int notification_type;

        while (true) {
            obinstream out;
            mailbox -> RecvNotification(out);
            out >> notification_type;

            if (notification_type == (int)(NOTIFICATION_TYPE::OBTAIN_CT)) {
                int n_id;
                uint64_t trxid;
                out >> n_id >> trxid;

                uint64_t ct;
                trx_p -> allocate_ct(ct);

                ibinstream in;
                int notification_type = (int)(NOTIFICATION_TYPE::ALLOCATED_CT);
                in << notification_type << trxid << ct;
                mailbox -> SendNotification(n_id, in);
            } else if (notification_type == (int)(NOTIFICATION_TYPE::OBTAIN_BT)) {
                int n_id;
                uint64_t trxid;
                out >> n_id >> trxid;

                uint64_t st;
                trx_p -> allocate_bt(st);

                ibinstream in;
                int notification_type = (int)(NOTIFICATION_TYPE::ALLOCATED_BT);
                in << notification_type << trxid << st;
                mailbox -> SendNotification(n_id, in);
            } else {
                CHECK(false);
            }
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

        thread listen(&Master::ProgListener, this);
        thread process(&Master::ProcessREQ, this);
        thread notification_listener(&Master::RecvNotification, this);

        int end_tag = 0;
        while (end_tag < node_.get_local_size()) {
            int tag = recv_data<int>(node_, MPI_ANY_SOURCE, true, MSCOMMUN_CHANNEL);
            if (tag == DONE) {
                end_tag++;
            }
        }

        listen.join();
        process.join();
        notification_listener.join();
    }

 private:
    Node & node_;
    vector<Node> & workers_;
    Config * config_;
    map<int, Progress> progress_map_;
    int client_num;
    TrxGlobalCoordinator * trx_p;
    AbstractMailbox * mailbox;

    bool is_end_;
    zmq::context_t context_;
    zmq::socket_t * socket_;

    inline int socket_code(int n_id, int t_id) {
        return config_ -> global_num_threads * n_id + t_id;
    }
};

#endif /* MASTER_HPP_ */
