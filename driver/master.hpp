/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
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

struct UpdateTrxStatusReq{
    uint64_t n_id;
    uint64_t trx_id;
    TRX_STAT new_status;
};

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
        time_stamp_ = 0;
        // TODO(nick): set initial trxid > all time stamp
        trxid_ = 0;
       
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
        // find worker with least tasks remained
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
            uint64_t trxid = trxid_++;
            uint64_t st = time_stamp_++;
            // DEBUG
            cout << "##### Master recvs request from Client: " << client_id
                    << " and reply " << target_engine_id << endl;

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

    // pop from queue and process requests of accessing the tables
    void ProcessTrxTableWriteReqs(){
      
        while(true){
            // pop a req
            UpdateTrxStatusReq req;
            pending_trx_table_updates_.WaitAndPop(req);

            DLOG(INFO) << "[Master] Processed a req: " << req.n_id << "\t" << req.trx_id << "\t" << (int)req.new_status << "\t" << std::endl;

            // check if P->V
            if(req.new_status == TRX_STAT::VALIDATING){
                uint64_t bt, ct;

                // query bt
                trx_p -> query_bt(req.trx_id, bt);
                // update state and get a ct
                trx_p -> modify_status(req.trx_id, req.new_status, ct);
                trx_p -> print_single_item(req.trx_id);

                // insert this transaction into RCT
                rct -> insert_trx(ct, req.trx_id);

                // query RCT
                std::set<uint64_t> trx_ids;
                rct -> query_trx(bt, ct, trx_ids);

                ibinstream in;
                in << trx_ids;
                // only when worker send P->V, it should wait for a reply
                mailbox -> Send_Notify(req.n_id, in);
            } else {
                // update state
                // worker shouldn't wait for reply since master willnot notify it
                trx_p -> modify_status(req.trx_id, req.new_status);
            }
        }
    }

    void ListenTrxTableWriteReqs(){
        int n_id;
        uint64_t trx_id;
        int status_i;

        while (true) {
            obinstream out;
            mailbox -> Recv_Notify(out);

            TRX_STAT new_status;           
            out >> n_id >> trx_id >> status_i;

            printf("Master recvs a update state req: %llx\t%llx\t%d\n", n_id, trx_id, status_i);

            UpdateTrxStatusReq req{n_id, trx_id, TRX_STAT(status_i)};
            pending_trx_table_updates_.Push(req);
        }
    }

    void Start() {
        cout << "[Master] Start()" <<endl; 
        // Register RDMA
        Buffer* buf = Buffer::GetInstance(&node_);     
        
        if (config_->global_use_rdma)
            mailbox = new RdmaMailbox(node_, node_, buf);
        else
            mailbox = new TCPMailbox(node_);
        mailbox->Init(workers_);

        trx_p = TrxGlobalCoordinator::GetInstance();
        rct = RCTable::GetInstance();

        // // ================Test====================
        // DLOG(INFO) << "[Master] Start() Test";
        // TRX_STAT status = TRX_STAT::VALIDATING;
        // uint64_t trx_id1 = 0x8000000000000001;
        // uint64_t trx_id2 = 0x8000000000000002;
        // uint64_t trx_id3 = 0x8000000000000003;
        // uint64_t ct;
        // trx_p -> print_single_item(trx_id1);
        // trx_p -> print_single_item(trx_id2);
        // trx_p -> print_single_item(trx_id3);
        // ================Test====================

        thread listen(&Master::ProgListener, this);
        thread process(&Master::ProcessREQ, this);
    
        thread trx_table_write_listener(&Master::ListenTrxTableWriteReqs, this);
        thread trx_table_write_executor(&Master::ProcessTrxTableWriteReqs, this);

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
    }

 private:
    Node & node_;
    vector<Node> & workers_;
    Config * config_;
    map<int, Progress> progress_map_;
    int client_num;
    ThreadSafeQueue<UpdateTrxStatusReq> pending_trx_table_updates_;
    TrxGlobalCoordinator * trx_p;
    RCTable * rct;
    AbstractMailbox * mailbox;

    uint64_t time_stamp_;
    uint64_t trxid_;

    bool is_end_;
    zmq::context_t context_;
    zmq::socket_t * socket_;
};

#endif /* MASTER_HPP_ */
