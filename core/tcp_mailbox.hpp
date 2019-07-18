/* Copyright 2019 Husky Data Lab, CUHK
 *
 * Authors: Created by Changji Li (cjli@cse.cuhk.edu.hk)
 *          Modified by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#pragma once

#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <tbb/concurrent_unordered_map.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/node_util.hpp"
#include "base/thread_safe_queue.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/message.hpp"
#include "utils/simple_spinlock_guard.hpp"
#include "utils/zmq.hpp"

#define CLINE 64

class TCPMailbox : public AbstractMailbox {
 private:
    typedef tbb::concurrent_unordered_map<int, zmq::socket_t *> socket_map;
    typedef vector<zmq::socket_t *> socket_vector;

    // The communication over zeromq, a socket library.
    zmq::context_t context;
    socket_vector receivers_;
    socket_map senders_;

    Node & my_node_;
    Node & master_;
    Config * config_;

    pthread_spinlock_t *locks;

    // each thread uses a round-robin strategy to check its physical-queues
    struct scheduler_t {
        // round-robin
        uint64_t rr_cnt;  // choosing local or remote
    } __attribute__((aligned(CLINE)));
    scheduler_t *schedulers;
    pthread_spinlock_t *recv_locks_ = nullptr;

    ThreadSafeQueue<Message>** local_msgs;

    // round-robin size for choosing local or remote msg
    // TODO(nick): Move to config
    int rr_size;

    inline int port_code (int nid, int tid) { return nid * config_->global_num_threads + tid; }

    // between worker & master: support tcp_trx_table_stub part
    // master part
    socket_vector trx_master_senders_;  // send replies to workers
    zmq::socket_t * trx_master_receiver_;  // receive requests from workers
    // worker part
    zmq::socket_t * trx_worker_sender_;  //
    zmq::socket_t * trx_worker_receiver_;  //

 public:
    TCPMailbox(Node & my_node, Node & master) : my_node_(my_node), master_(master), context(1) {
        config_ = Config::GetInstance();
    }

    ~TCPMailbox();

    void Init(vector<Node> & nodes) override;
    int Send(int tid, const Message & msg) override;
    void Recv(int tid, Message & msg) override;
    bool TryRecv(int tid, Message & msg) override;
    void Sweep(int tid) override;
    void SendNotification(int dst_nid, ibinstream& in) override;
    void RecvNotification(obinstream& out) override;
};
