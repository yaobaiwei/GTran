/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji Li (cjli@cse.cuhk.edu.hk)

*/

#include "core/tcp_mailbox.hpp"

TCPMailbox::~TCPMailbox() {
    for (auto &r : receivers_)
        if (r != NULL) delete r;

    for (auto &s : senders_) {
        if (s.second != NULL) {
            delete s.second;
            s.second = NULL;
        }
    }
}

void TCPMailbox::Init(vector<Node> &nodes) {
    if (my_node_.get_world_rank() == MASTER_RANK) {  // Master
        trx_master_receiver_ = new zmq::socket_t(context, ZMQ_PULL);
        char addr[64] = "";
        snprintf(addr, sizeof(addr), "tcp://*:%d",
                 my_node_.tcp_port + 1);  // TODO: check the port
        trx_master_receiver_->bind(addr);
        DLOG(INFO) << "[TCPMailbox::Init] Master bind " << string(addr);

        trx_master_senders_.resize(config_->global_num_workers);

        for (int nid = 0; nid < config_->global_num_workers; nid++) {
            trx_master_senders_[nid] = new zmq::socket_t(context, ZMQ_PUSH);
            char addr[64] = "";
            const Node& r_node = GetNodeById(nodes, nid + 1);  // remote worker node
            snprintf(addr, sizeof(addr), "tcp://%s:%d", r_node.hostname.c_str(),
                r_node.tcp_port + 2 + config_->global_num_threads);  // TODO: check the port
            trx_master_senders_[nid] -> connect(addr);
            DLOG(INFO) << "[TCPMailbox::Init] Master connect to " << string(addr);
        }
    } else {  // Worker
        receivers_.resize(config_->global_num_threads);

        for (int tid = 0; tid < config_->global_num_threads; tid++) {
            receivers_[tid] = new zmq::socket_t(context, ZMQ_PULL);
            char addr[64] = "";
            snprintf(addr, sizeof(addr), "tcp://*:%d",
                     my_node_.tcp_port + 1 + tid);
            receivers_[tid]->bind(addr);
            // DLOG(INFO) << "[TCPMailbox::Init] Worker bind " << string(addr);
        }

        for (int nid = 0; nid < config_->global_num_workers; nid++) {
            Node &r_node = GetNodeById(nodes, nid + 1);

            for (int tid = 0; tid < config_->global_num_threads; tid++) {
                int pcode = port_code(nid, tid);

                senders_[pcode] = new zmq::socket_t(context, ZMQ_PUSH);
                char addr[64] = "";
                snprintf(addr, sizeof(addr), "tcp://%s:%d", r_node.hostname.c_str(),
                         r_node.tcp_port + 1 + tid);
                // FIXME: check return value
                senders_[pcode]->connect(addr);
                DLOG(INFO) << "[TCPMailbox::Init] Worker " << my_node_.hostname << "connect to " << string(addr);
            }
        }

        locks = (pthread_spinlock_t *)malloc(
            sizeof(pthread_spinlock_t) *
            (config_->global_num_threads * config_->global_num_workers));
        for (int n = 0; n < config_->global_num_workers; n++) {
            for (int t = 0; t < config_->global_num_threads; t++)
                pthread_spin_init(&locks[n * config_->global_num_threads + t],
                                  0);
        }

        // tcp_trx
        string master_hostname = master_.hostname;
        trx_worker_sender_ = new zmq::socket_t(context, ZMQ_PUSH);
        char addr[64] = "";
        snprintf(addr, sizeof(addr), "tcp://%s:%d", master_hostname.c_str(),
            master_.tcp_port + 1);
        trx_worker_sender_->connect(addr);

        // receive replies from master
        trx_worker_receiver_ = new zmq::socket_t(context, ZMQ_PULL);
        snprintf(addr, sizeof(addr), "tcp://*:%d", my_node_.tcp_port + 2 + config_->global_num_threads);
        trx_worker_receiver_->bind(addr);
        DLOG(INFO) << "[TCPMailbox::Init] Worker " << my_node_.hostname << "bind " << string(addr);
    }
}

int TCPMailbox::Send(int tid, const Message & msg) {
    int pcode = port_code(msg.meta.recver_nid, msg.meta.recver_tid);

    ibinstream m;
    m << msg;

    zmq::message_t zmq_msg(m.size());
    memcpy((void *)zmq_msg.data(), m.get_buf(), m.size());

    pthread_spin_lock(&locks[pcode]);
    if (senders_.find(pcode) == senders_.end()) {
        cout << "Cannot find dst_node port num" << endl;
        return 0;
    }

    senders_[pcode]->send(zmq_msg, ZMQ_DONTWAIT);
    pthread_spin_unlock(&locks[pcode]);
}

int TCPMailbox::Send(int tid, const mailbox_data_t & data) {
    int pcode = port_code(data.dst_nid, data.dst_tid);

    zmq::message_t zmq_msg(data.stream.size());
    memcpy((void *)zmq_msg.data(), data.stream.get_buf(), data.stream.size());

    pthread_spin_lock(&locks[pcode]);
    if (senders_.find(pcode) == senders_.end()) {
        cout << "Cannot find dst_node port num" << endl;
        return 0;
    }

    senders_[pcode]->send(zmq_msg, ZMQ_DONTWAIT);
    pthread_spin_unlock(&locks[pcode]);
}

bool TCPMailbox::TryRecv(int tid, Message & msg) {
    zmq::message_t zmq_msg;
    obinstream um;

    if (receivers_[tid]->recv(&zmq_msg) < 0) {
        cout << "Node " << my_node_.get_local_rank() << " recvs with error " << strerror(errno) << std::endl;
        return false;
    } else {
        char* buf = new char[zmq_msg.size()];
        memcpy(buf, zmq_msg.data(), zmq_msg.size());
        um.assign(buf, zmq_msg.size(), 0);
        um >> msg;
        return true;
    }
}

// if worker send to master: dst_id should be global_num_workers
void TCPMailbox::Send_Notify(int dst_nid, ibinstream &in) { 
    if (my_node_.get_world_rank() == MASTER_RANK) {  // master sends to worker
        CHECK(dst_nid != config_ -> global_num_workers) << "[TCPMailbox::Send_Notify] wrong worker dst_id";
        zmq::message_t msg(in.size());
        memcpy(reinterpret_cast<void *>(msg.data()), in.get_buf(), in.size());
        trx_master_senders_[dst_nid]->send(msg);
    } else {  // worker sends to master
        CHECK_EQ(dst_nid, config_ -> global_num_workers) << "[TCPMailbox::Send_Notify] wrong master dst_id";
        zmq::message_t msg(in.size());
        memcpy(reinterpret_cast<void *>(msg.data()), in.get_buf(), in.size());
        trx_worker_sender_->send(msg);
    }
    return;
}

void TCPMailbox::Recv_Notify(obinstream &out) {
    zmq::message_t zmq_msg;
    if (my_node_.get_world_rank() == MASTER_RANK) {  // master recvs from worker

        CHECK_GT(trx_master_receiver_->recv(&zmq_msg), 0)
            << "[TCPMailbox::Recv_Notify] master recvs from worker failed";

        char *buf = new char[zmq_msg.size()];
        memcpy(buf, zmq_msg.data(), zmq_msg.size());
        out.assign(buf, zmq_msg.size(), 0);
    } else {  // worker recvs from master
        CHECK_GT(trx_worker_receiver_->recv(&zmq_msg), 0)
            << "[TCPMailbox::Recv_Notify] worker recvs from master failed";

        char *buf = new char[zmq_msg.size()];
        memcpy(buf, zmq_msg.data(), zmq_msg.size());
        out.assign(buf, zmq_msg.size(), 0);
    }
    return;
}

void TCPMailbox::Recv(int tid, Message & msg) { return; }
void TCPMailbox::Sweep(int tid) { return; }