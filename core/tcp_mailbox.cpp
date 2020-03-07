/* Copyright 2019 Husky Data Lab, CUHK
 *
 * Authors: Created by Changji Li (cjli@cse.cuhk.edu.hk)
 *          Modified by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 *          Modified by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
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

    for (int i = 0; i < config_->global_num_threads; i++) {
        delete local_msgs[i];
    }

    free(schedulers);
    free(local_msgs);
}

void TCPMailbox::Init(vector<Node> &nodes) {
    char addr[64] = "";

    notificaton_receiver_ = new zmq::socket_t(context, ZMQ_PULL);
    snprintf(addr, sizeof(addr), "tcp://*:%d", 
            my_node_->tcp_port + 2 + config_->global_num_threads);
    notificaton_receiver_->bind(addr);
    DLOG(INFO) << "[TCPMailbox::Init] Worker " << my_node_.hostname 
            << "'s Notificaton receiver binds " << string(addr);

    notification_senders_.resize(config_->global_num_workers);
    for (int nid = 0; nid < config_->global_num_workers; nid++) {
        notification_senders_[nid] = new zmq::socket_t(context, ZMQ_PUSH);
        const Node& r_node = GetNodeById(nodes, nid + 1);  // remote worker node
        snprintf(addr, sizeof(addr), "tcp://%s:%d", r_node.ibname.c_str(),
            r_node.tcp_port + 2 + config_->global_num_threads);  // TODO: check the port
        notification_senders_[nid] -> connect(addr);
        DLOG(INFO) << "[TCPMailbox::Init] Worker " << my_node_.hostname << "'s Notificaton senders connect to " << string(addr);
    }



    //The regular senders for threads[0, global_num_workers), by using constant +1 to distinguish with above channels (i.e., +2)
    for (int nid = 0; nid < config_->global_num_workers; nid++) {
        Node &r_node = GetNodeById(nodes, nid + 1);
        string ibname = r_node.ibname;

        for (int tid = 0; tid < config_->global_num_threads; tid++) {
            int pcode = port_code(nid, tid);

            senders_[pcode] = new zmq::socket_t(context, ZMQ_PUSH);
            char addr[64] = "";
            snprintf(addr, sizeof(addr), "tcp://%s:%d", ibname.c_str(),
                     r_node.tcp_port + 1 + tid);
            // FIXME: check return value
            senders_[pcode]->connect(addr);
            DLOG(INFO) << "[TCPMailbox::Init] Worker " << my_node_.hostname << " connect to " << string(addr);
        }
    }

    receivers_.resize(config_->global_num_threads);
    for (int tid = 0; tid < config_->global_num_threads; tid++) {
        receivers_[tid] = new zmq::socket_t(context, ZMQ_PULL);
        char addr[64] = "";
        snprintf(addr, sizeof(addr), "tcp://*:%d",
                 my_node_.tcp_port + 1 + tid);
        receivers_[tid]->bind(addr);
        DLOG(INFO) << "[TCPMailbox::Init] Worker " << my_node_.hostname << " binds " << string(addr);
    }

    locks = (pthread_spinlock_t *)malloc(
        sizeof(pthread_spinlock_t) *
        (config_->global_num_threads * config_->global_num_workers));
    for (int n = 0; n < config_->global_num_workers; n++)
        for (int t = 0; t < config_->global_num_threads; t++)
            pthread_spin_init(&locks[n * config_->global_num_threads + t], 0);

    recv_locks_ = (pthread_spinlock_t *)malloc(sizeof(pthread_spinlock_t) * config_->global_num_threads);
    for (int i = 0; i < config_->global_num_threads; i++) {
        pthread_spin_init(&recv_locks_[i], 0);
    }

    schedulers = (scheduler_t *)malloc(sizeof(scheduler_t) * config_->global_num_threads);
    memset(schedulers, 0, sizeof(scheduler_t) * config_->global_num_threads);

    local_msgs = (ThreadSafeQueue<Message> **)malloc(sizeof(ThreadSafeQueue<Message>*) * config_->global_num_threads);
    for (int i = 0; i < config_->global_num_threads; i++) {
        local_msgs[i] = new ThreadSafeQueue<Message>();
    }
    rr_size = 3;

    pthread_spin_init(&send_notification_lock_, 0);
}

int TCPMailbox::Send(int tid, const Message & msg) {
    if (msg.meta.recver_nid == my_node_.get_local_rank()) {
        local_msgs[msg.meta.recver_tid]->Push(msg);
    } else {
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
}

bool TCPMailbox::TryRecv(int tid, Message & msg) {
    SimpleSpinLockGuard lock_guard(recv_locks_ + tid);
    int type = (schedulers[tid].rr_cnt++) % rr_size;

    // Try local message queue in higher priority
    // Use round-robin to avoid starvation
    if (type != 0) {
        if (local_msgs[tid]->Size() != 0) {
            local_msgs[tid]->WaitAndPop(msg);
            return true;
        }
    }

    // Try tcp recv
    zmq::message_t zmq_msg;
    obinstream um;

    int recv_ret = receivers_[tid]->recv(&zmq_msg, ZMQ_DONTWAIT);

    if (recv_ret <= 0) {
        if (errno == EAGAIN) {
            // Do nothing.
            // Non-blocking mode was requested and no messages are available at the moment.
        } else {
            cout << "Node " << my_node_.get_local_rank() << " recvs with error " << strerror(errno) << std::endl;
        }
    } else {
        char* buf = new char[zmq_msg.size()];
        memcpy(buf, zmq_msg.data(), zmq_msg.size());
        um.assign(buf, zmq_msg.size(), 0);
        um >> msg;
        return true;
    }
    return false;
}

void TCPMailbox::SendNotification(int dst_nid, ibinstream &in) {
    zmq::message_t msg(in.size());
    memcpy(reinterpret_cast<void *>(msg.data()), in.get_buf(), in.size());

    SimpleSpinLockGuard lock_guard(&send_notification_lock_);
    notification_senders_[dst_nid]->send(msg);
}

void TCPMailbox::RecvNotification(obinstream &out) {
    zmq::message_t zmq_msg;
    CHECK_GT(notificaton_receiver_->recv(&zmq_msg), 0)
        << "[TCPMailbox::RecvNotification] worker recvs failed";

    char *buf = new char[zmq_msg.size()];
    memcpy(buf, zmq_msg.data(), zmq_msg.size());
    out.assign(buf, zmq_msg.size(), 0);
}

void TCPMailbox::Recv(int tid, Message & msg) { return; }
void TCPMailbox::Sweep(int tid) { return; }
