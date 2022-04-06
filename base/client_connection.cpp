// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
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

#include <stdio.h>
#include <string.h>

#include <iostream>

#include "utils/global.hpp"
#include "base/client_connection.hpp"

ClientConnection::~ClientConnection() {
    for (int i = 0 ; i < senders_.size(); i++) {
        delete senders_[i];
    }
    for (int i = 0 ; i < receivers_.size(); i++) {
        delete receivers_[i];
    }
}

void ClientConnection::Init(const vector<Node> & nodes) {
    senders_.resize(nodes.size());
    for (int i = 0 ; i < nodes.size(); i++) {
        if (i == MASTER_RANK) {
            senders_[i] = new zmq::socket_t(context_, ZMQ_REQ);
        } else {
            senders_[i] = new zmq::socket_t(context_, ZMQ_PUSH);
        }
        char addr[64];
        snprintf(addr, sizeof(addr), "tcp://%s:%d", nodes[i].hostname.c_str(), nodes[i].tcp_port);
        senders_[i]->connect(addr);
    }

    receivers_.resize(nodes.size()-1);
    for (int i = 0 ; i < nodes.size()-1; i++) {
        receivers_[i] = new zmq::socket_t(context_, ZMQ_PULL);
        char addr[64];
        snprintf(addr, sizeof(addr), "tcp://*:%d", nodes[i+1].tcp_port + i + 1);
        receivers_[i]->bind(addr);
    }
}

void ClientConnection::Send(int nid, ibinstream & m) {
    zmq::message_t msg(m.size());
    memcpy(reinterpret_cast<void *>(msg.data()), m.get_buf(), m.size());
    senders_[nid]->send(msg);
}

void ClientConnection::Recv(int nid, obinstream & um) {
    zmq::message_t msg;
    if (nid == MASTER_RANK) {
        if (senders_[nid]->recv(&msg) < 0) {
            std::cout << "Client recvs with error " << strerror(errno) << std::endl;
            exit(-1);
        }
    } else {
        if (receivers_[nid - 1]->recv(&msg) < 0) {
            std::cout << "Client recvs with error " << strerror(errno) << std::endl;
            exit(-1);
        }
    }

    char* buf = new char[msg.size()];
    memcpy(buf, msg.data(), msg.size());
    um.assign(buf, msg.size(), 0);
}
