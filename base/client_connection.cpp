/*
 * client_connection.cpp
 *
 *  Created on: Jun 27, 2018
 *      Author: Hongzhi Chen
 */

#include "base/client_connection.hpp"

#include <iostream>
#include <stdio.h>
#include <string.h>

ClientConnection::~ClientConnection(){
	for(int i = 0 ; i < senders_.size(); i++){
		delete senders_[i];
	}
}

void ClientConnection::Init(vector<Node> & nodes){
	senders_.resize(nodes.size());
	for(int i = 0 ; i < nodes.size(); i++){
		senders_[i] = new zmq::socket_t(context_, ZMQ_REQ);
		char addr[64];
		sprintf(addr, "tcp://%s:%d", nodes[i].hostname.c_str(), nodes[i].tcp_port);
		senders_[i]->connect(addr);
	}
}

void ClientConnection::Send(int nid, ibinstream & m){
	zmq::message_t msg(m.size());
	memcpy((void *)msg.data(), m.get_buf(), m.size());
	senders_[nid]->send(msg);
}

void ClientConnection::Recv(int nid, obinstream & um){
    zmq::message_t msg;
    if (senders_[nid]->recv(&msg) < 0) {
        std::cout << "Client recvs with error " << strerror(errno) << std::endl;
        exit(-1);
    }
    cout << "Client recvs a MSG with Size = " << msg.size() << endl;
    char* buf = new char[msg.size()];
    strncpy(buf, (char *)msg.data(), msg.size());
    um.assign(buf, msg.size(), 0);
}
