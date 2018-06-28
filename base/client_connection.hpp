/*
 * client_connection.hpp
 *
 *  Created on: Jun 27, 2018
 *      Author: Hongzhi Chen
 */

#ifndef CLIENT_CONNECTION_HPP_
#define CLIENT_CONNECTION_HPP_


#include <vector>
#include <unordered_map>
#include "base/node.hpp"
#include "utils/zmq.hpp"

class ClientConnection {
public:
	ClientConnection(const Node & master, vector<Node> & nodes):master_(master),nodes_(nodes){};

private:
	Node & master_;
	vector<Node> & nodes_;
	zmq::socket_t receiver;
	unordered_map<int, zmq::socket_t *> senders;
};

#endif /* ZMQ_COMMUN_HPP_ */
