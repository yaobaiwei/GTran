/*
 * client_connection.hpp
 *
 *  Created on: Jun 27, 2018
 *      Author: Hongzhi Chen
 */

#ifndef CLIENT_CONNECTION_HPP_
#define CLIENT_CONNECTION_HPP_


#include <vector>
#include "base/node.hpp"
#include "base/serialization.hpp"
#include "utils/zmq.hpp"


class ClientConnection {
public:
	~ClientConnection();
	void Init(vector<Node> & nodes);
	void Send(int nid, ibinstream & m);
	void Recv(int nid, obinstream & um);

private:
	zmq::context_t context_;
	vector<zmq::socket_t *> senders_;
};

#endif /* ZMQ_COMMUN_HPP_ */
