/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#ifndef CLIENT_CONNECTION_HPP_
#define CLIENT_CONNECTION_HPP_

#include "utils/zmq.hpp"

#include <vector>
#include "base/node.hpp"
#include "base/serialization.hpp"


class ClientConnection {
public:
	~ClientConnection();
	void Init(vector<Node> & nodes);
	void Send(int nid, ibinstream & m);
	void Recv(int nid, obinstream & um);

private:
	zmq::context_t context_;
	vector<zmq::socket_t *> senders_;
	vector<zmq::socket_t *> receivers_;
};

#endif /* ZMQ_COMMUN_HPP_ */
