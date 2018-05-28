/*
 * node.hpp
 *
 *  Created on: May 10, 2018
 *      Author: Hongzhi Chen
 */

#ifndef NODE_HPP_
#define NODE_HPP_

#include <string>
#include <sstream>

using namespace std;

struct Node {
public:
	uint32_t id;
	string hostname;
	int port;

	Node():id(0),port(0){}
	Node(int id_, string hostname_, int port_){
		id = id_;
		hostname = hostname_;
		port = port_;
	}

	std::string DebugString() const {
		std::stringstream ss;
		ss << "Node: { id = " << id << " hostname = " << hostname << " port = " << port << " }";
		return ss.str();
	}

	bool operator==(const Node &other) const {
		return id == other.id && hostname == other.hostname && port == other.port;
	}
};

#endif /* NODE_HPP_ */
