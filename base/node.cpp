/*
 * node.cpp
 *
 *  Created on: Jun 8, 2018
 *      Author: Hongzhi Chen
 */

#include "base/node.hpp"

std::string Node::DebugString() const {
		std::stringstream ss;
		ss << "Node: { id = " << id << " hostname = " << hostname << " port = " << port << " }";
		return ss.str();
	}

bool Node::operator==(const Node &other) const {
	return id == other.id && hostname == other.hostname && port == other.port;
}

ibinstream& operator<<(ibinstream& m, const Node& node)
{
	m << node.id;
	m << node.hostname;
	m << node.port;
	return m;
}

obinstream& operator>>(obinstream& m, Node& node)
{
	m >> node.id;
	m >> node.hostname;
	m >> node.port;
	return m;
}
