/*
 * node_util.hpp
 *
 *  Created on: Jun 28, 2018
 *      Author: Hongzhi Chen
 */

#ifndef NODE_UTIL_HPP_
#define NODE_UTIL_HPP_

#include <vector>
#include <string>

#include "base/node.hpp"

#include "glog/logging.h"

std::vector<Node> ParseFile(const std::string& filename);

Node GetNodeById(const std::vector<Node>& nodes, int id);

bool CheckUniquePort(const std::vector<Node>& nodes);

/*
 * Return true if id is in nodes, false otherwise
 */
bool HasNode(const std::vector<Node>& nodes, uint32_t id);

#endif /* NODE_UTIL_HPP_ */
