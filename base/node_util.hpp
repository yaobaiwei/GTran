/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#ifndef BASE_NODE_UTIL_HPP_
#define BASE_NODE_UTIL_HPP_

#include <vector>
#include <string>

#include "base/node.hpp"
#include "glog/logging.h"

std::vector<Node> ParseFile(const std::string& filename);

Node & GetNodeById(std::vector<Node>& nodes, int id);

bool CheckUniquePort(std::vector<Node>& nodes);

/*
 * Return true if id is in nodes, false otherwise
 */
bool HasNode(std::vector<Node>& nodes, uint32_t id);

#endif  // BASE_NODE_UTIL_HPP_
