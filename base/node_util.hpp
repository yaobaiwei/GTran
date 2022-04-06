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
