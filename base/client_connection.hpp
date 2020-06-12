// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
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

#ifndef BASE_CLIENT_CONNECTION_HPP_
#define BASE_CLIENT_CONNECTION_HPP_

#include <vector>

#include "base/node.hpp"
#include "base/serialization.hpp"
#include "utils/zmq.hpp"


class ClientConnection {
 public:
    ~ClientConnection();
    void Init(const vector<Node> & nodes);
    void Send(int nid, ibinstream & m);
    void Recv(int nid, obinstream & um);

 private:
    zmq::context_t context_;
    vector<zmq::socket_t *> senders_;
    vector<zmq::socket_t *> receivers_;
};

#endif  // BASE_CLIENT_CONNECTION_HPP_
