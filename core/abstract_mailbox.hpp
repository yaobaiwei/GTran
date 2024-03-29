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

#pragma once

#include <string>
#include <vector>
#include "core/message.hpp"
#include "base/node.hpp"

class AbstractMailbox {
 public:
    virtual ~AbstractMailbox() {}

    virtual void Init(vector<Node> & nodes) = 0;
    virtual int Send(int tid, const Message & msg) = 0;
    virtual bool TryRecv(int tid, Message & msg) = 0;
    virtual void Recv(int tid, Message & msg) = 0;
    virtual void Sweep(int tid) = 0;
    virtual void SendNotification(int dst_nid, ibinstream& in) = 0;
    virtual void RecvNotification(obinstream& out) = 0;
};
