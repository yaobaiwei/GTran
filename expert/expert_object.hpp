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
#include "utils/tool.hpp"
#include "base/type.hpp"

class Expert_Object {
 public:
    // type
    EXPERT_T expert_type;

    // parameters
    vector<value_t> params;

    // index of next expert
    int next_expert;
    // index of current expert(Identifier in transaction), for validation
    int index;

    // flag for sending data to remote nodes
    bool send_remote;

    Expert_Object() : next_expert(-1), send_remote(false) {}
    explicit Expert_Object(EXPERT_T type) : expert_type(type), next_expert(-1), send_remote(false) {}

    void AddParam(int key);
    bool AddParam(string s);
    bool ModifyParam(int key, int index);
    bool ModifyParam(string s, int index);
    bool IsBarrier() const;

    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const Expert_Object& msg);

obinstream& operator>>(obinstream& m, Expert_Object& msg);
