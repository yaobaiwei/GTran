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

#include "expert/expert_object.hpp"

#include <utility>

void Expert_Object::AddParam(int key) {
    value_t v;
    Tool::str2int(to_string(key), v);
    params.push_back(move(v));
}

bool Expert_Object::AddParam(string s) {
    value_t v;
    string s_value = Tool::trim(s, " ");  // delete all spaces
    if (!Tool::str2value_t(s_value, v)) {
        return false;
    }
    params.push_back(move(v));
    return true;
}

bool Expert_Object::ModifyParam(int key, int index) {
    value_t v;
    Tool::str2int(to_string(key), v);
    if (index < params.size()) {
        params[index] = move(v);
    } else {
        return false;
    }
    return true;
}

bool Expert_Object::ModifyParam(string s, int index) {
    value_t v;
    string s_value = Tool::trim(s, " ");  // delete all spaces
    if (!Tool::str2value_t(s_value, v)) {
        return false;
    }

    if (index < params.size()) {
        params[index] = move(v);
    } else {
        return false;
    }

    return true;
}

bool Expert_Object::IsBarrier() const {
    switch (expert_type) {
      case EXPERT_T::AGGREGATE:
      case EXPERT_T::COUNT:
      case EXPERT_T::CAP:
      case EXPERT_T::GROUP:
      case EXPERT_T::DEDUP:
      case EXPERT_T::MATH:
      case EXPERT_T::ORDER:
      case EXPERT_T::RANGE:
      case EXPERT_T::COIN:
      case EXPERT_T::END:
      case EXPERT_T::POSTVALIDATION:
        return true;
      default:
        return false;
    }
}

string Expert_Object::DebugString() const {
    string s = "Experttype: " + string(ExpertType[static_cast<int>(expert_type)]);
    s += ", params: ";
    for (auto v : params) {
        s += v.DebugString() + " ";
    }
    s += ", NextExpert: " + to_string(next_expert);
    s += ", Remote: ";
    s += send_remote ? "Yes" : "No";
    return s;
}

ibinstream& operator<<(ibinstream& m, const Expert_Object& obj) {
    m << obj.expert_type;
    m << obj.index;
    m << obj.next_expert;
    m << obj.send_remote;
    m << obj.params;
    return m;
}

obinstream& operator>>(obinstream& m, Expert_Object& obj) {
    m >> obj.expert_type;
    m >> obj.index;
    m >> obj.next_expert;
    m >> obj.send_remote;
    m >> obj.params;
    return m;
}
