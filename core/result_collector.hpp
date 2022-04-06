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

#ifndef RESULT_COLLECTOR_HPP_
#define RESULT_COLLECTOR_HPP_

#include <ext/hash_map>
#include <algorithm>
#include <iostream>
#include <list>
#include <vector>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_set>
#include <utility>

#include "base/type.hpp"
#include "base/thread_safe_queue.hpp"

using __gnu_cxx::hash_map;

enum class ReplyType {RESULT_NORMAL, RESULT_ABORT};

struct reply {
    uint64_t qid;
    vector<value_t> results;
    ReplyType reply_type;
};

class ResultCollector {
 public:
    void InsertResult(uint64_t qid, vector<value_t> & data) {
        reply re;
        re.results = move(data);
        re.qid = qid;
        re.reply_type = ReplyType::RESULT_NORMAL;
        reply_queue_.Push(move(re));
    }

    void InsertAbortResult(uint64_t qid, vector<value_t> & data) {
        reply re;
        re.results = move(data);
        re.qid = qid;
        re.reply_type = ReplyType::RESULT_ABORT;
        reply_queue_.Push(move(re));
    }

    void Pop(reply & result) {
        reply_queue_.WaitAndPop(result);
    }

 private:
    ThreadSafeQueue<reply> reply_queue_;
};

#endif /* RESULT_COLLECTOR_HPP_ */
