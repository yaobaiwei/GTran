/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)

*/

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
