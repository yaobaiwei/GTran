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

enum class ReplyType {NOTIFY_ABORT, RESULT_NORMAL, RESULT_ABORT};

struct reply {
    uint64_t qid;
    vector<value_t> results;
    ReplyType reply_type;
};

class ResultCollector {
 public:
    void Register(uint64_t qid) {
        lock_guard<mutex> lck(m_mutex_);
        qid_list_.insert(qid);
    }

    void Deregister(uint64_t qid) {
        lock_guard<mutex> lck(m_mutex_);
        auto it = qid_list_.find(qid);
        if (it == qid_list_.end()) {
            CHECK(false) << "ERROR: Impossible branch in ResultCollector::Deregister!\n";
        }
        qid_list_.erase(it);
    }

    void NotifyAbort(uint64_t qid) {
        reply re;
        re.qid = qid;
        re.reply_type = ReplyType::NOTIFY_ABORT;
        reply_queue_.Push(move(re));
    }

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
    mutex m_mutex_;
    unordered_set<uint64_t> qid_list_;
    ThreadSafeQueue<reply> reply_queue_;
};

#endif /* RESULT_COLLECTOR_HPP_ */
