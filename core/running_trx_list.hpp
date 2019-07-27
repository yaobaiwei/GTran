/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <assert.h>
#include <memory.h>
#include <pthread.h>

#include <string>
#include <unordered_map>

#include "base/communication.hpp"
#include "base/node.hpp"
#include "tbb/atomic.h"

// Containing a list of running transactions, from which we can get the minimum BT of them.
class RunningTrxList {
 public:
    // Only one thread will perform modification
    void InsertTrx(uint64_t bt);
    void EraseTrx(uint64_t bt);
    void UpdateMinBT(uint64_t bt);

    uint64_t GetMinBT() const {return min_bt_;}  // debug
    std::string PrintList() const;

    static RunningTrxList* GetInstance() {
        static RunningTrxList instance;
        return &instance;
    }

    void Init(const Node& node);

    void ProcessReadMinBTRequest();

    uint64_t GetGlobalMinBT();

 private:
    struct ListNode {
        explicit ListNode(uint64_t _bt) : bt(_bt), left(nullptr), right(nullptr) {}
        ListNode *left, *right;
        uint64_t bt;
    };

    Node node_;

    tbb::atomic<uint64_t> min_bt_ = 0;
    uint64_t max_bt_ = 0;

    std::unordered_map<uint64_t, ListNode*> list_node_map_;

    ListNode* head_ = nullptr;
    ListNode* tail_ = nullptr;

    mutable pthread_spinlock_t lock_;

    RunningTrxList();
    RunningTrxList(const RunningTrxList&);  // not to def
    RunningTrxList& operator=(const RunningTrxList&);  // not to def
    ~RunningTrxList() {}
};
