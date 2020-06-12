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

#pragma once

#include <assert.h>
#include <memory.h>
#include <pthread.h>

#include <string>
#include <unordered_map>

#include "base/communication.hpp"
#include "base/node.hpp"
#include "base/rdma.hpp"
#include "core/buffer.hpp"
#include "tbb/atomic.h"
#include "utils/config.hpp"
#include "utils/tid_pool_manager.hpp"

// A cache line (64B)
// Used for RDMA write
struct Uint64CLine {
    volatile uint64_t data[8] __attribute__((aligned(64)));

    // called by Master
    void SetValue(uint64_t val);
    // called by Worker
    bool GetValue(uint64_t& val) const;
} __attribute__((aligned(64)));

// Containing a list of running transactions, from which we can get the minimum BT of them.
class RunningTrxList {
 public:
    // Only one thread will perform modification
    void InsertTrx(uint64_t bt);
    void EraseTrx(uint64_t bt);

    uint64_t GetMinBT() const {return min_bt_;}
    std::string PrintList() const;

    static RunningTrxList* GetInstance() {
        static RunningTrxList instance;
        return &instance;
    }

    void Init(const Node& node);

    // For non-rdma implementation, a thread should be standing by
    // to send local MIN_BT to remote workers.
    void ProcessReadMinBTRequest();

    // Called by the GC thread.
    uint64_t UpdateGlobalMinBT();  // update global_min_bt_
    uint64_t GetGlobalMinBT();  // simply read global_min_bt_

 private:
    struct ListNode {
        explicit ListNode(uint64_t _bt) : bt(_bt), left(nullptr), right(nullptr) {}
        ListNode *left, *right;
        uint64_t bt;
    };

    void UpdateMinBT(uint64_t bt);

    Node node_;
    Config* config_;

    tbb::atomic<uint64_t> min_bt_ = 0;  // the min BT on this worker
    tbb::atomic<uint64_t> global_min_bt_ = 0;  // the global min BT, used by GCProducer: updated in UpdateGlobalMinBT(), read by GetGlobalMinBT()
    uint64_t max_bt_ = 0;

    // Enable fast erasure in the list
    std::unordered_map<uint64_t, ListNode*> list_node_map_;

    ListNode* head_ = nullptr;
    ListNode* tail_ = nullptr;

    mutable pthread_spinlock_t lock_;

    char* rdma_mem_ = nullptr;

    RunningTrxList();
    RunningTrxList(const RunningTrxList&);  // not to def
    RunningTrxList& operator=(const RunningTrxList&);  // not to def
    ~RunningTrxList() {}
};
