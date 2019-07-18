/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <assert.h>
#include <memory.h>
#include <pthread.h>

#include <string>
#include <unordered_map>

#include "tbb/atomic.h"

// A cache line (64B)
struct MinBTCLine {
    uint64_t data[8] __attribute__((aligned(64)));

    // called by Master
    void SetBT(uint64_t bt);
    // called by Worker
    bool GetMinBT(uint64_t& bt);
} __attribute__((aligned(64)));

// Containing a list of running transactions, from which we can get the minimum BT of them.
class RunningTrxList {
 public:
    // Only one thread will perform modification
    void InsertTrx(uint64_t bt);
    void EraseTrx(uint64_t bt);
    void UpdateMinBT(uint64_t bt);  // Also need to update RDMA mem
    void AttachRDMAMem(char* mem);

    uint64_t GetMinBT() const {return min_bt_;}  // debug
    std::string PrintList() const;

    static RunningTrxList* GetInstance() {
        static RunningTrxList console_single_instance;
        return &console_single_instance;
    }

 private:
    struct Node {
        explicit Node(uint64_t _bt) : bt(_bt), left(nullptr), right(nullptr) {}
        Node *left, *right;
        uint64_t bt;
    };

    tbb::atomic<uint64_t> min_bt_ = 0;
    MinBTCLine* mem_ = nullptr;  // attached RDMA memory to be RDMARead by workers
    uint64_t max_bt_ = 0;

    std::unordered_map<uint64_t, Node*> list_node_map_;

    Node* head_ = nullptr;
    Node* tail_ = nullptr;

    mutable pthread_spinlock_t lock_;

    RunningTrxList();
    RunningTrxList(const RunningTrxList&);  // not to def
    RunningTrxList& operator=(const RunningTrxList&);  // not to def
    ~RunningTrxList() {}
};
