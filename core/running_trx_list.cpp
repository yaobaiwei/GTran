/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "running_trx_list.hpp"

void MinBTCLine::SetBT(uint64_t bt) {
    uint64_t tmp_data[8] __attribute__((aligned(64)));
    tmp_data[0] = tmp_data[6] = bt;
    tmp_data[1] = tmp_data[7] = bt + 1;
    memcpy(data, tmp_data, 64);
}

bool MinBTCLine::GetMinBT(uint64_t& bt) {
    if (data[0] == data[6] && data[1] == data[7] && data[0] + 1 == data[1]) {
        bt = data[0];
        return true;
    }

    return false;
}

RunningTrxList::RunningTrxList() {
    pthread_spin_init(&lock_, 0);
}

void RunningTrxList::AttachRDMAMem(char* mem) {
     mem_ = (MinBTCLine*)mem;
     mem_->SetBT(min_bt_);
}

void RunningTrxList::InsertTrx(uint64_t bt) {
    Node* node = new Node(bt);

    pthread_spin_lock(&lock_);
    if (head_ == nullptr) {
        head_ = tail_ = node;
        UpdateMinBT(bt);
    } else {
        tail_->right = node;
        node->left = tail_;
        tail_ = node;
    }

    list_node_map_[bt] = node;
    max_bt_ = bt;
    pthread_spin_unlock(&lock_);
}

void RunningTrxList::EraseTrx(uint64_t bt) {
    pthread_spin_lock(&lock_);
    auto it = list_node_map_.find(bt);
    // server: /home/cghuan/data/sublime_oltp/oltp/core/running_trx_list.cpp:54: void RunningTrxList::EraseTrx(uint64_t): Assertion `it != list_node_map_.end()' failed.
    assert(it != list_node_map_.end());

    Node* node = it->second;

    if (node->left == nullptr && node->right == nullptr) {
        // only one node in this list
        head_ = tail_ = nullptr;
        UpdateMinBT(max_bt_);
    } else if (node->left == nullptr) {
        head_ = node->right;
        node->right->left = nullptr;
        UpdateMinBT(node->right->bt);
    } else if (node->right == nullptr) {
        tail_ = tail_->left;
        node->left->right = nullptr;
    } else {
        node->left->right = node->right;
        node->right->left = node->left;
    }
    list_node_map_.erase(it);
    pthread_spin_unlock(&lock_);

    delete node;
}

// not thread-safe
// call this in locked region
std::string RunningTrxList::PrintList() const {
    if (head_ == nullptr)
        return "{empty}";

    Node* node = head_;
    std::string ret = "{";
    while (node != nullptr) {
        ret += std::to_string(node->bt) + "->";
        node = node->right;
    }
    ret += "}";

    return ret;
}

// not thread-safe
// call this in locked region
void RunningTrxList::UpdateMinBT(uint64_t bt) {
    if (min_bt_ == bt)
        return;

    assert(min_bt_ < bt);

    min_bt_ = bt;

    if (mem_ != nullptr) {
        mem_->SetBT(min_bt_);
    }
}
