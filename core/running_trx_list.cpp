/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "running_trx_list.hpp"

RunningTrxList::RunningTrxList() {
    pthread_spin_init(&lock_, 0);
}

void RunningTrxList::InsertTrx(uint64_t bt) {
    ListNode* list_node = new ListNode(bt);

    pthread_spin_lock(&lock_);
    if (head_ == nullptr) {
        head_ = tail_ = list_node;
        UpdateMinBT(bt);
    } else {
        tail_->right = list_node;
        list_node->left = tail_;
        tail_ = list_node;
    }

    list_node_map_[bt] = list_node;
    max_bt_ = bt;
    pthread_spin_unlock(&lock_);
}

void RunningTrxList::Init(const Node& node) {
    node_ = node;
}

void RunningTrxList::EraseTrx(uint64_t bt) {
    pthread_spin_lock(&lock_);
    auto it = list_node_map_.find(bt);
    // server: /home/cghuan/data/sublime_oltp/oltp/core/running_trx_list.cpp:54: void RunningTrxList::EraseTrx(uint64_t): Assertion `it != list_node_map_.end()' failed.
    assert(it != list_node_map_.end());

    ListNode* list_node = it->second;

    if (list_node->left == nullptr && list_node->right == nullptr) {
        // only one list_node in this list
        head_ = tail_ = nullptr;
        UpdateMinBT(max_bt_ + 1);
    } else if (list_node->left == nullptr) {
        head_ = list_node->right;
        list_node->right->left = nullptr;
        UpdateMinBT(list_node->right->bt);
    } else if (list_node->right == nullptr) {
        tail_ = tail_->left;
        list_node->left->right = nullptr;
    } else {
        list_node->left->right = list_node->right;
        list_node->right->left = list_node->left;
    }
    list_node_map_.erase(it);
    pthread_spin_unlock(&lock_);

    delete list_node;
}

// not thread-safe
// call this in locked region
std::string RunningTrxList::PrintList() const {
    if (head_ == nullptr)
        return "{empty}";

    ListNode* list_node = head_;
    std::string ret = "{";
    while (list_node != nullptr) {
        ret += std::to_string(list_node->bt) + "->";
        list_node = list_node->right;
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
}

uint64_t RunningTrxList::GetGlobalMinBT() {
    uint64_t ret = 0;
    pthread_spin_lock(&lock_);
    for (int i = 0; i < node_.get_local_size(); i++) {
        uint64_t local_min_bt;
        if (i == node_.get_local_rank()) {
            local_min_bt = GetMinBT();
        } else {
            send_data(node_, node_.get_local_rank(), i, false, MINBT_REQUEST_CHANNEL);
            local_min_bt = recv_data<uint64_t>(node_, i, false, MINBT_REPLY_CHANNEL);
        }
        ret = max(ret, local_min_bt);
    }
    pthread_spin_unlock(&lock_);
    return ret;
}

void RunningTrxList::ProcessReadMinBTRequest() {
    while (1) {
        int n_id = recv_data<int>(node_, MPI_ANY_SOURCE, false, MINBT_REQUEST_CHANNEL);
        uint64_t min_bt = GetMinBT();
        send_data(node_, min_bt, n_id, false, MINBT_REPLY_CHANNEL);
        printf("sending back min_bt: %lu\n", min_bt);
    }
}
