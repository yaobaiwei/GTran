/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "running_trx_list.hpp"

RunningTrxList::RunningTrxList() {
    pthread_spin_init(&lock_, 0);
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
    printf("[RunningTrxList] After InsertTrx %lu: %s\n", bt, PrintList().c_str());
    pthread_spin_unlock(&lock_);
}

void RunningTrxList::EraseTrx(uint64_t bt) {
    pthread_spin_lock(&lock_);
    auto it = list_node_map_.find(bt);
    assert(it != list_node_map_.end());

    Node* node = it->second;

    if (node->left == nullptr && node->right == nullptr) {
        // only one node in this list
        head_ = tail_ = nullptr;
        UpdateMinBT(max_bt_);
    } else if (node->left == nullptr) {
        UpdateMinBT(node->right->bt);
    } else if (node->right == nullptr) {
        node->left->right = node->right;
    } else {
        node->left->right = node->right;
        node->right->left = node->left;
    }
    list_node_map_.erase(it);
    printf("[RunningTrxList] After EraseTrx %lu: %s\n", bt, PrintList().c_str());
    pthread_spin_unlock(&lock_);

    delete node;
}

// not thread safe
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

// not thread safe
// call this in locked region
void RunningTrxList::UpdateMinBT(uint64_t bt) {
    if (min_bt_ == bt)
        return;

    assert(min_bt_ < bt);

    printf("[UPDATE MIN_BT] %lu -> %lu\n", min_bt_, bt);
    min_bt_ = bt;
    // TODO(entityless): update min bt in RDMA mem,
    //                   and provide a interface to read it in TrxTableStub
}
