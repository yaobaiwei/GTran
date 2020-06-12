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

#include "running_trx_list.hpp"

void Uint64CLine::SetValue(uint64_t val) {
    uint64_t tmp_data[8] __attribute__((aligned(64)));
    tmp_data[0] = tmp_data[6] = val;
    tmp_data[1] = tmp_data[7] = val + 1;
    memcpy(data, tmp_data, 64);
}

bool Uint64CLine::GetValue(uint64_t& val) const {
    uint64_t tmp_data[8] __attribute__((aligned(64)));
    memcpy(tmp_data, data, 64);

    if (tmp_data[0] == tmp_data[6] && tmp_data[1] == tmp_data[7] && tmp_data[0] + 1 == tmp_data[1]) {
        val = tmp_data[0];
        return true;
    }

    return false;
}

RunningTrxList::RunningTrxList() {
    pthread_spin_init(&lock_, 0);
    config_ = Config::GetInstance();

    if (config_->global_use_rdma) {
        Buffer* buf = Buffer::GetInstance();
        rdma_mem_ = buf->GetMinBTBuf();
    }
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
    if (config_->global_use_rdma){
        for (int i = 0; i < node_.get_local_size(); i++){
            char* my_buff_addr = rdma_mem_ + i * sizeof(Uint64CLine);
            Uint64CLine* my_min_bt = (Uint64CLine*)my_buff_addr;
            my_min_bt->SetValue(0);
        }
    }
}

void RunningTrxList::EraseTrx(uint64_t bt) {
    pthread_spin_lock(&lock_);
    auto it = list_node_map_.find(bt);
    CHECK(it != list_node_map_.end());

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

// called by Worker::ProcessAllocatedTimestamp()
void RunningTrxList::UpdateMinBT(uint64_t bt) {
    if (min_bt_ == bt)
        return;

    CHECK(min_bt_ < bt);

    min_bt_ = bt;

    if (config_->global_use_rdma) {
        // copy to local rdma mem
        char* my_buff_addr = rdma_mem_ + node_.get_local_rank() * sizeof(Uint64CLine);
        Uint64CLine* my_min_bt = (Uint64CLine*)my_buff_addr;

        my_min_bt->SetValue(bt);
        // write to remote
        uint64_t off = config_->min_bt_buffer_offset + node_.get_local_rank() * sizeof(Uint64CLine);

        int t_id = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);
        RDMA &rdma = RDMA::get_rdma();
        for (int i = 0; i < node_.get_local_size(); i++) {
            if (i != node_.get_local_rank()) {
                rdma.dev->RdmaWrite(t_id, i, my_buff_addr, sizeof(Uint64CLine), off);
            }
        }
    }
}

uint64_t RunningTrxList::UpdateGlobalMinBT() {
    uint64_t global_min_bt = 0;
    if (config_->global_use_rdma) {
        // The remote workers will update rdma_mem_ with their MIN_BT via RDMA
        // Thus, from rdma_mem_, the global MIN_BT can be obtained.
        for (int i = 0; i < node_.get_local_size(); i++) {
            uint64_t min_bt;
            Uint64CLine* cline = (Uint64CLine*)(rdma_mem_ + i * sizeof(Uint64CLine));
            while (!cline->GetValue(min_bt));
            global_min_bt = max(global_min_bt, min_bt);
        }
    } else {
        pthread_spin_lock(&lock_);
        for (int i = 0; i < node_.get_local_size(); i++) {
            uint64_t min_bt;
            if (i == node_.get_local_rank()) {
                min_bt = GetMinBT();
            } else {
                // request remote MIN_BT
                send_data(node_, node_.get_local_rank(), i, false, MINBT_REQUEST_CHANNEL);
                min_bt = recv_data<uint64_t>(node_, i, false, MINBT_REPLY_CHANNEL);
            }
            global_min_bt = max(global_min_bt, min_bt);
        }
        pthread_spin_unlock(&lock_);
    }
    global_min_bt_ = global_min_bt;
}

uint64_t RunningTrxList::GetGlobalMinBT() {
    return global_min_bt_;
}

void RunningTrxList::ProcessReadMinBTRequest() {
    if (config_->global_use_rdma)
        return;
    //mapping with the code branch of !config_->global_use_rdma in UpdateGlobalMinBT
    while (1) {
        int n_id = recv_data<int>(node_, MPI_ANY_SOURCE, false, MINBT_REQUEST_CHANNEL);
        uint64_t min_bt = GetMinBT();
        // send local MIN_BT to the remote worker
        send_data(node_, min_bt, n_id, false, MINBT_REPLY_CHANNEL);
    }
}
