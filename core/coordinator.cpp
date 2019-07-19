/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "coordinator.hpp"

Coordinator::Coordinator() {
    next_trx_id_ = 1;
}

void Coordinator::Init(Node* node) {
    node_ = node;
    MPI_Comm_size(node_->local_comm, &comm_sz_);
    MPI_Comm_rank(node_->local_comm, &my_rank_);
}

void Coordinator::RegisterTrx(uint64_t& trx_id) {
    trx_id = 0x8000000000000000 | (((next_trx_id_) * comm_sz_ + my_rank_) << 8);
    next_trx_id_++;
}
