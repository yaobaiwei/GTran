/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include "base/node.hpp"
#include "base/type.hpp"
#include "utils/mpi_timestamper.hpp"

class Coordinator {
 public:
    void RegisterTrx(uint64_t& trx_id);
    void AllocateTimestamp();

    static Coordinator* GetInstance() {
        static Coordinator single_instance;
        return &single_instance;
    }

    void Init(Node* node);

    int GetWorkerFromTrxID(const uint64_t& trx_id);

 private:
    uint64_t next_trx_id_;
    Node* node_;
    int comm_sz_, my_rank_;  // in node_->local_comm
    MPITimestamper* timestamper_;

    Coordinator();
    Coordinator(const Coordinator&);  // not to def
    Coordinator& operator=(const Coordinator&);  // not to def
    ~Coordinator() {}
};
