/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "coordinator.hpp"

Coordinator::Coordinator() {
    next_trx_id_ = 1;
    timestamper_ = MPITimestamper::GetInstance();
    mpi_timestamper_initialized_ = false;
}

void Coordinator::Init(Node* node, ThreadSafeQueue<TimestampRequest>* pending_timestamp_request,
                       ThreadSafeQueue<AllocatedTimestamp>* pending_allocated_timestamp) {
    node_ = node;
    MPI_Comm_size(node_->local_comm, &comm_sz_);
    MPI_Comm_rank(node_->local_comm, &my_rank_);

    pending_timestamp_request_ = pending_timestamp_request;
    pending_allocated_timestamp_ = pending_allocated_timestamp;
}

void Coordinator::RegisterTrx(uint64_t& trx_id) {
    trx_id = 0x8000000000000000 | (((next_trx_id_) * comm_sz_ + my_rank_) << 8);
    next_trx_id_++;
}

int Coordinator::GetWorkerFromTrxID(const uint64_t& trx_id) {
    return ((trx_id ^ 0x8000000000000000) >> 8) % comm_sz_;
}

void Coordinator::WaitForMPITimestamperInit() {
    while (true) {
        usleep(1000);
        bool initialized = mpi_timestamper_initialized_;
        if (initialized)
            return;
    }
}

void Coordinator::ProcessObtainingTimestamp() {
    MPITimestamper::BindToLogicalCore(CPUInfoUtil::GetInstance()->GetTotalThreadCount() - 1);

    timestamper_->Init(node_->local_comm, 3, 1000);
    // calculate the system error of clock
    timestamper_->GlobalSystemErrorCalculate(1000, 10, 0.2);

    mpi_timestamper_initialized_ = true;

    while (true) {
        TimestampRequest req;
        pending_timestamp_request_->WaitAndPop(req);

        AllocatedTimestamp allocated_ts = AllocatedTimestamp(req.trx_id, req.is_ct, timestamper_->GetTimestamp());
        pending_allocated_timestamp_->Push(allocated_ts);
    }
}
