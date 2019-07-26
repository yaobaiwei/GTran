/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include "base/node.hpp"
#include "base/thread_safe_queue.hpp"
#include "base/type.hpp"
#include "base/cpuinfo_util.hpp"
#include "utils/mpi_timestamper.hpp"

struct TimestampRequest {
    TimestampRequest() : trx_id(0) {}
    TimestampRequest(uint64_t _trx_id, bool _is_ct) :
                     trx_id(_trx_id), is_ct(_is_ct) {}
    uint64_t trx_id;
    bool is_ct;
};

struct AllocatedTimestamp {
    AllocatedTimestamp() : trx_id(0) {}
    AllocatedTimestamp(uint64_t _trx_id, bool _is_ct, uint64_t _timestamp) :
                       trx_id(_trx_id), is_ct(_is_ct), timestamp(_timestamp) {}
    uint64_t trx_id;
    bool is_ct;
    uint64_t timestamp;
};

class Coordinator {
 public:
    void RegisterTrx(uint64_t& trx_id);

    static Coordinator* GetInstance() {
        static Coordinator single_instance;
        return &single_instance;
    }

    void Init(Node* node, ThreadSafeQueue<TimestampRequest>* pending_timestamp_request,
              ThreadSafeQueue<AllocatedTimestamp>* pending_allocated_timestamp);

    int GetWorkerFromTrxID(const uint64_t& trx_id);

    void WaitForMPITimestamperInit();
    void ProcessObtainingTimestamp();

 private:
    uint64_t next_trx_id_;
    Node* node_;
    int comm_sz_, my_rank_;  // in node_->local_comm
    MPITimestamper* timestamper_;
    atomic<bool> mpi_timestamper_initialized_;

    // queues in Worker
    ThreadSafeQueue<TimestampRequest>* pending_timestamp_request_;
    ThreadSafeQueue<AllocatedTimestamp>* pending_allocated_timestamp_;

    Coordinator();
    Coordinator(const Coordinator&);  // not to def
    Coordinator& operator=(const Coordinator&);  // not to def
    ~Coordinator() {}
};
