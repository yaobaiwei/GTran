/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <assert.h>
#include <memory.h>
#include <pthread.h>

#include <chrono>
#include <string>
#include <thread>
#include <unordered_map>

#include "base/cpuinfo_util.hpp"
#include "base/node.hpp"
#include "base/thread_safe_queue.hpp"
#include "base/type.hpp"
#include "base/rdma.hpp"
#include "core/buffer.hpp"
#include "tbb/atomic.h"
#include "utils/config.hpp"
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

// A cache line (64B)
// Used for RDMA write, with tag for checking
struct Uint64CLineWithTag {
    volatile uint64_t data[8] __attribute__((aligned(64)));

    // called by Master
    void SetValue(uint64_t val, uint64_t tag);
    // called by Worker
    bool GetValue(uint64_t& val, uint64_t tag);
} __attribute__((aligned(64)));

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

    void PerformCalibration();

 private:
    uint64_t next_trx_id_;
    Node* node_;
    int comm_sz_, my_rank_;  // in node_->local_comm
    MPITimestamper* timestamper_;
    atomic<bool> mpi_timestamper_initialized_;

    Config* config_;

    char* rdma_mem_;
    Uint64CLineWithTag* ts_cline_;  // the same as rdma_mem_
    uint64_t rdma_mem_offset_;

    // queues in Worker
    ThreadSafeQueue<TimestampRequest>* pending_timestamp_request_;
    ThreadSafeQueue<AllocatedTimestamp>* pending_allocated_timestamp_;

    void WriteTimestampToWorker(int worker_id, uint64_t ts, uint64_t tag);
    uint64_t ReadTimestampFromRDMAMem(uint64_t tag);


    Coordinator();
    Coordinator(const Coordinator&);  // not to def
    Coordinator& operator=(const Coordinator&);  // not to def
    ~Coordinator() {}
};
