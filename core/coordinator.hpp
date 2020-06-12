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
#include "core/RCT.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/tcp_mailbox.hpp"
#include "core/transaction_status_table.hpp"
#include "tbb/atomic.h"
#include "utils/config.hpp"
#include "utils/distributed_clock.hpp"
#include "utils/tid_pool_manager.hpp"

struct TimestampRequest {
    TimestampRequest() : trx_id(0) {}
    TimestampRequest(uint64_t _trx_id, TIMESTAMP_TYPE _ts_type) :
                     trx_id(_trx_id), ts_type(_ts_type) {}
    uint64_t trx_id;
    TIMESTAMP_TYPE ts_type;
};

struct AllocatedTimestamp {
    AllocatedTimestamp() : trx_id(0) {}
    AllocatedTimestamp(uint64_t _trx_id, TIMESTAMP_TYPE _ts_type, uint64_t _timestamp) :
                       trx_id(_trx_id), ts_type(_ts_type), timestamp(_timestamp) {}
    uint64_t trx_id;
    TIMESTAMP_TYPE ts_type;
    uint64_t timestamp;
};

struct QueryRCTRequest {
    QueryRCTRequest() : trx_id(0) {};
    QueryRCTRequest(int _n_id, uint64_t _trx_id, uint64_t _bt, uint64_t _ct) :
                    n_id(_n_id), trx_id(_trx_id), bt(_bt), ct(_ct) {}
    int n_id;
    uint64_t trx_id, bt, ct;
};

// A cache line (64B)
// Used for RDMA write, with tag for checking
struct Uint64CLineWithTag {
    volatile uint64_t data[8] __attribute__((aligned(64)));

    void SetValue(uint64_t val, uint64_t tag);
    bool GetValue(uint64_t& val, uint64_t tag);
} __attribute__((aligned(64)));

class Coordinator {
 public:
    void RegisterTrx(uint64_t& trx_id);

    static Coordinator* GetInstance() {
        static Coordinator single_instance;
        return &single_instance;
    }

    void Init(Node* node);

    void GetQueuesFromWorker(ThreadSafeQueue<TimestampRequest>* pending_timestamp_request,
                             ThreadSafeQueue<AllocatedTimestamp>* pending_allocated_timestamp,
                             ThreadSafeQueue<UpdateTrxStatusReq>* pending_trx_updates,
                             ThreadSafeQueue<ReadTrxStatusReq>* pending_trx_reads,
                             ThreadSafeQueue<QueryRCTRequest>* pending_rct_query_request
                             ) {
        pending_timestamp_request_ = pending_timestamp_request;
        pending_allocated_timestamp_ = pending_allocated_timestamp;
        pending_trx_updates_ = pending_trx_updates;
        pending_trx_reads_ = pending_trx_reads;
        pending_rct_query_request_ = pending_rct_query_request;
    }

    void GetInstancesFromWorker(TransactionStatusTable* trx_table, AbstractMailbox* mailbox,
                                RCTable* rct, const vector<Node>& nodes
                                ) {
        trx_table_ = trx_table;
        mailbox_ = mailbox;
        rct_ = rct;
        nodes_ = nodes; //delete master in nodes already, but the world_rank starting from 1
    }

    // Create tcp sockets
    void PrepareSockets();

    int GetWorkerFromTrxID(const uint64_t& trx_id);

    // Wait until DistributedClock have finished calibration
    void WaitForDistributedClockInit();

    //// Threads spawned in Worker::Start():
    // Obtains the timestamp
    void ProcessTimestampRequest();
    // Performs calibration of global clock
    void PerformCalibration();
    // Handles RCT query request for remote workers
    void ProcessQueryRCTRequest();
    // Handles TrxTable modification request
    void ProcessTrxTableWriteReqs();
    // For TCP, listens TrxTable reading requests from remote workers
    void ListenTCPTrxReads();
    // For TCP, handle those TrxTable listensing requests from remote workers
    void ProcessTCPTrxReads();

 private:
    atomic<uint64_t> next_trx_id_;
    Node* node_;
    int comm_sz_, my_rank_;  // in node_->local_comm
    DistributedClock* distributed_clock_;

    // Set false in the constructor.
    // After the calibration, it will be set true.
    // WaitForDistributedClockInit will check this variable.
    atomic<bool> distributed_clock_initialized_;

    Config* config_;

    char* rdma_mem_;
    Uint64CLineWithTag* ts_cline_;  // The same pointer as rdma_mem_
    uint64_t rdma_mem_offset_;  // RDMA mem offset used for RDMAWrite

    // Pointer of queues in Worker
    ThreadSafeQueue<TimestampRequest>* pending_timestamp_request_;
    ThreadSafeQueue<AllocatedTimestamp>* pending_allocated_timestamp_;
    ThreadSafeQueue<UpdateTrxStatusReq>* pending_trx_updates_;
    ThreadSafeQueue<ReadTrxStatusReq>* pending_trx_reads_;
    ThreadSafeQueue<QueryRCTRequest>* pending_rct_query_request_;

    TransactionStatusTable* trx_table_;
    AbstractMailbox* mailbox_;
    RCTable* rct_;

    vector<Node> nodes_;
    zmq::context_t context_;
    zmq::socket_t* trx_read_recv_socket_;
    vector<zmq::socket_t*> trx_read_rep_sockets_;

    // For calibration usage. Only called in PerformCalibration
    void WriteTimestampToWorker(int worker_id, uint64_t ts, uint64_t tag);
    uint64_t ReadTimestampFromRDMAMem(uint64_t tag);

    Coordinator();
    Coordinator(const Coordinator&);  // not to def
    Coordinator& operator=(const Coordinator&);  // not to def
    ~Coordinator() {}

    inline int socket_code(int n_id, int t_id) {
        return (config_ -> global_num_threads + 1) * n_id + t_id;
    }
};
