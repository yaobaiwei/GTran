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
#include "core/RCT.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/tcp_mailbox.hpp"
#include "core/transactions_table.hpp"
#include "tbb/atomic.h"
#include "utils/config.hpp"
#include "utils/distributed_clock.hpp"

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

    void GetInstancesFromWorker(TransactionTable* trx_table, AbstractMailbox* mailbox,
                                RCTable* rct, const vector<Node>& workers
                                ) {
        trx_table_ = trx_table;
        mailbox_ = mailbox;
        rct_ = rct;
        workers_ = workers;
    }

    // Create tcp sockets
    void PrepareSockets();

    int GetWorkerFromTrxID(const uint64_t& trx_id);

    // Wait until DistributedClock have finished calibration
    void WaitForDistributedClockInit();

    //// Threads:
    // Obtains the timestamp
    void ProcessObtainingTimestamp();
    // Performs calibration of global clocl
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
    uint64_t next_trx_id_;
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

    // queues in Worker
    ThreadSafeQueue<TimestampRequest>* pending_timestamp_request_;
    ThreadSafeQueue<AllocatedTimestamp>* pending_allocated_timestamp_;
    ThreadSafeQueue<UpdateTrxStatusReq>* pending_trx_updates_;
    ThreadSafeQueue<ReadTrxStatusReq>* pending_trx_reads_;
    ThreadSafeQueue<QueryRCTRequest>* pending_rct_query_request_;

    TransactionTable* trx_table_;
    AbstractMailbox* mailbox_;
    RCTable* rct_;

    vector<Node> workers_;
    zmq::context_t context_;
    zmq::socket_t* trx_read_recv_socket_;
    vector<zmq::socket_t*> trx_read_rep_sockets_;

    // Only called in PerformCalibration
    void WriteTimestampToWorker(int worker_id, uint64_t ts, uint64_t tag);
    uint64_t ReadTimestampFromRDMAMem(uint64_t tag);

    Coordinator();
    Coordinator(const Coordinator&);  // not to def
    Coordinator& operator=(const Coordinator&);  // not to def
    ~Coordinator() {}

    inline int socket_code(int n_id, int t_id) {
        return config_ -> global_num_threads * n_id + t_id;
    }
};
