/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#ifndef GLOBAL_HPP_
#define GLOBAL_HPP_

#include <mpi.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stddef.h>
#include <string.h>
#include <iostream>
#include <string>
#include <utility>

#include "tbb/concurrent_queue.h"

#include "base/node.hpp"
#include "base/thread_safe_queue.hpp"

enum MSG {
    START = 0,
    TERMINATE = 1,
    REPORT = 2,
    DONE = 3
};

struct ReadWriteRecord {
    uint64_t trxid;
    bool isRead;  // 0 for write_set, 1 for read_set
    int size;


    void record(uint64_t trxid_, int size_, bool isRead_) {
        trxid = trxid_;
        size = size_;
        isRead = isRead_;
    }

    string DebugString() {
        return to_string(trxid) + "\t" + (isRead ? "0" : "1") + "\t" + to_string(size) + "\n";
    }
};

#define MASTER_RANK 0

const int COMMUN_CHANNEL = 200;
const int MONITOR_CHANNEL = 201;
const int MSCOMMUN_CHANNEL = 202;
const int MINBT_REQUEST_CHANNEL = 203;
const int MINBT_REPLY_CHANNEL = 204;
const int COMMUN_TIME = 1;

// ============================
extern tbb::concurrent_queue<ReadWriteRecord> RW_SET_RECORD_QUEUE;
void PushToRWRecord(uint64_t trxid, int size, bool is_read);

// ============================

void InitMPIComm(int* argc, char*** argv, Node & node);
void worker_finalize(Node & node);
void worker_barrier(Node & node);
void node_finalize();
void node_barrier();

// ============================

void mk_dir(const char *dir);
void rm_dir(string path);
void check_dir(string path, bool force_write);
// =========================================================

#endif /* GLOBAL_HPP_ */
