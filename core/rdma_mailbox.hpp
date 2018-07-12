/*
 * rdma_mailbox.hpp
 *
 *  Created on: May 12, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include <vector>
#include <string>
#include <mutex>
#include <emmintrin.h>

#include "core/buffer.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/abstract_id_mapper.hpp"
#include "base/node.hpp"
#include "base/rdma.hpp"
#include "base/serialization.hpp"
#include "utils/config.hpp"
#include "utils/global.hpp"

#include "glog/logging.h"

#define CLINE 64

class RdmaMailbox : public AbstractMailbox {
public:
    RdmaMailbox(Node & node, Config* config, Buffer * buffer) :
        node_(node), config_(config), buffer_(buffer) {
    }

    virtual ~RdmaMailbox() {}

    void Init(vector<Node> & nodes);

    // When sent to the same recv buffer, the consistency relies on
    // the lock in the id_mapper
    int Send(int tid, const Message & msg) override;

    void Recv(int tid, Message & msg) override ;

    bool TryRecv(int tid, Message & msg) override;

private:
    struct rbf_rmeta_t {
         uint64_t tail; // write from here
         pthread_spinlock_t lock;
     } __attribute__ ((aligned (CLINE)));

     struct rbf_lmeta_t {
         uint64_t head; // read from here
         pthread_spinlock_t lock;
     } __attribute__ ((aligned (CLINE)));

     // each thread uses a round-robin strategy to check its physical-queues
     struct scheduler_t {
         uint64_t rr_cnt; // round-robin
     } __attribute__ ((aligned (CLINE)));

     bool CheckRecvBuf(int tid, int nid);
     void FetchMsgFromRecvBuf(int tid, int nid, obinstream & um);

     mutex mu_;
     Node & node_;
     Config* config_;
     Buffer * buffer_;

     rbf_rmeta_t *rmetas = NULL;
     rbf_lmeta_t *lmetas = NULL;
     scheduler_t *schedulers;
};
  
