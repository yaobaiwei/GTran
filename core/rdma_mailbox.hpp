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

class RdmaMailbox : public AbstractMailbox {
public:
    RdmaMailbox(Node & node, Config* config, AbstractIdMapper* id_mapper, Buffer * buffer) :
        node_(node), config_(config), id_mapper_(id_mapper), buffer_(buffer) {
    	scheduler_ = 0;
    }

    virtual ~RdmaMailbox() {}

    void Init(vector<Node> & nodes);

    // When sent to the same recv buffer, the consistency relies on
    // the lock in the id_mapper
    int Send(const int t_id, const Message & msg) override;

    Message Recv() override ;

    bool TryRecv(Message & msg) override;

private:
    Node & node_;
    Config* config_;
    AbstractIdMapper* id_mapper_;
    Buffer * buffer_;

    // round-robin scheduler
    uint64_t scheduler_;

    // lock for recv 
    std::mutex recv_mu_;
};
  
