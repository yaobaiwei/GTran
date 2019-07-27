/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#pragma once

#include <iostream>
#include "base/communication.hpp"
#include "base/node.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/buffer.hpp"
#include "core/common.hpp"
#include "core/rdma_mailbox.hpp"
#include "glog/logging.h"
#include "utils/config.hpp"
#include "utils/tid_mapper.hpp"

class TrxTableStub {
 protected:
    AbstractMailbox * mailbox_;
    Config* config_;
    Node node_;

 public:
    virtual bool Init() = 0;
    virtual bool update_status(uint64_t trx_id, TRX_STAT new_status, bool is_read_only = false) = 0;

    virtual bool read_status(uint64_t trx_id, TRX_STAT& status) = 0;

    // Read ct and trx status. ct = 0 when trx is processing or aborted
    virtual bool read_ct(uint64_t trx_id, TRX_STAT & status, uint64_t & ct) = 0;
};
