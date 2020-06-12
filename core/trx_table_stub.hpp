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

#include <iostream>
#include "base/communication.hpp"
#include "base/node.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/buffer.hpp"
#include "core/common.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/transaction_status_table.hpp"
#include "glog/logging.h"
#include "utils/config.hpp"
#include "utils/tid_pool_manager.hpp"

class TrxTableStub {
 protected:
    AbstractMailbox * mailbox_;
    Config* config_;
    Node node_;
    TransactionStatusTable* trx_table_;
    ThreadSafeQueue<UpdateTrxStatusReq>* pending_trx_updates_;

 public:
    virtual bool Init() = 0;
    virtual bool update_status(uint64_t trx_id, TRX_STAT new_status, bool is_read_only = false) = 0;

    virtual bool read_status(uint64_t trx_id, TRX_STAT& status) = 0;

    // Read ct and trx status. ct = 0 when trx is processing or aborted
    virtual bool read_ct(uint64_t trx_id, TRX_STAT & status, uint64_t & ct) = 0;
};
