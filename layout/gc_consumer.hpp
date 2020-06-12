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

#include <thread>

#include "core/RCT.hpp"
#include "core/running_trx_list.hpp"
#include "core/transaction_status_table.hpp"
#include "layout/gc_task.hpp"
#include "layout/index_store.hpp"
#include "utils/config.hpp"
#include "utils/tid_pool_manager.hpp"

class GarbageCollector;

class GCConsumer {
 public:
    static GCConsumer* GetInstance() {
        static GCConsumer worker;
        return &worker;
    }

    void Init();
    void Stop();

    // Each thread as GCConsumer will use Execute()
    // to try pop Jobs from GarbageCollector
    void Execute();

 private:
    GCConsumer();
    GCConsumer(const GCConsumer&);
    ~GCConsumer() {}

    vector<thread> consumer_thread_pool_;

    Config * config_;
    DataStorage * data_storage_;
    GarbageCollector * garbage_collector_;
    TidPoolManager * tid_pool_manager_;
    IndexStore * index_store_;
    RunningTrxList * running_trx_list_;
    RCTable * rct_table_;
    TransactionStatusTable * trx_table_;

    // Sleep period for GCConsumer to rest
    // (unit : us)
    const int POP_PERIOD = 5000;

    // ===========Execute Function for each Job===========
    void ExecuteEraseVJob(EraseVJob*);
    void ExecuteEraseOutEJob(EraseOutEJob*);
    void ExecuteEraseInEJob(EraseInEJob*);

    void ExecuteVMVCCGCJob(VMVCCGCJob*);
    void ExecuteVPMVCCGCJob(VPMVCCGCJob*);
    void ExecuteEPMVCCGCJob(EPMVCCGCJob*);
    void ExecuteEMVCCGCJob(EMVCCGCJob*);

    void ExecuteTopoRowListGCJob(TopoRowListGCJob*);
    void ExecuteTopoRowListDefragJob(TopoRowListDefragJob*);

    void ExecuteVPRowListGCJob(VPRowListGCJob*);
    void ExecuteVPRowListDefragJob(VPRowListDefragJob*);

    void ExecuteEPRowListGCJob(EPRowListGCJob*);
    void ExecuteEPRowListDefragJob(EPRowListDefragJob*);

    void ExecuteTopoIndexGCJob(TopoIndexGCJob*);
    void ExecutePropIndexGCJob(PropIndexGCJob*);

    void ExecuteRCTGCJob(RCTGCJob*);
    void ExecuteTrxStatusTableGCJob(TrxStatusTableGCJob*);
};
