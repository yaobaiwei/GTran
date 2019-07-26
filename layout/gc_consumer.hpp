/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
         Modified by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#pragma once

#include <thread>

#include "layout/gc_task.hpp"
#include "utils/config.hpp"
#include "utils/tid_mapper.hpp"

class GarbageCollector;

class GCConsumer {
 public:
    static GCConsumer* GetInstance() {
        static GCConsumer worker;
        return &worker;
    }

    void Init();
    void Stop();

    void Execute(int tid);

 private:
    GCConsumer();
    GCConsumer(const GCConsumer&);
    ~GCConsumer() {}

    vector<thread> consumer_thread_pool_;

    Config * config_;
    DataStorage * data_storage_;
    GarbageCollector * garbage_collector_;
    TidMapper * tid_mapper_;

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
};
