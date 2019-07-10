/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include "layout/gc_task.hpp"

class GCWroker {
 private:
    GCWroker() {}
    GCWroker(const GCWroker&);
    ~GCWroker() {}

 public:
    static GCWroker* GetInstance() {
        static GCWroker worker;
        return &worker;
    }

    // Filled by GCScanner, popped by GCWorker
    tbb::concurrent_queue<GCJob*> unfinished_job_queue_;

    void Work();

    void DoJob(GCJob*);

    // Filled by GCWorker when DoJob finished, popped by GCScanner in ScanFinishedTasks
    tbb::concurrent_queue<GCJob*> finished_job_queue_;
};
