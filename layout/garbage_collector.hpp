/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#pragma once

#include <tbb/concurrent_queue.h>

#include "layout/gc_task.hpp"

class GCProducer;

class GarbageCollector {
 public:
    static GarbageCollector* GetInstance() {
        static GarbageCollector gc;
        return &gc;
    }

    void Init();
    void Stop();

    void PushTaskToPendingQueue(AbstractGCJob*);
    void PushTaskToFinishedQueue(AbstractGCJob*);
    bool PopTaskFromPendingQueue(AbstractGCJob*);
    bool PopTaskFromFinishedQueue(AbstractGCJob*);

 private:
    GCProducer * gc_producer_;
    GarbageCollector();

    tbb::concurrent_queue<AbstractGCJob*> pending_job_queue;
    tbb::concurrent_queue<AbstractGCJob*> finished_job_queue;

    friend class GCProducer;
};
