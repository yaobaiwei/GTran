// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
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

#include <tbb/concurrent_queue.h>

#include "layout/gc_task.hpp"
#include "utils/config.hpp"

class GCProducer;
class GCConsumer;

// GarbageCollector is used to init GCProducer and
// GCConsumer. It also maintains the task queue for
// transferring task between GCProducer and GCConsumer
class GarbageCollector {
 public:
    static GarbageCollector* GetInstance() {
        static GarbageCollector gc;
        return &gc;
    }

    void Init();
    void Stop();

    void PushJobToPendingQueue(IndependentGCJob*);
    void PushJobToPendingQueue(DependentGCJob*);
    void PushJobToFinishedQueue(AbstractGCJob*);
    bool PopJobFromPendingQueue(AbstractGCJob*&);
    bool PopJobFromFinishedQueue(AbstractGCJob*&);

    void PushGCAbleEidToQueue(vector<pair<eid_t, bool>>*);
    bool PopGCAbleEidFromQueue(vector<pair<eid_t, bool>>*&);
    void PushGCAbleVidToQueue(vid_t);
    bool PopGCAbleVidFromQueue(vid_t&);

    // Used in StatusExpert
    string GetDepGCTaskStatusStatistics();

 private:
    GCProducer * gc_producer_;
    GCConsumer * gc_consumer_;
    GarbageCollector();

    Config * config_;

    tbb::concurrent_queue<AbstractGCJob*> pending_job_queue;
    tbb::concurrent_queue<AbstractGCJob*> finished_job_queue;

    // For TopoRowListGCTask and TopoRowListDefragTask, both of them
    // must executed before erase_e_map. Therefore, consumer need to
    // return gcable eid to produce to create erase_e_map task
    tbb::concurrent_queue<vector<pair<eid_t, bool>>*> gcable_eid_queue;
    tbb::concurrent_queue<vid_t> gcable_vid_queue;

    // the pointer of job instances in GCProducer
    DependentGCJob* producer_jobs_[(int)DepGCTaskType::COUNT];

    friend class GCProducer;
    friend class GCConsumer;
};
