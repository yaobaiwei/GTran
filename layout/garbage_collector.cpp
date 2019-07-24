/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#include "layout/garbage_collector.hpp"
#include "layout/gc_producer.hpp"

GarbageCollector::GarbageCollector() {
    gc_producer_ = GCProducer::GetInstance();
}

void GarbageCollector::Init() {
    gc_producer_->Init(); 
}

void GarbageCollector::Stop() {
    gc_producer_->Stop();
}

void GarbageCollector::PushTaskToPendingQueue(AbstractGCJob* job_ptr) {
    pending_job_queue.push(job_ptr);
}

void GarbageCollector::PushTaskToFinishedQueue(AbstractGCJob* job_ptr) {
    finished_job_queue.push(job_ptr);
}

bool GarbageCollector::PopTaskFromPendingQueue(AbstractGCJob* job) {
    return pending_job_queue.try_pop(job);
}

bool GarbageCollector::PopTaskFromFinishedQueue(AbstractGCJob* job) {
    return finished_job_queue.try_pop(job);
}
