/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#include "layout/garbage_collector.hpp"
#include "layout/gc_producer.hpp"
#include "layout/gc_consumer.hpp"

GarbageCollector::GarbageCollector() {
    gc_producer_ = GCProducer::GetInstance();
    gc_consumer_ = GCConsumer::GetInstance();
    config_ = Config::GetInstance();
}

void GarbageCollector::Init() {
    if (config_->global_enable_garbage_collect) {
        gc_producer_->Init();
        gc_consumer_->Init();
    }
}

void GarbageCollector::Stop() {
    if (config_->global_enable_garbage_collect) {
        gc_producer_->Stop();
        gc_consumer_->Stop();
    }
}

void GarbageCollector::PushJobToPendingQueue(AbstractGCJob* job_ptr) {
    pending_job_queue.push(job_ptr);
}

void GarbageCollector::PushJobToFinishedQueue(AbstractGCJob* job_ptr) {
    finished_job_queue.push(job_ptr);
}

bool GarbageCollector::PopJobFromPendingQueue(AbstractGCJob*& job) {
    return pending_job_queue.try_pop(job);
}

bool GarbageCollector::PopJobFromFinishedQueue(AbstractGCJob*& job) {
    return finished_job_queue.try_pop(job);
}

void GarbageCollector::PushGCAbleEidToQueue(vector<pair<eid_t, bool>>* vec_p) {
    gcable_eid_queue.push(vec_p);
}

bool GarbageCollector::PopGCAbleEidFromQueue(vector<pair<eid_t, bool>>*& vec_p) {
    return gcable_eid_queue.try_pop(vec_p);
}
