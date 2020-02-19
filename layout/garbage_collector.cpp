/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#include "layout/garbage_collector.hpp"
#include "layout/gc_producer.hpp"
#include "layout/gc_consumer.hpp"

const unordered_map<TaskStatus, string, EnumClassHash<TaskStatus>> task_status_string_map = {
    {TaskStatus::ACTIVE, "ACTIVE"},
    {TaskStatus::EMPTY, "EMPTY"},
    {TaskStatus::INVALID, "INVALID"},
    {TaskStatus::BLOCKED, "BLOCKED"},
    {TaskStatus::PUSHED, "PUSHED"},
    {TaskStatus::COUNT, "FINISHED"},
};
const unordered_map<DepGCTaskType, string, EnumClassHash<DepGCTaskType>> dep_gc_task_type_string_map {
    {DepGCTaskType::TOPO_ROW_LIST_GC, "TOPO_ROW_LIST_GC"},
    {DepGCTaskType::TOPO_ROW_LIST_DEFRAG, "TOPO_ROW_LIST_DEFRAG"},
    {DepGCTaskType::VP_ROW_LIST_GC, "VP_ROW_LIST_GC"},
    {DepGCTaskType::VP_ROW_LIST_DEFRAG, "VP_ROW_LIST_DEFRAG"},
    {DepGCTaskType::EP_ROW_LIST_GC, "EP_ROW_LIST_GC"},
    {DepGCTaskType::EP_ROW_LIST_DEFRAG, "EP_ROW_LIST_DEFRAG"},
};

/*
Since tbb::concurrent_hash_map cannot take an array as the value field, we need to pack an array with a struct.
For each task type, a TaskStatusStatisticsArray instance is created to store the count of tasks in different status.
For example, TaskStatusStatisticsArray[(int)TaskStatus::INVALID] indicates the current INVALID tasks' count.
Specifically, TaskStatusStatisticsArray[(int)TaskStatus::COUNT] indicates the finished tasks' count.
*/
struct TaskStatusStatisticsArray {
    int value[(int)TaskStatus::COUNT + 1];
    int& operator[] (int i) {return value[i];}
};

typedef tbb::concurrent_hash_map<DepGCTaskType, TaskStatusStatisticsArray> TaskStatisticsMap;
static TaskStatisticsMap task_statistics_map;

// increase or decrease the task status counter for a specific type of task
void IncreaseTaskStatusCounter(DepGCTaskType type, TaskStatus status) {
    TaskStatisticsMap::accessor ac;
    task_statistics_map.find(ac, type);

    ac->second[(int)status]++;
}

void DecreaseTaskStatusCounter(DepGCTaskType type, TaskStatus status) {
    TaskStatisticsMap::accessor ac;
    task_statistics_map.find(ac, type);

    ac->second[(int)status]--;
}


GarbageCollector::GarbageCollector() {
    gc_producer_ = GCProducer::GetInstance();
    gc_consumer_ = GCConsumer::GetInstance();
    config_ = Config::GetInstance();
}

void GarbageCollector::Init() {
    if (config_->global_enable_garbage_collect) {
        for (int i = 0; i < (int)DepGCTaskType::COUNT; i++) {
            TaskStatisticsMap::accessor ac;
            task_statistics_map.insert(ac, (DepGCTaskType)i);
            for (int j = 0; j < (int)TaskStatus::COUNT + 1; j++) {
                ac->second[j] = 0;
            }
        }

        gc_producer_->Init();
        gc_consumer_->Init();

        producer_jobs_[(int)DepGCTaskType::TOPO_ROW_LIST_GC] = &gc_producer_->topo_row_list_gc_job;
        producer_jobs_[(int)DepGCTaskType::TOPO_ROW_LIST_DEFRAG] = &gc_producer_->topo_row_list_defrag_job;
        producer_jobs_[(int)DepGCTaskType::VP_ROW_LIST_GC] = &gc_producer_->vp_row_list_gc_job;
        producer_jobs_[(int)DepGCTaskType::VP_ROW_LIST_DEFRAG] = &gc_producer_->vp_row_list_defrag_job;
        producer_jobs_[(int)DepGCTaskType::EP_ROW_LIST_GC] = &gc_producer_->ep_row_list_gc_job;
        producer_jobs_[(int)DepGCTaskType::EP_ROW_LIST_DEFRAG] = &gc_producer_->ep_row_list_defrag_job;
    }
}

void GarbageCollector::Stop() {
    if (config_->global_enable_garbage_collect) {
        gc_producer_->Stop();
        gc_consumer_->Stop();
    }
}

void GarbageCollector::PushJobToPendingQueue(DependentGCJob* job_ptr) {
    for (auto* task : job_ptr->tasks_) {
        if (task->GetTaskStatus() == TaskStatus::ACTIVE) {
            task->SetTaskStatus(TaskStatus::PUSHED);
        } else {
            CHECK(task->GetTaskStatus() == TaskStatus::INVALID) << task->GetTaskInfoStr();
        }
    }
    pending_job_queue.push(job_ptr);
}

void GarbageCollector::PushJobToPendingQueue(IndependentGCJob* job_ptr) {
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

void GarbageCollector::PushGCAbleVidToQueue(vid_t vid) {
    gcable_vid_queue.push(vid);
}

bool GarbageCollector::PopGCAbleVidFromQueue(vid_t& vid) {
    return gcable_vid_queue.try_pop(vid);
}

string GarbageCollector::GetDepGCTaskStatusStatistics() {
    string ret;
    for (int i = 0; i < (int)DepGCTaskType::COUNT; i++) {
        ret += dep_gc_task_type_string_map.at((DepGCTaskType)i) + ":\tsum_blocked_count_ = " + to_string(producer_jobs_[i]->sum_blocked_count_)
        + ", job->tasks_.size() = " + to_string(producer_jobs_[i]->tasks_.size()) + ", {";

        TaskStatisticsMap::accessor ac;
        task_statistics_map.find(ac, (DepGCTaskType)i);
        for (int j = 0; j < (int)TaskStatus::COUNT + 1; j++) {
            ret += task_status_string_map.at((TaskStatus)j) + ": " + to_string(ac->second[j]) + ",  ";
        }
        ret += "}\n";
    }
    return ret;
}
