/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
         Modified by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tbb/concurrent_queue.h"

#include "base/type.hpp"
#include "layout/data_storage.hpp"
#include "utils/config.hpp"
#include "utils/mymath.hpp"
#include "utils/tid_mapper.hpp"


/* =====================Struct Region============================== */

/**
 * ACTIVE: Task is active and could be executed anytime
 * EMPTY: The task is an empty task generated by its downstream task
 * INVALID: The task is no more valid, should be deleted
 * BLOCKED: Tha task is blocked by some downstream tasks, need to wait downstream task complete
 */
enum class TaskStatus {
    ACTIVE,
    EMPTY,
    INVALID,
    BLOCKED
};

// ID used for edge related dependent task
struct CompoundEPRowListID {
    eid_t eid;
    PropertyRowList<EdgePropertyRow>* ptr;

    CompoundEPRowListID() {}
    CompoundEPRowListID(eid_t eid_, PropertyRowList<EdgePropertyRow>* ptr_) : eid(eid_), ptr(ptr_) {}

    vid_t GetAttachedVid() {
        return eid.out_v;  // src_v
    }

    bool operator== (const CompoundEPRowListID& right_id) const {
        return (eid == right_id.eid && ptr == right_id.ptr);
    }
};

struct CompoundEPRowListIDHash {
    size_t operator()(const CompoundEPRowListID& _r) const {
        return mymath::hash_u64(_r.eid.value() ^ (uint64_t)_r.ptr);
    }
};

struct VidHash {
    size_t operator()(const vid_t& _r) const {
        return mymath::hash_u64(_r.value());
    }
};

struct EidHash {
    size_t operator()(const eid_t& _r) const {
        return mymath::hash_u64(_r.value());
    }
};

/* =====================Task Region============================== */

/* AbstractGCTask is the parent class for all GC tasks. */
class AbstractGCTask {
 public:
    // Cost for completing the task
    // different cost model for different task
    int cost_ = 1;

    // Task Status to indicate whether the task is
    // active, invalid or other status
    TaskStatus task_status_ = TaskStatus::ACTIVE;

    AbstractGCTask() {}
    AbstractGCTask(int cost) : cost_(cost) {}
};

class IndependentGCTask : public AbstractGCTask {
 public:
    IndependentGCTask() {}
    IndependentGCTask(int cost) : AbstractGCTask(cost) {}
};

/* DependentGCTask is the parent class of GC tasks with dependency.
 * Each dependent task is a vertex in the GCTaskDAG.
 * Thus, DependentGCTask need extra members and functions to support the features as a
 * vertex in the GCTaskDAG.
 */
class DependentGCTask : public AbstractGCTask {
 public:
    DependentGCTask() { task_status_ = TaskStatus::ACTIVE; blocked_count_ = 0; }
    DependentGCTask(int cost) : AbstractGCTask(cost) { task_status_ = TaskStatus::ACTIVE; blocked_count_ = 0; }
    ~DependentGCTask() {}

    // upstream_tasks_ stores the incoming neighbors (parent tasks)
    unordered_set<DependentGCTask*> upstream_tasks_;

    // downstream_tasks_ stores the incoming neighbors (parent tasks)
    unordered_set<DependentGCTask*> downstream_tasks_;

    // How many dependent tasks has been pushed out.
    // Notice that a task can be EMPTY when blocked_count_ > 0
    int blocked_count_;

    // There must be an id for each dependent task which will
    // be used as key in dependent map
};

// Erase a specific vid on vertex_map_
class EraseVTask : public IndependentGCTask {
 public:
    vid_t target;
    EraseVTask(vid_t target_) : target(target_) {}
};

// Erase a specific eid on out_e_map_
class EraseOutETask : public IndependentGCTask {
 public:
    eid_t target;

    EraseOutETask() {}
    EraseOutETask(eid_t& eid) : target(eid) {}
};

// Erase a specific eid on in_e_map_
class EraseInETask : public IndependentGCTask {
 public:
    eid_t target;

    EraseInETask() {}
    EraseInETask(eid_t& eid) : target(eid) {}
};

// Perform GC on V_MVCC
class VMVCCGCTask : public IndependentGCTask {
 public:
    VertexMVCCItem* target;
    VMVCCGCTask(VertexMVCCItem* target_, int cost) : target(target_), IndependentGCTask(cost) {}
};

// Perform GC on VP_MVCC
class VPMVCCGCTask : public IndependentGCTask {
 public:
    VPropertyMVCCItem* target;

    VPMVCCGCTask(VPropertyMVCCItem* target_, int cost) : target(target_), IndependentGCTask(cost) {}
};

// Perform GC on EMVCC
class EMVCCGCTask : public IndependentGCTask {
 public:
    EdgeMVCCItem* target;
    eid_t id;

    EMVCCGCTask(eid_t& id_, EdgeMVCCItem* target_, int cost) : target(target_), id(id_), IndependentGCTask(cost) {}
};

// Perform GC on EP_MVCC
class EPMVCCGCTask : public IndependentGCTask {
 public:
    EPropertyMVCCItem* target;

    EPMVCCGCTask(EPropertyMVCCItem* target_, int cost) : target(target_), IndependentGCTask(cost) {}
};

// Perform GC on IndexStore:TopoIndex
class TopoIndexGCTask : public IndependentGCTask {
 public:
    Element_T element_type;

    // The cost of TopoIndexGC is always 1 which makes sure
    // the task will be pushed out immediately.
    TopoIndexGCTask(Element_T element_type_) : element_type(element_type_), IndependentGCTask(1) {}
};

// Perform GC on IndexStore:PropIndex
class PropIndexGCTask : public IndependentGCTask {
 public:
    Element_T element_type;
    int pid;

    PropIndexGCTask(Element_T element_type_, int pid_, int cost) :
        element_type(element_type_), pid(pid_), IndependentGCTask(cost) {}
};

// Perform GC on RCT
class RCTGCTask : public IndependentGCTask {
 public:
    RCTGCTask(int cost) : IndependentGCTask(cost) {}
};

// Preform GC on TrxStatusTable
class TrxStatusTableGCTask : public IndependentGCTask {
 public:
    TrxStatusTableGCTask(int cost) : IndependentGCTask(cost) {}
};

// Free a TopoRowList
class TopoRowListGCTask : public DependentGCTask {
 public:
    TopologyRowList* target;
    vid_t id;

    TopoRowListGCTask() {}
    TopoRowListGCTask(TopologyRowList* target_, vid_t id_) : target(target_), id(id_) {}
};

// Defrag a TopoRowList
class TopoRowListDefragTask : public DependentGCTask {
 public:
    TopologyRowList* target;
    vid_t id;

    TopoRowListDefragTask(TopologyRowList* target_, vid_t id_, int cost) :
        target(target_), id(id_), DependentGCTask(cost) {}
};

// Free a VPRowList
class VPRowListGCTask : public DependentGCTask {
 public:
    PropertyRowList<VertexPropertyRow>* target;
    vid_t id;

    VPRowListGCTask(PropertyRowList<VertexPropertyRow>* target_, vid_t id_) : target(target_), id(id_) {}
};

// Defrag a VPRowList
class VPRowListDefragTask : public DependentGCTask {
 public:
    PropertyRowList<VertexPropertyRow>* target;
    vid_t id;

    VPRowListDefragTask(vid_t id_, PropertyRowList<VertexPropertyRow>* target_, int cost) :
        id(id_), target(target_), DependentGCTask(cost) {}
};

// Free a EPRowList
class EPRowListGCTask : public DependentGCTask {
 public:
    PropertyRowList<EdgePropertyRow>* target;
    CompoundEPRowListID id;

    EPRowListGCTask() {}
    EPRowListGCTask(eid_t& eid, PropertyRowList<EdgePropertyRow>* target_) : target(target_) {
        id = CompoundEPRowListID(eid, target_);
    }
};

// Defrag a EPRowList
class EPRowListDefragTask : public DependentGCTask {
 public:
    PropertyRowList<EdgePropertyRow>* target;
    CompoundEPRowListID id;

    EPRowListDefragTask(eid_t& eid, PropertyRowList<EdgePropertyRow>* target_, int cost) :
        target(target_), DependentGCTask(cost) {
        id = CompoundEPRowListID(eid, target_);
    }
};

/* =====================Job Region============================== */

class AbstractGCJob {
 public:
    // sum of cost of all tasks
    int sum_of_cost_;
    // Threshold for job to push out
    int COST_THRESHOLD;

    JobType job_t_;

    AbstractGCJob() {
        sum_of_cost_ = 0;
        COST_THRESHOLD = 0;
    }

    AbstractGCJob(JobType _job_t, int thres) : job_t_(_job_t), COST_THRESHOLD(thres) {
        sum_of_cost_ = 0;
    }

    // Add Task into Job
    // virtual void AddTask(AbstractGCTask task) = 0;

    // Check whether a Job is ready to be consumed
    virtual bool isReady() = 0;
    virtual void Clear() = 0;
};

class IndependentGCJob : public AbstractGCJob {
 public:
    vector<IndependentGCTask*> tasks_;

    IndependentGCJob() {}
    IndependentGCJob(JobType job_t, int thres) : AbstractGCJob(job_t, thres) {}
    ~IndependentGCJob() { for (auto t : tasks_) delete t; }

    void AddTask(IndependentGCTask* task) {
        tasks_.emplace_back(task);
        sum_of_cost_ += task->cost_;
    }

    void Clear() override {
        sum_of_cost_ = 0;
        tasks_.clear();
    }

    bool isReady() override {
        return sum_of_cost_ >= COST_THRESHOLD;
    }

    bool isEmpty() {
        return tasks_.size() == 0;
    }

    string DebugString() {
        string ret = "#Tasks: " + to_string(tasks_.size());
        ret += " Sum_Of_Cost: " + to_string(sum_of_cost_);
        ret += " CostThres: " + to_string(COST_THRESHOLD);
        return ret;
    }
};

class DependentGCJob : public AbstractGCJob {
 public:
    // sum of blocked tasks
    int sum_blocked_count_;
    vector<DependentGCTask*> tasks_;

    DependentGCJob() {}
    DependentGCJob(JobType job_t, int thres) : AbstractGCJob(job_t, thres) {}

    void Clear() override {
        sum_of_cost_ = 0;
        sum_blocked_count_ = 0;
        tasks_.clear();
    }

    bool isReady() override {
        return ((sum_of_cost_ >= COST_THRESHOLD) && (sum_blocked_count_ == 0));
    }

    bool isEmpty() {
        return tasks_.size() == 0;
    }

    void AddTask(DependentGCTask* task) {
        tasks_.emplace_back(task);
        if (task->task_status_ == TaskStatus::ACTIVE) {
            sum_of_cost_ += task->cost_;
        } else if (task->task_status_ == TaskStatus::BLOCKED) {
            sum_blocked_count_ += 1;
        }
    }

    // Make a task invalid, and GCConsumer will ignore it
    bool SetTaskInvalid(DependentGCTask* target_task) {
        vector<DependentGCTask*>::iterator itr = find(tasks_.begin(), tasks_.end(), target_task);
        if (itr == tasks_.end()) {
            // No such task
            return false;
        }

        target_task->task_status_ = TaskStatus::INVALID;
        sum_of_cost_ -= target_task->cost_;
        CHECK_GE(sum_of_cost_, 0);
        return true;
    }

    // Release blocked task
    bool ReduceTaskBlockCount(DependentGCTask* target_task) {
        if (find(tasks_.begin(), tasks_.end(), target_task) == tasks_.end()) {
            // No such task
            return false;
        }

        if (target_task->blocked_count_ > 0) {
            target_task->blocked_count_--;
        } else {
            return false;
        }

        if (target_task->blocked_count_ == 0) {
            sum_of_cost_ += target_task->cost_;
            target_task->task_status_ = TaskStatus::ACTIVE;
            sum_blocked_count_--;
            CHECK_GE(sum_blocked_count_, 0);
        }
        return true;
    }

    bool taskExists(DependentGCTask* target_task) {
        if (find(tasks_.begin(), tasks_.end(), target_task) == tasks_.end()) {
            return true;
        }
        return false;
    }

    string DebugString() {
        string ret = "#Tasks: " + to_string(tasks_.size());
        ret += " Sum_Of_Cost: " + to_string(sum_of_cost_);
        ret += " Blocked_Count: " + to_string(sum_blocked_count_);
        ret += " CostThres: " + to_string(COST_THRESHOLD);
        return ret;
    }
};

class EraseVJob : public IndependentGCJob {
 public:
    // Currently, threshold for job to be pushed
    // out is hard-coded and maybe put them into
    // config or to be calculated
    EraseVJob() : IndependentGCJob(JobType::EraseV, Config::GetInstance()->Erase_V_Task_THRESHOLD) {}
};

class EraseOutEJob : public IndependentGCJob {
 public:
    EraseOutEJob() : IndependentGCJob(JobType::EraseOutE, Config::GetInstance()->Erase_OUTE_Task_THRESHOLD) {}
};

class EraseInEJob : public IndependentGCJob {
 public:
    EraseInEJob() : IndependentGCJob(JobType::EraseInE, Config::GetInstance()->Erase_INE_Task_THRESHOLD) {}
};

class VMVCCGCJob : public IndependentGCJob {
 public:
    VMVCCGCJob() : IndependentGCJob(JobType::VMVCCGC, Config::GetInstance()->VMVCC_GC_Task_THRESHOLD) {}
};

class VPMVCCGCJob : public IndependentGCJob {
 public:
    VPMVCCGCJob() : IndependentGCJob(JobType::VPMVCCGC, Config::GetInstance()->VPMVCC_GC_Task_THRESHOLD) {}
};

class EPMVCCGCJob : public IndependentGCJob {
 public:
    EPMVCCGCJob() : IndependentGCJob(JobType::EPMVCCGC, Config::GetInstance()->EPMVCC_GC_Task_THRESHOLD) {}
};

class EMVCCGCJob : public IndependentGCJob {
 public:
    EMVCCGCJob() : IndependentGCJob(JobType::EMVCCGC, Config::GetInstance()->EMVCC_GC_Task_THRESHOLD) {}
};

class TopoIndexGCJob : public IndependentGCJob {
 public:
    TopoIndexGCJob() : IndependentGCJob(JobType::TopoIndexGC, Config::GetInstance()->Topo_Index_GC_Task_THRESHOLD) {}
};

class PropIndexGCJob : public IndependentGCJob {
 public:
    PropIndexGCJob() : IndependentGCJob(JobType::PropIndexGC, Config::GetInstance()->Prop_Index_GC_Task_THRESHOLD) {}
};

class RCTGCJob : public IndependentGCJob {
 public:
    RCTGCJob() : IndependentGCJob(JobType::RCTGC, Config::GetInstance()->RCT_GC_Task_THRESHOLD) {}
};

class TrxStatusTableGCJob : public IndependentGCJob {
 public:
    // Make threshold same with RCTGCJob, since we spawn rct_gc_task and trx_status_table_gc_tasks
    // together to avoid scanning status table which is expensive to scan;
    TrxStatusTableGCJob() : IndependentGCJob(JobType::TrxStatusTableGC, Config::GetInstance()->RCT_GC_Task_THRESHOLD) {}
};

class TopoRowListGCJob : public DependentGCJob {
 public:
    TopoRowListGCJob() : DependentGCJob(JobType::TopoRowGC, Config::GetInstance()->Topo_Row_GC_Task_THRESHOLD) {}
};

class TopoRowListDefragJob : public DependentGCJob {
 public:
    TopoRowListDefragJob() : DependentGCJob(JobType::TopoRowDefrag, Config::GetInstance()->Topo_Row_Defrag_Task_THRESHOLD) {}
};

class VPRowListGCJob : public DependentGCJob {
 public:
    VPRowListGCJob() : DependentGCJob(JobType::VPRowGC, Config::GetInstance()->VP_Row_GC_Task_THRESHOLD) {}
};

class VPRowListDefragJob : public DependentGCJob {
 public:
    VPRowListDefragJob() : DependentGCJob(JobType::VPRowDefrag, Config::GetInstance()->VP_Row_Defrag_Task_THRESHOLD) {}
};

class EPRowListGCJob : public DependentGCJob {
 public:
    EPRowListGCJob() : DependentGCJob(JobType::EPRowGC, Config::GetInstance()->EP_Row_GC_Task_THRESHOLD) {}
};

class EPRowListDefragJob : public DependentGCJob {
 public:
    EPRowListDefragJob() : DependentGCJob(JobType::EPRowDefrag, Config::GetInstance()->EP_Row_Defrag_Task_THRESHOLD) {}
};
