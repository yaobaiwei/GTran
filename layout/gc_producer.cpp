/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#include "layout/gc_producer.hpp"
#include "layout/garbage_collector.hpp"

GCTaskDAG::GCTaskDAG() {
    gc_producer_ = GCProducer::GetInstance();
    garbage_collector_ = GarbageCollector::GetInstance();
}

VPRowListGCTask* GCTaskDAG::InsertVPRowListGCTask(const vid_t& id, PropertyRowList<VertexPropertyRow>* target) {
    VPRowListGCTask* task = nullptr;

    if (vp_row_list_gc_tasks_map.find(id) != vp_row_list_gc_tasks_map.end()) {
        task = vp_row_list_gc_tasks_map.at(id);
        if (task->GetTaskStatus() != TaskStatus::EMPTY) {
            // already exists, ignore
            return nullptr;
        }

        CHECK(task->GetTaskStatus() == TaskStatus::EMPTY) << task->GetTaskInfoStr();

        CHECK(task->downstream_tasks_.size() == 1) << task->GetTaskInfoStr();
        auto* downstream_task = *task->downstream_tasks_.begin();

        // substantiate the existing EMPTY task
        task->target = target;
        // check its downstream task
        if (downstream_task->GetTaskStatus() == TaskStatus::PUSHED) {
            task->SetTaskStatus(TaskStatus::BLOCKED);
            task->IncreaseBLockedCount();
        } else {
            CHECK(downstream_task->GetTaskStatus() == TaskStatus::ACTIVE) << downstream_task->GetTaskInfoStr();
            task->SetTaskStatus(TaskStatus::ACTIVE);

            // set the ACTIVE downstream task INVALID, and disconnect it from the DAG
            gc_producer_->SetVPRowListDefragTaskInvalid(downstream_task);
            DisconnectInvalidVPRowListDefragTask(downstream_task);
        }
    } else {
        // create a new task
        task = new VPRowListGCTask(id, target);
        task->SetTaskStatus(TaskStatus::ACTIVE);
        vp_row_list_gc_tasks_map.emplace(id, task);
    }

    return task;
}

VPRowListDefragTask* GCTaskDAG::InsertVPRowListDefragTask(const vid_t& id, PropertyRowList<VertexPropertyRow>* target, int gcable_cell_count) {
    if (vp_row_list_defrag_tasks_map.find(id) != vp_row_list_defrag_tasks_map.end()) {
        // already exists, ignore
        return nullptr;
    }

    if (vp_row_list_gc_tasks_map.find(id) != vp_row_list_gc_tasks_map.end()) {
        // upstream task already exists
        return nullptr;
    }

    VPRowListDefragTask* task = new VPRowListDefragTask(id, target, gcable_cell_count);
    task->SetTaskStatus(TaskStatus::ACTIVE);
    vp_row_list_defrag_tasks_map.emplace(id, task);

    VPRowListGCTask* empty_upstream_task = new VPRowListGCTask(id, nullptr);
    empty_upstream_task->SetTaskStatus(TaskStatus::EMPTY);
    vp_row_list_gc_tasks_map.emplace(id, empty_upstream_task);

    task->upstream_tasks_.emplace(empty_upstream_task);
    empty_upstream_task->downstream_tasks_.emplace(task);

    return task;
}

void GCTaskDAG::DeleteVPRowListGCTask(VPRowListGCTask* task) {
    CHECK(vp_row_list_gc_tasks_map.find(task->id) != vp_row_list_gc_tasks_map.end()) << task->GetTaskInfoStr();
    vp_row_list_gc_tasks_map.erase(task->id);
}

void GCTaskDAG::DeleteVPRowListDefragTask(VPRowListDefragTask* task) {
    if (task->GetTaskStatus() == TaskStatus::INVALID)
        return;
    CHECK(task->GetTaskStatus() == TaskStatus::PUSHED) << task->GetTaskInfoStr();

    CHECK(vp_row_list_defrag_tasks_map.find(task->id) != vp_row_list_defrag_tasks_map.end()) << task->GetTaskInfoStr();

    VPRowListGCTask* upstream_task = *task->upstream_tasks_.begin();

    if (upstream_task->GetTaskStatus() == TaskStatus::EMPTY) {
        // delete the empty upstream task as it will no longer have any downstream task
        vp_row_list_gc_tasks_map.erase(task->id);
        delete upstream_task;
    } else {
        CHECK(upstream_task->GetTaskStatus() == TaskStatus::BLOCKED) << upstream_task->GetTaskInfoStr();
        upstream_task->downstream_tasks_.erase(task);

        gc_producer_->ReduceVPRowListGCJobBlockCount(upstream_task);
    }

    vp_row_list_defrag_tasks_map.erase(task->id);
}

void GCTaskDAG::DisconnectInvalidVPRowListDefragTask(VPRowListDefragTask* task) {
    vp_row_list_defrag_tasks_map.erase(task->id);

    VPRowListGCTask* upstream_task = *task->upstream_tasks_.begin();
    task->upstream_tasks_.clear();

    upstream_task->downstream_tasks_.erase(task);

    CHECK(upstream_task->downstream_tasks_.size() == 0) << upstream_task->GetTaskInfoStr();
    CHECK(upstream_task->GetTaskStatus() == TaskStatus::ACTIVE) << upstream_task->GetTaskInfoStr();
}

TopoRowListGCTask* GCTaskDAG::InsertTopoRowListGCTask(const vid_t& id, TopologyRowList* target) {
    TopoRowListGCTask* task = nullptr;
    if (topo_row_list_gc_tasks_map.find(id) != topo_row_list_gc_tasks_map.end()) {
        task = topo_row_list_gc_tasks_map.at(id);
        if (task->GetTaskStatus() != TaskStatus::EMPTY) {
            // already exists, ignore
            return nullptr;
        }

        // substaintiate the EMPTY task
        task->SetTaskStatus(TaskStatus::ACTIVE);
        task->target = target;

        // Copy task->downstream_tasks_, since task->downstream_tasks_ will be modified in the loop
        auto downstream_tasks_snapshot = task->downstream_tasks_;
        for (auto* downstream_task : downstream_tasks_snapshot) {
            if (downstream_task->GetTaskStatus() == TaskStatus::PUSHED) {
                task->IncreaseBLockedCount();
                continue;
            }

            // set the ACTIVE downstream task INVALID, and disconnect it from the DAG
            CHECK(downstream_task->GetTaskStatus() == TaskStatus::ACTIVE) << downstream_task->GetTaskInfoStr();
            if (downstream_task->GetTaskType() == DepGCTaskType::TOPO_ROW_LIST_DEFRAG) {
                gc_producer_->SetTopoRowListDefragTaskInvalid(downstream_task);
                DisconnectInvalidTopoRowListDefragTask(downstream_task);
            } else if (downstream_task->GetTaskType() == DepGCTaskType::EP_ROW_LIST_DEFRAG) {
                gc_producer_->SetEPRowListDefragTaskInvalid(downstream_task);
                DisconnectInvalidEPRowListDefragTask(downstream_task);
            } else {
                CHECK(false) << downstream_task->GetTaskInfoStr();
            }
        }

        if (task->GetBlockedCount() > 0) {
            task->SetTaskStatus(TaskStatus::BLOCKED);
        }
    } else {
        // create a new task
        task = new TopoRowListGCTask(id, target);
        task->SetTaskStatus(TaskStatus::ACTIVE);
        topo_row_list_gc_tasks_map.emplace(id, task);
    }

    return task;
}

TopoRowListDefragTask* GCTaskDAG::InsertTopoRowListDefragTask(const vid_t& id, TopologyRowList* target, int gcable_cell_count) {
    if (topo_row_list_defrag_tasks_map.find(id) != topo_row_list_defrag_tasks_map.end()) {
        // already exists, ignore
        return nullptr;
    }

    TopoRowListDefragTask* task = nullptr;

    if (topo_row_list_gc_tasks_map.find(id) != topo_row_list_gc_tasks_map.end()) {
        // Dependent Task Exists
        TopoRowListGCTask* dep_task = topo_row_list_gc_tasks_map.at(id);
        if (dep_task->GetTaskStatus() == TaskStatus::EMPTY) {
            // An empty task generated by EPRowListDefragTask
            task = new TopoRowListDefragTask(id, target, gcable_cell_count);
            task->SetTaskStatus(TaskStatus::ACTIVE);

            // Record Dependent relationship
            dep_task->downstream_tasks_.emplace(task);
            task->upstream_tasks_.emplace(dep_task);
        } else {
            // Do not generate a task, since the upstream task exists
            return nullptr;
        }
    } else {
        task = new TopoRowListDefragTask(id, target, gcable_cell_count);
        task->SetTaskStatus(TaskStatus::ACTIVE);

        // Create Empty Dep Task
        TopoRowListGCTask* topo_dep_task = new TopoRowListGCTask(id, nullptr);
        topo_dep_task->SetTaskStatus(TaskStatus::EMPTY);

        task->upstream_tasks_.emplace(topo_dep_task);
        topo_dep_task->downstream_tasks_.emplace(task);

        topo_row_list_gc_tasks_map.emplace(id, topo_dep_task);
    }

    topo_row_list_defrag_tasks_map.emplace(id, task);

    return task;
}

EPRowListGCTask* GCTaskDAG::InsertEPRowListGCTask(const CompoundEPRowListID& id, PropertyRowList<EdgePropertyRow>* target) {
    EPRowListGCTask* task = nullptr;

    if (ep_row_list_gc_tasks_map.find(id) != ep_row_list_gc_tasks_map.end()) {
        task = ep_row_list_gc_tasks_map.at(id);
        if (task->GetTaskStatus() != TaskStatus::EMPTY) {
            // already exists, ignore
            return nullptr;
        }

        CHECK(task->GetTaskStatus() == TaskStatus::EMPTY) << task->GetTaskInfoStr();

        EPRowListDefragTask* downstream_task = *task->downstream_tasks_.begin();

        // substantiate the existing EMPTY task
        task->target = target;
        if (downstream_task->GetTaskStatus() == TaskStatus::PUSHED) {
            task->IncreaseBLockedCount();
            task->SetTaskStatus(TaskStatus::BLOCKED);
        } else {
            CHECK(downstream_task->GetTaskStatus() == TaskStatus::ACTIVE) << downstream_task->GetTaskInfoStr();
            task->SetTaskStatus(TaskStatus::ACTIVE);

            // set the ACTIVE downstream task INVALID, and disconnect it from the DAG
            gc_producer_->SetEPRowListDefragTaskInvalid(downstream_task);
            DisconnectInvalidEPRowListDefragTask(downstream_task);
        }
    } else {
        // create a new task
        task = new EPRowListGCTask(id, target);
        task->SetTaskStatus(TaskStatus::ACTIVE);
        ep_row_list_gc_tasks_map.emplace(id, task);
    }

    return task;
}

EPRowListDefragTask* GCTaskDAG::InsertEPRowListDefragTask(const CompoundEPRowListID& id, PropertyRowList<EdgePropertyRow>* target, int gcable_cell_count) {
    if (ep_row_list_defrag_tasks_map.find(id) != ep_row_list_defrag_tasks_map.end()) {
        // already exists, ignore
        return nullptr;
    }

    vid_t src_v = id.GetAttachedVid();
    bool topo_row_dep_exist = (topo_row_list_gc_tasks_map.find(src_v) != topo_row_list_gc_tasks_map.end());
    bool ep_row_dep_exist = (ep_row_list_gc_tasks_map.find(id) != ep_row_list_gc_tasks_map.end());

    // Check depedency existence for ep_row
    if (ep_row_dep_exist) {
        // upstream task already exists
        return nullptr;
    }

    // Check depedency existence for topo_row
    TopoRowListGCTask* topo_dep_task;
    if (topo_row_dep_exist) {
        topo_dep_task = topo_row_list_gc_tasks_map.at(src_v);
        if (topo_dep_task->GetTaskStatus() != TaskStatus::EMPTY) {
            // non-empty upstream task already exists
            return nullptr;
        }
    } else {
        // create a new upstream task (TopoRowListGCTask)
        topo_dep_task = new TopoRowListGCTask(src_v, nullptr);
        topo_dep_task->SetTaskStatus(TaskStatus::EMPTY);
        topo_row_list_gc_tasks_map.emplace(src_v, topo_dep_task);
    }

    EPRowListDefragTask* task = new EPRowListDefragTask(id, target, gcable_cell_count);
    task->SetTaskStatus(TaskStatus::ACTIVE);

    topo_dep_task->downstream_tasks_.emplace(task);
    task->upstream_tasks_.emplace(topo_dep_task);

    // Create Empty EPRowListGC Task
    EPRowListGCTask* ep_dep_task = new EPRowListGCTask(id, nullptr);
    ep_dep_task->SetTaskStatus(TaskStatus::EMPTY);
    ep_row_list_gc_tasks_map.emplace(id, ep_dep_task);

    ep_dep_task->downstream_tasks_.emplace(task);
    task->upstream_tasks_.emplace(ep_dep_task);

    ep_row_list_defrag_tasks_map.emplace(id, task);

    return task;
}

void GCTaskDAG::DeleteTopoRowListGCTask(TopoRowListGCTask* task) {
    CHECK(topo_row_list_gc_tasks_map.find(task->id) != topo_row_list_gc_tasks_map.end()) << task->GetTaskInfoStr();
    topo_row_list_gc_tasks_map.erase(task->id);
}

void GCTaskDAG::DeleteTopoRowListDefragTask(TopoRowListDefragTask* task) {
    if (task->GetTaskStatus() == TaskStatus::INVALID)
        return;
    CHECK(task->GetTaskStatus() == TaskStatus::PUSHED) << task->GetTaskInfoStr();

    CHECK(topo_row_list_defrag_tasks_map.find(task->id) != topo_row_list_defrag_tasks_map.end()) << task->GetTaskInfoStr();

    TopoRowListGCTask* upstream_task = *task->upstream_tasks_.begin();

    upstream_task->downstream_tasks_.erase(task);

    if (upstream_task->GetTaskStatus() == TaskStatus::EMPTY) {
        if (upstream_task->downstream_tasks_.size() == 0) {
            // delete the empty upstream task as it will no longer have any downstream task
            topo_row_list_gc_tasks_map.erase(task->id);
            delete upstream_task;
        }
    } else {
        CHECK(upstream_task->GetTaskStatus() == TaskStatus::BLOCKED) << upstream_task->GetTaskInfoStr();
        gc_producer_->ReduceTopoRowListGCJobBlockCount(upstream_task);
    }

    topo_row_list_defrag_tasks_map.erase(task->id);
}

void GCTaskDAG::DeleteEPRowListGCTask(EPRowListGCTask* task) {
    CHECK(ep_row_list_gc_tasks_map.find(task->id) != ep_row_list_gc_tasks_map.end()) << task->GetTaskInfoStr();
    ep_row_list_gc_tasks_map.erase(task->id);
}

void GCTaskDAG::DeleteEPRowListDefragTask(EPRowListDefragTask* task) {
    if (task->GetTaskStatus() == TaskStatus::INVALID)
        return;
    CHECK(task->GetTaskStatus() == TaskStatus::PUSHED) << task->GetTaskInfoStr();

    CHECK(ep_row_list_defrag_tasks_map.find(task->id) != ep_row_list_defrag_tasks_map.end()) << task->GetTaskInfoStr();

    for (auto* upstream_task : task->upstream_tasks_) {
        upstream_task->downstream_tasks_.erase(task);

        if (upstream_task->GetTaskType() == DepGCTaskType::EP_ROW_LIST_GC) {
            if (upstream_task->GetTaskStatus() == TaskStatus::EMPTY) {
                CHECK(upstream_task->downstream_tasks_.size() == 0) << upstream_task->GetTaskInfoStr();
                // delete the empty upstream task as it will no longer have any downstream task
                ep_row_list_gc_tasks_map.erase(task->id);
                delete upstream_task;
            } else {
                CHECK(upstream_task->GetTaskStatus() == TaskStatus::BLOCKED) << upstream_task->GetTaskInfoStr();

                gc_producer_->ReduceEPRowListGCJobBlockCount(upstream_task);
            }
        } else if (upstream_task->GetTaskType() == DepGCTaskType::TOPO_ROW_LIST_GC) {
            vid_t vid = task->id.GetAttachedVid();
            if (upstream_task->GetTaskStatus() == TaskStatus::EMPTY) {
                if (upstream_task->downstream_tasks_.size() == 0) {
                    // delete the empty upstream task as it will no longer have any downstream task
                    topo_row_list_gc_tasks_map.erase(vid);
                    delete upstream_task;
                }
            } else {
                CHECK(upstream_task->GetTaskStatus() == TaskStatus::BLOCKED) << upstream_task->GetTaskInfoStr();

                gc_producer_->ReduceTopoRowListGCJobBlockCount(upstream_task);
            }
        } else {
            CHECK(false) << upstream_task->GetTaskInfoStr();
        }
    }

    ep_row_list_defrag_tasks_map.erase(task->id);
}

void GCTaskDAG::DisconnectInvalidTopoRowListDefragTask(TopoRowListDefragTask* task) {
    topo_row_list_defrag_tasks_map.erase(task->id);

    TopoRowListGCTask* upstream_task = *task->upstream_tasks_.begin();
    task->upstream_tasks_.clear();

    upstream_task->downstream_tasks_.erase(task);

    if (upstream_task->downstream_tasks_.size() == 0) {
        if (upstream_task->GetTaskStatus() == TaskStatus::EMPTY) {
            // erase and delete the upstream task since it is no longer connected
            topo_row_list_gc_tasks_map.erase(upstream_task->id);
            delete upstream_task;
        } else {
            CHECK(upstream_task->GetTaskStatus() == TaskStatus::ACTIVE) << upstream_task->GetTaskInfoStr();
        }
    }
}

void GCTaskDAG::DisconnectInvalidEPRowListDefragTask(EPRowListDefragTask* task) {
    ep_row_list_defrag_tasks_map.erase(task->id);

    int active_upstream_count = 0;

    for (auto* upstream_task : task->upstream_tasks_) {
        upstream_task->downstream_tasks_.erase(task);

        if (upstream_task->downstream_tasks_.size() == 0) {
            if (upstream_task->GetTaskStatus() == TaskStatus::EMPTY) {
                // erase and delete the upstream task since it is no longer connected
                if (upstream_task->GetTaskType() == DepGCTaskType::TOPO_ROW_LIST_GC) {
                    topo_row_list_gc_tasks_map.erase(static_cast<TopoRowListGCTask*>(upstream_task)->id);
                } else if (upstream_task->GetTaskType() == DepGCTaskType::EP_ROW_LIST_GC) {
                    ep_row_list_gc_tasks_map.erase(static_cast<EPRowListGCTask*>(upstream_task)->id);
                } else {
                    CHECK(false) << upstream_task->GetTaskInfoStr();
                }
                delete upstream_task;
            } else {
                CHECK(upstream_task->GetTaskStatus() == TaskStatus::ACTIVE) << upstream_task->GetTaskInfoStr();
                CHECK(upstream_task->GetTaskType() == DepGCTaskType::TOPO_ROW_LIST_GC || upstream_task->GetTaskType() == DepGCTaskType::EP_ROW_LIST_GC);
                active_upstream_count++;
            }
        }
    }

    // Must be one ACTIVE EPRowListGCTask or TopoRowListGCTask
    CHECK(active_upstream_count == 1) << active_upstream_count << task->GetTaskInfoStr();
    task->upstream_tasks_.clear();
}

void GCProducer::ReduceVPRowListGCJobBlockCount(VPRowListGCTask* task) {
    vp_row_list_gc_job.ReduceTaskBlockCount(task);

    if (vp_row_list_gc_job.isReady()) {
        VPRowListGCJob* new_job = new VPRowListGCJob();
        *new_job = vp_row_list_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        vp_row_list_gc_job.Clear();
    }
}

void GCProducer::ReduceEPRowListGCJobBlockCount(EPRowListGCTask* task) {
    ep_row_list_gc_job.ReduceTaskBlockCount(task);

    if (ep_row_list_gc_job.isReady()) {
        EPRowListGCJob* new_job = new EPRowListGCJob();
        *new_job = ep_row_list_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        ep_row_list_gc_job.Clear();
    }
}

void GCProducer::ReduceTopoRowListGCJobBlockCount(TopoRowListGCTask* task) {
    topo_row_list_gc_job.ReduceTaskBlockCount(task);

    if (topo_row_list_gc_job.isReady()) {
        TopoRowListGCJob* new_job = new TopoRowListGCJob();
        *new_job = topo_row_list_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        topo_row_list_gc_job.Clear();
    }
}

void GCProducer::SetVPRowListDefragTaskInvalid(VPRowListDefragTask* task) {
    vp_row_list_defrag_job.SetTaskInvalid(task);
}

void GCProducer::SetEPRowListDefragTaskInvalid(EPRowListDefragTask* task) {
    ep_row_list_defrag_job.SetTaskInvalid(task);
}

void GCProducer::SetTopoRowListDefragTaskInvalid(TopoRowListDefragTask* task) {
    topo_row_list_defrag_job.SetTaskInvalid(task);
}

void GCProducer::Init() {
    data_storage_ = DataStorage::GetInstance();
    gc_task_dag_ = GCTaskDAG::GetInstance();
    garbage_collector_ = GarbageCollector::GetInstance();
    index_store_ = IndexStore::GetInstance();
    config_ = Config::GetInstance();
    node_ = Node::StaticInstance();
    running_trx_list_ = RunningTrxList::GetInstance();
    rct_table_ = RCTable::GetInstance();

    // Put thread at the end of Init()
    scanner_ = thread(&GCProducer::Execute, this);
}

void GCProducer::Stop() {
    scanner_.join();
}

void GCProducer::Execute() {
    while (true) {
        // Do Scan with DFS for whole DataStorage
        uint64_t start_time = timer::get_usec();
        scan_vertex_map();

        // Scan Index Store
        scan_topo_index_update_region();
        scan_prop_index_update_region();

        // Scan RCT
        scan_rct();

        uint64_t end_time = timer::get_usec();

        cout << "[Node " << node_.get_local_rank() << "][GCProducer] Scan Time: " << ((end_time - start_time) / 1000)
             << "ms, container usage: " << data_storage_->GetContainerUsage() * 100 << "%" << endl;

        // Currently, sleep for a while and the do next scan
        sleep(SCAN_PERIOD);

        // After one round, check whether there are some tasks already done,
        // erase them from dependency dag; and check whether there are some edge
        // related task need to spawn
        check_finished_job();
        check_erasable_eid();
    }
}


void GCProducer::scan_vertex_map() {
    // Scan the vertex map
    ReaderLockGuard reader_lock_guard(data_storage_->vertex_map_erase_rwlock_);
    for (auto v_pair = data_storage_->vertex_map_.begin(); v_pair != data_storage_->vertex_map_.end(); v_pair++) {
        auto& v_item = v_pair->second;

        MVCCList<VertexMVCCItem>* mvcc_list = v_item.mvcc_list;
        if (mvcc_list == nullptr) { continue; }  // the insertion is not finished

        SimpleSpinLockGuard lock_guard(&(mvcc_list->lock_));

        VertexMVCCItem* mvcc_item = mvcc_list->GetHead();
        if (mvcc_item == nullptr) { continue; }  // already marked to be erased

        // Uncommitted new vertex, ignore
        if (mvcc_item->GetTransactionID() != 0) { continue; }

        // VertexMVCCList is different with other MVCCList since it only has at most
        // two versions and the second version must be deleted version
        // Therefore, if the first version is unvisible to any transaction
        // (i.e. version->end_time < MINIMUM_ACTIVE_TRANSACTION_BT), the vertex can be GC.
        vid_t vid;
        uint2vid_t(v_pair->first, vid);

        if (mvcc_item->GetEndTime() < running_trx_list_->GetGlobalMinBT()) {
            // Deleted vertex, GCable
            mvcc_list->head_ = nullptr;
            mvcc_list->tail_ = nullptr;
            mvcc_list->pre_tail_ = nullptr;
            mvcc_list->tmp_pre_tail_ = nullptr;

            spawn_erase_vertex_gctask(vid);
            spawn_v_mvcc_gctask(mvcc_item);
            spawn_vp_row_list_gctask(v_item.vp_row_list, vid);
            spawn_topo_row_list_gctask(v_item.ve_row_list, vid);
        } else {
            // go deeper, to prop first and then topo
            scan_prop_row_list(vid.value(), v_item.vp_row_list);
            scan_topo_row_list(vid, v_item.ve_row_list);
        }
    }
}

void GCProducer::scan_topo_row_list(const vid_t& vid, TopologyRowList* topo_row_list) {
    if (topo_row_list == nullptr) { return; }
    ReaderLockGuard reader_lock_guard(topo_row_list->gc_rwlock_);
    pthread_spin_lock(&(topo_row_list->lock_));
    VertexEdgeRow* row_ptr = topo_row_list->head_;
    if (row_ptr == nullptr) {
        pthread_spin_unlock(&(topo_row_list->lock_));
        return;
    }

    int edge_count_snapshot = topo_row_list->edge_count_;
    pthread_spin_unlock(&(topo_row_list->lock_));

    int gcable_cell_count = 0;
    for (int i = 0; i < edge_count_snapshot; i++) {
        int cell_id_in_row = i % VE_ROW_CELL_COUNT;
        if (i != 0 && cell_id_in_row == 0) {
            CHECK(row_ptr->next_ != nullptr) << topo_row_list->edge_count_;
            row_ptr = row_ptr->next_;
        }

        EdgeHeader* adjacent_edge_header = &row_ptr->cells_[cell_id_in_row];
        eid_t eid;
        construct_edge_id(vid, adjacent_edge_header, eid);

        MVCCList<EdgeMVCCItem>* cur_edge_mvcc_list = adjacent_edge_header->mvcc_list;
        // scan attached mvcc list and count empty cell
        if (scan_mvcc_list(eid.value(), cur_edge_mvcc_list)) {
            gcable_cell_count++;
        }
    }

    if (gcable_cell_count >= edge_count_snapshot % VE_ROW_CELL_COUNT && gcable_cell_count != 0) {
        spawn_topo_row_list_defrag_gctask(topo_row_list, vid, gcable_cell_count);
    }
}

void GCProducer::scan_topo_index_update_region() {
    // Vtx
    {
        ReaderLockGuard vtx_reader_lock_guard(index_store_->vtx_topo_gc_rwlock_);
        double ratio = static_cast<double>(config_->Topo_Index_GC_RATIO) / 100;
        int mergable_update_count = 0;
        for (auto & up_elem : index_store_->vtx_update_list) {
            if (up_elem.ct < running_trx_list_->GetGlobalMinBT()) {
                mergable_update_count++;
            }
        }

        // When mergable update element exceeding a ratio of all data,
        // task spawns
        if (mergable_update_count > index_store_->topo_vtx_data.size() * ratio) {
            spawn_topo_index_gctask(Element_T::VERTEX);
        }
    }

    // Edge
    {
        ReaderLockGuard edge_reader_lock_guard(index_store_->edge_topo_gc_rwlock_);
        double ratio = static_cast<double>(config_->Topo_Index_GC_RATIO) / 100;
        int mergable_update_count = 0;
        for (auto & up_elem : index_store_->edge_update_list) {
            if (up_elem.ct < running_trx_list_->GetGlobalMinBT()) {
                mergable_update_count++;
            }
        }

        // When mergable update element exceeding a ratio of all data,
        // task spawns
        if (mergable_update_count > index_store_->topo_edge_data.size() * ratio) {
            spawn_topo_index_gctask(Element_T::EDGE);
        }
    }
}

void GCProducer::scan_prop_index_update_region() {
    {
        ReaderLockGuard vp_reader_lock_guard(index_store_->vtx_prop_gc_rwlock_);
        double ratio = static_cast<double>(config_->Prop_Index_GC_RATIO) / 100;
        for (auto & pair : index_store_->vtx_prop_index) {
            // PropIndexGCTask spawned for each pid
            IndexStore::prop_up_map_const_accessor pcac;
            if (index_store_->vp_update_map.find(pcac, pair.first)) {
                int up_elem_counter = 0;
                for (auto & update_pair : pcac->second) {
                    up_elem_counter += update_pair.second.size();
                }

                // When mergable update element exceeding a ratio of all data,
                // task spawns
                if (up_elem_counter > pair.second.total * ratio) {
                    spawn_prop_index_gctask(Element_T::VERTEX, pair.first, up_elem_counter);
                }
            }
        }
    }

    {
        ReaderLockGuard ep_reader_lock_guard(index_store_->edge_prop_gc_rwlock_);
        double ratio = static_cast<double>(config_->Prop_Index_GC_RATIO) / 100;
        for (auto & pair : index_store_->edge_prop_index) {
            // PropIndexGCTask spawned for each pid
            IndexStore::prop_up_map_const_accessor pcac;
            if (index_store_->ep_update_map.find(pcac, pair.first)) {
                int up_elem_counter = 0;
                for (auto & update_pair : pcac->second) {
                    up_elem_counter += update_pair.second.size();
                }

                // When mergable update element exceeding a ratio of all data,
                // task spawns
                if (up_elem_counter > pair.second.total * ratio) {
                    spawn_prop_index_gctask(Element_T::EDGE, pair.first, up_elem_counter);
                }
            }
        }
    }
}

void GCProducer::scan_rct() {
    ReaderLockGuard reader_lock_guard(rct_table_->lock_);

    uint64_t cur_minimum_bt = running_trx_list_->GetGlobalMinBT();
    int num_gcable_record = 0;
    for (auto & pair : rct_table_->rct_map_) {
        if (pair.first < cur_minimum_bt) {
            num_gcable_record++;
        }
    }

    spawn_rct_gctask(num_gcable_record);
    spawn_trx_st_gctask(num_gcable_record);
}

void GCProducer::spawn_erase_vertex_gctask(vid_t & vid) {
    EraseVTask* task = new EraseVTask(vid);
    erase_v_job.AddTask(task);

    // Check whether job is ready to be consumed
    if (erase_v_job.isReady()) {
        EraseVJob* new_job = new EraseVJob();
        new_job[0] = erase_v_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        erase_v_job.Clear();
    }
}

void GCProducer::spawn_erase_out_edge_gctask(eid_t & eid) {
    EraseOutETask* task = new EraseOutETask(eid);
    erase_out_e_job.AddTask(task);

    // Check whether job is ready to be consumed
    if (erase_out_e_job.isReady()) {
        EraseOutEJob* new_job = new EraseOutEJob();
        new_job[0] = erase_out_e_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        erase_out_e_job.Clear();
    }
}

void GCProducer::spawn_erase_in_edge_gctask(eid_t & eid) {
    EraseInETask* task = new EraseInETask(eid);
    erase_in_e_job.AddTask(task);

    // Check whether job is ready to be consumed
    if (erase_in_e_job.isReady()) {
        EraseInEJob* new_job = new EraseInEJob();
        new_job[0] = erase_in_e_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        erase_in_e_job.Clear();
    }
}

void GCProducer::spawn_v_mvcc_gctask(VertexMVCCItem* mvcc_item) {
    VMVCCGCTask* task = new VMVCCGCTask(mvcc_item, 2);  // There must be only two version to be gc
    v_mvcc_gc_job.AddTask(task);

    if (v_mvcc_gc_job.isReady()) {
        VMVCCGCJob* new_job = new VMVCCGCJob();
        new_job[0] = v_mvcc_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        v_mvcc_gc_job.Clear();
    }
}

void GCProducer::spawn_vp_mvcc_list_gctask(VPropertyMVCCItem* gc_header, const int& gc_version_count) {
    VPMVCCGCTask* task = new VPMVCCGCTask(gc_header, gc_version_count);
    vp_mvcc_gc_job.AddTask(task);

    if (vp_mvcc_gc_job.isReady()) {
        VPMVCCGCJob* new_job = new VPMVCCGCJob();
        new_job[0] = vp_mvcc_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        vp_mvcc_gc_job.Clear();
    }
}

void GCProducer::spawn_ep_mvcc_list_gctask(EPropertyMVCCItem* gc_header, const int& gc_version_count) {
    EPMVCCGCTask* task = new EPMVCCGCTask(gc_header, gc_version_count);
    ep_mvcc_gc_job.AddTask(task);

    if (ep_mvcc_gc_job.isReady()) {
        EPMVCCGCJob* new_job = new EPMVCCGCJob();
        new_job[0] = ep_mvcc_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        ep_mvcc_gc_job.Clear();
    }
}

void GCProducer::spawn_edge_mvcc_list_gctask(EdgeMVCCItem* gc_header,
        const uint64_t& eid_value, const int& gc_version_count) {
    eid_t eid;
    uint2eid_t(eid_value, eid);

    EMVCCGCTask* task = new EMVCCGCTask(eid, gc_header, gc_version_count);
    edge_mvcc_gc_job.AddTask(task);

    EdgeMVCCItem* itr = gc_header;
    // For each version, attached EPRowList should be gc as well
    while (true) {
        // get ep_row_list when setting it as nullptr, to make sure that only EPRowListGCTask can access this ep_row_list
        PropertyRowList<EdgePropertyRow>* row_list = itr->CutEPRowList();
        if (row_list != nullptr)
            spawn_ep_row_list_gctask(row_list, eid);
        if (itr->next != nullptr) {
            itr = itr->next;
        } else {
            break;
        }
    }

    if (edge_mvcc_gc_job.isReady()) {
        EMVCCGCJob* new_job = new EMVCCGCJob();
        new_job[0] = edge_mvcc_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        edge_mvcc_gc_job.Clear();
    }
}

void GCProducer::spawn_topo_row_list_gctask(TopologyRowList* topo_row_list, vid_t & vid) {
    TopoRowListGCTask* task = gc_task_dag_->InsertTopoRowListGCTask(vid, topo_row_list);

    // Do not Need to Add Task into Job when Insert Failed
    if (task != nullptr) {
        topo_row_list_gc_job.AddTask(task);
    } else {
        return;
    }

    if (topo_row_list_gc_job.isReady()) {
        TopoRowListGCJob* new_job = new TopoRowListGCJob();
        new_job[0] = topo_row_list_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        topo_row_list_gc_job.Clear();
    }
}

void GCProducer::spawn_topo_row_list_defrag_gctask(TopologyRowList* row_list,
        const vid_t& vid, const int& gcable_cell_count) {
    TopoRowListDefragTask* task = gc_task_dag_->InsertTopoRowListDefragTask(vid, row_list, gcable_cell_count);

    // Do not Need to Add Task into Job when Insert Failed
    if (task != nullptr) {
        topo_row_list_defrag_job.AddTask(task);
    } else {
        return;
    }

    if (topo_row_list_defrag_job.isReady()) {
        TopoRowListDefragJob* new_job = new TopoRowListDefragJob();
        new_job[0] = topo_row_list_defrag_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        topo_row_list_defrag_job.Clear();
    }
}

void GCProducer::spawn_vp_row_list_gctask(PropertyRowList<VertexPropertyRow>* prop_row_list, vid_t & vid) {
    VPRowListGCTask* task = gc_task_dag_->InsertVPRowListGCTask(vid, prop_row_list);

    // Do not Need to Add Task into Job when Insert Failed
    if (task != nullptr) {
        vp_row_list_gc_job.AddTask(task);
    } else {
        return;
    }

    if (vp_row_list_gc_job.isReady()) {
        VPRowListGCJob* new_job = new VPRowListGCJob();
        new_job[0] = vp_row_list_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        vp_row_list_gc_job.Clear();
    }
}

void GCProducer::spawn_vp_row_defrag_gctask(PropertyRowList<VertexPropertyRow>* row_list,
                                            const uint64_t& element_id, const int& gcable_cell_count) {
    vid_t vid;
    uint2vid_t(element_id, vid);
    VPRowListDefragTask* task = gc_task_dag_->InsertVPRowListDefragTask(vid, row_list, gcable_cell_count);

    // Do not Need to Add Task into Job when Insert Failed
    if (task != nullptr) {
        vp_row_list_defrag_job.AddTask(task);
    } else {
        return;
    }

    if (vp_row_list_defrag_job.isReady()) {
        VPRowListDefragJob* new_job = new VPRowListDefragJob();
        new_job[0] = vp_row_list_defrag_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        vp_row_list_defrag_job.Clear();
    }
}

void GCProducer::spawn_ep_row_list_gctask(PropertyRowList<EdgePropertyRow>* row_list, eid_t& eid) {
    CompoundEPRowListID id(eid, row_list);
    EPRowListGCTask* task = gc_task_dag_->InsertEPRowListGCTask(id, row_list);

    // Do not Need to Add Task into Job when Insert Failed
    if (task != nullptr) {
        ep_row_list_gc_job.AddTask(task);
    } else {
        return;
    }

    if (ep_row_list_gc_job.isReady()) {
        EPRowListGCJob* new_job = new EPRowListGCJob();
        new_job[0] = ep_row_list_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        ep_row_list_gc_job.Clear();
    }
}

void GCProducer::spawn_ep_row_defrag_gctask(PropertyRowList<EdgePropertyRow>* row_list,
        const uint64_t& element_id, const int& gcable_cell_count) {
    eid_t eid;
    uint2eid_t(element_id, eid);
    CompoundEPRowListID id(eid, row_list);
    EPRowListDefragTask* task = gc_task_dag_->InsertEPRowListDefragTask(id, row_list, gcable_cell_count);

    // Do not Need to Add Task into Job when Insert Failed
    if (task != nullptr) {
        ep_row_list_defrag_job.AddTask(task);
    } else {
        return;
    }

    if (ep_row_list_defrag_job.isReady()) {
        EPRowListDefragJob* new_job = new EPRowListDefragJob();
        new_job[0] = ep_row_list_defrag_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        ep_row_list_defrag_job.Clear();
    }
}

void GCProducer::spawn_topo_index_gctask(Element_T type) {
    TopoIndexGCTask* task = new TopoIndexGCTask(type);
    topo_index_gc_job.AddTask(task);

    if (topo_index_gc_job.isReady()) {
        TopoIndexGCJob* new_job = new TopoIndexGCJob();
        new_job[0] = topo_index_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        topo_index_gc_job.Clear();
    }
}

void GCProducer::spawn_prop_index_gctask(Element_T type, const int& pid, const int& cost) {
    PropIndexGCTask* task = new PropIndexGCTask(type, pid, cost);
    prop_index_gc_job.AddTask(task);

    if (prop_index_gc_job.isReady()) {
        PropIndexGCJob* new_job = new PropIndexGCJob();
        new_job[0] = prop_index_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        prop_index_gc_job.Clear();
    }
}

void GCProducer::spawn_rct_gctask(const int& cost) {
    if (rct_gc_job.isEmpty()) {
        RCTGCTask* task = new RCTGCTask(cost);
        rct_gc_job.AddTask(task);
    } else {
        rct_gc_job.sum_of_cost_ = cost;;
    }

    if (rct_gc_job.isReady()) {
        RCTGCJob * new_job = new RCTGCJob();
        new_job[0] = rct_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        rct_gc_job.Clear();
    }
}

void GCProducer::spawn_trx_st_gctask(const int& cost) {
    if (trx_st_gc_job.isEmpty()) {
        TrxStatusTableGCTask* task = new TrxStatusTableGCTask(cost);
        trx_st_gc_job.AddTask(task);
    } else {
        trx_st_gc_job.sum_of_cost_ = cost;
    }

    if (trx_st_gc_job.isReady()) {
        TrxStatusTableGCJob * new_job = new TrxStatusTableGCJob();
        new_job[0] = trx_st_gc_job;
        garbage_collector_->PushJobToPendingQueue(new_job);
        trx_st_gc_job.Clear();
    }
}

void GCProducer::construct_edge_id(const vid_t& v1, EdgeHeader* adjacent_edge_header, eid_t& eid) {
    if (adjacent_edge_header->is_out) {
        eid = eid_t(adjacent_edge_header->conn_vtx_id.value(), v1.value());
    } else {
        eid = eid_t(v1.value(), adjacent_edge_header->conn_vtx_id.value());
    }
}

void GCProducer::check_finished_job() {
    while (true) {
        AbstractGCJob* job;
        if (!garbage_collector_->PopJobFromFinishedQueue(job)) {
            break;
        }

        // Delete finished dependent job
        switch (job->job_t_) {
          case JobType::EraseV:
          case JobType::EraseOutE:
          case JobType::EraseInE:
          case JobType::VMVCCGC:
          case JobType::VPMVCCGC:
          case JobType::EPMVCCGC:
          case JobType::EMVCCGC:
          case JobType::TopoIndexGC:
          case JobType::PropIndexGC:
          case JobType::RCTGC:
          case JobType::TrxStatusTableGC:
            break;
          case JobType::TopoRowGC:
            for (auto t : static_cast<DependentGCJob*>(job)->tasks_) {
                gc_task_dag_->DeleteTopoRowListGCTask(static_cast<TopoRowListGCTask*>(t));
            }
            break;
          case JobType::TopoRowDefrag:
            for (auto t : static_cast<DependentGCJob*>(job)->tasks_) {
                gc_task_dag_->DeleteTopoRowListDefragTask(static_cast<TopoRowListDefragTask*>(t));
            }
            break;
          case JobType::VPRowGC:
            for (auto t : static_cast<DependentGCJob*>(job)->tasks_) {
                gc_task_dag_->DeleteVPRowListGCTask(static_cast<VPRowListGCTask*>(t));
            }
            break;
          case JobType::VPRowDefrag:
            for (auto t : static_cast<DependentGCJob*>(job)->tasks_) {
                gc_task_dag_->DeleteVPRowListDefragTask(static_cast<VPRowListDefragTask*>(t));
            }
            break;
          case JobType::EPRowGC:
            for (auto t : static_cast<DependentGCJob*>(job)->tasks_) {
                gc_task_dag_->DeleteEPRowListGCTask(static_cast<EPRowListGCTask*>(t));
            }
            break;
          case JobType::EPRowDefrag:
            for (auto t : static_cast<DependentGCJob*>(job)->tasks_) {
                gc_task_dag_->DeleteEPRowListDefragTask(static_cast<EPRowListDefragTask*>(t));
            }
            break;
          default:
            cout << "[GCProducer] Unexpected JobType in Queue" << endl;
            CHECK(false);
        }

        delete job;
    }
}

void GCProducer::check_erasable_eid() {
    while (true) {
        vector<pair<eid_t, bool>>* returned_edges = new vector<pair<eid_t, bool>>();
        if (!garbage_collector_->PopGCAbleEidFromQueue(returned_edges)) {
            break;
        }

        for (auto & pair : returned_edges[0]) {
            if (pair.second) {  // is_out
                spawn_erase_out_edge_gctask(pair.first);
            } else {
                spawn_erase_in_edge_gctask(pair.first);
            }
        }

        delete returned_edges;
    }
}

void GCProducer::DebugPrint() {
    // Note: when debugging, figure out in which machine
    // you are using and the data stored;
    if (node_.get_local_rank() == 2) {
        cout << "[GCProducer] Debug Printing...." << endl;
        cout << "\t====DataStorage Usage=============" << endl;
        cout << "\tGC::ve_row_pool_: " + data_storage_->ve_row_pool_->UsageString() << endl;
        cout << "\tGC::vp_row_pool_: " + data_storage_->vp_row_pool_->UsageString() << endl;
        cout << "\tGC::ep_row_pool_: " + data_storage_->ep_row_pool_->UsageString() << endl;
        cout << "\tGC::vp_mvcc_pool_: " + data_storage_->vp_mvcc_pool_->UsageString() << endl;
        cout << "\tGC::ep_mvcc_pool_: " + data_storage_->ep_mvcc_pool_->UsageString() << endl;
        cout << "\tGC::vertex_mvcc_pool_: " + data_storage_->vertex_mvcc_pool_->UsageString() << endl;
        cout << "\tGC::edge_mvcc_pool_: " + data_storage_->edge_mvcc_pool_->UsageString() << endl;
        cout << "\tGC::vp_store_: " + data_storage_->vp_store_->UsageString() << endl;
        cout << "\tGC::ep_store_: " + data_storage_->ep_store_->UsageString() << endl;

        cout << "\t===Job Creation=================="  << endl;
        cout << "\tGC::EraseVJob: " << erase_v_job.DebugString() << endl;
        cout << "\tGC::EraseOutEJob: " << erase_out_e_job.DebugString() << endl;
        cout << "\tGC::EraseInEJob: " << erase_in_e_job.DebugString() << endl;

        cout << "\tGC::VMVCCGCJob: " << v_mvcc_gc_job.DebugString() << endl;
        cout << "\tGC::VPMVCCGCJob: " << vp_mvcc_gc_job.DebugString() << endl;
        cout << "\tGC::EPMVCCGCJob: " << ep_mvcc_gc_job.DebugString() << endl;
        cout << "\tGC::EMVCCGCJob: " << edge_mvcc_gc_job.DebugString() << endl;

        cout << "\tGC::TopoRowListGC: " << topo_row_list_gc_job.DebugString() << endl;
        cout << "\tGC::TopoRowListDefrag: " << topo_row_list_defrag_job.DebugString() << endl;

        cout << "\tGC::VPRowListGC: " << vp_row_list_gc_job.DebugString() << endl;
        cout << "\tGC::VPRowListDefrag: " << vp_row_list_defrag_job.DebugString() << endl;

        cout << "\tGC::EPRowListGC: " << ep_row_list_gc_job.DebugString() << endl;
        cout << "\tGC::EPRowListDefrag: " << ep_row_list_defrag_job.DebugString() << endl;

        cout << "\tGC::TopoIndexGC: " << topo_index_gc_job.DebugString() << endl;
        cout << "\tGC::PropIndexGC: " << prop_index_gc_job.DebugString() << endl;
        cout << endl;
    }
}
