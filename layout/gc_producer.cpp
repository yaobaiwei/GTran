/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#include "layout/gc_producer.hpp"
#include "layout/garbage_collector.hpp"

GCTaskDAG::GCTaskDAG() {
    gc_producer_ = GCProducer::GetInstance();
    garbage_collector_ = GarbageCollector::GetInstance();
}

void GCProducer::Init() {
    scanner_ = thread(&GCProducer::Execute, this);
    data_storage_ = DataStorage::GetInstance();
    gc_task_dag_ = GCTaskDAG::GetInstance();
    garbage_collector_ = GarbageCollector::GetInstance();
    node_ = Node::StaticInstance();
}

void GCProducer::Stop() {
    scanner_.join();
}

void GCProducer::Execute() {
    while (true) {
        // Do Scan with DFS for whole datastorage
        scan_vertex_map();

        // Currently, sleep for a while and the do next scan
        sleep(SCAN_PERIOD);

        // After one round, check whether there are some tasks already done,
        // erase them from dependency dag; and check whether there are some edge
        // related task need to spawn
        check_finished_job();
        check_returned_edge();

        DebugPrint();
    }
}

bool GCTaskDAG::InsertVPRowListGCTask(VPRowListGCTask* task) {
    if (vp_row_list_gc_tasks_map.find(task->id) != vp_row_list_gc_tasks_map.end()) {
        // Already Exists
        return false;
    }

    if (vp_row_list_defrag_tasks_map.find(task->id) != vp_row_list_defrag_tasks_map.end()) {
        VPRowListDefragTask * depender_task = vp_row_list_defrag_tasks_map.at(task->id);
        // Try to set depender_task to invalid, if failed, depender_task already pushed out
        if (!gc_producer_->vp_row_list_defrag_job.SetTaskInvalid(depender_task)) {
            // Task already pushed, set blocked_conunt to current task
            task->blocked_count_ += 1;
            task->task_status = TaskStatus::BLOCKED;
        }
    }

    vp_row_list_gc_tasks_map.emplace(task->id, task);  // Insert to dependency map
    return true;
}

bool GCTaskDAG::InsertVPRowListDefragTask(VPRowListDefragTask* task) {
    if (vp_row_list_defrag_tasks_map.find(task->id) != vp_row_list_defrag_tasks_map.end()) {
        // Already Exists
        return false;
    }

    if (vp_row_list_gc_tasks_map.find(task->id) != vp_row_list_gc_tasks_map.end()) {
        // Already exists dependee task
        return false;
    } else {
        vp_row_list_defrag_tasks_map.emplace(task->id, task);
    }

    return true;
}

void GCTaskDAG::DeleteVPRowListGCTask(vid_t& vid) {
    if (vp_row_list_gc_tasks_map.find(vid) != vp_row_list_gc_tasks_map.end()) {
        delete vp_row_list_gc_tasks_map.at(vid);
        vp_row_list_gc_tasks_map.erase(vid);
    }
}

void GCTaskDAG::DeleteVPRowListDefragTask(vid_t& vid) {
    // Find defrag task
    if (vp_row_list_defrag_tasks_map.find(vid) != vp_row_list_defrag_tasks_map.end()) {
        VPRowListDefragTask* defrag_task = vp_row_list_defrag_tasks_map.at(vid);
        // Find related gc task
        if (vp_row_list_gc_tasks_map.find(vid) != vp_row_list_gc_tasks_map.end()) {
            // Check whether gc task is blocked
            VPRowListGCTask* gc_task = vp_row_list_gc_tasks_map.at(vid);
            CHECK(gc_producer_->vp_row_list_gc_job.ReduceTaskBlockCount(gc_task));

            if (gc_producer_->vp_row_list_gc_job.isReady()) {
                VPRowListGCJob* new_job = new VPRowListGCJob();
                new_job[0] = gc_producer_->vp_row_list_gc_job;
                garbage_collector_->PushJobToPendingQueue(new_job);
                gc_producer_->vp_row_list_gc_job.Clear();
            }
        }

        delete defrag_task;
        vp_row_list_defrag_tasks_map.erase(vid);
    }
}

bool GCTaskDAG::InsertTopoRowListGCTask(TopoRowListGCTask* task) {
    if (topo_row_list_gc_tasks_map.find(task->id) != topo_row_list_gc_tasks_map.end()) {
        // Already exists
        TopoRowListGCTask* self = topo_row_list_gc_tasks_map.at(task->id);
        if (self->task_status != TaskStatus::EMPTY) {
            return false;
        }

        // Delete all downstream task
        for (auto task_ptr : self->downstream_tasks_) {
            if (typeid(*task_ptr) == typeid(TopoRowListDefragTask)) {
                if (!gc_producer_->topo_row_list_defrag_job.SetTaskInvalid(task_ptr)) {
                    task->blocked_count_ += 1;
                    task->task_status = TaskStatus::BLOCKED;
                }
            } else if (typeid(*task_ptr) == typeid(EPRowListDefragTask)) {
                if (!gc_producer_->ep_row_list_defrag_job.SetTaskInvalid(task_ptr)) {
                    task->blocked_count_ += 1;
                    task->task_status = TaskStatus::BLOCKED;
                }
            } else {
                cout << "[GCProducer] Unexpected type for downstream task store in InsertTopoRowListGCTask" << endl;
            }
        }

        delete self;
        topo_row_list_gc_tasks_map.at(task->id) = task;
    } else {
        // Directly insert into dependency map
        topo_row_list_gc_tasks_map.emplace(task->id, task);
    }

    return true;
}

bool GCTaskDAG::InsertTopoRowListDefragTask(TopoRowListDefragTask* task) {
    if (topo_row_list_defrag_tasks_map.find(task->id) != topo_row_list_defrag_tasks_map.end()) {
        return false;
    }

    if (topo_row_list_gc_tasks_map.find(task->id) != topo_row_list_gc_tasks_map.end()) {
        // Dependent Task Exists
        TopoRowListGCTask* dep_task = topo_row_list_gc_tasks_map.at(task->id);
        if (dep_task->task_status == TaskStatus::EMPTY) {
            // Record Dependent relationship
            dep_task->downstream_tasks_.emplace(task);
            task->upstream_tasks_.emplace(dep_task);

            topo_row_list_defrag_tasks_map.emplace(task->id, task);
        } else {
            return false;
        }
    } else {
        // Create Empty Dep Task
        TopoRowListGCTask* topo_dep_task = new TopoRowListGCTask();
        topo_dep_task->downstream_tasks_.emplace(task);
        topo_dep_task->task_status = TaskStatus::EMPTY;
        topo_row_list_gc_tasks_map.emplace(task->id, topo_dep_task);

        // Insert Self as well
        task->upstream_tasks_.emplace(topo_dep_task);
        topo_row_list_defrag_tasks_map.emplace(task->id, task);
    }

    return true;
}

bool GCTaskDAG::InsertEPRowListGCTask(EPRowListGCTask* task) {
    if (ep_row_list_gc_tasks_map.find(task->id) != ep_row_list_gc_tasks_map.end()) {
        EPRowListGCTask* self = ep_row_list_gc_tasks_map.at(task->id);
        if (self->task_status != TaskStatus::EMPTY) {
            return false;
        }

        // Delete all downstream task
        for (auto task_ptr : self->downstream_tasks_) {
            CHECK(typeid(*task_ptr) == typeid(EPRowListDefragTask));
            if (!gc_producer_->ep_row_list_defrag_job.SetTaskInvalid(task_ptr)) {
                task->blocked_count_ += 1;
                task->task_status = TaskStatus::BLOCKED;
            }
        }

        delete self;
        ep_row_list_gc_tasks_map.at(task->id) = task;
    } else {
        ep_row_list_gc_tasks_map.emplace(task->id, task);
    }

    return true;
}

bool GCTaskDAG::InsertEPRowListDefragTask(EPRowListDefragTask* task) {
    if (ep_row_list_defrag_tasks_map.find(task->id) != ep_row_list_defrag_tasks_map.end()) {
        return false;
    }

    vid_t src_v = task->id.GetAttachedVid();
    bool topo_row_dep_exist = (topo_row_list_gc_tasks_map.find(src_v) != topo_row_list_gc_tasks_map.end());
    bool ep_row_dep_exist = (ep_row_list_gc_tasks_map.find(task->id) != ep_row_list_gc_tasks_map.end());

    bool topo_row_dep_empty = false;
    bool ep_row_dep_empty = false;
    if (topo_row_dep_exist) {
        TopoRowListGCTask * topo_dep_task = topo_row_list_gc_tasks_map.at(src_v);
        if (topo_dep_task->task_status == TaskStatus::EMPTY) {
            topo_row_dep_empty = true;
        } else {
            return false;
        }
    }

    if (ep_row_dep_exist) {
        EPRowListGCTask* ep_dep_task = ep_row_list_gc_tasks_map.at(task->id);
        if (ep_dep_task->task_status == TaskStatus::EMPTY) {
            ep_row_dep_empty = false;
        } else {
            return false;
        }
    }

    if (topo_row_dep_exist) {
        TopoRowListGCTask* topo_dep_task = topo_row_list_gc_tasks_map.at(src_v);
        if (topo_row_dep_empty) {
            topo_dep_task->downstream_tasks_.emplace(task);
            task->upstream_tasks_.emplace(topo_dep_task);
        }
    } else {
        // Create Empty Dep Task
        TopoRowListGCTask * topo_dep_task = new TopoRowListGCTask();
        topo_dep_task->downstream_tasks_.emplace(task);
        topo_dep_task->task_status == TaskStatus::EMPTY;
        topo_row_list_gc_tasks_map.emplace(src_v, topo_dep_task);

        task->upstream_tasks_.emplace(topo_dep_task);
    }

    if (ep_row_dep_exist) {
        EPRowListGCTask* ep_dep_task = ep_row_list_gc_tasks_map.at(task->id);
        if (ep_row_dep_empty) {
            ep_dep_task->downstream_tasks_.emplace(task);
            task->upstream_tasks_.emplace(ep_dep_task);
        }
    } else {
        // Create Empty Dep Task
        EPRowListGCTask * ep_dep_task = new EPRowListGCTask();
        ep_dep_task->downstream_tasks_.emplace(task);
        ep_dep_task->task_status == TaskStatus::EMPTY;
        ep_row_list_gc_tasks_map.emplace(task->id, ep_dep_task);

        task->upstream_tasks_.emplace(ep_dep_task);
    }

    ep_row_list_defrag_tasks_map.emplace(task->id, task);
    return true;
}

void GCTaskDAG::DeleteTopoRowListGCTask(vid_t& vid) {
    if (topo_row_list_gc_tasks_map.find(vid) != topo_row_list_gc_tasks_map.end()) {
        delete topo_row_list_gc_tasks_map.at(vid);
        topo_row_list_gc_tasks_map.erase(vid);
    }
}

void GCTaskDAG::DeleteTopoRowListDefragTask(vid_t& vid) {
    if (topo_row_list_defrag_tasks_map.find(vid) != topo_row_list_defrag_tasks_map.end()) {
        TopoRowListDefragTask* task_ptr = topo_row_list_defrag_tasks_map.at(vid);
        for (auto upstream_t : task_ptr->upstream_tasks_) {
            if (upstream_t->task_status == TaskStatus::BLOCKED) {
                CHECK(gc_producer_->topo_row_list_gc_job.ReduceTaskBlockCount(upstream_t));
            }

            if (gc_producer_->topo_row_list_gc_job.isReady()) {
                TopoRowListGCJob* new_job = new TopoRowListGCJob();
                new_job[0] = gc_producer_->topo_row_list_gc_job;
                garbage_collector_->PushJobToPendingQueue(new_job);
                gc_producer_->topo_row_list_gc_job.Clear();
            }
        }

        delete task_ptr;
        topo_row_list_defrag_tasks_map.erase(vid);
    }
}

void GCTaskDAG::DeleteEPRowListGCTask(CompoundEPRowListID& id) {
    if (ep_row_list_gc_tasks_map.find(id) != ep_row_list_gc_tasks_map.end()) {
        delete ep_row_list_gc_tasks_map.at(id);
        ep_row_list_gc_tasks_map.erase(id);
    }
}

void GCTaskDAG::DeleteEPRowListDefragTask(CompoundEPRowListID& id) {
    if (ep_row_list_defrag_tasks_map.find(id) != ep_row_list_defrag_tasks_map.end()) {
        EPRowListDefragTask* task_ptr = ep_row_list_defrag_tasks_map.at(id);
        for (auto upstream_t : task_ptr->upstream_tasks_) {
            if (gc_producer_->ep_row_list_gc_job.taskExists(upstream_t)) {
                if (upstream_t->task_status == TaskStatus::BLOCKED) {
                    CHECK(gc_producer_->ep_row_list_gc_job.ReduceTaskBlockCount(upstream_t));
                }
                if (gc_producer_->ep_row_list_gc_job.isReady()) {
                    EPRowListGCJob* new_job = new EPRowListGCJob();
                    new_job[0] = gc_producer_->ep_row_list_gc_job;
                    garbage_collector_->PushJobToPendingQueue(new_job);
                    gc_producer_->ep_row_list_gc_job.Clear();
                }
            } else if (gc_producer_->topo_row_list_gc_job.taskExists(upstream_t)) {
                if (upstream_t->task_status == TaskStatus::BLOCKED) {
                    CHECK(gc_producer_->topo_row_list_gc_job.ReduceTaskBlockCount(upstream_t));
                }
                if (gc_producer_->topo_row_list_gc_job.isReady()) {
                    TopoRowListGCJob* new_job = new TopoRowListGCJob();
                    new_job[0] = gc_producer_->topo_row_list_gc_job;
                    garbage_collector_->PushJobToPendingQueue(new_job);
                    gc_producer_->topo_row_list_gc_job.Clear();
                }
            } else {
                cout << "[GCProducer] Unexpected Task Type for DeleteEPRowListDefragTask()" << endl;
                CHECK(false);
            }
        }

        delete task_ptr;
        ep_row_list_defrag_tasks_map.erase(id);
    }
}

void GCProducer::scan_vertex_map() {
    // Do Scan to Vertex map
    for (auto v_pair = data_storage_->vertex_map_.begin(); v_pair != data_storage_->vertex_map_.end(); v_pair++) {
        auto& v_item = v_pair->second;

        MVCCList<VertexMVCCItem>* mvcc_list = v_item.mvcc_list;
        if (mvcc_list == nullptr) { continue; }

        VertexMVCCItem* mvcc_item = mvcc_list->GetHead();
        if (mvcc_item == nullptr) { continue; }

        // VertexMVCCList is different with other MVCCList since it only has at most
        // two versions and the second version must be deleted version
        // Therefore, if the first version is unvisible to any transaction
        // (i.e. version->end_time < MINIMUM_ACTIVE_TRANSACTION_BT), the vertex can be GC.
        vid_t vid;
        uint2vid_t(v_pair->first, vid);
        if (mvcc_item->GetEndTime() < MINIMUM_ACTIVE_TRANSACTION_BT) {
            // Vertex GCable
            mvcc_list->head_ = nullptr;
            mvcc_list->tail_ = nullptr;
            mvcc_list->pre_tail_ = nullptr;
            mvcc_list->tmp_pre_tail_ = nullptr;

            spawn_vertex_map_gctask(vid);
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
    VertexEdgeRow* row_ptr = topo_row_list->head_;
    int edge_count_snapshot = topo_row_list->edge_count_;

    int gcable_cell_count = 0;
    for (int i = 0; i < edge_count_snapshot; i++) {
        int cell_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i != 0 && cell_id_in_row == 0) {
            row_ptr = row_ptr->next_;
        }

        EdgeHeader* adjacent_edge_header = &row_ptr->cells_[cell_id_in_row];
        eid_t eid;
        construct_edge_id(vid, adjacent_edge_header, eid);

        MVCCList<EdgeMVCCItem>* cur_edge_mvcc_list = adjacent_edge_header->mvcc_list;
        if (scan_mvcc_list(eid.value(), cur_edge_mvcc_list)) {
            gcable_cell_count++;
        }
    }

    if (gcable_cell_count >= edge_count_snapshot % VE_ROW_ITEM_COUNT) {
        spawn_topo_row_list_defrag_gctask(topo_row_list, vid, gcable_cell_count);
    }
}

void GCProducer::spawn_vertex_map_gctask(vid_t & vid) {
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

void GCProducer::spawn_edge_mvcc_list_gctask(EdgeMVCCItem* gc_header, const uint64_t& eid_value, const int& gc_version_count) {
    eid_t eid;
    uint2eid_t(eid_value, eid);

    EMVCCGCTask* task = new EMVCCGCTask(eid, gc_header, gc_version_count);
    edge_mvcc_gc_job.AddTask(task);

    EdgeMVCCItem* itr = gc_header;
    while (true) {
        PropertyRowList<EdgePropertyRow>* row_list = itr->GetValue().ep_row_list;
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
    TopoRowListGCTask* task = new TopoRowListGCTask(topo_row_list, vid);
    if (gc_task_dag_->InsertTopoRowListGCTask(task)) {
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

void GCProducer::spawn_topo_row_list_defrag_gctask(TopologyRowList* row_list, const vid_t& vid, const int& gcable_cell_counter) {
    TopoRowListDefragTask* task = new TopoRowListDefragTask(row_list, vid, gcable_cell_counter);

    if (gc_task_dag_->InsertTopoRowListDefragTask(task)) {
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
    VPRowListGCTask* task = new VPRowListGCTask(prop_row_list, vid);

    if (gc_task_dag_->InsertVPRowListGCTask(task)) {
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

void GCProducer::spawn_vp_row_defrag_gctask(PropertyRowList<VertexPropertyRow>* row_list, const uint64_t& element_id, const int& gcable_cell_counter) {
    vid_t vid;
    uint2vid_t(element_id, vid);
    VPRowListDefragTask* task = new VPRowListDefragTask(vid, row_list, gcable_cell_counter);

    if (gc_task_dag_->InsertVPRowListDefragTask(task)) {
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
    EPRowListGCTask* task = new EPRowListGCTask(eid, row_list);

    if (gc_task_dag_->InsertEPRowListGCTask(task)) {
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

void GCProducer::spawn_ep_row_defrag_gctask(PropertyRowList<EdgePropertyRow>* row_list, const uint64_t& element_id, const int& gcable_cell_counter) {
    eid_t eid;
    uint2eid_t(element_id, eid);
    EPRowListDefragTask* task = new EPRowListDefragTask(eid, row_list, gcable_cell_counter);

    if (gc_task_dag_->InsertEPRowListDefragTask(task)) {
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

        switch (job->job_t) {
          case JobType::EraseV:
          case JobType::EraseOutE:
          case JobType::EraseInE:
          case JobType::VMVCCGC:
          case JobType::VPMVCCGC:
          case JobType::EPMVCCGC:
          case JobType::EMVCCGC:
            break;
          case JobType::TopoRowGC:
            for (auto t : static_cast<DependentGCJob*>(job)->tasks_) {
                gc_task_dag_->DeleteTopoRowListGCTask(static_cast<TopoRowListGCTask*>(t)->id);
            }
            break;
          case JobType::TopoRowDefrag:
            for (auto t : static_cast<DependentGCJob*>(job)->tasks_) {
                gc_task_dag_->DeleteTopoRowListDefragTask(static_cast<TopoRowListDefragTask*>(t)->id);
            }
            break;
          case JobType::VPRowGC:
            for (auto t : static_cast<DependentGCJob*>(job)->tasks_) {
                gc_task_dag_->DeleteVPRowListGCTask(static_cast<VPRowListGCTask*>(t)->id);
            }
            break;
          case JobType::VPRowDefrag:
            for (auto t : static_cast<DependentGCJob*>(job)->tasks_) {
                gc_task_dag_->DeleteVPRowListDefragTask(static_cast<VPRowListDefragTask*>(t)->id);
            }
            break;
          case JobType::EPRowGC:
            for (auto t : static_cast<DependentGCJob*>(job)->tasks_) {
                gc_task_dag_->DeleteEPRowListGCTask(static_cast<EPRowListGCTask*>(t)->id);
            }
            break;
          case JobType::EPRowDefrag:
            for (auto t : static_cast<DependentGCJob*>(job)->tasks_) {
                gc_task_dag_->DeleteEPRowListDefragTask(static_cast<EPRowListDefragTask*>(t)->id);
            }
            break;
          default:
            cout << "[GCProducer] Unexpected JobType in Queue" << endl;
            CHECK(false);
        }
    }
}

void GCProducer::check_returned_edge() {
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
        cout << endl;
    }
}
