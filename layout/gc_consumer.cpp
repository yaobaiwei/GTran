/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#include "layout/gc_consumer.hpp"
#include "layout/garbage_collector.hpp"
#include "layout/data_storage.hpp"

GCConsumer::GCConsumer() {
    config_ = Config::GetInstance();
    data_storage_ = DataStorage::GetInstance();
    tid_mapper_ = TidMapper::GetInstance();
    index_store_ = IndexStore::GetInstance();
    running_trx_list_ = RunningTrxList::GetInstance();
    rct_table_ = RCTable::GetInstance();
}

void GCConsumer::Init() {
    garbage_collector_ = GarbageCollector::GetInstance();
    for (int i = 0; i < config_->num_gc_consumer; i++) {
        int tid = config_->global_num_threads + 1 + i;
        consumer_thread_pool_.emplace_back(&GCConsumer::Execute, this, tid);
    }
}

void GCConsumer::Stop() {
    for (auto & thread : consumer_thread_pool_) {
        thread.join();
    }
}

void GCConsumer::Execute(int tid) {
    tid_mapper_->Register(tid);
    while (true) {
        AbstractGCJob * job;
        if (!garbage_collector_->PopJobFromPendingQueue(job)) {
            // Give a sleep to GCConsumer rather than urgent pop
            // to reduce lock contention
            usleep(POP_PERIOD);
            continue;
        }

        switch (job->job_t_) {
          case JobType::EraseV:
            ExecuteEraseVJob(job);
            break;
          case JobType::EraseOutE:
            ExecuteEraseOutEJob(job);
            break;
          case JobType::EraseInE:
            ExecuteEraseInEJob(job);
            break;
          case JobType::VMVCCGC:
            ExecuteVMVCCGCJob(job);
            break;
          case JobType::VPMVCCGC:
            ExecuteVPMVCCGCJob(job);
            break;
          case JobType::EPMVCCGC:
            ExecuteEPMVCCGCJob(job);
            break;
          case JobType::EMVCCGC:
            ExecuteEMVCCGCJob(job);
            break;
          case JobType::TopoIndexGC:
            ExecuteTopoIndexGCJob(job);
            break;
          case JobType::PropIndexGC:
            ExecutePropIndexGCJob(job);
            break;
          case JobType::RCTGC:
            ExecuteRCTGCJob(job);
            break;
          case JobType::TopoRowGC:
            ExecuteTopoRowListGCJob(job);
            break;
          case JobType::TopoRowDefrag:
            ExecuteTopoRowListDefragJob(job);
            break;
          case JobType::VPRowGC:
            ExecuteVPRowListGCJob(job);
            break;
          case JobType::VPRowDefrag:
            ExecuteVPRowListDefragJob(job);
            break;
          case JobType::EPRowGC:
            ExecuteEPRowListGCJob(job);
            break;
          case JobType::EPRowDefrag:
            ExecuteEPRowListDefragJob(job);
            break;
          default:
            cout << "[GCConsumer] Unexpected JobType in Queue" << endl;
            CHECK(false);
        }

        // After finish a task, push it back to GarbageCollector
        garbage_collector_->PushJobToFinishedQueue(job);
    }
}

void GCConsumer::ExecuteEraseVJob(EraseVJob * job) {
    // Erase a set of vertices from vertex_map in data_storage
    WriterLockGuard writer_lock_guard(data_storage_->vertex_map_erase_rwlock_);
    for (auto t : job->tasks_) {
        if (t == nullptr || t->task_status_ != TaskStatus::ACTIVE) { continue; }
        data_storage_->vertex_map_.unsafe_erase(static_cast<EraseVTask*>(t)->target.value());
    }
}

void GCConsumer::ExecuteEraseOutEJob(EraseOutEJob * job) {
    // Erase a set of edges (out_edge) from out_e_map
    WriterLockGuard writer_lock_guard(data_storage_->out_edge_erase_rwlock_);
    for (auto t : job->tasks_) {
        if (t == nullptr || t->task_status_ != TaskStatus::ACTIVE) { continue; }

        uint64_t eid_value = static_cast<EraseOutETask*>(t)->target.value();
        DataStorage::OutEdgeAccessor ac;
        if (data_storage_->out_edge_map_.Find(ac, eid_value)) {
            // Erase mvcc_list linked on the edge
            MVCCList<EdgeMVCCItem>* mvcc_list = ac->second.mvcc_list;
            if (mvcc_list->head_ == nullptr) { continue; }
            CHECK(mvcc_list == nullptr);
            delete mvcc_list;
        }

        data_storage_->out_edge_map_.unsafe_erase(eid_value);
    }
}

void GCConsumer::ExecuteEraseInEJob(EraseInEJob * job) {
    // Erase a set of edges (in_edge) from in_e_map
    WriterLockGuard writer_lock_guard(data_storage_->in_edge_erase_rwlock_);
    for (auto t : job->tasks_) {
        if (t == nullptr || t->task_status_ != TaskStatus::ACTIVE) { continue; }

        uint64_t eid_value = static_cast<EraseInETask*>(t)->target.value();
        DataStorage::InEdgeAccessor ac;
        if (data_storage_->in_edge_map_.Find(ac, eid_value)) {
            // Erase mvcc_list linked on the edge
            MVCCList<EdgeMVCCItem>* mvcc_list = ac->second.mvcc_list;
            if (mvcc_list->head_ == nullptr) { continue; }
            CHECK(mvcc_list == nullptr);
            delete mvcc_list;
        }

        data_storage_->in_edge_map_.unsafe_erase(eid_value);
    }
}

void GCConsumer::ExecuteVMVCCGCJob(VMVCCGCJob* job) {
    // Free a list of VMVCC
    // No need to lock since the list of MVCC has already been cut
    // Same for all other MVCCGCTask
    for (auto t : job->tasks_) {
        if (t->task_status_ != TaskStatus::ACTIVE) { continue; }
        VertexMVCCItem* cur_item = static_cast<VMVCCGCTask*>(t)->target;
        while (cur_item != nullptr) {
            auto* to_free = cur_item;
            cur_item = cur_item->next;
            // Clean
            to_free->next = nullptr;
            data_storage_->vertex_mvcc_pool_->Free(to_free, tid_mapper_->GetTidUnique());
        }
    }
}

void GCConsumer::ExecuteVPMVCCGCJob(VPMVCCGCJob* job) {
    // Free a list of VPMVCC
    for (auto t : job->tasks_) {
        if (t->task_status_ != TaskStatus::ACTIVE) { continue; }
        VPropertyMVCCItem* cur_item = static_cast<VPMVCCGCTask*>(t)->target;
        while (cur_item != nullptr) {
            auto* to_free = cur_item;
            cur_item = cur_item->next;
            // Clean
            to_free->next = nullptr;
            to_free->ValueGC();
            data_storage_->vp_mvcc_pool_->Free(to_free, tid_mapper_->GetTidUnique());
        }
    }
}

void GCConsumer::ExecuteEPMVCCGCJob(EPMVCCGCJob* job) {
    // Free a list of EPMVCC
    for (auto t : job->tasks_) {
        if (t->task_status_ != TaskStatus::ACTIVE) { continue; }
        EPropertyMVCCItem* cur_item = static_cast<EPMVCCGCTask*>(t)->target;
        while (cur_item != nullptr) {
            auto* to_free = cur_item;
            cur_item = cur_item->next;
            // Clean
            to_free->next = nullptr;
            to_free->ValueGC();
            data_storage_->ep_mvcc_pool_->Free(to_free, tid_mapper_->GetTidUnique());
        }
    }
}

void GCConsumer::ExecuteEMVCCGCJob(EMVCCGCJob* job) {
    // Free a list of EMVCC
    for (auto t : job->tasks_) {
        if (t->task_status_ != TaskStatus::ACTIVE) { continue; }
        EdgeMVCCItem* cur_item = static_cast<EMVCCGCTask*>(t)->target;
        while (cur_item != nullptr) {
            auto* to_free = cur_item;
            cur_item = cur_item->next;
            // Clean
            to_free->next = nullptr;
            to_free->ValueGC();
            data_storage_->edge_mvcc_pool_->Free(to_free, tid_mapper_->GetTidUnique());
        }
    }
}

void GCConsumer::ExecuteTopoIndexGCJob(TopoIndexGCJob* job) {
    for (auto t : job->tasks_) {
        if (t->task_status_ != TaskStatus::ACTIVE) { continue; }
        Element_T type = static_cast<TopoIndexGCTask*>(t)->element_type;
        if (type == Element_T::VERTEX) {
            // Lock inside
            index_store_->VtxSelfGarbageCollect(running_trx_list_->GetGlobalMinBT());
        } else if (type == Element_T::EDGE) {
            // Lock inside
            index_store_->EdgeSelfGarbageCollect(running_trx_list_->GetGlobalMinBT());
        }
    }
}

void GCConsumer::ExecutePropIndexGCJob(PropIndexGCJob* job) {
    for (auto t : job->tasks_) {
        if (t->task_status_ != TaskStatus::ACTIVE) { continue; }
        Element_T type = static_cast<PropIndexGCTask*>(t)->element_type;
        int pid = static_cast<PropIndexGCTask*>(t)->pid;
        // Lock inside
        index_store_->PropSelfGarbageCollect(running_trx_list_->GetGlobalMinBT(), pid, type);
    }
}

void GCConsumer::ExecuteRCTGCJob(RCTGCJob* job) {
    // There must be only one task in RCTGCJob
    // Lock inside
    rct_table_->erase_trxs(running_trx_list_->GetGlobalMinBT());
}

void GCConsumer::ExecuteTopoRowListGCJob(TopoRowListGCJob* job) {
    // Prepare for transfer eids to GCProducer
    vector<pair<eid_t, bool>>* gcable_eid = new vector<pair<eid_t, bool>>();
    for (auto t : job->tasks_) {
        if (t->task_status_ != TaskStatus::ACTIVE) { continue; }
        TopologyRowList* target = static_cast<TopoRowListGCTask*>(t)->target;
        if (target == nullptr) { continue; }
        target->SelfGarbageCollect(static_cast<TopoRowListGCTask*>(t)->id, gcable_eid);
    }
    garbage_collector_->PushGCAbleEidToQueue(gcable_eid);
}

void GCConsumer::ExecuteTopoRowListDefragJob(TopoRowListDefragJob* job) {
    // Prepare for transfer eids to GCProducer
    vector<pair<eid_t, bool>>* gcable_eid = new vector<pair<eid_t, bool>>();
    for (auto t : job->tasks_) {
        if (t->task_status_ != TaskStatus::ACTIVE) { continue; }
        TopologyRowList* target = static_cast<TopoRowListGCTask*>(t)->target;
        if (target == nullptr) { continue; }
        target->SelfDefragment(static_cast<TopoRowListGCTask*>(t)->id, gcable_eid);
    }
    garbage_collector_->PushGCAbleEidToQueue(gcable_eid);
}

void GCConsumer::ExecuteVPRowListGCJob(VPRowListGCJob* job) {
    for (auto t : job->tasks_) {
        if (t->task_status_ != TaskStatus::ACTIVE) { continue; }
        PropertyRowList<VertexPropertyRow>* target = static_cast<VPRowListGCTask*>(t)->target;
        if (target == nullptr) { continue; }
        target->SelfGarbageCollect();
    }
}

void GCConsumer::ExecuteVPRowListDefragJob(VPRowListDefragJob* job) {
    for (auto t : job->tasks_) {
        if (t->task_status_ != TaskStatus::ACTIVE) { continue; }
        PropertyRowList<VertexPropertyRow>* target = static_cast<VPRowListDefragTask*>(t)->target;
        if (target == nullptr) { continue; }
        target->SelfDefragment();
    }
}

void GCConsumer::ExecuteEPRowListGCJob(EPRowListGCJob* job) {
    for (auto t : job->tasks_) {
        if (t->task_status_ != TaskStatus::ACTIVE) { continue; }
        PropertyRowList<EdgePropertyRow>* target = static_cast<EPRowListGCTask*>(t)->target;
        if (target == nullptr) { continue; }
        target->SelfGarbageCollect();
    }
}

void GCConsumer::ExecuteEPRowListDefragJob(EPRowListDefragJob* job) {
    for (auto t : job->tasks_) {
        if (t->task_status_ != TaskStatus::ACTIVE) { continue; }
        PropertyRowList<EdgePropertyRow>* target = static_cast<EPRowListDefragTask*>(t)->target;
        if (target == nullptr) { continue; }
        target->SelfDefragment();
    }
}
