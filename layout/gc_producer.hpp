/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
         Modified by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#pragma once

#include <thread>
#include <unordered_map>
#include <unistd.h>

#include "base/node.hpp"
#include "core/running_trx_list.hpp"
#include "core/RCT.hpp"
#include "layout/gc_task.hpp"
#include "layout/index_store.hpp"
#include "utils/config.hpp"
#include "utils/simple_spinlock_guard.hpp"

/* GCProducer encapsulates methods to scan the whole data layout and generate garbage collection tasks to
 * free memory allocated for those objects that are invisible to all transactions in the system.
 *
 * In GCProducer, a single thread will regularly scan the whole data layout and generates GC tasks.
 * If the sum of costs of a specific type of GC task has reach the given threshold, all tasks of this
 * type will be packed as a Job and push to GCConsumer.
 *
 * GCProducer maintains containers of tasks without dependency. For tasks with dependency, their containers
 * are in GCTaskDAG.
 */

class GarbageCollector;

// The container of dependent tasks
//  Insert Function will check dependency and erase downsteam tasks or delete self
//  since upstream tasks existing
//
//  Delete Function will delete finished tasks and release blocking tasks
class GCTaskDAG {
 private:
    GCTaskDAG();
    GCTaskDAG(const GCTaskDAG&);
    ~GCTaskDAG() {}

    GCProducer* gc_producer_;
    GarbageCollector* garbage_collector_;

 public:
    static GCTaskDAG* GetInstance() {
        static GCTaskDAG task_dag;
        return &task_dag;
    }

    /*
     * Task dependency DAG 1:
     *  VPRowListGCTask ----> VPRowListDefragTask
     **/
    unordered_map<vid_t, VPRowListGCTask*, VidHash> vp_row_list_gc_tasks_map;
    unordered_map<vid_t, VPRowListDefragTask*, VidHash> vp_row_list_defrag_tasks_map;

    bool InsertVPRowListGCTask(VPRowListGCTask*);
    bool InsertVPRowListDefragTask(VPRowListDefragTask*);
    void DeleteVPRowListGCTask(vid_t&);
    void DeleteVPRowListDefragTask(vid_t&);

    /*
     * Task dependency DAG 2:
     *  TopoRowListGCTask ----> TopoRowListDefragTask
     *                \
     *                 \
     *                  ---->
     *  EPRowListGCTask ----> EPRowListDefragTask
     **/
    unordered_map<vid_t, TopoRowListGCTask*, VidHash> topo_row_list_gc_tasks_map;
    unordered_map<vid_t, TopoRowListDefragTask*, VidHash> topo_row_list_defrag_tasks_map;
    unordered_map<CompoundEPRowListID, EPRowListGCTask*, CompoundEPRowListIDHash> ep_row_list_gc_tasks_map;
    unordered_map<CompoundEPRowListID, EPRowListDefragTask*, CompoundEPRowListIDHash> ep_row_list_defrag_tasks_map;

    bool InsertTopoRowListGCTask(TopoRowListGCTask*);
    bool InsertTopoRowListDefragTask(TopoRowListDefragTask*);
    bool InsertEPRowListGCTask(EPRowListGCTask*);
    bool InsertEPRowListDefragTask(EPRowListDefragTask*);

    void DeleteTopoRowListGCTask(vid_t&);
    void DeleteTopoRowListDefragTask(vid_t&);
    void DeleteEPRowListGCTask(CompoundEPRowListID&);
    void DeleteEPRowListDefragTask(CompoundEPRowListID&);
};

class GCProducer {
 public:
    static GCProducer* GetInstance() {
        static GCProducer producer;
        return &producer;
    }

    void Init();
    void Stop();

    // The function that the GCProducer thread loops
    void Execute();

    friend class GCTaskDAG;

 private:
    GCProducer() {}
    GCProducer(const GCProducer&);
    ~GCProducer() {}

    // -------GC Job For Each Tyep---------
    EraseVJob erase_v_job;
    EraseOutEJob erase_out_e_job;
    EraseInEJob erase_in_e_job;

    VMVCCGCJob v_mvcc_gc_job;
    VPMVCCGCJob vp_mvcc_gc_job;
    EPMVCCGCJob ep_mvcc_gc_job;
    EMVCCGCJob edge_mvcc_gc_job;

    TopoRowListGCJob topo_row_list_gc_job;
    TopoRowListDefragJob topo_row_list_defrag_job;

    VPRowListGCJob vp_row_list_gc_job;
    VPRowListDefragJob vp_row_list_defrag_job;

    EPRowListGCJob ep_row_list_gc_job;
    EPRowListDefragJob ep_row_list_defrag_job;

    // Index Store GC
    TopoIndexGCJob topo_index_gc_job;
    PropIndexGCJob prop_index_gc_job;

    // RCT
    RCTGCJob rct_gc_job;

    // TrxStatusTable
    TrxStatusTableGCJob trx_st_gc_job;

    // -------Sys Components------------
    GCTaskDAG * gc_task_dag_;
    DataStorage * data_storage_;
    GarbageCollector * garbage_collector_;
    IndexStore * index_store_;
    Config * config_;
    Node node_;
    RunningTrxList * running_trx_list_;
    RCTable * rct_table_;

    // Thread to scan data store
    thread scanner_;

    // Scan Period (unit:sec)
    // For every SCAN_PERIOD, producer scan once;
    const int SCAN_PERIOD = 5;

    // -------Scanning Function---------
    void scan_vertex_map();
    void scan_topo_row_list(const vid_t&, TopologyRowList*);
    // Scan RowList && MVCCList
    template <class PropertyRow>
    void scan_prop_row_list(const uint64_t& element_id, PropertyRowList<PropertyRow>*);
    template <class MVCCItem>
    bool scan_mvcc_list(const uint64_t& element_id, MVCCList<MVCCItem>*);

    // Index Store Scan
    void scan_topo_index_update_region();
    void scan_prop_index_update_region();

    // RCT Scan
    // Note: This scan will also spawn trx_status_table_gctask
    // since scan status table is expensive
    void scan_rct();

    // -------Task spawning function-------
    void spawn_erase_vertex_gctask(vid_t&);
    void spawn_erase_out_edge_gctask(eid_t&);
    void spawn_erase_in_edge_gctask(eid_t&);

    void spawn_v_mvcc_gctask(VertexMVCCItem*);
    void spawn_vp_mvcc_list_gctask(VPropertyMVCCItem*, const int& cost);
    void spawn_ep_mvcc_list_gctask(EPropertyMVCCItem*, const int& cost);
    void spawn_edge_mvcc_list_gctask(EdgeMVCCItem*, const uint64_t& element_id, const int& cost);

    void spawn_topo_row_list_gctask(TopologyRowList*, vid_t&);
    void spawn_topo_row_list_defrag_gctask(TopologyRowList*, const vid_t&, const int& cost);

    void spawn_vp_row_list_gctask(PropertyRowList<VertexPropertyRow>*, vid_t&);
    void spawn_vp_row_defrag_gctask(PropertyRowList<VertexPropertyRow>*, const uint64_t& element_id, const int& cost);

    void spawn_ep_row_list_gctask(PropertyRowList<EdgePropertyRow>*, eid_t&);
    void spawn_ep_row_defrag_gctask(PropertyRowList<EdgePropertyRow>*, const uint64_t& element_id, const int& cost);

    template <class MVCCItem>
    void spawn_mvcc_list_gctask(MVCCItem*, const uint64_t& element_id, const int& cost);
    template <class PropertyRow>
    void spawn_prop_row_defrag_gctask(PropertyRowList<PropertyRow>*, const uint64_t& element_id, const int& cost);

    // Index Store Task Spawn
    void spawn_topo_index_gctask(Element_T);
    void spawn_prop_index_gctask(Element_T type, const int& pid, const int& cost);

    // RCT Task Spawn
    void spawn_rct_gctask(const int& cost);

    // TrxST Task Spawn
    void spawn_trx_st_gctask(const int& cost);

    // -------Other help functions--------
    void construct_edge_id(const vid_t&, EdgeHeader*, eid_t&);
    void check_finished_job();
    void check_returned_edge();

    void DebugPrint();
};

#include "layout/gc_producer.tpp"
