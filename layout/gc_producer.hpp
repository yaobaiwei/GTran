/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
         Modified by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#pragma once

#include <thread>

#include "layout/gc_consumer.hpp"
#include "layout/gc_task.hpp"

#include "utils/simple_spinlock_guard.hpp"

/*
The Scanning procedure:
  Notes: A single line means "one to one";
         Double lines means "one to many";

    vertex_map
        ||
        ||
        ||
      Vertex----------------------------MVCCList<V>
         |               |
         |               |
         |               |
    VPRowList       TopoRowList
        ||               ||
        ||               ||
        ||               ||
     VPHeader        EdgeHeader
         |               |
         |               |
         |               |
    MVCCList<VP>    MVCCList<E>
                         ||
                         ||
                         ||
                    EdgeMVCCItem
                         |
                         |
                         |
                        Edge
                         |
                         |
                         |
                     EPRowList
                         ||
                         ||
                         ||
                      EPHeader
                         |
                         |
                         |
                    MVCCList<EP>

*/

/* GCProducer encapsulates methods to scan the whole data layout and generate garbage collection tasks to
 * free memory allocated for those objects that are invisible to all transactions in the system.
 *
 * In GCProducer, a single thread will regularly scan the whole data layout and generates GC tasks.
 * If the sum of costs of a specific type of GC task has reach the given threshold, all tasks of this
 * type will be packed as a Job and push to GCExecutor.
 *
 * GCProducer maintains containers of tasks without dependency. For tasks with dependency, their containers
 * are in GCTaskDAG.
 */

// Fake one;
static constexpr int MINIMUM_ACTIVE_TRANSACTION_BT = 0;

class GCProducer {
 public:
    static GCProducer* GetInstance() {
        static GCProducer producer;
        return &producer;
    }

    void Init();
    void Stop();

    // The function that the GCProducer thread loops
    void Scan();

 private:
    GCProducer() {}
    GCProducer(const GCProducer&);
    ~GCProducer() {}

    // The container of dependent tasks
    class GCTaskDAG {
     private:
        GCTaskDAG() {}
        GCTaskDAG(const GCTaskDAG&);
        ~GCTaskDAG() {}

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
    };

    GCTaskDAG * gc_task_dag_;
    DataStorage * data_storage_;

    static GCJob<EraseVTask> erase_v_job;
    static GCJob<VMVCCGCTask> v_mvcc_gc_job;
    static GCJob<VPMVCCGCTask> vp_mvcc_gc_job;
    static GCJob<EPMVCCGCTask> ep_mvcc_gc_job;
    static GCJob<EMVCCGCTask> edge_mvcc_gc_job;

    static GCJob<TopoRowListGCTask> topo_row_list_gc_job;
    static GCJob<TopoRowListDefragTask> topo_row_list_defrag_job;

    static GCJob<VPRowListGCTask> vp_row_list_gc_job;
    static GCJob<VPRowListDefragTask> vp_row_list_defrag_job;

    static GCJob<EPRowListGCTask> ep_row_list_gc_job;
    static GCJob<EPRowListDefragTask> ep_row_list_defrag_job;

    // Thread to scan data store
    thread scanner_;

    // -------Scanning Function--------- 
    void scan_vertex_map();
    void scan_topo_row_list(const vid_t&, TopologyRowList*);
    // Scan RowList && MVCCList
    template <class PropertyRow>
    void scan_prop_row_list(const uint64_t&, PropertyRowList<PropertyRow>*);
    template <class MVCCItem>
    bool scan_mvcc_list(const uint64_t&, MVCCList<MVCCItem>*);

    // -------Task spawning function-------
    void spawn_vertex_map_gctask(vid_t&);
    void spawn_v_mvcc_gctask(VertexMVCCItem*);
    void spawn_vp_mvcc_list_gctask(VPropertyMVCCItem*, const int&);
    void spawn_ep_mvcc_list_gctask(EPropertyMVCCItem*, const int&);
    void spawn_edge_mvcc_list_gctask(EdgeMVCCItem*, const uint64_t&, const int&);

    void spawn_topo_row_list_gctask(TopologyRowList*, vid_t&);
    void spawn_topo_row_list_defrag_gctask(TopologyRowList*, const vid_t&, const int&);

    void spawn_vp_row_list_gctask(PropertyRowList<VertexPropertyRow>*, vid_t&);
    void spawn_vp_row_defrag_gctask(PropertyRowList<VertexPropertyRow>*, const uint64_t&, const int&);

    void spawn_ep_row_list_gctask(PropertyRowList<EdgePropertyRow>*, eid_t&);
    void spawn_ep_row_defrag_gctask(PropertyRowList<EdgePropertyRow>*, const uint64_t&, const int&);

    void spawn_edge_erase_gctask(eid_t eid);  // ???

    template <class MVCCItem>
    void spawn_mvcc_list_gctask(MVCCItem*, const uint64_t&, const int&);
    template <class PropertyRow>
    void spawn_prop_row_defrag_gctask(PropertyRowList<PropertyRow>*, const uint64_t&, const int&);

    // -------Other help functions--------
    void construct_edge_id(const vid_t&, EdgeHeader*, eid_t&); 

    template <class GCTaskT>
    void push_job(GCJob<GCTaskT>);
};

#include "layout/gc_producer.tpp"
