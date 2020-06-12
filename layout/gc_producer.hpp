// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
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
#include "utils/timer.hpp"

/* GCProducer encapsulates methods to scan the whole data layout and generate garbage collection tasks to
 * free memory allocated for those objects that are invisible to all transactions in the system.
 *
 * In GCProducer, a single thread will regularly scan the whole data layout and generates GC tasks.
 * If the sum of costs of a specific type of GC task has reach the given threshold, all tasks of this
 * type will be packed as a Job and push to GCConsumer.
 *
 * GCProducer maintains containers (jobs) of unpushed tasks.
 * For tasks with dependency, their pointers are also stored in the GCTaskDAG.
 */

class GarbageCollector;

/*
The dependency of dependent tasks can form a DAG.


The task DAG is stored in two parts (nodes and connections):
    - 6 maps of tasks indexed by task id. Each type of dependent task is in different maps, respectively.
    - The connections are stored in the tasks themselves, by two sets: upstream_tasks_ and downstream_tasks_.
      For example, For TopoRowListGCTask *a and EPRowListDefragTask *b with dependency: a->downstream_tasks_ will contains b, and b->upstream_tasks_ will contains a.
Thus, "connect A (upstream task) with B (downstream task) in the DAG means":
    Add the pointer of B in A->upstream_tasks_
    Add the pointer of A in B->downstream_tasks_


Dependency management:
    INVALID task
        1. A task will become INVALID when its upstream task is converted from EMPTY to ACTIVE.
        2. When a task is converted to INVALID, it will be removed from the map, and all its connections will be removed.
    EMPTY task
        1. An EMPTY task will be added to the DAG when its downstream task is created.
        2. When a same-type task with the same id is to be created, it will just replace the EMPTY task.
            (1). If any of its downstream tasks are PUSHED, the task will be BLOCKED.
            (2). All of its ACTIVE downstream tasks will be marked as INVALID and disconnect from the DAG.
        3. If all of its downstream tasks disappear (become INVALID, and disconnect with the EMPTY task), the EMPTY task will be erased from the DAG and deleted.
    BLOCKED task
        1. A task is BLOCKED when one or more of its downstream tasks are PUSHED.
        2. When all of its PUSHED tasks are finished and deleted, this task will become ACTIVE.
    PUSHED task
        1. An ACTIVE task will become PUSHED when pushed from GCProducer to GCConsumer.
        2. Notice that an INVALID task will remain INVALID when pushed to GCConsumer!
*/
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

    // Insert*Task functions will return the pointer of inserted task if the insertion is successful. Else, nullptr will be returned.
    VPRowListGCTask* InsertVPRowListGCTask(const vid_t&, PropertyRowList<VertexPropertyRow>*);
    VPRowListDefragTask* InsertVPRowListDefragTask(const vid_t&, PropertyRowList<VertexPropertyRow>*, int);

    // Delete*Task functions will be called when a task is finished.
    void DeleteVPRowListGCTask(VPRowListGCTask*);
    void DeleteVPRowListDefragTask(VPRowListDefragTask*);

    // DisconnectInvalid*Task functions will be called when a EMPTY upstream task is substaintiated and its ACTIVE downstream tasks becomes INVALID
    void DisconnectInvalidVPRowListDefragTask(VPRowListDefragTask*);

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

    TopoRowListGCTask* InsertTopoRowListGCTask(const vid_t&, TopologyRowList*);
    TopoRowListDefragTask* InsertTopoRowListDefragTask(const vid_t&, TopologyRowList*, int);
    EPRowListGCTask* InsertEPRowListGCTask(const CompoundEPRowListID&, PropertyRowList<EdgePropertyRow>*);
    EPRowListDefragTask* InsertEPRowListDefragTask(const CompoundEPRowListID&, PropertyRowList<EdgePropertyRow>*, int);

    void DeleteTopoRowListGCTask(TopoRowListGCTask*);
    void DeleteTopoRowListDefragTask(TopoRowListDefragTask*);
    void DeleteEPRowListGCTask(EPRowListGCTask*);
    void DeleteEPRowListDefragTask(EPRowListDefragTask*);

    void DisconnectInvalidTopoRowListDefragTask(TopoRowListDefragTask*);
    void DisconnectInvalidEPRowListDefragTask(EPRowListDefragTask*);
};

/*
This is the scanning process of in GCProducer::Execute(): (||: one to many, |: one to one)

    vertex_map
        ||
        ||
        ||
      Vertex---------------------------------------------------MVCCList<V> (EraseVTask, VMVCCGCTask, VPRowListGCTask, TopoRowListGCTask)
         |                                       |
         |                                       |
         |                                       |
    VPRowList (VPRowListDefragTask)         TopoRowList (TopoRowListDefragTask)
        ||                                       ||
        ||                                       ||
        ||                                       ||
     VPHeader                                EdgeHeader
         |                                       |
         |                                       |
         |                                       |
    MVCCList<VP> (VPMVCCGCTask)             MVCCList<E> (EMVCCGCTask, EPRowListGCTask)
                                                 ||
                                                 ||
                                                 ||
                                            EdgeMVCCItem
                                                 |
                                                 |
                                                 |
                                            EdgeVersion
                                                 |
                                                 |
                                                 |
                                             EPRowList (EPRowListDefragTask)
                                                 ||
                                                 ||
                                                 ||
                                              EPHeader
                                                 |
                                                 |
                                                 |
                                            MVCCList<EP> (EPMVCCGCTask)


The execution of TopoRowListGCTask will and the abort of transaction can produce some gcable eids.
In GCProducer::check_erasable_eid(), EraseInETask and EraseOutETask will spawn from those tasks.


Besides, for other components outside DataStorage in the code, GC is also needed, and GCProducer will spawn their GCTasks:
    RCTable, IndexStore, TransactionStatusTable, PrimitiveRCTTable.


Dependent task insertion rules:
    For downstream tasks (VPRowListGCTask, EPRowListGCTask, and TopoRowListGCTask):
        Firstly, check if the same-type task with the same id exists. If so, do not generate the new task.
        Then, check if its potential upstream task exists:
            Exists:
                If the upstream task is EMPTY:
                    1. Generate an ACTIVE task, add this task to the related job and GCTaskDAG.
                    2. Connect the new task and the EMPTY upstream task in the GCTaskDAG.
                Else, do not generate the new task.
            Does not exist:
                1. Generate an ACTIVE task, add this task to the related job and GCTaskDAG.
                2. Generate an EMPTY upstream task in the GCTaskDAG, and connect it with the new ACTIVE task.

    For upstream tasks (VPRowListDefragTask, EPRowListDefragTask and TopoRowListDefragTask)
        Check if the same-type tasl with the same id exists.
            Exists:
                Empty:
                    1. Substantiate the empty task with specific target.
                    2. All ACTIVE downstream tasks will become INVALID, and be disconnected from the DAG.
                    3. If any PUSHED downstream tasks remain, the task will become BLOCKED. Else, the task will become ACTIVE.
                    4. Add this task to the related job. If this task is BLOCKED, the job will be blocked until PUSHED downstream tasks are finished.
                Not empty:
                    Just do not generate.
            Does not exist:
                Generate the ACTIVE task, and add it to GCTaskDAG and the related job.


When a job is finished, for all of its PUSHED tasks:
    1. Erase them from the GCTaskDAG.
    2. If some of their EMPTY upstream tasks are no longer connected, those EMPTY tasks should be erased and deleted.
    3. Reduce the blocked_count_ of their BLOCKED upstream tasks. Also, the sum_blocked_count_ in corresponding jobs will be reduced.
*/

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
    friend class GarbageCollector;

 private:
    GCProducer() {}
    GCProducer(const GCProducer&);
    ~GCProducer() {}

    // -------GC Job For Each Type---------
    // When a job is ready to be pushed, a new job will be created and copy the ready job,
    // and the new job will be pushed when the original job below will be cleared.

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

    // Called when deleting finished tasks
    // Reduce block count of upstream jobs, and push out them if ready
    void ReduceVPRowListGCJobBlockCount(VPRowListGCTask*);
    void ReduceEPRowListGCJobBlockCount(EPRowListGCTask*);
    void ReduceTopoRowListGCJobBlockCount(TopoRowListGCTask*);

    // Set task invalid, if its non-empty upstream task is generated
    void SetVPRowListDefragTaskInvalid(VPRowListDefragTask*);
    void SetEPRowListDefragTaskInvalid(EPRowListDefragTask*);
    void SetTopoRowListDefragTaskInvalid(TopoRowListDefragTask*);

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
    void check_erasable_eid();
    void check_erasable_vid();

    void DebugPrint();
};

#include "layout/gc_producer.tpp"
