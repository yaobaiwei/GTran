/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include "layout/gc_worker.hpp"

// #define 

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

/* GCScanner encapsulates methods to scan the whole data layout and generate garbage collection tasks to
 * free memory allocated for those objects that are invisible to all transactions in the system.
 *
 * In GCScanner, a single thread will regularly scan the whole data layout and generates GC tasks.
 * If the sum of costs of a specific type of GC task has reach the given threshold, all tasks of this
 * type will be packed as a Job and push to GCExecutor.
 *
 * GCScanner maintains containers of tasks without dependency. For tasks with dependency, their containers
 * are in GCTaskDAG.
 */

class GCScanner {
 private:
    GCScanner() {}
    GCScanner(const GCScanner&);
    ~GCScanner() {}

 public:
    static GCScanner* GetInstance() {
        static GCScanner scanner;
        return &scanner;
    }

    // The function that the GCScanner thread loops
    void Scan();

    // Before scanning the vertex_map_, delete nodes of the finished tasks on the dependency DAG
    void ScanFinishedTasks();

    // The entry of the scanning process
    // Scan MVCCList<VertexMVCCItem> here. If the whole MVCCList is invisible,
    // generates VMVCCGCTask, EraseVMapTask, VPRowListGCTask and TopoRowListGCTask.
    void ScanVertexMap();

    // Called in ScanVertexMap.
    // If the count of empty cells in VPRowList is greater than a threshold, VPRowListDefragTask will be generated.
    void ScanVPRowList(PropertyRowList<VertexPropertyRow>*);

    // Called in ScanVPRowList.
    // Prunes invisible versions and generate VPMVCCGCTask.
    // Returns true if the whole MVCCList is empty after prunning.
    bool ScanVPMVCCList(MVCCList<VPropertyMVCCItem>*);

    // Called in ScanVertexMap.
    // If the count of empty cells in TopoRowList is greater than a threshold, TopoRowListDefragTask will be generated.
    void ScanTopoRowList(TopologyRowList*);

    // Called in ScanTopoRowList.
    // Prunes invisible versions and generate EmptyEdgeGCTask and EdgeEntityGCTask.
    // Returns true if the whole MVCCList is empty after prunning,
    // as well as generates EraseOutEMapTask and EraseInEMapTask.
    bool ScanEdgeMVCCList(MVCCList<EdgeMVCCItem>*);

    // Called in ScanEdgeMVCCList.
    // If the count of empty cells in EPRowList is greater than a threshold, EPRowListDefragTask will be generated.
    void ScanEPRowList(PropertyRowList<EdgePropertyRow>*);

    // Called in ScanEPRowList.
    // Prunes invisible versions and generate EPMVCCGCTask.
    // Returns true if the whole MVCCList is empty after prunning.
    bool ScanEPMVCCList(MVCCList<EPropertyMVCCItem>*);

    // In case of passing parameter
    vid_t current_vid_;
    eid_t current_eid_;

    // Independent tasks are managed here
    vector<EraseVTask*> erase_v_map_tasks_;
    vector<VMVCCGCTask*> v_mvcc_gc_tasks_;
    vector<EraseOutETask*> erase_oute_map_tasks_;
    vector<EraseInETask*> erase_ine_map_tasks_;
    vector<VPMVCCGCTask*> vp_mvcc_gc_tasks_;
    vector<EmptyEdgeGCTask*> empty_edge_gc_tasks_;
    vector<EPMVCCGCTask*> ep_mvcc_gc_tasks_;

    void PushJob(vector<AbstractGCTask>&);
};
