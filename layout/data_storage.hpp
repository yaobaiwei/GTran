/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <cstdio>
#include <vector>

#include "layout/concurrent_mem_pool.hpp"
#include "layout/hdfs_data_loader.hpp"
#include "layout/mpi_snapshot_manager.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/mvcc_value_store.hpp"
#include "layout/property_row_list.hpp"
#include "layout/row_definition.hpp"
#include "layout/topology_row_list.hpp"
#include "utils/config.hpp"

struct VertexItem {
    label_t label;
    TopologyRowList* ve_row_list;
    PropertyRowList<VertexPropertyRow>* vp_row_list;
    MVCCList<VertexMVCC>* mvcc_list;
};
// EdgeItem defined in mvcc_definition.hpp

struct TransactionItem {
    enum ProcessType {
        PROCESS_ADD_V,
        PROCESS_ADD_E,
        PROCESS_DROP_V,
        PROCESS_DROP_E,
        PROCESS_ADD_VP,
        PROCESS_ADD_EP,
        PROCESS_DROP_VP,
        PROCESS_DROP_EP,
        PROCESS_MODIFY_VP,
        PROCESS_MODIFY_EP
    };

    struct ProcessItem {
        ProcessType type;
        void* mvcc_list;
        ProcessItem() {}
    };

    // TODO(entityless): figure out what if mvcc_list duplicates
    std::vector<ProcessItem> process_list;
};

class DataStorage {
 private:
    DataStorage() {}
    DataStorage(const DataStorage&);

    bool ReadSnapshot();  // atomic, all or nothing
    void WriteSnapshot();
    void CreateContainer();
    void FillContainer();  // since MVCC is used, the initial data will be treated as the first version

    Config* config_ = nullptr;
    Node node_;
    SimpleIdMapper* id_mapper_ = nullptr;

    // from vid & eid to the first row of the entity
    // the MVCCList<EdgeMVCC>* pointer will point to the same instance
    // in this edge's src_v's VertexEdgeRow's element's mvcc_list
    tbb::concurrent_hash_map<uint64_t, MVCCList<EdgeMVCC>*> edge_map_;
    typedef tbb::concurrent_hash_map<uint64_t, MVCCList<EdgeMVCC>*>::accessor EdgeAccessor;
    typedef tbb::concurrent_hash_map<uint64_t, MVCCList<EdgeMVCC>*>::const_accessor EdgeConstAccessor;
    tbb::concurrent_hash_map<uint32_t, VertexItem> vertex_map_;
    typedef tbb::concurrent_hash_map<uint32_t, VertexItem>::accessor VertexAccessor;
    typedef tbb::concurrent_hash_map<uint32_t, VertexItem>::const_accessor VertexConstAccessor;
    MVCCValueStore* vp_store_ = nullptr;
    MVCCValueStore* ep_store_ = nullptr;

    // for data initial
    HDFSDataLoader* hdfs_data_loader_ = nullptr;
    MPISnapshotManager* snapshot_manager_ = nullptr;

    // "schema" (indexes)
    string_index* indexes_ = nullptr;

    // Containers
    OffsetConcurrentMemPool<EdgePropertyRow>* ep_row_pool_ = nullptr;
    OffsetConcurrentMemPool<VertexEdgeRow>* ve_row_pool_ = nullptr;
    OffsetConcurrentMemPool<VertexPropertyRow>* vp_row_pool_ = nullptr;
    OffsetConcurrentMemPool<PropertyMVCC>* property_mvcc_pool_ = nullptr;
    OffsetConcurrentMemPool<VertexMVCC>* vertex_mvcc_pool_ = nullptr;
    OffsetConcurrentMemPool<EdgeMVCC>* edge_mvcc_pool_ = nullptr;

    // VID related. Used when adding a new vertex.
    std::atomic_int vid_to_assign_divided_;
    int worker_rank_, worker_size_;
    vid_t AssignVID();

    // MVCC processing related
    tbb::concurrent_hash_map<uint64_t, TransactionItem> transaction_map_;
    typedef tbb::concurrent_hash_map<uint64_t, TransactionItem>::accessor TransactionAccessor;
    typedef tbb::concurrent_hash_map<uint64_t, TransactionItem>::const_accessor TransactionConstAccessor;

 public:
    // MVCC processing stage related
    // fail if return vid = 0
    vid_t ProcessAddVertex(const label_t& label, const uint64_t& trx_id, const uint64_t& begin_time);
    bool ProcessDropVertex(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time);
    bool ProcessModifyVP(const vpid_t& pid, const value_t& value, const uint64_t& trx_id, const uint64_t& begin_time);
    bool ProcessModifyEP(const epid_t& pid, const value_t& value, const uint64_t& trx_id, const uint64_t& begin_time);
    // TODO(entityless): Finish unfinished process functions

    // MVCC abort or commit
    void Commit(const uint64_t& trx_id, const uint64_t& commit_time);
    void Abort(const uint64_t& trx_id);

    // data access
    void GetVP(const vpid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& ret);
    void GetEP(const epid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& ret);
    void GetVP(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
               vector<pair<label_t, value_t>>& ret);
    void GetEP(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time,
               vector<pair<label_t, value_t>>& ret);
    void GetVPidList(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                     vector<vpid_t>& ret);
    void GetEPidList(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time,
                     vector<epid_t>& ret);
    label_t GetVL(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time);
    label_t GetEL(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time);  // TODO(entityless): add "read_only" flag

    // do not need to implement traversal from edge since eid_t contains in_v and out_v

    // traversal from vertex
    // if label == 0, then do not filter by label
    void GetConnectedEdgeList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                              const uint64_t& trx_id, const uint64_t& begin_time, vector<eid_t>& ret);
    void GetConnectedVertexList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                const uint64_t& trx_id, const uint64_t& begin_time, vector<vid_t>& ret);

    // TODO(entityless): Figure out how to run two functions below efficiently
    void GetAllVertex(const uint64_t& trx_id, const uint64_t& begin_time, vector<vid_t>& ret);
    void GetAllEdge(const uint64_t& trx_id, const uint64_t& begin_time, vector<eid_t>& ret);

    //// Indexed data access
    void GetNameFromIndex(const Index_T& type, const label_t& id, string& str);

    static DataStorage* GetInstance() {
        static DataStorage* data_storage_instance_ptr = nullptr;

        if (data_storage_instance_ptr == nullptr) {
            data_storage_instance_ptr = new DataStorage();
        }

        return data_storage_instance_ptr;
    }

    // Initial related
    void Init();

    // Dependency Read
    void GetDepReadTrxList(uint64_t trxID, vector<uint64_t> & homoTrxDList, vector<uint64_t> & heteroTrxIDList);
    void CleanDepReadTrxList(uint64_t trxID);

    void PrintLoadedData();  // TODO(entityless): remove this in the future
    void PropertyMVCCTest();
};
