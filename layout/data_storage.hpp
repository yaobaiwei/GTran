/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <cstdio>
#include <vector>

#include "layout/concurrent_mem_pool.hpp"
#include "layout/hdfs_data_loader.hpp"
#include "layout/mpi_snapshot_manager.hpp"
#include "layout/mvcc_kv_store.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/property_row_list.hpp"
#include "layout/row_definition.hpp"
#include "layout/topology_row_list.hpp"
#include "utils/config.hpp"

struct VertexItem {
    label_t label;
    TopologyRowList* ve_row_list;
    PropertyRowList<VertexPropertyRow>* vp_row_list;
    MVCCList<TopologyMVCC>* mvcc_list;
};

struct EdgeItem {
    label_t label;
    PropertyRowList<EdgePropertyRow>* ep_row_list;
    // this mvcc_list pointer will point to the same MVCCList instance in VERow
    MVCCList<TopologyMVCC>* mvcc_list;
};

class DataStorage {
 private:
    DataStorage() {}
    DataStorage(const DataStorage&);

    bool ReadSnapshot();  // atomic, all or nothing
    void WriteSnapshot();
    void CreateContainer();
    void FillContainer();  // since MVCC is used, the initial data will be treated as the first version

    Config* config_;
    Node node_;

    // from vid & eid to the first row of the entity
    tbb::concurrent_hash_map<uint64_t, EdgeItem> edge_map_;
    typedef tbb::concurrent_hash_map<uint64_t, EdgeItem>::accessor EdgeAccessor;
    typedef tbb::concurrent_hash_map<uint64_t, EdgeItem>::const_accessor EdgeConstAccessor;
    tbb::concurrent_hash_map<uint32_t, VertexItem> vertex_map_;
    typedef tbb::concurrent_hash_map<uint32_t, VertexItem>::accessor VertexAccessor;
    typedef tbb::concurrent_hash_map<uint32_t, VertexItem>::const_accessor VertexConstAccessor;
    MVCCKVStore* vp_store_ = nullptr;
    MVCCKVStore* ep_store_ = nullptr;

    // for data initial
    HDFSDataLoader* hdfs_data_loader_ = nullptr;
    MPISnapshotManager* snapshot_manager_ = nullptr;

    // "schema" (indexes)
    string_index* indexes_ = nullptr;

    // Containers
    OffsetConcurrentMemPool<EdgePropertyRow>* ep_row_pool_ = nullptr;
    OffsetConcurrentMemPool<VertexEdgeRow>* ve_row_pool_ = nullptr;
    OffsetConcurrentMemPool<VertexPropertyRow>* vp_row_pool_ = nullptr;
    OffsetConcurrentMemPool<TopologyMVCC>* topology_mvcc_pool_ = nullptr;
    OffsetConcurrentMemPool<PropertyMVCC>* property_mvcc_pool_ = nullptr;

 public:
    // "Get" prefix in DataStorage while "Read" prefix in 3 "Row" classes

    // data access
    void GetVP(const vpid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& ret);
    void GetEP(const epid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& ret);
    void GetVP(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
               vector<pair<label_t, value_t>>& ret);
    void GetEP(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time,
               vector<pair<label_t, value_t>>& ret);
    label_t GetVL(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time);
    label_t GetEL(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time);

    // do not need to implement traversal from edge
    // since eid_t contains in_v and out_v

    // traversal from vertex
    // if label == 0, then do not filter by label
    void GetConnectedEdgeList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                              const uint64_t& trx_id, const uint64_t& begin_time, vector<eid_t>& ret);
    void GetConnectedVertexList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                const uint64_t& trx_id, const uint64_t& begin_time, vector<vid_t>& ret);

    // TODO(entityless): Figure out how to construct InitMsg
    // TODO(entityless): Use mvcc info for two function below
    // as InitMsg (used by g.V(), g.E() without index) will be changed over time
    void GetAllVertex(const uint64_t& trx_id, const uint64_t& begin_time, vector<vid_t>& ret);
    void GetAllEdge(const uint64_t& trx_id, const uint64_t& begin_time, vector<eid_t>& ret);

    //// Indexed data access
    void GetNameFromIndex(const Index_T& type, const label_t& id, string& str);

    //// Non-storage function
    static DataStorage* GetInstance() {
        static DataStorage* data_storage_instance_ptr = nullptr;

        if (data_storage_instance_ptr == nullptr) {
            data_storage_instance_ptr = new DataStorage();
        }

        return data_storage_instance_ptr;
    }

    // Initial related
    void Init();

    void PrintLoadedData();  // TODO(entityless): remove this in the future
};
