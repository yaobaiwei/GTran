/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <cstdio>
#include <vector>

#include "layout/concurrent_mem_pool.hpp"
#include "layout/edge_property_row.hpp"
#include "layout/hdfs_data_loader.hpp"
#include "layout/mvcc_kv_store.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/vtx_edge_row.hpp"
#include "layout/vtx_property_row.hpp"
#include "utils/config.hpp"

// Single instance for each process.
// During the OLTP developing, class DataStorage should be the
// only way to access "backend storage".
// When OLTP is runnable, we can do some optimizations, and seperate
// some function to other classes.

// TODO(entityless): Discuss the name of this struct (to store vertex label)
struct VertexItem {
    label_t label;
    VertexEdgeRow* ve_row;
    VertexPropertyRow* vp_row;
};

struct EdgeItem {
    label_t label;
    EdgePropertyRow* ep_row;
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

    // "schema" (indexes)
    string_index* indexes_ = nullptr;

    // Containers
    OffsetConcurrentMemPool<EdgePropertyRow>* ep_row_pool_ = nullptr;
    OffsetConcurrentMemPool<VertexEdgeRow>* ve_row_pool_ = nullptr;
    OffsetConcurrentMemPool<VertexPropertyRow>* vp_row_pool_ = nullptr;
    OffsetConcurrentMemPool<TopoMVCC>* topo_mvcc_pool_ = nullptr;
    OffsetConcurrentMemPool<PropertyMVCC>* property_mvcc_pool_ = nullptr;

 public:
    //// WARNING: those interface below have not been finished yet.
    //// they will be defined after the implementation of data loading.

    //// Data modification, must be thread safe

    // TODO(entityless): figure out (discussion) how to define Timestamp?
    void AddEdge(eid_t eid, uint64_t trx_id, uint64_t begin_time);  // unfinished
    void AddVertex(vid_t vid, uint64_t trx_id, uint64_t begin_time);  // unfinished
    void DropEdge(eid_t eid, uint64_t trx_id, uint64_t begin_time);  // unfinished
    void DropVertex(vid_t vid, uint64_t trx_id, uint64_t begin_time);  // unfinished

    // property
    // construct a new PropertyMVCC list
    void AddVP(vpid_t pid, value_t value, uint64_t trx_id, uint64_t begin_time);  // unfinished
    void AddEP(epid_t pid, value_t value, uint64_t trx_id, uint64_t begin_time);  // unfinished
    // append to existing PropertyMVCC list
    void ModifyVP(vpid_t pid, const value_t& value, uint64_t trx_id, uint64_t begin_time);  // unfinished
    void ModifyEP(epid_t pid, const value_t& value, uint64_t trx_id, uint64_t begin_time);  // unfinished
    void DropVP(vpid_t pid, uint64_t trx_id, uint64_t begin_time);  // unfinished
    void DropEP(epid_t pid, uint64_t trx_id, uint64_t begin_time);  // unfinished

    // "Get" prefix in DataStorage while "Read" prefix in 3 "Row" classes

    // data access
    value_t GetVP(vpid_t pid, uint64_t trx_id, uint64_t begin_time);
    value_t GetEP(epid_t pid, uint64_t trx_id, uint64_t begin_time);
    vector<pair<label_t, value_t>> GetVP(vid_t vid, uint64_t trx_id, uint64_t begin_time);
    vector<pair<label_t, value_t>> GetEP(eid_t eid, uint64_t trx_id, uint64_t begin_time);
    label_t GetVL(vid_t vid, uint64_t trx_id, uint64_t begin_time);
    label_t GetEL(eid_t eid, uint64_t trx_id, uint64_t begin_time);


    // do not need to implement traversal from edge
    // since eid_t contains in_v and out_v

    // traversal from vertex
    // if label == 0, then do not filter by label
    vector<eid_t> GetInEdgeList(vid_t vid, label_t edge_label, uint64_t trx_id, uint64_t begin_time);
    vector<eid_t> GetOutEdgeList(vid_t vid, label_t edge_label, uint64_t trx_id, uint64_t begin_time);
    vector<eid_t> GetBothEdgeList(vid_t vid, label_t edge_label, uint64_t trx_id, uint64_t begin_time);
    vector<vid_t> GetInVertexList(vid_t vid, label_t edge_label, uint64_t trx_id, uint64_t begin_time);
    vector<vid_t> GetOutVertexList(vid_t vid, label_t edge_label, uint64_t trx_id, uint64_t begin_time);
    vector<vid_t> GetBothVertexList(vid_t vid, label_t edge_label, uint64_t trx_id, uint64_t begin_time);

    // TODO(entityless): Figure out how to construct InitMsg
    // TODO(entityless): Use mvcc info for two function below
    // as InitMsg (used by g.V(), g.E() without index) will be changed over time
    vector<vid_t> GetAllVertex(uint64_t trx_id, uint64_t begin_time);  // unfinished
    vector<eid_t> GetAllEdge(uint64_t trx_id, uint64_t begin_time);  // unfinished

    //// Indexed data access
    void GetNameFromIndex(Index_T type, label_t id, string& str);

    //// Non-storage function
    static DataStorage* GetInstance() {
        static DataStorage* data_storage_instance_ptr = nullptr;

        if (data_storage_instance_ptr == nullptr) {
            data_storage_instance_ptr = new DataStorage();
        }

        return data_storage_instance_ptr;
    }

    // Initial related
    void Initial();

    void Test();  // TODO(entityless): remove this in the future
};
