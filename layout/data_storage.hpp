/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <cstdio>

#include "layout/concurrent_mem_pool.hpp"
#include "layout/edge_property_row.hpp"
#include "layout/hdfs_data_loader.hpp"
#include "layout/mvcc_kv_store.hpp"
#include "layout/vtx_edge_row.hpp"
#include "layout/vtx_property_row.hpp"
#include "utils/config.hpp"

// Single instance for each process.
// During the OLTP developing, class DataStorage should be the
// only way to access "backend storage".
// When OLTP is runnable, we can do some optimizations, and seperate
// some function to other classes.

// // TODO(entityless): Discuss the name of this struct (to store vertex label)
// struct LocalVertexItem
// {
//     ;
// }

class DataStorage
{
private:
    DataStorage(){};
    DataStorage(const DataStorage&);

    Config* config_;
    Node node_;

    // from vid & eid to the first row of the entity
    tbb::concurrent_hash_map<uint64_t, EdgePropertyRow*> edge_map_;
    typedef tbb::concurrent_hash_map<uint64_t, EdgePropertyRow*>::accessor EdgeAccessor;
    typedef tbb::concurrent_hash_map<uint64_t, EdgePropertyRow*>::const_accessor EdgeConstAccessor;
    tbb::concurrent_hash_map<uint32_t, pair<VertexEdgeRow*, VertexPropertyRow*>> vertex_map_;
    typedef tbb::concurrent_hash_map<uint32_t, pair<VertexEdgeRow*, VertexPropertyRow*>>::accessor VertexAccessor;
    typedef tbb::concurrent_hash_map<uint32_t, pair<VertexEdgeRow*, VertexPropertyRow*>>::const_accessor VertexConstAccessor;
    MVCCKVStore* vp_store_ = nullptr;
    MVCCKVStore* ep_store_ = nullptr;

    // for data initial
    HDFSDataLoader* hdfs_data_loader_ = nullptr;

    // "schema" (indexes)
    string_index* indexes_;

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
    // topo
    void AddEdge();
    void AddVertex();
    void DropEdge();
    void DropVertex();
    // property
    void AddVP();
    void AddEP();
    void ModifyVP();
    void ModifyEP();
    void DropVP();
    void DropEP();

    //// Normal data access. May be remote; parameter TBD.
    // TODO: grant multi-key access
    value_t ReadVP();
    value_t ReadEP();
    vector<uint64_t> GetInEdgeList();
    vector<uint64_t> GetOutEdgeList();
    vector<uint64_t> GetBothEdgeList();

    //// Indexed data access, TBD

    //// Non-storage function
    static DataStorage* GetInstance()
    {
        static DataStorage* data_storage_instance_ptr = nullptr;

        if(data_storage_instance_ptr == nullptr)
        {
            data_storage_instance_ptr = new DataStorage();
        }

        return data_storage_instance_ptr;
    }

    // Initial related
    void Initial();
    bool ReadSnapshot();  // atomic, all or nothing
    void WriteSnapshot();
    void CreateContainer();
    void FillContainer();  // since MVCC is used, the initial data will be treated as the first version
};
