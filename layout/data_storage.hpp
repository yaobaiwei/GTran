/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <cstdio>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "core/factory.hpp"
#include "layout/concurrent_mem_pool.hpp"
#include "layout/hdfs_data_loader.hpp"
#include "layout/gc_executor.hpp"
#include "layout/mpi_snapshot_manager.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/mvcc_value_store.hpp"
#include "layout/property_row_list.hpp"
#include "layout/row_definition.hpp"
#include "layout/topology_row_list.hpp"
#include "utils/config.hpp"
#include "utils/mymath.hpp"
#include "utils/tid_mapper.hpp"

// Edge defined in mvcc_definition.hpp

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

        bool operator== (const ProcessItem& right_item) const {
            return mvcc_list == right_item.mvcc_list;
        }
    };

    struct ProcessItemHash {
        size_t operator()(const ProcessItem& _r) const {
            return mymath::hash_u64((uint64_t)_r.mvcc_list);
        }
    };

    std::vector<ProcessItem> process_vector;

    // extra metadata is needed when abort AddV
    // we need to find the Vertex in the vertex_map_
    // and free any element attached to the Vertex
    // mapping from mvcc_list to vid
    std::unordered_map<void*, uint32_t> addv_map;
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
    int nthreads_;

    // from vid & eid to the first row of the entity
    // the MVCCList<EdgeMVCCItem>* pointer will point to the same instance
    // in this edge's src_v's VertexEdgeRow's element's mvcc_list
    tbb::concurrent_hash_map<uint64_t, MVCCList<EdgeMVCCItem>*> out_edge_map_;
    tbb::concurrent_hash_map<uint64_t, MVCCList<EdgeMVCCItem>*> in_edge_map_;
    // since edge properties are attached to out_e, the in_e instance does not record any properties
    typedef tbb::concurrent_hash_map<uint64_t, MVCCList<EdgeMVCCItem>*>::accessor EdgeAccessor;
    typedef tbb::concurrent_hash_map<uint64_t, MVCCList<EdgeMVCCItem>*>::const_accessor EdgeConstAccessor;
    tbb::concurrent_hash_map<uint32_t, Vertex> vertex_map_;
    typedef tbb::concurrent_hash_map<uint32_t, Vertex>::accessor VertexAccessor;
    typedef tbb::concurrent_hash_map<uint32_t, Vertex>::const_accessor VertexConstAccessor;
    MVCCValueStore* vp_store_ = nullptr;
    MVCCValueStore* ep_store_ = nullptr;

    // for data initial
    HDFSDataLoader* hdfs_data_loader_ = nullptr;
    MPISnapshotManager* snapshot_manager_ = nullptr;

    // Containers
    ConcurrentMemPool<EdgePropertyRow>* ep_row_pool_ = nullptr;
    ConcurrentMemPool<VertexEdgeRow>* ve_row_pool_ = nullptr;
    ConcurrentMemPool<VertexPropertyRow>* vp_row_pool_ = nullptr;
    ConcurrentMemPool<VPropertyMVCCItem>* vp_mvcc_pool_ = nullptr;
    ConcurrentMemPool<EPropertyMVCCItem>* ep_mvcc_pool_ = nullptr;
    ConcurrentMemPool<VertexMVCCItem>* vertex_mvcc_pool_ = nullptr;
    ConcurrentMemPool<EdgeMVCCItem>* edge_mvcc_pool_ = nullptr;

    // VID related. Used when adding a new vertex.
    std::atomic_int vid_to_assign_divided_;
    int worker_rank_, worker_size_;
    vid_t AssignVID();

    // MVCC processing related
    tbb::concurrent_hash_map<uint64_t, TransactionItem> transaction_process_map_;
    typedef tbb::concurrent_hash_map<uint64_t, TransactionItem>::accessor TransactionAccessor;
    typedef tbb::concurrent_hash_map<uint64_t, TransactionItem>::const_accessor TransactionConstAccessor;
    void InsertTrxProcessMapStd(const uint64_t& trx_id, const TransactionItem::ProcessType& type, void* mvcc_list);
    void InsertTrxProcessMapAddV(const uint64_t& trx_id, const TransactionItem::ProcessType& type,
                                 void* mvcc_list, vid_t vid);

    // DataStore compatible
    unordered_map<agg_t, vector<value_t>> agg_data_table;
    mutex agg_mutex;

    // TrxTableStub
    TrxTableStub * trx_table_stub_ = nullptr;

    // Garbage Collection
    GCExecutor* gc_executor_ = nullptr;

    READ_STAT GetOutEdgeItem(EdgeConstAccessor& e_accessor, const eid_t& eid, const uint64_t& trx_id,
                             const uint64_t& begin_time, const bool& read_only, Edge& item_ref);

    READ_STAT CheckVertexVisibility(VertexConstAccessor& v_accessor, const uint64_t& trx_id,
                                    const uint64_t& begin_time, const bool& read_only);
    READ_STAT CheckVertexVisibility(VertexAccessor& v_accessor, const uint64_t& trx_id,
                                    const uint64_t& begin_time, const bool& read_only);

 public:
    // any public data access interfaces
    // data access
    READ_STAT GetVPByPKey(const vpid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time,
                          const bool& read_only, value_t& ret);
    READ_STAT GetAllVP(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                       const bool& read_only, vector<pair<label_t, value_t>>& ret);
    READ_STAT GetVPByPKeyList(const vid_t& vid, const vector<label_t>& p_key,
                              const uint64_t& trx_id, const uint64_t& begin_time,
                              const bool& read_only, vector<pair<label_t, value_t>>& ret);
    READ_STAT GetVPidList(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                          const bool& read_only, vector<vpid_t>& ret);
    READ_STAT GetVL(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                    const bool& read_only, label_t& ret);

    READ_STAT GetEPByPKey(const epid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time,
                          const bool& read_only, value_t& ret);
    READ_STAT GetAllEP(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time,
                       const bool& read_only, vector<pair<label_t, value_t>>& ret);
    READ_STAT GetEPByPKeyList(const eid_t& eid, const vector<label_t>& p_key,
                         const uint64_t& trx_id, const uint64_t& begin_time,
                         const bool& read_only, vector<pair<label_t, value_t>>& ret);
    READ_STAT GetEPidList(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time,
                          const bool& read_only, vector<epid_t>& ret);
    READ_STAT GetEL(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time,
                    const bool& read_only, label_t& ret);
    // do not need to implement traversal from edge since eid_t contains in_v and out_v

    // traversal from vertex
    // if label == 0, then do not filter by label
    READ_STAT GetConnectedVertexList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                     const uint64_t& trx_id, const uint64_t& begin_time,
                                     const bool& read_only, vector<vid_t>& ret);
    READ_STAT GetConnectedEdgeList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                   const uint64_t& trx_id, const uint64_t& begin_time,
                                   const bool& read_only, vector<eid_t>& ret);

    // TODO(entityless): Figure out how to run two functions efficiently
    READ_STAT GetAllVertices(const uint64_t& trx_id, const uint64_t& begin_time,
                             const bool& read_only, vector<vid_t>& ret);
    READ_STAT GetAllEdges(const uint64_t& trx_id, const uint64_t& begin_time,
                          const bool& read_only, vector<eid_t>& ret);

    // MVCC processing stage related
    vid_t ProcessAddV(const label_t& label, const uint64_t& trx_id, const uint64_t& begin_time);
    bool ProcessDropV(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                      vector<eid_t>& in_eids, vector<eid_t>& out_eids);
    bool ProcessAddE(const eid_t& eid, const label_t& label, const bool& is_out,
                     const uint64_t& trx_id, const uint64_t& begin_time);
    bool ProcessDropE(const eid_t& eid, const bool& is_out, const uint64_t& trx_id, const uint64_t& begin_time);
    bool ProcessModifyVP(const vpid_t& pid, const value_t& value, const uint64_t& trx_id, const uint64_t& begin_time);
    bool ProcessModifyEP(const epid_t& pid, const value_t& value, const uint64_t& trx_id, const uint64_t& begin_time);
    bool ProcessDropVP(const vpid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time);
    bool ProcessDropEP(const epid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time);

    // MVCC abort or commit
    void Commit(const uint64_t& trx_id, const uint64_t& commit_time);
    void Abort(const uint64_t& trx_id);

    //// Indexed data access
    void GetNameFromIndex(const Index_T& type, const label_t& id, string& str);

    static DataStorage* GetInstance() {
        static DataStorage* data_storage_instance_ptr = nullptr;

        if (data_storage_instance_ptr == nullptr) {
            data_storage_instance_ptr = new DataStorage();
        }

        return data_storage_instance_ptr;
    }

    // DataStore compatible
    // TODO(entityless): Optimize this, as these implementations are extremely inefficient
    void InsertAggData(agg_t key, vector<value_t> & data);
    void GetAggData(agg_t key, vector<value_t> & data);
    void DeleteAggData(uint64_t qid);

    bool VPKeyIsLocal(vpid_t vp_id);
    bool EPKeyIsLocal(epid_t ep_id);


    // Initial related
    void Init();

    // Dependency Read
    void GetDepReadTrxList(uint64_t trxID, vector<uint64_t> & homoTrxDList, vector<uint64_t> & heteroTrxIDList);
    void CleanDepReadTrxList(uint64_t trxID);

    void PrintLoadedData();  // TODO(entityless): remove this in the future

    // "schema" (indexes)
    string_index* indexes_ = nullptr;
};
