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
#include "layout/mpi_snapshot_manager.hpp"
#include "layout/mvcc_list.hpp"
#include "layout/mvcc_value_store.hpp"
#include "layout/property_row_list.hpp"
#include "layout/row_definition.hpp"
#include "layout/topology_row_list.hpp"
#include "utils/concurrent_unordered_map.hpp"
#include "utils/config.hpp"
#include "utils/mymath.hpp"
#include "utils/tid_mapper.hpp"
#include "utils/write_prior_rwlock.hpp"

struct TrxProcessHistory {
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

    /* When we want to abort AddV,
     * we need to find the Vertex in the vertex_map_,
     * and free all elements attached to the Vertex.
     * Thus, addv_map is used to map from mvcc_list pointer to vid,
     * as this map will be modified in ProcessAddV function.
     */
    std::unordered_map<void*, uint32_t> addv_map;
};

class GCProducer;
class GCConsumer;
class GarbageCollector;

class DataStorage {
 private:
    DataStorage() {}
    DataStorage(const DataStorage&);

    bool ReadSnapshot();  // atomic, all or nothing
    void WriteSnapshot();
    void CreateContainer();
    // since MVCC is used, the initial data will be treated as the first version
    void FillVertexContainer();
    void FillEdgeContainer();

    Config* config_ = nullptr;
    Node node_;
    SimpleIdMapper* id_mapper_ = nullptr;
    GarbageCollector* garbage_collector_;
    int nthreads_;

    /* the MVCCList<EdgeMVCCItem>* pointer will be the same in the EdgeHeader in
     * corresponding Vertex's ve_row_list
     */
    ConcurrentUnorderedMap<uint64_t, OutEdge> out_edge_map_;
    ConcurrentUnorderedMap<uint64_t, InEdge> in_edge_map_;
    // since edge properties are attached to out_e, the in_e instance does not record any properties
    typedef ConcurrentUnorderedMap<uint64_t, OutEdge>::accessor OutEdgeAccessor;
    typedef ConcurrentUnorderedMap<uint64_t, OutEdge>::const_accessor OutEdgeConstAccessor;
    typedef ConcurrentUnorderedMap<uint64_t, InEdge>::accessor InEdgeAccessor;
    typedef ConcurrentUnorderedMap<uint64_t, InEdge>::const_accessor InEdgeConstAccessor;
    // the vid of Vertex is unique
    ConcurrentUnorderedMap<uint32_t, Vertex> vertex_map_;
    typedef ConcurrentUnorderedMap<uint32_t, Vertex>::accessor VertexAccessor;
    typedef ConcurrentUnorderedMap<uint32_t, Vertex>::const_accessor VertexConstAccessor;
    // These three locks are only used to avoid conflict between
    // erase operator (always batch erase) and others (insert, find);
    // write_lock -> erase
    // read_lock -> others
    // concurrency for others is guaranteed by ConcurrentUnorderedMap
    WritePriorRWLock vertex_map_erase_rwlock_;
    WritePriorRWLock out_edge_erase_rwlock_;
    WritePriorRWLock in_edge_erase_rwlock_;

    MVCCValueStore* vp_store_ = nullptr;
    MVCCValueStore* ep_store_ = nullptr;

    // for data initialization
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

    // Notice that even if a vertex is dropped, num_of_vertex_ will not decrease.
    std::atomic_int num_of_vertex_;
    int worker_rank_, worker_size_;
    vid_t AssignVID();

    /* Records the process history of a transaction in Process phase, used in Abort or Commit
     *     uint64_t          : trx_id
     *     TrxProcessHistory : process history
     */
    tbb::concurrent_hash_map<uint64_t, TrxProcessHistory> transaction_history_map_;
    typedef tbb::concurrent_hash_map<uint64_t, TrxProcessHistory>::accessor TransactionAccessor;
    typedef tbb::concurrent_hash_map<uint64_t, TrxProcessHistory>::const_accessor TransactionConstAccessor;

    // Record process type and pointer of MVCCList.
    // Should be called in each ProcessXXX function except ProcessAddV
    void InsertTrxHistoryMap(const uint64_t& trx_id, const TrxProcessHistory::ProcessType& type, void* mvcc_list);
    // Alternative of InsertTrxHistoryMap for ProcessAddV
    void InsertTrxHistoryMapAddV(const uint64_t& trx_id, void* mvcc_list, vid_t vid);

    // aggregated data related
    unordered_map<agg_t, vector<value_t>> agg_data_table;
    mutex agg_mutex;

    // TrxTableStub
    TrxTableStub * trx_table_stub_ = nullptr;

    // e_accessor and item_ref will be modified
    READ_STAT GetOutEdgeItem(OutEdgeConstAccessor& out_e_accessor, const eid_t& eid, const uint64_t& trx_id,
                             const uint64_t& begin_time, const bool& read_only, EdgeVersion& item_ref);

    READ_STAT CheckVertexVisibility(const VertexConstAccessor& v_accessor, const uint64_t& trx_id,
                                    const uint64_t& begin_time, const bool& read_only);

    friend class GCProducer;
    friend class GCConsumer;

 public:
    // data access of vertices
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
    // data access of edges
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

    /* do not need to implement GetInV and GetOutV of EdgeVersion since eid_t contains in_v and out_v
     * if edge_label == 0, then do not filter by edge_label
     */
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

    // Check visibility for vertex/edge
    bool CheckVertexVisibility(const uint64_t& trx_id, const uint64_t& begin_time, const bool& read_only, vid_t& vid);
    bool CheckEdgeVisibility(const uint64_t& trx_id, const uint64_t& begin_time, const bool& read_only, eid_t& eid);

    // Transaction processing stage related
    vid_t ProcessAddV(const label_t& label, const uint64_t& trx_id, const uint64_t& begin_time);
    PROCESS_STAT ProcessDropV(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                              vector<eid_t>& in_eids, vector<eid_t>& out_eids);
    PROCESS_STAT ProcessAddE(const eid_t& eid, const label_t& label, const bool& is_out,
                             const uint64_t& trx_id, const uint64_t& begin_time);
    PROCESS_STAT ProcessDropE(const eid_t& eid, const bool& is_out, const uint64_t& trx_id, const uint64_t& begin_time);
    PROCESS_STAT ProcessModifyVP(const vpid_t& pid, const value_t& value, value_t& old_value, const uint64_t& trx_id, const uint64_t& begin_time);
    PROCESS_STAT ProcessModifyEP(const epid_t& pid, const value_t& value, value_t& old_value, const uint64_t& trx_id, const uint64_t& begin_time);
    PROCESS_STAT ProcessDropVP(const vpid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& old_value);
    PROCESS_STAT ProcessDropEP(const epid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& old_value);

    /* Transaction abort or commit
     * For each transaction, on each worker, Commit or Abort will need to be called only once.
     */
    void Commit(const uint64_t& trx_id, const uint64_t& commit_time);
    void Abort(const uint64_t& trx_id);

    // Indexed data access
    // Dynamic label (V, E, VP, EP) creation is not enabled.
    void GetNameFromIndex(const Index_T& type, const label_t& id, string& str);

    static DataStorage* GetInstance() {
        static DataStorage* data_storage_instance_ptr = nullptr;

        if (data_storage_instance_ptr == nullptr) {
            data_storage_instance_ptr = new DataStorage();
        }

        return data_storage_instance_ptr;
    }

    // aggregated data related
    // TODO(entityless): Optimize this, as these implementations are extremely inefficient
    void InsertAggData(agg_t key, vector<value_t> & data);
    void GetAggData(agg_t key, vector<value_t> & data);
    void DeleteAggData(uint64_t qid);

    bool VPKeyIsLocal(vpid_t vp_id);
    bool EPKeyIsLocal(epid_t ep_id);

    // Initialization related
    void Init();

    // Dependency Read
    void GetDepReadTrxList(uint64_t trxID, set<uint64_t> & homoTrxDList, set<uint64_t> & heteroTrxIDList);
    void CleanDepReadTrxList(uint64_t trxID);

    // "schema" (indexes)
    string_index* indexes_ = nullptr;
};
