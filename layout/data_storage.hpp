// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
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

#include <cstdio>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tbb/concurrent_unordered_map.h"

#include "core/factory.hpp"
#include "layout/hdfs_data_loader.hpp"
#include "layout/layout_type.hpp"
#include "utils/config.hpp"
#include "utils/mymath.hpp"


/* For non-readonly transaction, arbitrary MVCCLists can be modified during the Processing Phase.
 * In the Commit/Abort Phase, to commit or rollback the transaction, we just need pointers of modified MVCCLists.
 *
 * Specifically, due to the implementation, to rollback AddV operation, vid is needed beside MVCCList*.
 *
 * On each worker, one TrxProcessHistory object corresponds to one non-readonly transaction, and contains
 *      all MVCCLists* and vids to be used in the Commit/Abort phase.
 */

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

    struct ProcessRecord {
        ProcessType type;
        void* mvcc_list;
        ProcessRecord() {}

        bool operator== (const ProcessRecord& right_item) const {
            return mvcc_list == right_item.mvcc_list;
        }
    };

    struct ProcessRecordHash {
        size_t operator()(const ProcessRecord& _r) const {
            return mymath::hash_u64((uint64_t)_r.mvcc_list);
        }
    };

    std::vector<ProcessRecord> process_vector;

    /* Mapping from MVCCList* to vid.
     * Introduced here since aborting AddV needs both MVCCList* and vid, while ProcessRecord only records MVCCList*.
     * Modified in DataStorage::InsertTrxAddVHistory.
     * Used in DataStorage::Abort.
     */
    std::unordered_map<void*, uint32_t> mvcclist_to_vid_map;
};

class GCProducer;
class GCConsumer;
class GarbageCollector;
class ThroughputMonitor;

class DataStorage {
 private:
    DataStorage() {}
    DataStorage(const DataStorage&);

    // ================ Creating and filling containers ================
    void CreateContainer();
    void FillVertexContainer();
    void FillEdgeContainer();


    // ================ Printing the loading progress ================
    // For each type of tmp container (V, InE, OutE), how many line will be printed during its loading process.
    static constexpr int progress_print_count_ = 10;

    // For example, when progress_print_count_ == 10, and the size of tmp container of V is 50,
    // threshold_print_progress_v_[2] will be 15, which indicates that when the loading of 15th
    // element in tmp container of V is finished, the 3rd line of progress (indicating 30% is finished)
    // will be printed.
    int threshold_print_progress_v_[progress_print_count_];
    int threshold_print_progress_out_e_[progress_print_count_];
    int threshold_print_progress_in_e_[progress_print_count_];

    // Functions related to progress printing
    void InitPrintFillVProgress();
    void InitPrintFillEProgress();
    void PrintFillingProgress(int idx, int& printed_count, const int thresholds[], string progress_header);


    // ================ Printing the usage of containers ================
    typedef map<string, pair<size_t, size_t>> UsageMap;  // pair<used, total>
    string GetUsageString(UsageMap usage_map);
    void PredictVertexContainerUsage();
    void PredictEdgeContainerUsage();
    UsageMap GetContainerUsageMap();


    // ================ Containers ================
    int container_nthreads_;  // count of threads that will use ConcurrentMemPool and MVCCValueStore
    ConcurrentMemPool<EdgePropertyRow>* ep_row_pool_ = nullptr;
    ConcurrentMemPool<VertexEdgeRow>* ve_row_pool_ = nullptr;
    ConcurrentMemPool<VertexPropertyRow>* vp_row_pool_ = nullptr;
    ConcurrentMemPool<VPropertyMVCCItem>* vp_mvcc_pool_ = nullptr;
    ConcurrentMemPool<EPropertyMVCCItem>* ep_mvcc_pool_ = nullptr;
    ConcurrentMemPool<VertexMVCCItem>* vertex_mvcc_pool_ = nullptr;
    ConcurrentMemPool<EdgeMVCCItem>* edge_mvcc_pool_ = nullptr;
    MVCCValueStore* vp_store_ = nullptr;
    MVCCValueStore* ep_store_ = nullptr;


    // ================ Vertex map and edge maps ================
    /* When adding an out edge with eid on a vertex, an MVCCList<EdgeMVCCItem> will be created and attached to TopologyRowList. Then,
     * The pointer of MVCCList<EdgeMVCCItem> will be inserted to the out_edge_map_. Similar for adding an in edge.

        Edge properties are attached to the out edge. Thus, for looking up edge properties, we need to get the OutEdge in the out_edge_map_.
     */
    tbb::concurrent_unordered_map<uint64_t, OutEdge> out_edge_map_;
    tbb::concurrent_unordered_map<uint64_t, InEdge> in_edge_map_;
    // since edge properties are attached to out_e, the in_e instance does not record any properties
    typedef tbb::concurrent_unordered_map<uint64_t, OutEdge>::iterator OutEdgeIterator;
    typedef tbb::concurrent_unordered_map<uint64_t, OutEdge>::const_iterator OutEdgeConstIterator;
    typedef tbb::concurrent_unordered_map<uint64_t, InEdge>::iterator InEdgeIterator;
    typedef tbb::concurrent_unordered_map<uint64_t, InEdge>::const_iterator InEdgeConstIterator;

    tbb::concurrent_unordered_map<uint32_t, Vertex> vertex_map_;
    typedef tbb::concurrent_unordered_map<uint32_t, Vertex>::iterator VertexIterator;
    typedef tbb::concurrent_unordered_map<uint32_t, Vertex>::const_iterator VertexConstIterator;

    // These three locks are only used to avoid conflict between
    // erase operator (always batch erase) and others (insert, find);
    // write_lock -> erase
    // read_lock -> others
    // concurrency for others is guaranteed by concurrent_unordered_map
    WritePriorRWLock vertex_map_erase_rwlock_;
    WritePriorRWLock out_edge_erase_rwlock_;
    WritePriorRWLock in_edge_erase_rwlock_;


    // ================ Vid assignment ================
    std::atomic_int num_of_vertex_local_;  // the number of vertices on this worker, modified in AssignVID
    // Notice that even if a vertex is dropped, num_of_vertex_local_ will not decrease.
    vid_t AssignVID();


    // ================ Recording modifications in transactions ================
    // Map of TrxProcessHistory indexed with trx_id.
    tbb::concurrent_hash_map<uint64_t, TrxProcessHistory> transaction_process_history_map_;
    typedef tbb::concurrent_hash_map<uint64_t, TrxProcessHistory>::accessor TransactionAccessor;
    typedef tbb::concurrent_hash_map<uint64_t, TrxProcessHistory>::const_accessor TransactionConstAccessor;

    // Record process type and pointer of MVCCList for non-readonly transaction.
    void InsertTrxProcessHistory(const uint64_t& trx_id, const TrxProcessHistory::ProcessType& type, void* mvcc_list);
    // Specifically, for AddV operation, vid need to be recorded in addition
    void InsertTrxAddVHistory(const uint64_t& trx_id, void* mvcc_list, vid_t vid);


    // ================ Locate a vertex or an edge in the maps ================
    READ_STAT CheckVertexVisibility(const VertexConstIterator& v_iterator, const uint64_t& trx_id,
                                    const uint64_t& begin_time, const bool& read_only);
    READ_STAT GetVertexIterator(VertexConstIterator& v_iterator, const vid_t& vid, const uint64_t& trx_id,
                                const uint64_t& begin_time, const bool& read_only);
    // for an eid, there can be multiple versions of edges
    READ_STAT GetOutEdgeVersion(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time,
                                const bool& read_only, EdgeVersion& item_ref);


    // ================ Aggregated data related ================
    unordered_map<agg_t, vector<value_t>> agg_data_table;
    mutex agg_mutex;


    // ================ Other instances and variables ================
    Config* config_ = nullptr;
    Node node_;
    SimpleIdMapper* id_mapper_ = nullptr;
    GarbageCollector* garbage_collector_;
    HDFSDataLoader* hdfs_data_loader_ = nullptr;
    TrxTableStub * trx_table_stub_ = nullptr;
    int worker_rank_, worker_size_;


    // ================ Friend classes ================
    friend class GCProducer;
    friend class GCConsumer;
    friend class ThroughputMonitor;

 public:
    // ================ Data access of vertices ================
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
    READ_STAT GetAllVertices(const uint64_t& trx_id, const uint64_t& begin_time,
                             const bool& read_only, vector<vid_t>& ret);
    READ_STAT GetConnectedVertexList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                     const uint64_t& trx_id, const uint64_t& begin_time,
                                     const bool& read_only, vector<vid_t>& ret);
    READ_STAT GetConnectedEdgeList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                   const uint64_t& trx_id, const uint64_t& begin_time,
                                   const bool& read_only, vector<eid_t>& ret, bool need_read_lock = true);
    bool CheckVertexVisibilityWithVid(const uint64_t& trx_id, const uint64_t& begin_time, const bool& read_only, vid_t& vid);


    // ================ Data access of edges ================
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
    READ_STAT GetAllEdges(const uint64_t& trx_id, const uint64_t& begin_time,
                          const bool& read_only, vector<eid_t>& ret);
    bool CheckEdgeVisibilityWithEid(const uint64_t& trx_id, const uint64_t& begin_time, const bool& read_only, eid_t& eid);


    // ================ Data modification ================
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

    // Transaction abort or commit. For each transaction, on each worker, Commit or Abort will need to be called only once.
    void Commit(const uint64_t& trx_id, const uint64_t& commit_time);
    void Abort(const uint64_t& trx_id);


    // ================ Data modification ================
    static DataStorage* GetInstance() {
        static DataStorage* data_storage_instance_ptr = nullptr;

        if (data_storage_instance_ptr == nullptr) {
            data_storage_instance_ptr = new DataStorage();
        }

        return data_storage_instance_ptr;
    }

    // aggregated data related
    void InsertAggData(agg_t key, vector<value_t> & data);
    void GetAggData(agg_t key, vector<value_t> & data);
    void DeleteAggData(uint64_t qid);

    // Initialization related
    void Init();

    // Dependency Read
    void GetDepReadTrxList(uint64_t trxID, set<uint64_t> & homoTrxDList, set<uint64_t> & heteroTrxIDList);
    void CleanDepReadTrxList(uint64_t trxID);

    // "Schema" (indexes)
    void GetNameFromIndex(const Index_T& type, const label_t& id, string& str);
    string_index* indexes_ = nullptr;

    // display container usage at runtime
    string GetContainerUsageString();
    double GetContainerUsage();
};
