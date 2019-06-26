/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "layout/data_storage.hpp"

template<class MVCC> ConcurrentMemPool<MVCC>* MVCCList<MVCC>::mem_pool_ = nullptr;
template<class PropertyRow> ConcurrentMemPool<PropertyRow>* PropertyRowList<PropertyRow>::mem_pool_ = nullptr;
template<class PropertyRow> MVCCValueStore* PropertyRowList<PropertyRow>::value_storage_ = nullptr;
ConcurrentMemPool<VertexEdgeRow>* TopologyRowList::mem_pool_ = nullptr;
MVCCValueStore* VPropertyMVCCItem::value_store = nullptr;
MVCCValueStore* EPropertyMVCCItem::value_store = nullptr;

void DataStorage::Init() {
    node_ = Node::StaticInstance();
    config_ = Config::GetInstance();
    id_mapper_ = SimpleIdMapper::GetInstance();
    snapshot_manager_ = MPISnapshotManager::GetInstance();
    worker_rank_ = node_.get_local_rank();
    worker_size_ = node_.get_local_size();
    nthreads_ = config_->global_num_threads + 1;  // allow the main thread to use memory pool
    TidMapper::GetInstance()->Register(config_->global_num_threads);  // register the main thread in TidMapper

    node_.Rank0PrintfWithWorkerBarrier(
                      "VE_ROW_ITEM_COUNT = %d, sizeof(EdgeHeader) = %d, sizeof(VertexEdgeRow) = %d\n",
                       VE_ROW_ITEM_COUNT, sizeof(EdgeHeader), sizeof(VertexEdgeRow));
    node_.Rank0PrintfWithWorkerBarrier(
                       "VP_ROW_ITEM_COUNT = %d, sizeof(VPHeader) = %d, sizeof(VertexPropertyRow) = %d\n",
                       VP_ROW_ITEM_COUNT, sizeof(VPHeader), sizeof(VertexPropertyRow));
    node_.Rank0PrintfWithWorkerBarrier(
                       "EP_ROW_ITEM_COUNT = %d, sizeof(EPHeader) = %d, sizeof(EdgePropertyRow) = %d\n",
                       EP_ROW_ITEM_COUNT, sizeof(EPHeader), sizeof(EdgePropertyRow));
    node_.Rank0PrintfWithWorkerBarrier(
                       "sizeof(PropertyMVCCItem) = %d, sizeof(VertexMVCCItem) = %d, sizeof(EdgeMVCCItem) = %d\n",
                       sizeof(PropertyMVCCItem), sizeof(VertexMVCCItem), sizeof(EdgeMVCCItem));

    CreateContainer();

    snapshot_manager_->SetRootPath(config_->SNAPSHOT_PATH);
    snapshot_manager_->AppendConfig("HDFS_INDEX_PATH", config_->HDFS_INDEX_PATH);
    snapshot_manager_->AppendConfig("HDFS_VTX_SUBFOLDER", config_->HDFS_VTX_SUBFOLDER);
    snapshot_manager_->AppendConfig("HDFS_VP_SUBFOLDER", config_->HDFS_VP_SUBFOLDER);
    snapshot_manager_->AppendConfig("HDFS_EP_SUBFOLDER", config_->HDFS_EP_SUBFOLDER);
    snapshot_manager_->SetComm(node_.local_comm);
    snapshot_manager_->ConfirmConfig();

    hdfs_data_loader_ = HDFSDataLoader::GetInstance();
    hdfs_data_loader_->LoadData();
    FillContainer();
    PrintLoadedData();
    hdfs_data_loader_->FreeMemory();

    trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();

    gc_executor_ = GCExecutor::GetInstance();
    gc_executor_->Init(&out_edge_map_, &in_edge_map_, &vertex_map_, vp_store_, ep_store_);

    node_.Rank0PrintfWithWorkerBarrier("DataStorage::Init() all finished\n");
}

void DataStorage::CreateContainer() {
    ve_row_pool_ = ConcurrentMemPool<VertexEdgeRow>::GetInstance(
                            nullptr, config_->global_ve_row_pool_size, nthreads_);
    vp_row_pool_ = ConcurrentMemPool<VertexPropertyRow>::GetInstance(
                            nullptr, config_->global_vp_row_pool_size, nthreads_);
    ep_row_pool_ = ConcurrentMemPool<EdgePropertyRow>::GetInstance(
                            nullptr, config_->global_ep_row_pool_size, nthreads_);
    vp_mvcc_pool_ = ConcurrentMemPool<VPropertyMVCCItem>::GetInstance(
                            nullptr, config_->global_property_mvcc_pool_size, nthreads_);
    ep_mvcc_pool_ = ConcurrentMemPool<EPropertyMVCCItem>::GetInstance(
                            nullptr, config_->global_property_mvcc_pool_size, nthreads_);
    vertex_mvcc_pool_ = ConcurrentMemPool<VertexMVCCItem>::GetInstance(
                            nullptr, config_->global_topo_mvcc_pool_size, nthreads_);
    edge_mvcc_pool_ = ConcurrentMemPool<EdgeMVCCItem>::GetInstance(
                            nullptr, config_->global_topo_mvcc_pool_size, nthreads_);

    MVCCList<VPropertyMVCCItem>::SetGlobalMemoryPool(vp_mvcc_pool_);
    MVCCList<EPropertyMVCCItem>::SetGlobalMemoryPool(ep_mvcc_pool_);
    MVCCList<VertexMVCCItem>::SetGlobalMemoryPool(vertex_mvcc_pool_);
    MVCCList<EdgeMVCCItem>::SetGlobalMemoryPool(edge_mvcc_pool_);
    PropertyRowList<EdgePropertyRow>::SetGlobalMemoryPool(ep_row_pool_);
    PropertyRowList<VertexPropertyRow>::SetGlobalMemoryPool(vp_row_pool_);
    TopologyRowList::SetGlobalMemoryPool(ve_row_pool_);

    uint64_t vp_sz = GiB2B(config_->global_vertex_property_kv_sz_gb);
    uint64_t ep_sz = GiB2B(config_->global_edge_property_kv_sz_gb);
    vp_store_ = new MVCCValueStore(nullptr, vp_sz / MEM_ITEM_SIZE, nthreads_);
    ep_store_ = new MVCCValueStore(nullptr, ep_sz / MEM_ITEM_SIZE, nthreads_);
    PropertyRowList<VertexPropertyRow>::SetGlobalValueStore(vp_store_);
    PropertyRowList<EdgePropertyRow>::SetGlobalValueStore(ep_store_);
    VPropertyMVCCItem::SetGlobalValueStore(vp_store_);
    EPropertyMVCCItem::SetGlobalValueStore(ep_store_);
}

void DataStorage::FillContainer() {
    indexes_ = hdfs_data_loader_->indexes_;

    // access elements in HDFSDataLoader
    hash_map<uint32_t, TMPVertex*>& v_map = hdfs_data_loader_->vtx_part_map_;
    hash_map<uint64_t, TMPEdge*>& e_map = hdfs_data_loader_->edge_part_map_;

    int max_vid = worker_rank_;

    for (auto vtx : hdfs_data_loader_->shuffled_vtx_) {
        VertexAccessor v_accessor;
        vertex_map_.insert(v_accessor, vtx.id.value());

        if (max_vid < vtx.id.value())
            max_vid = vtx.id.value();

        v_accessor->second.label = vtx.label;
        // create containers that attached to a Vertex
        v_accessor->second.vp_row_list = new PropertyRowList<VertexPropertyRow>;
        v_accessor->second.ve_row_list = new TopologyRowList;

        v_accessor->second.vp_row_list->Init();
        v_accessor->second.ve_row_list->Init(vtx.id);

        v_accessor->second.mvcc_list = new MVCCList<VertexMVCCItem>;
        v_accessor->second.mvcc_list->AppendInitialVersion()[0] = true;
        // true means that the Vertex is visible

        // Insert in edges
        for (auto in_nb : vtx.in_nbs) {
            eid_t eid = eid_t(vtx.id.vid, in_nb.vid);

            InEdgeAccessor in_e_accessor;
            in_edge_map_.insert(in_e_accessor, eid.value());

            // "false" means that is_out = false, as this edge is an inE to the Vertex
            auto* mvcc_list = v_accessor->second.ve_row_list
                              ->InsertInitialCell(false, in_nb, e_map[eid.value()]->label, nullptr);

            // pointer of MVCCList<EdgeMVCCItem> is shared with in_edge_map_
            in_e_accessor->second.mvcc_list = mvcc_list;
        }

        // Insert out edges
        for (auto out_nb : vtx.out_nbs) {
            eid_t eid = eid_t(out_nb.vid, vtx.id.vid);

            OutEdgeAccessor out_e_accessor;
            out_edge_map_.insert(out_e_accessor, eid.value());
            auto* ep_row_list = new PropertyRowList<EdgePropertyRow>;
            ep_row_list->Init();

            // "true" means that is_out = true, as this edge is an outE to the Vertex
            auto* mvcc_list = v_accessor->second.ve_row_list
                              ->InsertInitialCell(true, out_nb, e_map[eid.value()]->label, ep_row_list);

            // pointer of MVCCList<EdgeMVCCItem> is shared with out_edge_map_
            out_e_accessor->second.mvcc_list = mvcc_list;
        }

        // Insert vertex properties
        for (int i = 0; i < vtx.vp_label_list.size(); i++) {
            v_accessor->second.vp_row_list->InsertInitialCell(vpid_t(vtx.id, vtx.vp_label_list[i]),
                                                                 vtx.vp_value_list[i]);
        }
    }

    num_of_vertex_ = (max_vid - worker_rank_) / worker_size_;

    node_.Rank0PrintfWithWorkerBarrier("DataStorage::FillContainer() load vtx finished\n");

    // Insert edge properties
    for (auto edge : hdfs_data_loader_->shuffled_edge_) {
        OutEdgeConstAccessor out_e_accessor;
        out_edge_map_.find(out_e_accessor, edge.id.value());

        EdgeMVCCItem* edge_mvcc;

        EdgeVersion edge_version;
        out_e_accessor->second.mvcc_list->GetVisibleVersion(0, 0, true, edge_version);

        for (int i = 0; i < edge.ep_label_list.size(); i++) {
            edge_version.ep_row_list->InsertInitialCell(epid_t(edge.id, edge.ep_label_list[i]), edge.ep_value_list[i]);
        }
    }

    #ifdef OFFSET_MEMORY_POOL_DEBUG
    node_.LocalSequentialDebugPrint("ve_row_pool_: " + ve_row_pool_->UsageString());
    node_.LocalSequentialDebugPrint("vp_row_pool_: " + vp_row_pool_->UsageString());
    node_.LocalSequentialDebugPrint("ep_row_pool_: " + ep_row_pool_->UsageString());
    node_.LocalSequentialDebugPrint("vp_mvcc_pool_: " + vp_mvcc_pool_->UsageString());
    node_.LocalSequentialDebugPrint("ep_mvcc_pool_: " + ep_mvcc_pool_->UsageString());
    node_.LocalSequentialDebugPrint("vertex_mvcc_pool_: " + vertex_mvcc_pool_->UsageString());
    node_.LocalSequentialDebugPrint("edge_mvcc_pool_: " + edge_mvcc_pool_->UsageString());
    #endif  // OFFSET_MEMORY_POOL_DEBUG
    #ifdef MVCC_VALUE_STORE_DEBUG
    node_.LocalSequentialDebugPrint("vp_store_: " + vp_store_->UsageString());
    node_.LocalSequentialDebugPrint("ep_store_: " + ep_store_->UsageString());
    #endif  // MVCC_VALUE_STORE_DEBUG

    node_.Rank0PrintfWithWorkerBarrier("DataStorage::FillContainer() finished\n");
}

READ_STAT DataStorage::GetOutEdgeItem(OutEdgeConstAccessor& out_e_accessor, const eid_t& eid,
                                      const uint64_t& trx_id, const uint64_t& begin_time,
                                      const bool& read_only, EdgeVersion& item_ref) {
    bool found = out_edge_map_.find(out_e_accessor, eid.value());

    if (!found) {
        printf("Impossible branch in DataStorage::GetOutEdgeItem\n");
        assert(false);
    }

    pair<bool, bool> is_visible = out_e_accessor->second.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, item_ref);

    if (!is_visible.first)
        return READ_STAT::ABORT;
    if (!is_visible.second)
        return READ_STAT::NOTFOUND;

    return READ_STAT::SUCCESS;
}

READ_STAT DataStorage::CheckVertexVisibility(const VertexConstAccessor& v_accessor, const uint64_t& trx_id,
                                             const uint64_t& begin_time, const bool& read_only) {
    VertexMVCCItem* visible_version;
    bool exists;
    pair<bool, bool> is_visible = v_accessor->second.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, exists);
    if (!is_visible.first)
        return READ_STAT::ABORT;
    // How to deal with "not found" is determined by the function calling it
    if (!is_visible.second)
        return READ_STAT::NOTFOUND;
    if (!exists)
        return READ_STAT::NOTFOUND;
    return READ_STAT::SUCCESS;
}

READ_STAT DataStorage::GetVPByPKey(const vpid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time,
                                   const bool& read_only, value_t& ret) {
    vid_t vid = pid.vid;

    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    if (!found) {
        printf("Impossible branch in DataStorage::GetVPByPKey\n");
        assert(false);
    }

    /* Check if the vertex with given vid is invisible, which means that the read dependency (vertex)
     * of this transaction has been modified.
     * Similarly hereinafter.
     */
    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {

        /* Update the global status of this transaction to ABORT.
         * Similarly hereinafter.
         */
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    auto stat = v_accessor->second.vp_row_list->ReadProperty(pid, trx_id, begin_time, read_only, ret);

    if (stat == READ_STAT::ABORT)
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

    return stat;
}

READ_STAT DataStorage::GetAllVP(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                                const bool& read_only, vector<pair<label_t, value_t>>& ret) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    if (!found) {
        printf("Impossible branch in DataStorage::GetAllVP\n");
        assert(false);
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    auto stat = v_accessor->second.vp_row_list->ReadAllProperty(trx_id, begin_time, read_only, ret);

    if (stat == READ_STAT::ABORT)
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

    return stat;
}

READ_STAT DataStorage::GetVPByPKeyList(const vid_t& vid, const vector<label_t>& p_key,
                                       const uint64_t& trx_id, const uint64_t& begin_time,
                                       const bool& read_only, vector<pair<label_t, value_t>>& ret) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    if (!found) {
        printf("Impossible branch in DataStorage::GetVPByPKeyList\n");
        assert(false);
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    auto stat = v_accessor->second.vp_row_list->ReadPropertyByPKeyList(p_key, trx_id, begin_time, read_only, ret);

    if (stat == READ_STAT::ABORT)
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

    return stat;
}

READ_STAT DataStorage::GetVPidList(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                                   const bool& read_only, vector<vpid_t>& ret) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    if (!found) {
        printf("Impossible branch in DataStorage::GetVPidList\n");
        assert(false);
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    auto stat = v_accessor->second.vp_row_list->ReadPidList(trx_id, begin_time, read_only, ret);

    if (stat == READ_STAT::ABORT)
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

    return stat;
}

READ_STAT DataStorage::GetVL(const vid_t& vid, const uint64_t& trx_id,
                             const uint64_t& begin_time, const bool& read_only, label_t& ret) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    if (!found) {
        printf("Impossible branch in DataStorage::GetVL\n");
        assert(false);
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    ret = v_accessor->second.label;

    return READ_STAT::SUCCESS;
}

READ_STAT DataStorage::GetEPByPKey(const epid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time,
                                   const bool& read_only, value_t& ret) {
    eid_t eid = eid_t(pid.in_vid, pid.out_vid);

    OutEdgeConstAccessor out_e_accessor;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_accessor, eid, trx_id, begin_time, read_only, edge_version);
    if (read_stat != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    if (edge_version.Exist()) {
        auto stat = edge_version.ep_row_list->ReadProperty(pid, trx_id, begin_time, read_only, ret);

        if (stat == READ_STAT::ABORT)
            trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

        return stat;
    }

    /* The edge with given pid.eid is invisible, which means that the read dependency (edge)
     * of this transaction has been modified.
     * Similarly hereinafter.
     */
    trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
    return READ_STAT::ABORT;
}

READ_STAT DataStorage::GetAllEP(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time,
                                const bool& read_only, vector<pair<label_t, value_t>>& ret) {
    OutEdgeConstAccessor out_e_accessor;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_accessor, eid, trx_id, begin_time, read_only, edge_version);
    if (read_stat != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    if (edge_version.Exist()) {
       auto stat = edge_version.ep_row_list->ReadAllProperty(trx_id, begin_time, read_only, ret);

       if (stat == READ_STAT::ABORT)
            trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

        return stat;
    }

    trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
    return READ_STAT::ABORT;
}

READ_STAT DataStorage::GetEPByPKeyList(const eid_t& eid, const vector<label_t>& p_key,
                                       const uint64_t& trx_id, const uint64_t& begin_time,
                                       const bool& read_only, vector<pair<label_t, value_t>>& ret) {
    OutEdgeConstAccessor out_e_accessor;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_accessor, eid, trx_id, begin_time, read_only, edge_version);
    if (read_stat != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    if (edge_version.Exist()) {
        auto stat = edge_version.ep_row_list->ReadPropertyByPKeyList(p_key, trx_id, begin_time, read_only, ret);

        if (stat == READ_STAT::ABORT)
            trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

        return stat;
    }

    trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
    return READ_STAT::ABORT;
}

READ_STAT DataStorage::GetEPidList(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time,
                                   const bool& read_only, vector<epid_t>& ret) {
    OutEdgeConstAccessor out_e_accessor;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_accessor, eid, trx_id, begin_time, read_only, edge_version);
    if (read_stat != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    if (edge_version.Exist()) {
        auto stat = edge_version.ep_row_list->ReadPidList(trx_id, begin_time, read_only, ret);

        if (stat == READ_STAT::ABORT)
            trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

        return stat;
    }

    trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
    return READ_STAT::ABORT;
}

READ_STAT DataStorage::GetEL(const eid_t& eid, const uint64_t& trx_id,
                             const uint64_t& begin_time, const bool& read_only, label_t& ret) {
    OutEdgeConstAccessor out_e_accessor;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_accessor, eid, trx_id, begin_time, read_only, edge_version);
    if (read_stat != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    if (edge_version.Exist()) {
        ret = edge_version.label;
        return READ_STAT::SUCCESS;
    }

    trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
    return READ_STAT::ABORT;
}

READ_STAT DataStorage::GetConnectedVertexList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                              const uint64_t& trx_id, const uint64_t& begin_time,
                                              const bool& read_only, vector<vid_t>& ret) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    if (!found) {
        printf("Impossible branch in DataStorage::GetConnectedVertexList\n");
        assert(false);
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    auto stat = v_accessor->second.ve_row_list->ReadConnectedVertex(direction, edge_label,
                                                                    trx_id, begin_time, read_only, ret);

    if (stat == READ_STAT::ABORT)
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

    return stat;
}

READ_STAT DataStorage::GetConnectedEdgeList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                            const uint64_t& trx_id, const uint64_t& begin_time,
                                            const bool& read_only, vector<eid_t>& ret) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    if (!found) {
        printf("Impossible branch in DataStorage::GetConnectedEdgeList\n");
        assert(false);
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    auto stat = v_accessor->second.ve_row_list->ReadConnectedEdge(direction, edge_label,
                                                                  trx_id, begin_time, read_only, ret);

    if (stat == READ_STAT::ABORT)
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

    return stat;
}

READ_STAT DataStorage::GetAllVertices(const uint64_t& trx_id, const uint64_t& begin_time,
                                      const bool& read_only, vector<vid_t>& ret) {
    for (auto v_pair = vertex_map_.begin(); v_pair != vertex_map_.end(); v_pair++) {
        auto& v_item = v_pair->second;

        bool exists;
        MVCCList<VertexMVCCItem>* mvcc_list = v_item.mvcc_list;

        // just inserted
        if (mvcc_list == nullptr)
            continue;

        pair<bool, bool> is_visible = mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, exists);
        if (!is_visible.first) {
            trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
            return READ_STAT::ABORT;
        }

        if (!is_visible.second)
            continue;

        // if this vertice is visible
        if (exists)
            ret.emplace_back(vid_t(v_pair->first));
    }

    return READ_STAT::SUCCESS;
}

READ_STAT DataStorage::GetAllEdges(const uint64_t& trx_id, const uint64_t& begin_time,
                                   const bool& read_only, vector<eid_t>& ret) {
    // TODO(entityless): Simplify the code by editing eid_t::value()
    for (auto e_pair = out_edge_map_.begin(); e_pair != out_edge_map_.end(); e_pair++) {
        EdgeMVCCItem* visible_version;
        EdgeVersion edge_version;

        MVCCList<EdgeMVCCItem>* mvcc_list = e_pair->second.mvcc_list;

        // just inserted
        if (mvcc_list == nullptr)
            continue;

        pair<bool, bool> is_visible = mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, edge_version);
        if (!is_visible.first) {
            trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
            return READ_STAT::ABORT;
        }

        if (!is_visible.second)
            continue;

        if (edge_version.Exist()) {
            uint64_t eid_fetched = e_pair->first;
            eid_t* tmp_eid_p = reinterpret_cast<eid_t*>(&eid_fetched);
            ret.emplace_back(eid_t(tmp_eid_p->out_v, tmp_eid_p->in_v));
        }
    }

    return READ_STAT::SUCCESS;
}

bool DataStorage::CheckVertexVisibility(const uint64_t& trx_id, const uint64_t& begin_time,
                                        const bool& read_only, vid_t& vid) {
    // Check visibility of the vertex
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    if (!found) {
        printf("Impossible branch in DataStorage::CheckVertexVisibility\n");
        assert(false);
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        // Invisible
        return false;
    }
    return true;
}

bool DataStorage::CheckEdgeVisibility(const uint64_t& trx_id, const uint64_t& begin_time,
                                      const bool& read_only, eid_t& eid) {
    // Check visibility of the edge 
    OutEdgeConstAccessor out_e_accessor;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_accessor, eid, trx_id, begin_time, read_only, edge_version);
    if (read_stat != READ_STAT::SUCCESS) {
        // Invisible
        return false;
    }

    if (!edge_version.Exist()) {
        // Invisible
        return false;
    }

    return true;
}

void DataStorage::InsertAggData(agg_t key, vector<value_t> & data) {
    lock_guard<mutex> lock(agg_mutex);

    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
    if (itr == agg_data_table.end()) {
        // Not Found, insert
        agg_data_table.insert(pair<agg_t, vector<value_t>>(key, data));
    } else {
        agg_data_table.at(key).insert(agg_data_table.at(key).end(), data.begin(), data.end());
    }
}

void DataStorage::GetAggData(agg_t key, vector<value_t> & data) {
    lock_guard<mutex> lock(agg_mutex);

    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
    if (itr != agg_data_table.end()) {
        data = itr->second;
    }
}

void DataStorage::DeleteAggData(uint64_t qid) {
    lock_guard<mutex> lock(agg_mutex);

    uint8_t se_label = 0;
    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(agg_t(qid, se_label++));
    while (itr != agg_data_table.end()) {
        agg_data_table.erase(itr);
        itr = agg_data_table.find(agg_t(qid, se_label++));
    }
}

bool DataStorage::VPKeyIsLocal(vpid_t vp_id) {
    if (id_mapper_->IsVPropertyLocal(vp_id)) {
        return true;
    } else {
        return false;
    }
}

bool DataStorage::EPKeyIsLocal(epid_t ep_id) {
    if (id_mapper_->IsEPropertyLocal(ep_id)) {
        return true;
    } else {
        return false;
    }
}

void DataStorage::GetNameFromIndex(const Index_T& type, const label_t& id, string& str) {
    unordered_map<label_t, string>::const_iterator itr;

    switch (type) {
        case Index_T::E_LABEL:
            itr = indexes_->el2str.find(id);
            if (itr == indexes_->el2str.end())
                return;
            else
                str = itr->second;
            break;
        case Index_T::E_PROPERTY:
            itr = indexes_->epk2str.find(id);
            if (itr == indexes_->epk2str.end())
                return;
            else
                str = itr->second;
            break;
        case Index_T::V_LABEL:
            itr = indexes_->vl2str.find(id);
            if (itr == indexes_->vl2str.end())
                return;
            else
                str = itr->second;
            break;
        case Index_T::V_PROPERTY:
            itr = indexes_->vpk2str.find(id);
            if (itr == indexes_->vpk2str.end())
                return;
            else
                str = itr->second;
            break;
        default:
            return;
    }
}

void DataStorage::GetDepReadTrxList(uint64_t trxID, set<uint64_t> & homoTrxIDList,
                                    set<uint64_t> & heteroTrxIDList) {
    dep_trx_const_accessor c_accessor;

    if (dep_trx_map.find(c_accessor, trxID)) {
        homoTrxIDList = c_accessor->second.homo_trx_list;
        heteroTrxIDList =  c_accessor->second.hetero_trx_list;
    }
}

void DataStorage::CleanDepReadTrxList(uint64_t trxID) {
    dep_trx_accessor accessor;

    if (dep_trx_map.find(accessor, trxID)) {
        dep_trx_map.erase(accessor);
    }
}

void DataStorage::PrintLoadedData() {
    node_.LocalSequentialStart();

    if (hdfs_data_loader_->shuffled_vtx_.size() < 20 && hdfs_data_loader_->shuffled_edge_.size() < 40) {
        for (auto vtx : hdfs_data_loader_->shuffled_vtx_) {
            TMPVertex tmp_vtx;
            tmp_vtx.id = vtx.id;

            GetVL(vtx.id, 0x8000000000000001, 1, true, tmp_vtx.label);

            // vector<pair<label_t, value_t>> properties;
            // GetVP(vtx.id, 0x8000000000000001, 1, true, properties);
            // for (auto p : properties) {
            //     tmp_vtx.vp_label_list.push_back(p.first);
            //     tmp_vtx.vp_value_list.push_back(p.second);
            // }

            // for (int i = 0; i < vtx.vp_label_list.size(); i++) {
            //     tmp_vtx.vp_label_list.push_back(vtx.vp_label_list[i]);
            //     value_t val;
            //     GetAllVP(vpid_t(vtx.id, vtx.vp_label_list[i]), 0x8000000000000001, 1, true, val);
            //     tmp_vtx.vp_value_list.push_back(val);
            // }

            // vector<vpid_t> vpids;
            // GetVPidList(vtx.id, 0x8000000000000001, 1, true, vpids);
            // for (int i = 0; i < vpids.size(); i++) {
            //     tmp_vtx.vp_label_list.push_back(vpids[i].pid);
            //     value_t val;
            //     GetVPByPKey(vpids[i], 0x8000000000000001, 1, true, val);
            //     tmp_vtx.vp_value_list.push_back(val);
            // }

            vector<pair<label_t, value_t>> properties;
            GetVPByPKeyList(vtx.id, vtx.vp_label_list, 0x8000000000000001, 1, true, properties);
            for (auto p : properties) {
                tmp_vtx.vp_label_list.push_back(p.first);
                tmp_vtx.vp_value_list.push_back(p.second);
            }

            // vector<vid_t> in_nbs;
            // GetConnectedVertexList(vtx.id, 0, IN, 0x8000000000000001, 1, true, in_nbs);
            // for (auto in_nb : in_nbs) {
            //     tmp_vtx.in_nbs.push_back(in_nb);
            // }

            // vector<eid_t> in_es;
            // GetConnectedEdgeList(vtx.id, 0, IN, 0x8000000000000001, 1, true, in_es);
            // for (auto in_e : in_es) {
            //     tmp_vtx.in_nbs.push_back(in_e.out_v);
            // }

            // vector<vid_t> out_nbs;
            // GetConnectedVertexList(vtx.id, 0, OUT, 0x8000000000000001, 1, true, out_nbs);
            // for (auto out_nb : out_nbs) {
            //     tmp_vtx.out_nbs.push_back(out_nb);
            // }

            // vector<eid_t> out_es;
            // GetConnectedEdgeList(vtx.id, 0, OUT, 0x8000000000000001, 1, true, out_es);
            // for (auto out_e : out_es) {
            //     tmp_vtx.out_nbs.push_back(out_e.in_v);
            // }

            // vector<vid_t> both_nbs;
            // GetConnectedVertexList(vtx.id, 0, BOTH, 0x8000000000000001, 1, true, both_nbs);
            // for (auto both_nb : both_nbs) {
            //     tmp_vtx.in_nbs.push_back(both_nb);
            // }

            vector<eid_t> both_es;
            GetConnectedEdgeList(vtx.id, 0, BOTH, 0x8000000000000001, 1, true, both_es);
            for (auto out_e : both_es) {
                if (out_e.in_v == vtx.id)
                    tmp_vtx.in_nbs.push_back(out_e.out_v);
                else
                    tmp_vtx.out_nbs.push_back(out_e.in_v);
            }

            printf(("   original: " + vtx.DebugString() + "\n").c_str());
            printf(("constructed: " + tmp_vtx.DebugString() + "\n").c_str());
        }

        vector<vid_t> all_vtx_id;
        GetAllVertices(0x8000000000000001, 1, true, all_vtx_id);
        printf("all vtx id: [");
        for (auto vtx_id : all_vtx_id) {
            printf("%d ", vtx_id.value());
        }
        printf("]\n");

        fflush(stdout);

        for (auto edge : hdfs_data_loader_->shuffled_edge_) {
            TMPEdge tmp_edge;
            tmp_edge.id = edge.id;

             GetEL(edge.id, 0x8000000000000001, 1, true, tmp_edge.label);

            // vector<pair<label_t, value_t>> properties;
            // GetAllEP(edge.id, 0x8000000000000001, 1, true, properties);
            // for (auto p : properties) {
            //     tmp_edge.ep_label_list.push_back(p.first);
            //     tmp_edge.ep_value_list.push_back(p.second);
            // }

            // for (int i = 0; i < edge.ep_label_list.size(); i++) {
            //     tmp_edge.ep_label_list.push_back(edge.ep_label_list[i]);
            //     value_t val;
            //     GetEPByPKey(epid_t(edge.id, edge.ep_label_list[i]), 0x8000000000000001, 1, true, val);
            //     tmp_edge.ep_value_list.push_back(val);
            // }

            // vector<epid_t> epids;
            // GetEPidList(edge.id, 0x8000000000000001, 1, true, epids);
            // for (int i = 0; i < edge.ep_label_list.size(); i++) {
            //     tmp_edge.ep_label_list.push_back(epids[i].pid);
            //     value_t val;
            //     GetEPByPKey(epids[i], 0x8000000000000001, 1, true, val);
            //     tmp_edge.ep_value_list.push_back(val);
            // }

            vector<pair<label_t, value_t>> properties;
            GetEPByPKeyList(edge.id, edge.ep_label_list, 0x8000000000000001, 1, true, properties);
            for (auto p : properties) {
                tmp_edge.ep_label_list.push_back(p.first);
                tmp_edge.ep_value_list.push_back(p.second);
            }

            printf(("   original: " + edge.DebugString() + "\n").c_str());
            printf(("constructed: " + tmp_edge.DebugString() + "\n").c_str());
        }

        vector<eid_t> all_edge_id;
        GetAllEdges(0x8000000000000001, 1, true, all_edge_id);
        printf("all edge id: [");
        for (auto edge_id : all_edge_id) {
            printf("%d->%d ", edge_id.out_v, edge_id.in_v);
        }
        printf("]\n");

        fflush(stdout);
    }

    node_.LocalSequentialEnd();
}

vid_t DataStorage::AssignVID() {
    int vid_local = ++num_of_vertex_;
    return vid_t(vid_local * worker_size_ + worker_rank_);
}

/* For each Process function, a MVCCList instance will be modified. InsertTrxHistoryMap will record the pointer
 * of MVCCList in corresponding trx's TrxProcessHistory, which will be necessary when calling Abort or Commit.
 */
void DataStorage::InsertTrxHistoryMap(const uint64_t& trx_id, const TrxProcessHistory::ProcessType& type,
                                         void* mvcc_list) {
    assert(type != TrxProcessHistory::PROCESS_ADD_V);
    TransactionAccessor t_accessor;
    transaction_history_map_.insert(t_accessor, trx_id);

    TrxProcessHistory::ProcessItem q_item;
    q_item.type = type;
    q_item.mvcc_list = mvcc_list;

    t_accessor->second.process_vector.emplace_back(q_item);
}

/* However, if we want to abort AddV, the pointer of MVCCList is not enough, since we need to free vp_row_list
 * and ve_row_list attached to the Vertex added. InsertTrxHistoryMapAddV requires one more parameter (vid) than
 * InsertTrxHistoryMap, dedicated for ProcessAddV.
 */
void DataStorage::InsertTrxHistoryMapAddV(const uint64_t& trx_id, void* mvcc_list, vid_t vid) {
    TransactionAccessor t_accessor;
    transaction_history_map_.insert(t_accessor, trx_id);

    TrxProcessHistory::ProcessItem q_item;
    q_item.type = TrxProcessHistory::PROCESS_ADD_V;
    q_item.mvcc_list = mvcc_list;

    t_accessor->second.process_vector.emplace_back(q_item);

    // this map will be used when the trx is aborted
    t_accessor->second.addv_map[mvcc_list] = vid.value();
}

vid_t DataStorage::ProcessAddV(const label_t& label, const uint64_t& trx_id, const uint64_t& begin_time) {
    /* Guaranteed that the vid is identical in the whole system.
     * Thus, it's impossible to insert two vertex with the same vid
     */
    vid_t vid = AssignVID();

    VertexAccessor v_accessor;
    vertex_map_.insert(v_accessor, vid.value());

    v_accessor->second.label = label;
    v_accessor->second.vp_row_list = new PropertyRowList<VertexPropertyRow>;
    v_accessor->second.ve_row_list = new TopologyRowList;

    v_accessor->second.ve_row_list->Init(vid);
    v_accessor->second.vp_row_list->Init();

    auto* mvcc_list = new MVCCList<VertexMVCCItem>;

    // this won't fail as it's the first version in the list
    mvcc_list->AppendVersion(trx_id, begin_time)[0] = true;

    v_accessor->second.mvcc_list = mvcc_list;

    /* The only usage of InsertTrxHistoryMapAddV in the project,
     * since vid is needed when aborting AddV.
     */
    InsertTrxHistoryMapAddV(trx_id, v_accessor->second.mvcc_list, v_accessor->first);

    return vid;
}

bool DataStorage::ProcessDropV(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                               vector<eid_t>& in_eids, vector<eid_t>& out_eids) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    if (!found) {
        printf("Impossible branch in DataStorage::ProcessDropV\n");
        assert(false);
    }

    vector<eid_t> all_connected_edge;
    auto read_stat = GetConnectedEdgeList(vid, 0, BOTH, trx_id, begin_time, false, all_connected_edge);

    if (read_stat == READ_STAT::ABORT) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    bool* mvcc_value_ptr = v_accessor->second.mvcc_list->AppendVersion(trx_id, begin_time);

    /* If AppendVersion returns nullptr, the transaction should be aborted.
     * Similarly hereinafter.
     */
    if (mvcc_value_ptr == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    // false means invisible
    mvcc_value_ptr[0] = false;

    InsertTrxHistoryMap(trx_id, TrxProcessHistory::PROCESS_DROP_V, v_accessor->second.mvcc_list);

    // return connected edges via references of vectors (out_eids and in_eids)
    for (auto eid : all_connected_edge) {
        if (eid.out_v == vid.value()) {
            // this is an out edge
            out_eids.emplace_back(eid);
        } else {
            // this is an in edge
            in_eids.emplace_back(eid);
        }
    }

    return true;
}


/* If we are going to add an edge from v1 (on worker1) to v2 (on worker2) with edge id e1,
 * on worker1:
 *      ProcessAddE(e1, label, true, trx_id, begin_time);
 * on worker2:
 *      ProcessAddE(e1, label, false, trx_id, begin_time);
 */
bool DataStorage::ProcessAddE(const eid_t& eid, const label_t& label, const bool& is_out,
                              const uint64_t& trx_id, const uint64_t& begin_time) {
    InEdgeAccessor in_e_accessor;
    OutEdgeAccessor out_e_accessor;
    bool is_new;
    vid_t src_vid = eid.out_v, dst_vid = eid.in_v;
    vid_t conn_vid, local_vid;

    /* if is_out, this function will add an outE, which means that src_vid is on this node;
     *      else, this function will add an inE, which means that dst_vid is on this node.
     */
    if (is_out) {
        local_vid = src_vid;
        conn_vid = dst_vid;
    } else {
        local_vid = dst_vid;
        conn_vid = src_vid;
    }

    // anyway, need to check if the vertex exists or not
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, local_vid.value());

    if (!found) {
        printf("Impossible branch in DataStorage::ProcessAddE\n");
        assert(false);
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, false) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    if (is_out) {
        is_new = out_edge_map_.insert(out_e_accessor, eid.value());
    } else {
        is_new = in_edge_map_.insert(in_e_accessor, eid.value());
    }

    MVCCList<EdgeMVCCItem>* mvcc_list;

    if (is_new) {
        // a new MVCCList<EdgeVersion> will be created; a cell in VertexEdgeRow will be allocated
        PropertyRowList<EdgePropertyRow>* ep_row_list;
        if (is_out) {
            // edge properties are only attached to outE
            ep_row_list = new PropertyRowList<EdgePropertyRow>;
            ep_row_list->Init();
        } else {
            ep_row_list = nullptr;
        }
        mvcc_list = v_accessor->second.ve_row_list
                    ->ProcessAddEdge(is_out, conn_vid, label, ep_row_list, trx_id, begin_time);
        if (is_out) {
            out_e_accessor->second.mvcc_list = mvcc_list;
        } else {
            in_e_accessor->second.mvcc_list = mvcc_list;
        }
    } else {
        // do not need to access VertexEdgeRow
        if (is_out) {
            mvcc_list = out_e_accessor->second.mvcc_list;
        } else {
            mvcc_list = in_e_accessor->second.mvcc_list;
        }
        EdgeVersion* e_item = mvcc_list->AppendVersion(trx_id, begin_time);

        if (e_item == nullptr) {
            trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
            return false;
        }

        PropertyRowList<EdgePropertyRow>* ep_row_list;
        if (is_out) {
            ep_row_list = new PropertyRowList<EdgePropertyRow>;
            ep_row_list->Init();
        } else {
            ep_row_list = nullptr;
        }

        e_item->label = label;
        e_item->ep_row_list = ep_row_list;
    }

    InsertTrxHistoryMap(trx_id, TrxProcessHistory::PROCESS_ADD_E, mvcc_list);

    return true;
}

bool DataStorage::ProcessDropE(const eid_t& eid, const bool& is_out,
                               const uint64_t& trx_id, const uint64_t& begin_time) {
    InEdgeConstAccessor in_e_accessor;
    OutEdgeConstAccessor out_e_accessor;
    bool found;
    vid_t src_vid = eid.out_v, dst_vid = eid.in_v;
    vid_t conn_vid;

    /* if is_out, this function will add an outE, which means that src_vid is on this node;
     *      else, this function will add an inE, which means that dst_vid is on this node.
     */
    if (is_out) {
        found = out_edge_map_.find(out_e_accessor, eid.value());
        conn_vid = dst_vid;
    } else {
        found = in_edge_map_.find(in_e_accessor, eid.value());
        conn_vid = src_vid;
    }

    // do nothing
    if (!found)
        return true;

    MVCCList<EdgeMVCCItem>* mvcc_list;

    if (is_out) {
        mvcc_list = out_e_accessor->second.mvcc_list;
    } else {
        mvcc_list = in_e_accessor->second.mvcc_list;
    }
    EdgeVersion* e_item = mvcc_list->AppendVersion(trx_id, begin_time);

    if (e_item == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    // label == 0 represents that the edge does not exists
    e_item->label = 0;
    e_item->ep_row_list = nullptr;


    InsertTrxHistoryMap(trx_id, TrxProcessHistory::PROCESS_DROP_E, mvcc_list);

    return true;
}

bool DataStorage::ProcessModifyVP(const vpid_t& pid, const value_t& value,
                                  const uint64_t& trx_id, const uint64_t& begin_time) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid_t(pid.vid).value());

    if (!found) {
        printf("Impossible branch in DataStorage::ProcessModifyVP\n");
        assert(false);
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, false) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    // Modify the property in vp_row_list
    auto ret = v_accessor->second.vp_row_list->ProcessModifyProperty(pid, value, trx_id, begin_time);

    // ret.second: pointer of MVCCList<VP>
    if (ret.second == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    TrxProcessHistory::ProcessType process_type;
    // ret.first == true means that the property already exists, and the transaction modified it.
    if (ret.first)
        process_type = TrxProcessHistory::PROCESS_MODIFY_VP;
    else
        process_type = TrxProcessHistory::PROCESS_ADD_VP;

    InsertTrxHistoryMap(trx_id, process_type, ret.second);

    return true;
}

bool DataStorage::ProcessModifyEP(const epid_t& pid, const value_t& value,
                                  const uint64_t& trx_id, const uint64_t& begin_time) {
    OutEdgeConstAccessor out_e_accessor;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_accessor, eid_t(pid.in_vid, pid.out_vid), trx_id, begin_time, false, edge_version);

    if (read_stat != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    if (!edge_version.Exist()) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    // Modify the property in ep_row_list
    auto ret = edge_version.ep_row_list->ProcessModifyProperty(pid, value, trx_id, begin_time);

    // ret.second: pointer of MVCCList<EP>
    if (ret.second == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    TrxProcessHistory::ProcessType process_type;
    // ret.first == true means that the property already exists, and the transaction modified it.
    if (ret.first)
        process_type = TrxProcessHistory::PROCESS_MODIFY_EP;
    else
        process_type = TrxProcessHistory::PROCESS_ADD_EP;

    InsertTrxHistoryMap(trx_id, process_type, ret.second);

    return true;
}

bool DataStorage::ProcessDropVP(const vpid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid_t(pid.vid).value());

    if (!found) {
        printf("Impossible branch in DataStorage::ProcessDropVP\n");
        assert(false);
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, false) == READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    // ret: pointer of MVCCList<VP>
    auto ret = v_accessor->second.vp_row_list->ProcessDropProperty(pid, trx_id, begin_time);

    if (ret == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    InsertTrxHistoryMap(trx_id, TrxProcessHistory::PROCESS_DROP_VP, ret);

    return true;
}

bool DataStorage::ProcessDropEP(const epid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time) {
    OutEdgeConstAccessor out_e_accessor;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_accessor, eid_t(pid.in_vid, pid.out_vid), trx_id, begin_time, false, edge_version);

    if (read_stat != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    if (!edge_version.Exist()) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    // ret: pointer of MVCCList<EP>
    auto ret = edge_version.ep_row_list->ProcessDropProperty(pid, trx_id, begin_time);

    if (ret == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    InsertTrxHistoryMap(trx_id, TrxProcessHistory::PROCESS_DROP_EP, ret);

    return true;
}

/* Commit the transaction on this worker.
 * Operates on the MVCCList modified by the transaction:
 *      make the last version of the MVCCList visible to transactions that
 *      begin after the commit_time
 */
void DataStorage::Commit(const uint64_t& trx_id, const uint64_t& commit_time) {
    TransactionAccessor t_accessor;
    if (!transaction_history_map_.find(t_accessor, trx_id)) {
        return;
    }

    auto& process_vec_ref = t_accessor->second.process_vector;

    // One MVCCList may be modified for multiple times in one transaction
    // Use set to make sure that any MVCCList will only be commited once
    unordered_set<TrxProcessHistory::ProcessItem, TrxProcessHistory::ProcessItemHash> process_set;

    for (int i = 0; i < process_vec_ref.size(); i++) {
        auto& process_item = process_vec_ref[i];
        if (process_set.count(process_item) > 0)
            continue;
        process_set.emplace(process_item);
        if (process_item.type == TrxProcessHistory::PROCESS_MODIFY_VP ||
            process_item.type == TrxProcessHistory::PROCESS_ADD_VP ||
            process_item.type == TrxProcessHistory::PROCESS_DROP_VP) {
            // VP related
            MVCCList<VPropertyMVCCItem>* vp_mvcc_list = process_item.mvcc_list;
            vp_mvcc_list->CommitVersion(trx_id, commit_time);
        } else if (process_item.type == TrxProcessHistory::PROCESS_MODIFY_EP ||
                   process_item.type == TrxProcessHistory::PROCESS_ADD_EP ||
                   process_item.type == TrxProcessHistory::PROCESS_DROP_EP) {
            // EP related
            MVCCList<EPropertyMVCCItem>* ep_mvcc_list = process_item.mvcc_list;
            ep_mvcc_list->CommitVersion(trx_id, commit_time);
        } else if (process_item.type == TrxProcessHistory::PROCESS_ADD_V ||
                   process_item.type == TrxProcessHistory::PROCESS_DROP_V) {
            // V related
            MVCCList<VertexMVCCItem>* v_mvcc_list = process_item.mvcc_list;
            v_mvcc_list->CommitVersion(trx_id, commit_time);
        } else if (process_item.type == TrxProcessHistory::PROCESS_ADD_E ||
                   process_item.type == TrxProcessHistory::PROCESS_DROP_E) {
            // E related
            MVCCList<EdgeMVCCItem>* e_mvcc_list = process_item.mvcc_list;
            e_mvcc_list->CommitVersion(trx_id, commit_time);
        }
    }

    transaction_history_map_.erase(t_accessor);
}

/* Abort the transaction on this worker.
 * Operates on the MVCCList modified by the transaction:
 *      remove the version appended by the transaction.
 * For aborting AddV, ep_row_list and ve_row_list on Vertex will be freed.
 */
void DataStorage::Abort(const uint64_t& trx_id) {
    TransactionAccessor t_accessor;
    if (!transaction_history_map_.find(t_accessor, trx_id)) {
        return;
    }

    auto& process_vec_ref = t_accessor->second.process_vector;
    auto& vid_map_ref = t_accessor->second.addv_map;
    unordered_set<TrxProcessHistory::ProcessItem, TrxProcessHistory::ProcessItemHash> process_set;

    /* Reverse abort.
     * Considering a special case: (Q1). Add vertex V1; (Q2). Add property VP1 on V1;
     *   Both (Q1) and (Q2) will be needed to be aborted.
     *   If we abort (Q1) before aborting (Q2), the MVCCList for (Q2) will be deallocated before
     *   aborting (Q2), which will causes undefined behavior.
     *   Thus, we need to abort (Q2) before aborting (Q1).
     */
    for (int i = process_vec_ref.size() - 1; i >= 0; i--) {
        auto& process_item = process_vec_ref[i];
        if (process_set.count(process_item) > 0)
            continue;
        process_set.emplace(process_item);
        // Pointer of MVCCList will be unique in process_set
        if (process_item.type == TrxProcessHistory::PROCESS_MODIFY_VP ||
            process_item.type == TrxProcessHistory::PROCESS_ADD_VP ||
            process_item.type == TrxProcessHistory::PROCESS_DROP_VP) {
            // VP related
            MVCCList<VPropertyMVCCItem>* vp_mvcc_list = process_item.mvcc_list;
            vp_mvcc_list->AbortVersion(trx_id);
        } else if (process_item.type == TrxProcessHistory::PROCESS_MODIFY_EP ||
                   process_item.type == TrxProcessHistory::PROCESS_ADD_EP ||
                   process_item.type == TrxProcessHistory::PROCESS_DROP_EP) {
            // EP related
            MVCCList<EPropertyMVCCItem>* ep_mvcc_list = process_item.mvcc_list;
            ep_mvcc_list->AbortVersion(trx_id);
        } else if (process_item.type == TrxProcessHistory::PROCESS_DROP_V ||
                   process_item.type == TrxProcessHistory::PROCESS_ADD_V) {
            // V related
            // New item in vertex_map will not deallocated here
            MVCCList<VertexMVCCItem>* v_mvcc_list = process_item.mvcc_list;
            v_mvcc_list->AbortVersion(trx_id);
            if (process_item.type == TrxProcessHistory::PROCESS_ADD_V) {
                // access the item in the v_map
                VertexAccessor v_accessor;
                vertex_map_.find(v_accessor, vid_map_ref[v_mvcc_list]);
                v_accessor->second.ve_row_list->SelfGarbageCollect();
                delete v_accessor->second.ve_row_list;
                v_accessor->second.ve_row_list = nullptr;
                v_accessor->second.vp_row_list->SelfGarbageCollect();
                delete v_accessor->second.vp_row_list;
                v_accessor->second.vp_row_list = nullptr;
                v_accessor->second.mvcc_list->SelfGarbageCollect();
                // do not delete v_accessor->second.mvcc_list
            }
        } else if (process_item.type == TrxProcessHistory::PROCESS_ADD_E ||
                   process_item.type == TrxProcessHistory::PROCESS_DROP_E) {
            // E related
            /* If the trx allocates new item in edge_map,
             * the item in the map will not be deallocated here.
            */
            MVCCList<EdgeMVCCItem>* e_mvcc_list = process_item.mvcc_list;
            e_mvcc_list->AbortVersion(trx_id);
        }
    }

    transaction_history_map_.erase(t_accessor);
}
