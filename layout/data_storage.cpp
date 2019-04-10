/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "layout/data_storage.hpp"

template<class MVCC> OffsetConcurrentMemPool<MVCC>* MVCCList<MVCC>::mem_pool_ = nullptr;
template<class PropertyRow> OffsetConcurrentMemPool<PropertyRow>* PropertyRowList<PropertyRow>::mem_pool_ = nullptr;
template<class PropertyRow> MVCCValueStore* PropertyRowList<PropertyRow>::value_storage_ = nullptr;
OffsetConcurrentMemPool<VertexEdgeRow>* TopologyRowList::mem_pool_ = nullptr;
MVCCValueStore* VPropertyMVCC::value_store = nullptr;
MVCCValueStore* EPropertyMVCC::value_store = nullptr;

void DataStorage::Init() {
    node_ = Node::StaticInstance();
    config_ = Config::GetInstance();
    id_mapper_ = SimpleIdMapper::GetInstance();
    snapshot_manager_ = MPISnapshotManager::GetInstance();
    worker_rank_ = node_.get_local_rank();
    worker_size_ = node_.get_local_size();

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
                       "sizeof(PropertyMVCC) = %d, sizeof(VertexMVCC) = %d, sizeof(EdgeMVCC) = %d\n",
                       sizeof(PropertyMVCC), sizeof(VertexMVCC), sizeof(EdgeMVCC));

    CreateContainer();

    snapshot_manager_->SetRootPath(config_->SNAPSHOT_PATH);
    snapshot_manager_->AppendConfig("HDFS_INDEX_PATH", config_->HDFS_INDEX_PATH);
    snapshot_manager_->AppendConfig("HDFS_VTX_SUBFOLDER", config_->HDFS_VTX_SUBFOLDER);
    snapshot_manager_->AppendConfig("HDFS_VP_SUBFOLDER", config_->HDFS_VP_SUBFOLDER);
    snapshot_manager_->AppendConfig("HDFS_EP_SUBFOLDER", config_->HDFS_EP_SUBFOLDER);
    snapshot_manager_->SetComm(node_.local_comm);
    snapshot_manager_->ConfirmConfig();

    vid_to_assign_divided_ = worker_rank_;

    hdfs_data_loader_ = HDFSDataLoader::GetInstance();
    hdfs_data_loader_->LoadData();
    FillContainer();
    PrintLoadedData();
    hdfs_data_loader_->FreeMemory();

    trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();

    node_.Rank0PrintfWithWorkerBarrier("DataStorage::Init() all finished\n");
}

void DataStorage::CreateContainer() {
    ve_row_pool_ = OffsetConcurrentMemPool<VertexEdgeRow>::GetInstance(nullptr, config_->global_ve_row_pool_size);
    vp_row_pool_ = OffsetConcurrentMemPool<VertexPropertyRow>::GetInstance(nullptr, config_->global_vp_row_pool_size);
    ep_row_pool_ = OffsetConcurrentMemPool<EdgePropertyRow>::GetInstance(nullptr, config_->global_ep_row_pool_size);
    vp_mvcc_pool_ = OffsetConcurrentMemPool<VPropertyMVCC>::GetInstance(
                          nullptr, config_->global_property_mvcc_pool_size);
    ep_mvcc_pool_ = OffsetConcurrentMemPool<EPropertyMVCC>::GetInstance(
                          nullptr, config_->global_property_mvcc_pool_size);
    vertex_mvcc_pool_ = OffsetConcurrentMemPool<VertexMVCC>::GetInstance(nullptr, config_->global_topo_mvcc_pool_size);
    edge_mvcc_pool_ = OffsetConcurrentMemPool<EdgeMVCC>::GetInstance(nullptr, config_->global_topo_mvcc_pool_size);

    MVCCList<VPropertyMVCC>::SetGlobalMemoryPool(vp_mvcc_pool_);
    MVCCList<EPropertyMVCC>::SetGlobalMemoryPool(ep_mvcc_pool_);
    MVCCList<VertexMVCC>::SetGlobalMemoryPool(vertex_mvcc_pool_);
    MVCCList<EdgeMVCC>::SetGlobalMemoryPool(edge_mvcc_pool_);
    PropertyRowList<EdgePropertyRow>::SetGlobalMemoryPool(ep_row_pool_);
    PropertyRowList<VertexPropertyRow>::SetGlobalMemoryPool(vp_row_pool_);
    TopologyRowList::SetGlobalMemoryPool(ve_row_pool_);

    uint64_t vp_sz = GiB2B(config_->global_vertex_property_kv_sz_gb);
    uint64_t ep_sz = GiB2B(config_->global_edge_property_kv_sz_gb);
    vp_store_ = new MVCCValueStore(nullptr, vp_sz / MemItemSize);
    ep_store_ = new MVCCValueStore(nullptr, ep_sz / MemItemSize);
    PropertyRowList<VertexPropertyRow>::SetGlobalValueStore(vp_store_);
    PropertyRowList<EdgePropertyRow>::SetGlobalValueStore(ep_store_);
    VPropertyMVCC::SetGlobalValueStore(vp_store_);
    EPropertyMVCC::SetGlobalValueStore(ep_store_);
}

void DataStorage::FillContainer() {
    indexes_ = hdfs_data_loader_->indexes_;

    // access elements in HDFSDataLoader
    hash_map<uint32_t, TMPVertex*>& v_map = hdfs_data_loader_->vtx_part_map_;
    hash_map<uint64_t, TMPEdge*>& e_map = hdfs_data_loader_->edge_part_map_;

    for (auto vtx : hdfs_data_loader_->shuffled_vtx_) {
        VertexAccessor v_accessor;
        vertex_map_.insert(v_accessor, vtx.id.value());

        if (vid_to_assign_divided_ <= vtx.id.value())
            vid_to_assign_divided_ = vtx.id.value() + worker_size_;

        v_accessor->second.label = vtx.label;
        v_accessor->second.vp_row_list = new PropertyRowList<VertexPropertyRow>;
        v_accessor->second.ve_row_list = new TopologyRowList;

        v_accessor->second.vp_row_list->Init();
        v_accessor->second.ve_row_list->Init(vtx.id);

        v_accessor->second.mvcc_list = new MVCCList<VertexMVCC>;
        v_accessor->second.mvcc_list->AppendInitialVersion()[0] = true;

        for (auto in_nb : vtx.in_nbs) {
            eid_t eid = eid_t(vtx.id.vid, in_nb.vid);

            EdgeAccessor e_accessor;
            in_edge_map_.insert(e_accessor, eid.value());

            // "false" means that is_out = false
            auto* mvcc_list = v_accessor->second.ve_row_list
                              ->InsertInitialCell(false, in_nb, e_map[eid.value()]->label, nullptr);

            e_accessor->second = mvcc_list;
        }

        for (auto out_nb : vtx.out_nbs) {
            eid_t eid = eid_t(out_nb.vid, vtx.id.vid);

            EdgeAccessor e_accessor;
            out_edge_map_.insert(e_accessor, eid.value());
            auto* ep_row_list = new PropertyRowList<EdgePropertyRow>;
            ep_row_list->Init();

            // "true" means that is_out = true
            auto* mvcc_list = v_accessor->second.ve_row_list
                              ->InsertInitialCell(true, out_nb, e_map[eid.value()]->label, ep_row_list);
            e_accessor->second = mvcc_list;
        }

        for (int i = 0; i < vtx.vp_label_list.size(); i++) {
            v_accessor->second.vp_row_list->InsertInitialCell(vpid_t(vtx.id, vtx.vp_label_list[i]),
                                                                 vtx.vp_value_list[i]);
        }
    }
    vid_to_assign_divided_ = (vid_to_assign_divided_ - worker_rank_) / worker_size_;

    node_.Rank0PrintfWithWorkerBarrier("DataStorage::FillContainer() load vtx finished\n");

    for (auto edge : hdfs_data_loader_->shuffled_edge_) {
        EdgeConstAccessor e_accessor;
        out_edge_map_.find(e_accessor, edge.id.value());

        EdgeMVCC* edge_mvcc;
        e_accessor->second->GetVisibleVersion(0, 0, true, edge_mvcc);
        auto edge_item = edge_mvcc->GetValue();

        for (int i = 0; i < edge.ep_label_list.size(); i++) {
            edge_item.ep_row_list->InsertInitialCell(epid_t(edge.id, edge.ep_label_list[i]), edge.ep_value_list[i]);
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

READ_STAT DataStorage::CheckVertexVisibility(VertexConstAccessor& v_accessor, const uint64_t& trx_id,
                                        const uint64_t& begin_time, const bool& read_only) {
    VertexMVCC* visible_version;
    bool success = v_accessor->second.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, visible_version);
    if (!success)
        return READ_STAT::ABORT;
    // how to deal with "not found" is decided by the function calling it
    if (visible_version == nullptr)
        return READ_STAT::NOTFOUND;
    if (!visible_version->GetValue())
        return READ_STAT::NOTFOUND;
    return READ_STAT::SUCCESS;
}

READ_STAT DataStorage::CheckVertexVisibility(VertexAccessor& v_accessor, const uint64_t& trx_id,
                                        const uint64_t& begin_time, const bool& read_only) {
    VertexMVCC* visible_version;
    bool success = v_accessor->second.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, visible_version);
    if (!success)
        return READ_STAT::ABORT;
    // how to deal with "not found" is decided by the function calling it
    if (visible_version == nullptr)
        return READ_STAT::NOTFOUND;
    if (!visible_version->GetValue())
        return READ_STAT::NOTFOUND;
    return READ_STAT::SUCCESS;
}

READ_STAT DataStorage::GetVPByPKey(const vpid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time,
                              const bool& read_only, value_t& ret) {
    vid_t vid = pid.vid;

    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    // system error, need to handle it in the future
    if (!found)
        return READ_STAT::ABORT;

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS)
        return READ_STAT::ABORT;

    return v_accessor->second.vp_row_list->ReadProperty(pid, trx_id, begin_time, read_only, ret);
}

READ_STAT DataStorage::GetAllVP(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                           const bool& read_only, vector<pair<label_t, value_t>>& ret) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    // system error, need to handle it in the future
    if (!found)
        return READ_STAT::ABORT;

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS)
        return READ_STAT::ABORT;

    return v_accessor->second.vp_row_list->ReadAllProperty(trx_id, begin_time, read_only, ret);
}

READ_STAT DataStorage::GetVPByPKeyList(const vid_t& vid, const vector<label_t>& p_key,
                                  const uint64_t& trx_id, const uint64_t& begin_time,
                                  const bool& read_only, vector<pair<label_t, value_t>>& ret) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    // system error, need to handle it in the future
    if (!found)
        return READ_STAT::ABORT;

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS)
        return READ_STAT::ABORT;

    return v_accessor->second.vp_row_list->ReadPropertyByPKeyList(p_key, trx_id, begin_time, read_only, ret);
}

READ_STAT DataStorage::GetVPidList(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                              const bool& read_only, vector<vpid_t>& ret) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    // system error, need to handle it in the future
    if (!found)
        return READ_STAT::ABORT;

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS)
        return READ_STAT::ABORT;

    return v_accessor->second.vp_row_list->ReadPidList(trx_id, begin_time, read_only, ret);
}

READ_STAT DataStorage::GetVL(const vid_t& vid, const uint64_t& trx_id,
                           const uint64_t& begin_time, const bool& read_only, label_t& ret) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    // system error, need to handle it in the future
    if (!found)
        return READ_STAT::ABORT;

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS)
        return READ_STAT::ABORT;

    ret = v_accessor->second.label;

    return READ_STAT::SUCCESS;
}

READ_STAT DataStorage::GetEPByPKey(const epid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time,
                              const bool& read_only, value_t& ret) {
    eid_t eid = eid_t(pid.in_vid, pid.out_vid);

    EdgeConstAccessor e_accessor;
    EdgeItem edge_item;
    auto read_stat = GetOutEdgeItem(e_accessor, eid, trx_id, begin_time, read_only, edge_item);
    if (read_stat != READ_STAT::SUCCESS)
        return READ_STAT::ABORT;

    if (edge_item.Exist())
        return edge_item.ep_row_list->ReadProperty(pid, trx_id, begin_time, read_only, ret);
    return READ_STAT::ABORT;
}

READ_STAT DataStorage::GetAllEP(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time,
                           const bool& read_only, vector<pair<label_t, value_t>>& ret) {
    EdgeConstAccessor e_accessor;
    EdgeItem edge_item;
    auto read_stat = GetOutEdgeItem(e_accessor, eid, trx_id, begin_time, read_only, edge_item);
    if (read_stat != READ_STAT::SUCCESS)
        return READ_STAT::ABORT;

    if (edge_item.Exist())
       return edge_item.ep_row_list->ReadAllProperty(trx_id, begin_time, read_only, ret);
    return READ_STAT::ABORT;
}

READ_STAT DataStorage::GetEPByPKeyList(const eid_t& eid, const vector<label_t>& p_key,
                                  const uint64_t& trx_id, const uint64_t& begin_time,
                                  const bool& read_only, vector<pair<label_t, value_t>>& ret) {
    EdgeConstAccessor e_accessor;
    EdgeItem edge_item;
    auto read_stat = GetOutEdgeItem(e_accessor, eid, trx_id, begin_time, read_only, edge_item);
    if (read_stat != READ_STAT::SUCCESS)
        return READ_STAT::ABORT;

    if (edge_item.Exist())
        return edge_item.ep_row_list->ReadPropertyByPKeyList(p_key, trx_id, begin_time, read_only, ret);
    return READ_STAT::ABORT;
}

READ_STAT DataStorage::GetEPidList(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time,
                              const bool& read_only, vector<epid_t>& ret) {
    EdgeConstAccessor e_accessor;
    EdgeItem edge_item;
    auto read_stat = GetOutEdgeItem(e_accessor, eid, trx_id, begin_time, read_only, edge_item);
    if (read_stat != READ_STAT::SUCCESS)
        return READ_STAT::ABORT;

    if (edge_item.Exist())
        return edge_item.ep_row_list->ReadPidList(trx_id, begin_time, read_only, ret);
    return READ_STAT::ABORT;
}

READ_STAT DataStorage::GetEL(const eid_t& eid, const uint64_t& trx_id,
                           const uint64_t& begin_time, const bool& read_only, label_t& ret) {
    EdgeConstAccessor e_accessor;
    EdgeItem edge_item;
    auto read_stat = GetOutEdgeItem(e_accessor, eid, trx_id, begin_time, read_only, edge_item);
    if (read_stat != READ_STAT::SUCCESS)
        return READ_STAT::ABORT;

    // an deleted edge will returns 0
    ret = edge_item.label;
    if (edge_item.Exist())
        return READ_STAT::SUCCESS;
    else
        return READ_STAT::ABORT;
}

READ_STAT DataStorage::GetConnectedVertexList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                         const uint64_t& trx_id, const uint64_t& begin_time,
                                         const bool& read_only, vector<vid_t>& ret) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    // system error, need to handle it in the future
    if (!found)
        return READ_STAT::ABORT;

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS)
        return READ_STAT::ABORT;

    return v_accessor->second.ve_row_list->ReadConnectedVertex(direction, edge_label,
                                                               trx_id, begin_time, read_only, ret);
}

READ_STAT DataStorage::GetConnectedEdgeList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                       const uint64_t& trx_id, const uint64_t& begin_time,
                                       const bool& read_only, vector<eid_t>& ret) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    if (!found)
        return READ_STAT::ABORT;

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, read_only) != READ_STAT::SUCCESS)
        return READ_STAT::ABORT;

    return v_accessor->second.ve_row_list->ReadConnectedEdge(direction, edge_label, trx_id, begin_time, read_only, ret);
}

READ_STAT DataStorage::GetAllVertices(const uint64_t& trx_id, const uint64_t& begin_time,
                                 const bool& read_only, vector<vid_t>& ret) {
    for (auto v_pair = vertex_map_.begin(); v_pair != vertex_map_.end(); v_pair++) {
        auto& v_item = v_pair->second;

        VertexMVCC* visible_version;
        bool success = v_item.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, visible_version);
        if (!success)
            return READ_STAT::ABORT;
        if (visible_version == nullptr)
            continue;

        // if this vertice is visible
        if (visible_version->GetValue())
            ret.emplace_back(vid_t(v_pair->first));
    }

    return READ_STAT::SUCCESS;
}

READ_STAT DataStorage::GetAllEdges(const uint64_t& trx_id, const uint64_t& begin_time,
                              const bool& read_only, vector<eid_t>& ret) {
    // TODO(entityless): Simplify the code by editing eid_t::value()
    for (auto e_pair = out_edge_map_.begin(); e_pair != out_edge_map_.end(); e_pair++) {
        EdgeMVCC* visible_version;
        bool success = e_pair->second->GetVisibleVersion(trx_id, begin_time, read_only, visible_version);
        if (!success)
            return READ_STAT::ABORT;
        if (visible_version == nullptr)
            continue;

        if (visible_version->GetValue().Exist()) {
            uint64_t eid_fetched = e_pair->first;
            eid_t* tmp_eid_p = reinterpret_cast<eid_t*>(&eid_fetched);
            ret.emplace_back(eid_t(tmp_eid_p->out_v, tmp_eid_p->in_v));
        }
    }

    return READ_STAT::SUCCESS;
}

READ_STAT DataStorage::GetOutEdgeItem(EdgeConstAccessor& e_accessor, const eid_t& eid,
                                     const uint64_t& trx_id, const uint64_t& begin_time,
                                     const bool& read_only, EdgeItem& item_ref) {
    bool found = out_edge_map_.find(e_accessor, eid.value());

    // system error, need to handle it in the future
    if (!found)
        return READ_STAT::ABORT;

    EdgeMVCC* visible_version;
    bool success = e_accessor->second->GetVisibleVersion(trx_id, begin_time, read_only, visible_version);

    if (!success)
        return READ_STAT::ABORT;
    if (visible_version == nullptr)
        return READ_STAT::NOTFOUND;

    item_ref = visible_version->GetValue();

    return READ_STAT::SUCCESS;
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

void DataStorage::DeleteAggData(agg_t key) {
    lock_guard<mutex> lock(agg_mutex);

    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
    if (itr != agg_data_table.end()) {
        agg_data_table.erase(itr);
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

void DataStorage::GetDepReadTrxList(uint64_t trxID, vector<uint64_t> & homoTrxIDList,
                                    vector<uint64_t> & heteroTrxIDList) {
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
    int vid_local = vid_to_assign_divided_++;
    return vid_t(vid_local * worker_size_ + worker_rank_);
}

void DataStorage::InsertTrxProcessMap(const uint64_t& trx_id, const TransactionItem::ProcessType& type,
                                      void* mvcc_list) {
    TransactionAccessor t_accessor;
    transaction_process_map_.insert(t_accessor, trx_id);

    TransactionItem::ProcessItem q_item;
    q_item.type = type;
    q_item.mvcc_list = mvcc_list;

    t_accessor->second.process_set.emplace(q_item);
}

vid_t DataStorage::ProcessAddV(const label_t& label, const uint64_t& trx_id, const uint64_t& begin_time) {
    // guaranteed that the vid is identical in the whole system
    // so that it's impossible to insert two vertex with the same vid
    vid_t vid = AssignVID();

    VertexAccessor v_accessor;
    vertex_map_.insert(v_accessor, vid.value());

    v_accessor->second.label = label;
    v_accessor->second.vp_row_list = new PropertyRowList<VertexPropertyRow>;
    v_accessor->second.ve_row_list = new TopologyRowList;

    v_accessor->second.ve_row_list->Init(vid);
    v_accessor->second.vp_row_list->Init();

    auto* mvcc_list = new MVCCList<VertexMVCC>;

    // this won't fail as it's the first version in the list
    mvcc_list->AppendVersion(trx_id, begin_time)[0] = true;

    v_accessor->second.mvcc_list = mvcc_list;

    InsertTrxProcessMap(trx_id, TransactionItem::PROCESS_ADD_V, v_accessor->second.mvcc_list);

    return vid;
}

bool DataStorage::ProcessDropV(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                                    vector<eid_t>& in_eids, vector<eid_t>& out_eids) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid.value());

    if (!found) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    vector<eid_t> all_connected_edge;
    auto read_stat = GetConnectedEdgeList(vid, 0, BOTH, trx_id, begin_time, false, all_connected_edge);

    if (read_stat == READ_STAT::ABORT) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    bool* mvcc_value_ptr = v_accessor->second.mvcc_list->AppendVersion(trx_id, begin_time);

    if (mvcc_value_ptr == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    mvcc_value_ptr[0] = false;

    InsertTrxProcessMap(trx_id, TransactionItem::PROCESS_DROP_V, v_accessor->second.mvcc_list);

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

bool DataStorage::ProcessAddE(const eid_t& eid, const label_t& label, const bool& is_out,
                              const uint64_t& trx_id, const uint64_t& begin_time) {
    EdgeAccessor e_accessor;
    bool is_new;
    vid_t src_vid = eid.out_v, dst_vid = eid.in_v;
    vid_t conn_vid, local_vid;

    // if is_out, this function will add an outE, which means that src_vid is on this node
    //      else, this function will add an inE, which means that dst_vid is on this node
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

    // system error, need to handle it in the future
    if (!found) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, false) != READ_STAT::SUCCESS)
        return false;

    if (is_out) {
        is_new = out_edge_map_.insert(e_accessor, eid.value());
    } else {
        is_new = in_edge_map_.insert(e_accessor, eid.value());
    }

    if (is_new) {
        // a new MVCCList<EdgeItem> will be created; a cell in VertexEdgeRow will be allocated
        // it must exists for the limitation of query
        PropertyRowList<EdgePropertyRow>* ep_row_list;
        if (is_out) {
            ep_row_list = new PropertyRowList<EdgePropertyRow>;
            ep_row_list->Init();
        } else {
            ep_row_list = nullptr;
        }
        auto* mvcc_list = v_accessor->second.ve_row_list
                          ->ProcessAddEdge(is_out, conn_vid, label, ep_row_list, trx_id, begin_time);
        e_accessor->second = mvcc_list;
    } else {
        // do not need to access VertexEdgeRow
        EdgeItem* e_item = e_accessor->second->AppendVersion(trx_id, begin_time);
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

    InsertTrxProcessMap(trx_id, TransactionItem::PROCESS_ADD_E, e_accessor->second);

    return true;
}

bool DataStorage::ProcessDropE(const eid_t& eid, const bool& is_out,
                               const uint64_t& trx_id, const uint64_t& begin_time) {
    // TODO(entityless): confirm that if this will happens on a edge that does not exists
    EdgeConstAccessor e_accessor;
    bool found;
    vid_t src_vid = eid.out_v, dst_vid = eid.in_v;
    vid_t conn_vid;

    // if is_out, this function will drop an outE, which means that src_vid is on this node
    //      else, this function will drop an inE, which means that dst_vid is on this node

    if (is_out) {
        found = out_edge_map_.find(e_accessor, eid.value());
        conn_vid = dst_vid;
    } else {
        found = in_edge_map_.find(e_accessor, eid.value());
        conn_vid = src_vid;
    }

    // do nothing
    if (!found) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return true;
    }

    EdgeItem* e_item = e_accessor->second->AppendVersion(trx_id, begin_time);
    if (e_item == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    e_item->label = 0;
    e_item->ep_row_list = nullptr;

    InsertTrxProcessMap(trx_id, TransactionItem::PROCESS_DROP_E, e_accessor->second);

    return true;
}

bool DataStorage::ProcessModifyVP(const vpid_t& pid, const value_t& value,
                                  const uint64_t& trx_id, const uint64_t& begin_time) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid_t(pid.vid).value());

    // system error, need to handle it in the future
    if (!found) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, false) != READ_STAT::SUCCESS)
        return false;

    auto ret = v_accessor->second.vp_row_list->ProcessModifyProperty(pid, value, trx_id, begin_time);

    if (ret.second == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    TransactionItem::ProcessType process_type;
    if (ret.first)
        process_type = TransactionItem::PROCESS_MODIFY_VP;
    else
        process_type = TransactionItem::PROCESS_ADD_VP;

    InsertTrxProcessMap(trx_id, process_type, ret.second);

    return true;
}

bool DataStorage::ProcessModifyEP(const epid_t& pid, const value_t& value,
                                  const uint64_t& trx_id, const uint64_t& begin_time) {
    EdgeConstAccessor e_accessor;
    EdgeItem edge_item;
    auto read_stat = GetOutEdgeItem(e_accessor, eid_t(pid.in_vid, pid.out_vid), trx_id, begin_time, false, edge_item);

    if (read_stat != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    if (!edge_item.Exist()) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    auto ret = edge_item.ep_row_list->ProcessModifyProperty(pid, value, trx_id, begin_time);

    if (ret.second == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    TransactionItem::ProcessType process_type;
    if (ret.first)
        process_type = TransactionItem::PROCESS_MODIFY_EP;
    else
        process_type = TransactionItem::PROCESS_ADD_EP;

    InsertTrxProcessMap(trx_id, process_type, ret.second);

    return true;
}

bool DataStorage::ProcessDropVP(const vpid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time) {
    VertexConstAccessor v_accessor;
    bool found = vertex_map_.find(v_accessor, vid_t(pid.vid).value());

    // system error, need to handle it in the future
    if (!found) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    if (CheckVertexVisibility(v_accessor, trx_id, begin_time, false) == READ_STAT::SUCCESS)
        return false;

    auto ret = v_accessor->second.vp_row_list->ProcessDropProperty(pid, trx_id, begin_time);

    if (ret == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    InsertTrxProcessMap(trx_id, TransactionItem::PROCESS_DROP_VP, ret);

    return true;
}

bool DataStorage::ProcessDropEP(const epid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time) {
    EdgeConstAccessor e_accessor;
    EdgeItem edge_item;
    auto read_stat = GetOutEdgeItem(e_accessor, eid_t(pid.in_vid, pid.out_vid), trx_id, begin_time, false, edge_item);

    if (read_stat != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    if (!edge_item.Exist()) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    auto ret = edge_item.ep_row_list->ProcessDropProperty(pid, trx_id, begin_time);

    if (ret == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return false;
    }

    InsertTrxProcessMap(trx_id, TransactionItem::PROCESS_DROP_EP, ret);

    return true;
}

void DataStorage::Commit(const uint64_t& trx_id, const uint64_t& commit_time) {
    TransactionAccessor t_accessor;
    if (!transaction_process_map_.find(t_accessor, trx_id)) {
        return;
    }

    for (auto process_item : t_accessor->second.process_set) {
        if (process_item.type == TransactionItem::PROCESS_MODIFY_VP ||
            process_item.type == TransactionItem::PROCESS_ADD_VP ||
            process_item.type == TransactionItem::PROCESS_DROP_VP) {
            MVCCList<VPropertyMVCC>* mvcc_list = process_item.mvcc_list;
            mvcc_list->CommitVersion(trx_id, commit_time);
        } else if (process_item.type == TransactionItem::PROCESS_MODIFY_EP ||
                   process_item.type == TransactionItem::PROCESS_ADD_EP ||
                   process_item.type == TransactionItem::PROCESS_DROP_EP) {
            MVCCList<EPropertyMVCC>* mvcc_list = process_item.mvcc_list;
            mvcc_list->CommitVersion(trx_id, commit_time);
        } else if (process_item.type == TransactionItem::PROCESS_ADD_V ||
                   process_item.type == TransactionItem::PROCESS_DROP_V) {
            MVCCList<VertexMVCC>* mvcc_list = process_item.mvcc_list;
            mvcc_list->CommitVersion(trx_id, commit_time);
        } else if (process_item.type == TransactionItem::PROCESS_ADD_E ||
                   process_item.type == TransactionItem::PROCESS_DROP_E) {
            MVCCList<EdgeMVCC>* mvcc_list = process_item.mvcc_list;
            mvcc_list->CommitVersion(trx_id, commit_time);
        }
    }

    transaction_process_map_.erase(t_accessor);
}

void DataStorage::Abort(const uint64_t& trx_id) {
    TransactionAccessor t_accessor;
    if (!transaction_process_map_.find(t_accessor, trx_id)) {
        return;
    }

    for (auto process_item : t_accessor->second.process_set) {
        if (process_item.type == TransactionItem::PROCESS_MODIFY_VP ||
            process_item.type == TransactionItem::PROCESS_ADD_VP ||
            process_item.type == TransactionItem::PROCESS_DROP_VP) {
            MVCCList<VPropertyMVCC>* mvcc_list = process_item.mvcc_list;
            auto header_to_free = mvcc_list->AbortVersion(trx_id);
            if (!header_to_free.IsEmpty())
                vp_store_->FreeValue(header_to_free);
        } else if (process_item.type == TransactionItem::PROCESS_MODIFY_EP ||
                   process_item.type == TransactionItem::PROCESS_ADD_EP ||
                   process_item.type == TransactionItem::PROCESS_DROP_EP) {
            MVCCList<EPropertyMVCC>* mvcc_list = process_item.mvcc_list;
            auto header_to_free = mvcc_list->AbortVersion(trx_id);
            if (!header_to_free.IsEmpty())
                ep_store_->FreeValue(header_to_free);
        } else if (process_item.type == TransactionItem::PROCESS_DROP_V ||
                   process_item.type == TransactionItem::PROCESS_ADD_V) {
            MVCCList<VertexMVCC>* mvcc_list = process_item.mvcc_list;
            mvcc_list->AbortVersion(trx_id);
        } else if (process_item.type == TransactionItem::PROCESS_ADD_E ||
                   process_item.type == TransactionItem::PROCESS_DROP_E) {
            MVCCList<EdgeMVCC>* mvcc_list = process_item.mvcc_list;
            auto e_item_to_free = mvcc_list->AbortVersion(trx_id);
            if (e_item_to_free.ep_row_list != nullptr) {
                e_item_to_free.ep_row_list->SelfGarbageCollect();
                delete e_item_to_free.ep_row_list;
            }
        }
    }

    transaction_process_map_.erase(t_accessor);
}
