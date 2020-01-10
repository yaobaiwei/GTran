/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "layout/data_storage.hpp"
#include "layout/garbage_collector.hpp"

template<class MVCC> ConcurrentMemPool<MVCC>* MVCCList<MVCC>::mem_pool_ = nullptr;
template<class PropertyRow> ConcurrentMemPool<PropertyRow>* PropertyRowList<PropertyRow>::mem_pool_ = nullptr;
template<class PropertyRow> MVCCValueStore* PropertyRowList<PropertyRow>::value_storage_ = nullptr;
ConcurrentMemPool<VertexEdgeRow>* TopologyRowList::mem_pool_ = nullptr;
MVCCValueStore* VPropertyMVCCItem::value_store = nullptr;
MVCCValueStore* EPropertyMVCCItem::value_store = nullptr;

template<class MVCC> Config* MVCCList<MVCC>::config_ = Config::GetInstance();

void DataStorage::Init() {
    node_ = Node::StaticInstance();
    config_ = Config::GetInstance();
    id_mapper_ = SimpleIdMapper::GetInstance();
    snapshot_manager_ = MPISnapshotManager::GetInstance();
    worker_rank_ = node_.get_local_rank();
    worker_size_ = node_.get_local_size();
    // allow the main thread and GCConsumer threads to use memory pool
    nthreads_ = config_->global_num_threads + 1 + config_->num_gc_consumer;
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

    hdfs_data_loader_->GetStringIndexes();

    hdfs_data_loader_->LoadVertexData();
    if (config_->predict_container_usage)
        PredictVertexContainerUsage();
    FillVertexContainer();
    hdfs_data_loader_->FreeVertexMemory();

    hdfs_data_loader_->LoadEdgeData();
    if (config_->predict_container_usage)
        PredictEdgeContainerUsage();
    FillEdgeContainer();
    hdfs_data_loader_->FreeEdgeMemory();

    delete snapshot_manager_;
    delete hdfs_data_loader_;

    garbage_collector_ = GarbageCollector::GetInstance();

    trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();

    node_.Rank0PrintfWithWorkerBarrier("DataStorage::Init() all finished\n");
}

void DataStorage::CreateContainer() {
    ve_row_pool_ = ConcurrentMemPool<VertexEdgeRow>::GetInstance(
                            nullptr, config_->global_ve_row_pool_size, nthreads_,
                            config_->global_enable_mem_pool_utilization_record);
    vp_row_pool_ = ConcurrentMemPool<VertexPropertyRow>::GetInstance(
                            nullptr, config_->global_vp_row_pool_size, nthreads_,
                            config_->global_enable_mem_pool_utilization_record);
    ep_row_pool_ = ConcurrentMemPool<EdgePropertyRow>::GetInstance(
                            nullptr, config_->global_ep_row_pool_size, nthreads_,
                            config_->global_enable_mem_pool_utilization_record);
    vp_mvcc_pool_ = ConcurrentMemPool<VPropertyMVCCItem>::GetInstance(
                            nullptr, config_->global_vp_mvcc_pool_size, nthreads_,
                            config_->global_enable_mem_pool_utilization_record);
    ep_mvcc_pool_ = ConcurrentMemPool<EPropertyMVCCItem>::GetInstance(
                            nullptr, config_->global_ep_mvcc_pool_size, nthreads_,
                            config_->global_enable_mem_pool_utilization_record);
    vertex_mvcc_pool_ = ConcurrentMemPool<VertexMVCCItem>::GetInstance(
                            nullptr, config_->global_v_mvcc_pool_size, nthreads_,
                            config_->global_enable_mem_pool_utilization_record);
    edge_mvcc_pool_ = ConcurrentMemPool<EdgeMVCCItem>::GetInstance(
                            nullptr, config_->global_e_mvcc_pool_size, nthreads_,
                            config_->global_enable_mem_pool_utilization_record);

    MVCCList<VPropertyMVCCItem>::SetGlobalMemoryPool(vp_mvcc_pool_);
    MVCCList<EPropertyMVCCItem>::SetGlobalMemoryPool(ep_mvcc_pool_);
    MVCCList<VertexMVCCItem>::SetGlobalMemoryPool(vertex_mvcc_pool_);
    MVCCList<EdgeMVCCItem>::SetGlobalMemoryPool(edge_mvcc_pool_);
    PropertyRowList<EdgePropertyRow>::SetGlobalMemoryPool(ep_row_pool_);
    PropertyRowList<VertexPropertyRow>::SetGlobalMemoryPool(vp_row_pool_);
    TopologyRowList::SetGlobalMemoryPool(ve_row_pool_);

    uint64_t vp_sz = GiB2B(config_->global_vertex_property_kv_sz_gb);
    uint64_t ep_sz = GiB2B(config_->global_edge_property_kv_sz_gb);
    vp_store_ = new MVCCValueStore(nullptr, vp_sz / (MEM_ITEM_SIZE + sizeof(OffsetT)), nthreads_,
                                   config_->global_enable_mem_pool_utilization_record);
    ep_store_ = new MVCCValueStore(nullptr, ep_sz / (MEM_ITEM_SIZE + sizeof(OffsetT)), nthreads_,
                                   config_->global_enable_mem_pool_utilization_record);
    PropertyRowList<VertexPropertyRow>::SetGlobalValueStore(vp_store_);
    PropertyRowList<EdgePropertyRow>::SetGlobalValueStore(ep_store_);
    VPropertyMVCCItem::SetGlobalValueStore(vp_store_);
    EPropertyMVCCItem::SetGlobalValueStore(ep_store_);
}

// Calculate the thresholds of printing progress lines in the "load V" loop.
void DataStorage::InitPrintFillVProgress() {
    int vtx_sz = hdfs_data_loader_->shuffled_vtx_.size();

    for (int i = 0; i < progress_print_count_; i++) {
        // Calculate the threshold of printing a specific (i + 1) line
        threshold_print_progress_v_[i] = vtx_sz * (i + 1.0) / progress_print_count_;
        
        // In case of precision loss of floating-point calculation
        if (threshold_print_progress_v_[i] > vtx_sz)
            threshold_print_progress_v_[i] = vtx_sz;
    }
}

// Calculate the thresholds of printing progress lines in "load inE" and "load outE" loops.
// Similar to InitPrintFillVProgress.
void DataStorage::InitPrintFillEProgress() {
    int out_e_sz = hdfs_data_loader_->shuffled_out_edge_.size();
    int in_e_sz = hdfs_data_loader_->shuffled_in_edge_.size();

    for (int i = 0; i < progress_print_count_; i++) {
        threshold_print_progress_out_e_[i] = out_e_sz * (i + 1.0) / progress_print_count_;

        if (threshold_print_progress_out_e_[i] > out_e_sz)
            threshold_print_progress_out_e_[i] = out_e_sz;

        threshold_print_progress_in_e_[i] = in_e_sz * (i + 1.0) / progress_print_count_;

        if (threshold_print_progress_in_e_[i] > in_e_sz)
            threshold_print_progress_in_e_[i] = in_e_sz;
    }
}

// Print lines of loading progress of a "filling V/inE/outE" loop.
void DataStorage::PrintFillingProgress(int idx, int& printed_count, const int thresholds[], string progress_header) {
    if (printed_count == progress_print_count_)
        return;

    // Check if a specific percentage of data has been loaded.
    if (idx >= thresholds[printed_count]) {
        double ratio_to_print = 100 * (printed_count + 1.0) / progress_print_count_;

        // Barrier invoked. Need to guarantee that all workers call Rank0PrintfWithWorkerBarrier as many times.
        node_.Rank0PrintfWithWorkerBarrier("%s, %.2f%% finished\n", progress_header.c_str(), ratio_to_print);
        printed_count++;

        // Recursive calling. Make sure that all lines will be printed even if the loop length is less than the count of progress lines.
        PrintFillingProgress(idx, printed_count, thresholds, progress_header);
    }
}

string DataStorage::GetUsageString(UsageMap usage_map) {
    string ret;

    for (auto usage_pair : usage_map) {
        ret += "[" + usage_pair.first + "]: ";
        ret += "usage: " + to_string(usage_pair.second.first);
        ret += ", total: " + to_string(usage_pair.second.second);
        ret += ", usage ratio: " + to_string(usage_pair.second.first * 100.0 / usage_pair.second.second) + "%\n";
    }

    return ret;
}

void DataStorage::PredictVertexContainerUsage() {
    size_t vp_store_item_count = GiB2B(config_->global_vertex_property_kv_sz_gb) / (MEM_ITEM_SIZE + sizeof(OffsetT));

    size_t vp_row_usage = 0;
    size_t vp_mvcc_usage = 0;
    size_t v_mvcc_usage = hdfs_data_loader_->shuffled_vtx_.size();
    size_t vp_store_cell_usage = 0;

    // Emulate the insertion of vertices
    for (auto vtx : hdfs_data_loader_->shuffled_vtx_) {
        int vp_row_cost = vtx.vp_label_list.size() / VP_ROW_ITEM_COUNT;
        if (vp_row_cost * VP_ROW_ITEM_COUNT != vtx.vp_label_list.size())
            vp_row_cost++;
        vp_row_usage += vp_row_cost;

        vp_mvcc_usage += vtx.vp_label_list.size();

        for (int i = 0; i < vtx.vp_label_list.size(); i++) {
            int cell_cost = (vtx.vp_value_list[i].content.size() + 1) / MEM_ITEM_SIZE;
            if (cell_cost * MEM_ITEM_SIZE != vtx.vp_value_list[i].content.size() + 1)
                cell_cost++;
            vp_store_cell_usage += cell_cost;
        }
    }

    UsageMap usage_map;
    usage_map["V mvcc"] = {v_mvcc_usage, config_->global_v_mvcc_pool_size};
    usage_map["VP row"] = {vp_row_usage, config_->global_vp_row_pool_size};
    usage_map["VP mvcc"] = {vp_mvcc_usage, config_->global_vp_mvcc_pool_size};
    usage_map["VP store cell"] = {vp_store_cell_usage, vp_store_item_count};

    node_.Rank0PrintfWithWorkerBarrier(("Predicted container usage for vertices:\n" + GetUsageString(usage_map)).c_str());
}

void DataStorage::PredictEdgeContainerUsage() {
    size_t ep_store_item_count = GiB2B(config_->global_edge_property_kv_sz_gb) / (MEM_ITEM_SIZE + sizeof(OffsetT));

    size_t ep_row_usage = 0;
    size_t ep_mvcc_usage = 0;
    size_t e_mvcc_usage = hdfs_data_loader_->shuffled_out_edge_.size() + hdfs_data_loader_->shuffled_in_edge_.size();
    size_t ep_store_cell_usage = 0;
    size_t ve_row_usage = 0;

    // To emulate the insertion of VE row, necessary for getting ve_row_usage
    unordered_map<uint32_t, uint32_t> vtx_edge_cell_count;

    // Emulate the insertion of out edges
    for (auto edge : hdfs_data_loader_->shuffled_out_edge_) {
        int ep_row_cost = edge.ep_label_list.size() / EP_ROW_ITEM_COUNT;
        if (ep_row_cost * EP_ROW_ITEM_COUNT != edge.ep_label_list.size())
            ep_row_cost++;
        ep_row_usage += ep_row_cost;

        ep_mvcc_usage += edge.ep_label_list.size();

        for (int i = 0; i < edge.ep_label_list.size(); i++) {
            int cell_cost = (edge.ep_value_list[i].content.size() + 1) / MEM_ITEM_SIZE;
            if (cell_cost * MEM_ITEM_SIZE != edge.ep_value_list[i].content.size() + 1)
                cell_cost++;
            ep_store_cell_usage += cell_cost;
        }

        uint32_t src_vid = edge.id.out_v;
        if (vtx_edge_cell_count.count(src_vid) == 0)
            vtx_edge_cell_count.insert({src_vid, 1});
        else
            vtx_edge_cell_count.at(src_vid)++;

        if (id_mapper_->IsVertexLocal(edge.id.in_v)) {
            e_mvcc_usage++;

            uint32_t dst_vid = edge.id.in_v;
            if (vtx_edge_cell_count.count(dst_vid) == 0)
                vtx_edge_cell_count.insert({dst_vid, 1});
            else
                vtx_edge_cell_count.at(dst_vid)++;
        }
    }

    // Emulate the insertion of in edges
    for (auto edge : hdfs_data_loader_->shuffled_in_edge_) {
        uint32_t dst_vid = edge.id.in_v;
        if (vtx_edge_cell_count.count(dst_vid) == 0)
            vtx_edge_cell_count.insert({dst_vid, 1});
        else
            vtx_edge_cell_count.at(dst_vid)++;
    }

    // Only when the (emulated) insertion is finished, can ve_row_usage be estimated.
    for (auto p : vtx_edge_cell_count) {
        int ve_row_cost = p.second / VE_ROW_ITEM_COUNT;
        if (ve_row_cost * VE_ROW_ITEM_COUNT != p.second)
            ve_row_cost++;
        ve_row_usage += ve_row_cost;
    }

    UsageMap usage_map;

    usage_map["E mvcc"] = {e_mvcc_usage, config_->global_e_mvcc_pool_size};
    usage_map["EP row"] = {ep_row_usage, config_->global_ep_row_pool_size};
    usage_map["EP mvcc"] = {ep_mvcc_usage, config_->global_ep_mvcc_pool_size};
    usage_map["EP store cell"] = {ep_store_cell_usage, ep_store_item_count};
    usage_map["VE row"] = {ve_row_usage, config_->global_ve_row_pool_size};

    node_.Rank0PrintfWithWorkerBarrier(("Predicted container usage for edges:\n" + GetUsageString(usage_map)).c_str());
}

DataStorage::UsageMap DataStorage::GetContainerUsageMap() {
    UsageMap usage_map;

    usage_map["E mvcc"] = edge_mvcc_pool_->UsageStatistic();
    usage_map["EP row"] = ep_row_pool_->UsageStatistic();
    usage_map["EP mvcc"] = ep_mvcc_pool_->UsageStatistic();
    usage_map["EP store"] = ep_store_->UsageStatistic();

    usage_map["V mvcc"] = vertex_mvcc_pool_->UsageStatistic();
    usage_map["VE row"] = ve_row_pool_->UsageStatistic();
    usage_map["VP row"] = vp_row_pool_->UsageStatistic();
    usage_map["VP mvcc"] = vp_mvcc_pool_->UsageStatistic();
    usage_map["VP store"] = vp_store_->UsageStatistic();

    size_t total_container_size = 0, total_container_usage = 0;

    for (auto usage_pair : usage_map) {
        total_container_usage += usage_pair.second.first;
        total_container_size += usage_pair.second.second;
    }

    usage_map["total"] = {total_container_usage, total_container_size};
    return usage_map;
}

string DataStorage::GetContainerUsageString() {
    UsageMap usage_map = GetContainerUsageMap();

    return GetUsageString(usage_map);
}

double DataStorage::GetContainerUsage() {
    UsageMap usage_map = GetContainerUsageMap();

    return usage_map["total"].first * 1.0 / usage_map["total"].second;
}

void DataStorage::FillVertexContainer() {
    indexes_ = hdfs_data_loader_->indexes_;

    int max_vid = worker_rank_;

    InitPrintFillVProgress();
    for (int i = 0; i < hdfs_data_loader_->shuffled_vtx_.size(); i++) {
        PrintFillingProgress(i, v_printed_progress_, threshold_print_progress_v_, "DataStorage::FillVertexContainer");

        const TMPVertex& vtx = hdfs_data_loader_->shuffled_vtx_[i];

        // std::pair<VertexIterator iterator_to_inserted_item, bool insert_success>
        auto insert_result = vertex_map_.insert(pair<uint32_t, Vertex>(vtx.id.value(), Vertex()));
        VertexIterator v_itr = insert_result.first;

        if (max_vid < vtx.id.value())
            max_vid = vtx.id.value();

        v_itr->second.label = vtx.label;
        // create containers that attached to a Vertex
        v_itr->second.vp_row_list = new PropertyRowList<VertexPropertyRow>;
        v_itr->second.ve_row_list = new TopologyRowList;

        v_itr->second.vp_row_list->Init();
        v_itr->second.ve_row_list->Init(vtx.id);

        v_itr->second.mvcc_list = new MVCCList<VertexMVCCItem>;
        v_itr->second.mvcc_list->AppendInitialVersion()[0] = true;
        // true means that the Vertex is visible

        // Insert vertex properties
        for (int i = 0; i < vtx.vp_label_list.size(); i++) {
            v_itr->second.vp_row_list->InsertInitialCell(vpid_t(vtx.id, vtx.vp_label_list[i]),
                                                                vtx.vp_value_list[i]);
        }
    }
    PrintFillingProgress(hdfs_data_loader_->shuffled_vtx_.size(), v_printed_progress_, threshold_print_progress_v_, "DataStorage::FillVertexContainer");

    num_of_vertex_ = (max_vid - worker_rank_) / worker_size_;

    node_.LocalSequentialDebugPrint("vp_row_pool_: " + vp_row_pool_->UsageString());
    node_.LocalSequentialDebugPrint("vp_mvcc_pool_: " + vp_mvcc_pool_->UsageString());
    node_.LocalSequentialDebugPrint("vertex_mvcc_pool_: " + vertex_mvcc_pool_->UsageString());
    node_.LocalSequentialDebugPrint("vp_store_: " + vp_store_->UsageString());

    node_.Rank0PrintfWithWorkerBarrier("DataStorage::FillVertexContainer() finished\n");
}

void DataStorage::FillEdgeContainer() {
    InitPrintFillEProgress();

    // Insert edge properties and outE
    for (int i = 0; i < hdfs_data_loader_->shuffled_out_edge_.size(); i++) {
        PrintFillingProgress(i, out_e_printed_progress_,
                             threshold_print_progress_out_e_, "DataStorage::FillEdgeContainer, out edge");

        const TMPOutEdge& edge = hdfs_data_loader_->shuffled_out_edge_[i];

        auto insert_result = out_edge_map_.insert(pair<uint64_t, OutEdge>(edge.id.value(), OutEdge()));
        OutEdgeIterator out_e_itr = insert_result.first;

        VertexIterator v_itr = vertex_map_.find(edge.id.out_v);

        auto* ep_row_list = new PropertyRowList<EdgePropertyRow>;
        ep_row_list->Init();

        // "true" means that is_out = true, as this edge is an outE to the Vertex
        auto* mvcc_list = v_itr->second.ve_row_list
                          ->InsertInitialCell(true, edge.id.in_v, edge.label, ep_row_list);
        out_e_itr->second.mvcc_list = mvcc_list;

        EdgeVersion edge_version;
        out_e_itr->second.mvcc_list->GetVisibleVersion(0, 0, true, edge_version);

        for (int i = 0; i < edge.ep_label_list.size(); i++) {
            edge_version.ep_row_list->InsertInitialCell(epid_t(edge.id, edge.ep_label_list[i]), edge.ep_value_list[i]);
        }

        // check if the dst_v on this node
        if (id_mapper_->IsVertexLocal(edge.id.in_v)) {
            auto insert_result = in_edge_map_.insert(pair<uint64_t, InEdge>(edge.id.value(), InEdge()));
            InEdgeIterator in_e_itr = insert_result.first;

            VertexIterator v_itr = vertex_map_.find(edge.id.in_v);

            // "false" means that is_out = false, as this edge is an inE to the Vertex
            auto* mvcc_list = v_itr->second.ve_row_list
                              ->InsertInitialCell(false, edge.id.out_v, edge.label, nullptr);

            // pointer of MVCCList<EdgeMVCCItem> is shared with in_edge_map_
            in_e_itr->second.mvcc_list = mvcc_list;
        }
    }
    PrintFillingProgress(hdfs_data_loader_->shuffled_out_edge_.size(), out_e_printed_progress_,
                         threshold_print_progress_out_e_, "DataStorage::FillEdgeContainer, out edge");

    for (int i = 0; i < hdfs_data_loader_->shuffled_in_edge_.size(); i++) {
        PrintFillingProgress(i, in_e_printed_progress_,
                             threshold_print_progress_in_e_, "DataStorage::FillEdgeContainer, in edge");

        const TMPInEdge& edge = hdfs_data_loader_->shuffled_in_edge_[i];
    
        auto insert_result = in_edge_map_.insert(pair<uint64_t, InEdge>(edge.id.value(), InEdge()));
        InEdgeIterator in_e_itr = insert_result.first;

        VertexIterator v_itr = vertex_map_.find(edge.id.in_v);

        // "false" means that is_out = false, as this edge is an inE to the Vertex
        auto* mvcc_list = v_itr->second.ve_row_list
                          ->InsertInitialCell(false, edge.id.out_v, edge.label, nullptr);

        // pointer of MVCCList<EdgeMVCCItem> is shared with in_edge_map_
        in_e_itr->second.mvcc_list = mvcc_list;
    }
    PrintFillingProgress(hdfs_data_loader_->shuffled_in_edge_.size(), in_e_printed_progress_,
                         threshold_print_progress_in_e_, "DataStorage::FillEdgeContainer, in edge");

    node_.LocalSequentialDebugPrint("ve_row_pool_: " + ve_row_pool_->UsageString());
    node_.LocalSequentialDebugPrint("ep_row_pool_: " + ep_row_pool_->UsageString());
    node_.LocalSequentialDebugPrint("ep_mvcc_pool_: " + ep_mvcc_pool_->UsageString());
    node_.LocalSequentialDebugPrint("edge_mvcc_pool_: " + edge_mvcc_pool_->UsageString());
    node_.LocalSequentialDebugPrint("ep_store_: " + ep_store_->UsageString());

    node_.Rank0PrintfWithWorkerBarrier("DataStorage::FillEdgeContainer() finished\n");
}

READ_STAT DataStorage::GetOutEdgeItem(OutEdgeConstIterator& out_e_iterator, const eid_t& eid,
                                      const uint64_t& trx_id, const uint64_t& begin_time,
                                      const bool& read_only, EdgeVersion& item_ref) {
    ReaderLockGuard reader_lock_guard(out_edge_erase_rwlock_);

    out_e_iterator = out_edge_map_.find(eid.value());

    if (out_e_iterator == out_edge_map_.end()) {   // Not Found
        return READ_STAT::NOTFOUND;
    }

    pair<bool, bool> is_visible = out_e_iterator->second.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, item_ref);

    if (!is_visible.first)
        return READ_STAT::ABORT;
    if (!is_visible.second)
        return READ_STAT::NOTFOUND;

    return READ_STAT::SUCCESS;
}

READ_STAT DataStorage::CheckVertexVisibility(const VertexConstIterator& v_iterator, const uint64_t& trx_id,
                                             const uint64_t& begin_time, const bool& read_only) {
    VertexMVCCItem* visible_version;
    bool exists;
    pair<bool, bool> is_visible = v_iterator->second.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, exists);

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
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
    vid_t vid = pid.vid;

    VertexConstIterator v_iterator = vertex_map_.find(vid.value());

    if (v_iterator == vertex_map_.end()) {
        return READ_STAT::NOTFOUND;
    }

    /* Check if the vertex with given vid is invisible, which means that the read dependency (vertex)
     * of this transaction has been modified.
     * Similarly hereinafter.
     */
    if (CheckVertexVisibility(v_iterator, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        /* Update the global status of this transaction to ABORT.
         * Similarly hereinafter.
         */
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    auto stat = v_iterator->second.vp_row_list->ReadProperty(pid, trx_id, begin_time, read_only, ret);

    if (stat == READ_STAT::ABORT)
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

    return stat;
}

READ_STAT DataStorage::GetAllVP(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                                const bool& read_only, vector<pair<label_t, value_t>>& ret) {
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
    VertexConstIterator v_iterator = vertex_map_.find(vid.value());

    if (v_iterator == vertex_map_.end()) {
        return READ_STAT::NOTFOUND;
    }

    if (CheckVertexVisibility(v_iterator, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    auto stat = v_iterator->second.vp_row_list->ReadAllProperty(trx_id, begin_time, read_only, ret);

    if (stat == READ_STAT::ABORT)
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

    return stat;
}

READ_STAT DataStorage::GetVPByPKeyList(const vid_t& vid, const vector<label_t>& p_key,
                                       const uint64_t& trx_id, const uint64_t& begin_time,
                                       const bool& read_only, vector<pair<label_t, value_t>>& ret) {
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
    VertexConstIterator v_iterator = vertex_map_.find(vid.value());

    if (v_iterator == vertex_map_.end()) {
        return READ_STAT::NOTFOUND;
    }

    if (CheckVertexVisibility(v_iterator, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    auto stat = v_iterator->second.vp_row_list->ReadPropertyByPKeyList(p_key, trx_id, begin_time, read_only, ret);

    if (stat == READ_STAT::ABORT)
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

    return stat;
}

READ_STAT DataStorage::GetVPidList(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                                   const bool& read_only, vector<vpid_t>& ret) {
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
    VertexConstIterator v_iterator = vertex_map_.find(vid.value());

    if (v_iterator == vertex_map_.end()) {
        return READ_STAT::NOTFOUND;
    }

    if (CheckVertexVisibility(v_iterator, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    auto stat = v_iterator->second.vp_row_list->ReadPidList(trx_id, begin_time, read_only, ret);

    if (stat == READ_STAT::ABORT)
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

    return stat;
}

READ_STAT DataStorage::GetVL(const vid_t& vid, const uint64_t& trx_id,
                             const uint64_t& begin_time, const bool& read_only, label_t& ret) {
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
    VertexConstIterator v_iterator = vertex_map_.find(vid.value());

    if (v_iterator == vertex_map_.end()) {
        return READ_STAT::NOTFOUND;
    }

    if (CheckVertexVisibility(v_iterator, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    ret = v_iterator->second.label;

    return READ_STAT::SUCCESS;
}

READ_STAT DataStorage::GetEPByPKey(const epid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time,
                                   const bool& read_only, value_t& ret) {
    eid_t eid = eid_t(pid.in_vid, pid.out_vid);

    OutEdgeConstIterator out_e_iterator;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_iterator, eid, trx_id, begin_time, read_only, edge_version);
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
    OutEdgeConstIterator out_e_iterator;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_iterator, eid, trx_id, begin_time, read_only, edge_version);
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
    OutEdgeConstIterator out_e_iterator;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_iterator, eid, trx_id, begin_time, read_only, edge_version);
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
    OutEdgeConstIterator out_e_iterator;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_iterator, eid, trx_id, begin_time, read_only, edge_version);
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
    OutEdgeConstIterator out_e_iterator;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_iterator, eid, trx_id, begin_time, read_only, edge_version);
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
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
    VertexConstIterator v_iterator = vertex_map_.find(vid.value());

    if (v_iterator == vertex_map_.end()) {
        return READ_STAT::NOTFOUND;
    }

    if (CheckVertexVisibility(v_iterator, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    auto stat = v_iterator->second.ve_row_list->ReadConnectedVertex(direction, edge_label,
                                                                    trx_id, begin_time, read_only, ret);

    if (stat == READ_STAT::ABORT)
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

    return stat;
}

READ_STAT DataStorage::GetConnectedEdgeList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                            const uint64_t& trx_id, const uint64_t& begin_time,
                                            const bool& read_only, vector<eid_t>& ret, bool need_read_lock) {
    WritePriorRWLock* lock_ptr = need_read_lock ? &vertex_map_erase_rwlock_ : nullptr;
    ReaderLockGuard reader_lock_guard(*lock_ptr);
    VertexConstIterator v_iterator = vertex_map_.find(vid.value());

    if (v_iterator == vertex_map_.end()) {
        return READ_STAT::NOTFOUND;
    }

    if (CheckVertexVisibility(v_iterator, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return READ_STAT::ABORT;
    }

    auto stat = v_iterator->second.ve_row_list->ReadConnectedEdge(direction, edge_label,
                                                                  trx_id, begin_time, read_only, ret);

    if (stat == READ_STAT::ABORT)
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);

    return stat;
}

READ_STAT DataStorage::GetAllVertices(const uint64_t& trx_id, const uint64_t& begin_time,
                                      const bool& read_only, vector<vid_t>& ret) {
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
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
    ReaderLockGuard reader_lock_guard(out_edge_erase_rwlock_);
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

bool DataStorage::CheckVertexVisibilityWithVid(const uint64_t& trx_id, const uint64_t& begin_time,
                                        const bool& read_only, vid_t& vid) {
    // Check visibility of the vertex
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
    VertexConstIterator v_iterator = vertex_map_.find(vid.value());

    if (v_iterator == vertex_map_.end()) {
        return false;
    }

    if (CheckVertexVisibility(v_iterator, trx_id, begin_time, read_only) != READ_STAT::SUCCESS) {
        // Invisible
        return false;
    }
    return true;
}

bool DataStorage::CheckEdgeVisibilityWithEid(const uint64_t& trx_id, const uint64_t& begin_time,
                                      const bool& read_only, eid_t& eid) {
    // Check visibility of the edge
    OutEdgeConstIterator out_e_iterator;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_iterator, eid, trx_id, begin_time, read_only, edge_version);
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

vid_t DataStorage::AssignVID() {
    int vid_local = ++num_of_vertex_;
    return vid_t(vid_local * worker_size_ + worker_rank_);
}

/* For each Process function, a MVCCList instance will be modified. InsertTrxProcessHistory will record the pointer
 * of MVCCList in corresponding trx's TrxProcessHistory, which will be necessary when calling Abort or Commit.
 */
void DataStorage::InsertTrxProcessHistory(const uint64_t& trx_id, const TrxProcessHistory::ProcessType& type,
                                         void* mvcc_list) {
    CHECK(type != TrxProcessHistory::PROCESS_ADD_V);
    TransactionAccessor t_accessor;
    transaction_process_history_map_.insert(t_accessor, trx_id);

    TrxProcessHistory::ProcessRecord q_item;
    q_item.type = type;
    q_item.mvcc_list = mvcc_list;

    t_accessor->second.process_vector.emplace_back(q_item);
}

/* However, if we want to abort AddV, the pointer of MVCCList is not enough, since we need to free vp_row_list
 * and ve_row_list attached to the Vertex added. InsertTrxAddVHistory requires one more parameter (vid) than
 * InsertTrxProcessHistory, dedicated for ProcessAddV.
 */
void DataStorage::InsertTrxAddVHistory(const uint64_t& trx_id, void* mvcc_list, vid_t vid) {
    TransactionAccessor t_accessor;
    transaction_process_history_map_.insert(t_accessor, trx_id);

    TrxProcessHistory::ProcessRecord q_item;
    q_item.type = TrxProcessHistory::PROCESS_ADD_V;
    q_item.mvcc_list = mvcc_list;

    t_accessor->second.process_vector.emplace_back(q_item);

    // this map will be used when the trx is aborted
    t_accessor->second.mvcclist_to_vid_map[mvcc_list] = vid.value();
}

vid_t DataStorage::ProcessAddV(const label_t& label, const uint64_t& trx_id, const uint64_t& begin_time) {
    /* Guaranteed that the vid is identical in the whole system.
     * Thus, it's impossible to insert two vertex with the same vid
     */
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
    vid_t vid = AssignVID();

    // std::pair<VertexIterator iterator, bool insert_success>
    auto insert_result = vertex_map_.insert(pair<uint32_t, Vertex>(vid.value(), Vertex()));
    VertexIterator v_iterator = insert_result.first;
    CHECK(insert_result.second) << "Vid " << to_string(vid.value()) << " already exist in vertex_map_";

    v_iterator->second.label = label;
    v_iterator->second.vp_row_list = new PropertyRowList<VertexPropertyRow>;
    v_iterator->second.ve_row_list = new TopologyRowList;

    v_iterator->second.ve_row_list->Init(vid);
    v_iterator->second.vp_row_list->Init();

    auto* mvcc_list = new MVCCList<VertexMVCCItem>;

    // this won't fail as it's the first version in the list
    mvcc_list->AppendVersion(trx_id, begin_time)[0] = true;

    v_iterator->second.mvcc_list = mvcc_list;

    /* The only usage of InsertTrxAddVHistory in the project,
     * since vid is needed when aborting AddV.
     */
    InsertTrxAddVHistory(trx_id, v_iterator->second.mvcc_list, v_iterator->first);

    return vid;
}

PROCESS_STAT DataStorage::ProcessDropV(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                                       vector<eid_t>& in_eids, vector<eid_t>& out_eids) {
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
    VertexConstIterator v_iterator = vertex_map_.find(vid.value());

    if (v_iterator == vertex_map_.end()) {
        return PROCESS_STAT::SUCCESS;
    }

    vector<eid_t> all_connected_edge;
    // Do not need to acquire read lock for vertex_map_erase_rwlock_ in GetConnectedEdgeList
    auto read_stat = GetConnectedEdgeList(vid, 0, BOTH, trx_id, begin_time, false, all_connected_edge, false);

    if (read_stat != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_DROP_V_GET_CONN_E;
    }

    bool* mvcc_value_ptr = v_iterator->second.mvcc_list->AppendVersion(trx_id, begin_time);

    /* If AppendVersion returns nullptr, the transaction should be aborted.
     * Similarly hereinafter.
     */
    if (mvcc_value_ptr == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_DROP_V_APPEND;
    }

    // false means invisible
    mvcc_value_ptr[0] = false;

    InsertTrxProcessHistory(trx_id, TrxProcessHistory::PROCESS_DROP_V, v_iterator->second.mvcc_list);

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

    return PROCESS_STAT::SUCCESS;
}


/* If we are going to add an edge from v1 (on worker1) to v2 (on worker2) with edge id e1,
 * on worker1:
 *      ProcessAddE(e1, label, true, trx_id, begin_time);
 * on worker2:
 *      ProcessAddE(e1, label, false, trx_id, begin_time);
 */
PROCESS_STAT DataStorage::ProcessAddE(const eid_t& eid, const label_t& label, const bool& is_out,
                                      const uint64_t& trx_id, const uint64_t& begin_time) {
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
    WritePriorRWLock* erase_rwlock_;

    InEdgeIterator in_e_iterator;
    OutEdgeIterator out_e_iterator;
    bool is_new;
    vid_t src_vid = eid.out_v, dst_vid = eid.in_v;
    vid_t conn_vid, local_vid;

    /* if is_out, this function will add an outE, which means that src_vid is on this node;
     *      else, this function will add an inE, which means that dst_vid is on this node.
     */
    if (is_out) {
        erase_rwlock_ = &out_edge_erase_rwlock_;
        local_vid = src_vid;
        conn_vid = dst_vid;
    } else {
        erase_rwlock_ = &in_edge_erase_rwlock_;
        local_vid = dst_vid;
        conn_vid = src_vid;
    }

    ReaderLockGuard edge_map_rlock_guard(*erase_rwlock_);

    // anyway, need to check if the vertex exists or not
    VertexConstIterator v_iterator = vertex_map_.find(local_vid.value());

    if (v_iterator == vertex_map_.end()) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_ADD_E_INVISIBLE_V;
    }

    if (CheckVertexVisibility(v_iterator, trx_id, begin_time, false) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_ADD_E_INVISIBLE_V;
    }

    if (is_out) {
        // std::pair<OutEdgeIterator itr, bool insert_occurred>
        auto insert_result = out_edge_map_.insert(pair<uint64_t, OutEdge>(eid.value(), OutEdge()));
        out_e_iterator = insert_result.first; 
        is_new = insert_result.second;
    } else {
        auto insert_result = in_edge_map_.insert(pair<uint64_t, InEdge>(eid.value(), InEdge()));
        in_e_iterator = insert_result.first; 
        is_new = insert_result.second;
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
        mvcc_list = v_iterator->second.ve_row_list
                    ->ProcessAddEdge(is_out, conn_vid, label, ep_row_list, trx_id, begin_time);
        if (is_out) {
            out_e_iterator->second.mvcc_list = mvcc_list;
        } else {
            in_e_iterator->second.mvcc_list = mvcc_list;
        }
    } else {
        // do not need to access VertexEdgeRow
        if (is_out) {
            mvcc_list = out_e_iterator->second.mvcc_list;
        } else {
            mvcc_list = in_e_iterator->second.mvcc_list;
        }
        EdgeVersion* e_item = mvcc_list->AppendVersion(trx_id, begin_time);

        if (e_item == nullptr) {
            trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
            return PROCESS_STAT::ABORT_ADD_E_APPEND;
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

    InsertTrxProcessHistory(trx_id, TrxProcessHistory::PROCESS_ADD_E, mvcc_list);

    return PROCESS_STAT::SUCCESS;
}

PROCESS_STAT DataStorage::ProcessDropE(const eid_t& eid, const bool& is_out,
                                       const uint64_t& trx_id, const uint64_t& begin_time) {
    WritePriorRWLock* erase_rwlock_;
    if (is_out)
        erase_rwlock_ = &out_edge_erase_rwlock_;
    else
        erase_rwlock_ = &in_edge_erase_rwlock_;

    ReaderLockGuard edge_map_rlock_guard(*erase_rwlock_);

    InEdgeConstIterator in_e_iterator;
    OutEdgeConstIterator out_e_iterator;
    bool found;
    vid_t src_vid = eid.out_v, dst_vid = eid.in_v;
    vid_t conn_vid;

    /* if is_out, this function will add an outE, which means that src_vid is on this node;
     *      else, this function will add an inE, which means that dst_vid is on this node.
     */
    if (is_out) {
        out_e_iterator = out_edge_map_.find(eid.value());
        found = (out_e_iterator != out_edge_map_.end());
        conn_vid = dst_vid;
    } else {
        in_e_iterator = in_edge_map_.find(eid.value());
        found = (in_e_iterator != in_edge_map_.end());
        conn_vid = src_vid;
    }

    // do nothing
    if (!found)
        return PROCESS_STAT::SUCCESS;

    MVCCList<EdgeMVCCItem>* mvcc_list;

    if (is_out) {
        mvcc_list = out_e_iterator->second.mvcc_list;
    } else {
        mvcc_list = in_e_iterator->second.mvcc_list;
    }
    EdgeVersion* e_item = mvcc_list->AppendVersion(trx_id, begin_time);

    if (e_item == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_DROP_E_APPEND;
    }

    // label == 0 represents that the edge does not exists
    e_item->label = 0;
    e_item->ep_row_list = nullptr;


    InsertTrxProcessHistory(trx_id, TrxProcessHistory::PROCESS_DROP_E, mvcc_list);

    return PROCESS_STAT::SUCCESS;
}

PROCESS_STAT DataStorage::ProcessModifyVP(const vpid_t& pid, const value_t& value, value_t& old_value,
                                          const uint64_t& trx_id, const uint64_t& begin_time) {
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
    VertexConstIterator v_iterator = vertex_map_.find(vid_t(pid.vid).value());

    if (v_iterator == vertex_map_.end()) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_MODIFY_VP_INVISIBLE_V;
    }

    if (CheckVertexVisibility(v_iterator, trx_id, begin_time, false) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_MODIFY_VP_INVISIBLE_V;
    }

    // Modify the property in vp_row_list
    auto ret = v_iterator->second.vp_row_list->ProcessModifyProperty(pid, value, old_value, trx_id, begin_time);

    // ret.second: pointer of MVCCList<VP>
    if (ret.second == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_MODIFY_VP_APPEND;
    }

    TrxProcessHistory::ProcessType process_type;
    // ret.first == true means that the property already exists, and the transaction modified it.
    if (ret.first)
        process_type = TrxProcessHistory::PROCESS_MODIFY_VP;
    else
        process_type = TrxProcessHistory::PROCESS_ADD_VP;

    InsertTrxProcessHistory(trx_id, process_type, ret.second);

    return PROCESS_STAT::SUCCESS;
}

PROCESS_STAT DataStorage::ProcessModifyEP(const epid_t& pid, const value_t& value, value_t& old_value,
                                          const uint64_t& trx_id, const uint64_t& begin_time) {
    OutEdgeConstIterator out_e_iterator;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_iterator, eid_t(pid.in_vid, pid.out_vid), trx_id, begin_time, false, edge_version);

    if (read_stat != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_MODIFY_EP_EITEM;
    }

    if (!edge_version.Exist()) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_MODIFY_EP_DELETED_E;
    }

    // Modify the property in ep_row_list
    auto ret = edge_version.ep_row_list->ProcessModifyProperty(pid, value, old_value, trx_id, begin_time);

    // ret.second: pointer of MVCCList<EP>
    if (ret.second == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_MODIFY_EP_MODIFY;
    }

    TrxProcessHistory::ProcessType process_type;
    // ret.first == true means that the property already exists, and the transaction modified it.
    if (ret.first)
        process_type = TrxProcessHistory::PROCESS_MODIFY_EP;
    else
        process_type = TrxProcessHistory::PROCESS_ADD_EP;

    InsertTrxProcessHistory(trx_id, process_type, ret.second);

    return PROCESS_STAT::SUCCESS;
}

PROCESS_STAT DataStorage::ProcessDropVP(const vpid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t & old_value) {
    ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
    VertexConstIterator v_iterator = vertex_map_.find(vid_t(pid.vid).value());

    if (v_iterator == vertex_map_.end()) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_DROP_VP_INVISIBLE_V;
    }

    if (CheckVertexVisibility(v_iterator, trx_id, begin_time, false) != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_DROP_VP_INVISIBLE_V;
    }

    // ret: pointer of MVCCList<VP>
    auto ret = v_iterator->second.vp_row_list->ProcessDropProperty(pid, trx_id, begin_time, old_value);

    if (ret == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_DROP_VP_DROP;
    }

    InsertTrxProcessHistory(trx_id, TrxProcessHistory::PROCESS_DROP_VP, ret);

    return PROCESS_STAT::SUCCESS;
}

PROCESS_STAT DataStorage::ProcessDropEP(const epid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t & old_value) {
    OutEdgeConstIterator out_e_iterator;
    EdgeVersion edge_version;
    auto read_stat = GetOutEdgeItem(out_e_iterator, eid_t(pid.in_vid, pid.out_vid), trx_id, begin_time, false, edge_version);

    if (read_stat != READ_STAT::SUCCESS) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_DROP_EP_EITEM;
    }

    if (!edge_version.Exist()) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_DROP_EP_DELETED_E;
    }

    // ret: pointer of MVCCList<EP>
    auto ret = edge_version.ep_row_list->ProcessDropProperty(pid, trx_id, begin_time, old_value);

    if (ret == nullptr) {
        trx_table_stub_->update_status(trx_id, TRX_STAT::ABORT);
        return PROCESS_STAT::ABORT_DROP_EP_DROP;
    }

    InsertTrxProcessHistory(trx_id, TrxProcessHistory::PROCESS_DROP_EP, ret);

    return PROCESS_STAT::SUCCESS;
}

/* Commit the transaction with trx_id on this worker.
 * For each MVCCList altered by the transaction:
 *      Makes the last version of the MVCCList visible to transactions with begin_time > commit_time.
 */
void DataStorage::Commit(const uint64_t& trx_id, const uint64_t& commit_time) {
    TransactionAccessor t_accessor;
    if (!transaction_process_history_map_.find(t_accessor, trx_id)) {
        return;
    }

    auto& process_vector = t_accessor->second.process_vector;

    // One MVCCList could be modified for multiple times and repeadedly occur in the process_vector.
    // However, each MVCCList just needs to be committed/aborted only once in the Commit/Abort Phase,
    //      even if it was modified for multiple times.
    // Thus, the set below is introduced to guarantee this.
    unordered_set<TrxProcessHistory::ProcessRecord, TrxProcessHistory::ProcessRecordHash> touched_mvcclist_set;

    for (int i = 0; i < process_vector.size(); i++) {
        auto& process_item = process_vector[i];
        if (touched_mvcclist_set.count(process_item) > 0)
            continue;
        touched_mvcclist_set.emplace(process_item);

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

    transaction_process_history_map_.erase(t_accessor);
}

/* Abort the transaction with trx_id on this worker.
 * For each MVCCList altered by the transaction:
 *      Removes the last version of the MVCCList.
 * Specifically, for aborting AddV, ep_row_list and ve_row_list on the Vertex will be freed.
 */
void DataStorage::Abort(const uint64_t& trx_id) {
    TransactionAccessor t_accessor;
    if (!transaction_process_history_map_.find(t_accessor, trx_id)) {
        return;
    }

    auto& process_vector = t_accessor->second.process_vector;

    auto& mvcclist_to_vid_map = t_accessor->second.mvcclist_to_vid_map;

    unordered_set<TrxProcessHistory::ProcessRecord, TrxProcessHistory::ProcessRecordHash> touched_mvcclist_set;

    /* Reverse abort.
     * Considering a special case: (Q1). Add vertex V1; (Q2). Add property VP1 on V1;
     *   Both (Q1) and (Q2) will be needed to be aborted.
     *   If we abort (Q1) before aborting (Q2), the MVCCList for (Q2) will be deallocated before
     *   aborting (Q2), which will causes undefined behavior.
     *   Thus, we need to abort (Q2) before aborting (Q1).
     */
    for (int i = process_vector.size() - 1; i >= 0; i--) {
        auto& process_item = process_vector[i];
        if (touched_mvcclist_set.count(process_item) > 0)
            continue;
        touched_mvcclist_set.emplace(process_item);

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
            // Notice that the erasure of vertex_map will be done by GC.
            MVCCList<VertexMVCCItem>* v_mvcc_list = process_item.mvcc_list;
            v_mvcc_list->AbortVersion(trx_id);
            if (process_item.type == TrxProcessHistory::PROCESS_ADD_V) {
                // access the item in the v_map
                ReaderLockGuard reader_lock_guard(vertex_map_erase_rwlock_);
                VertexIterator v_iterator = vertex_map_.find(mvcclist_to_vid_map[v_mvcc_list]);

                if (v_iterator == vertex_map_.end()) {
                    CHECK(false) << "[DataStorage] Cannot find vertex when aborting";
                }

                // Attached eid need to be deleted; GC will handle it
                vector<pair<eid_t, bool>> * deletable_eids = new vector<pair<eid_t, bool>>();
                vid_t vid;
                uint2vid_t(mvcclist_to_vid_map[v_mvcc_list], vid);
                v_iterator->second.ve_row_list->SelfGarbageCollect(vid, deletable_eids);
                garbage_collector_->PushGCAbleEidToQueue(deletable_eids);

                delete v_iterator->second.ve_row_list;
                v_iterator->second.ve_row_list = nullptr;
                v_iterator->second.vp_row_list->SelfGarbageCollect();
                delete v_iterator->second.vp_row_list;
                v_iterator->second.vp_row_list = nullptr;
                v_iterator->second.mvcc_list->SelfGarbageCollect();
                // Do not delete v_iterator->second.mvcc_list, since it will still be referred.
            }
        } else if (process_item.type == TrxProcessHistory::PROCESS_ADD_E ||
                   process_item.type == TrxProcessHistory::PROCESS_DROP_E) {
            // E related
            // Notice that the erasure of in_edge_map and out_edge_map will be done by GC.
            MVCCList<EdgeMVCCItem>* e_mvcc_list = process_item.mvcc_list;
            e_mvcc_list->AbortVersion(trx_id);
        }
    }

    transaction_process_history_map_.erase(t_accessor);
}
