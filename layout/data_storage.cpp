/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "layout/data_storage.hpp"

template<class MVCC> OffsetConcurrentMemPool<MVCC>* MVCCList<MVCC>::pool_ptr_ = nullptr;
template<class PropertyRow> OffsetConcurrentMemPool<PropertyRow>* PropertyRowList<PropertyRow>::pool_ptr_ = nullptr;
template<class PropertyRow> MVCCKVStore* PropertyRowList<PropertyRow>::kvs_ptr_ = nullptr;
OffsetConcurrentMemPool<VertexEdgeRow>* TopologyRowList::pool_ptr_ = nullptr;

void DataStorage::Init() {
    config_ = Config::GetInstance();
    node_ = Node::StaticInstance();
    snapshot_manager_ = MPISnapshotManager::GetInstance();

    node_.Rank0Printf("VE_ROW_ITEM_COUNT = %d, sizeof(EdgeHeader) = %d, sizeof(VertexEdgeRow) = %d\n",
                       VE_ROW_ITEM_COUNT, sizeof(EdgeHeader), sizeof(VertexEdgeRow));
    node_.Rank0Printf("VP_ROW_ITEM_COUNT = %d, sizeof(VPHeader) = %d, sizeof(VertexPropertyRow) = %d\n",
                       VP_ROW_ITEM_COUNT, sizeof(VPHeader), sizeof(VertexPropertyRow));
    node_.Rank0Printf("EP_ROW_ITEM_COUNT = %d, sizeof(EPHeader) = %d, sizeof(EdgePropertyRow) = %d\n",
                       EP_ROW_ITEM_COUNT, sizeof(EPHeader), sizeof(EdgePropertyRow));
    node_.Rank0Printf("sizeof(TopologyMVCC) = %d, sizeof(PropertyMVCC) = %d\n",
                       sizeof(TopologyMVCC), sizeof(PropertyMVCC));

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

    node_.Rank0PrintfWithWorkerBarrier("DataStorage::Init() all finished\n");
}

void DataStorage::CreateContainer() {
    ve_row_pool_ = OffsetConcurrentMemPool<VertexEdgeRow>::GetInstance(nullptr, config_->global_ve_row_pool_size);
    vp_row_pool_ = OffsetConcurrentMemPool<VertexPropertyRow>::GetInstance(nullptr, config_->global_vp_row_pool_size);
    ep_row_pool_ = OffsetConcurrentMemPool<EdgePropertyRow>::GetInstance(nullptr, config_->global_ep_row_pool_size);
    topology_mvcc_pool_ = OffsetConcurrentMemPool<TopologyMVCC>::GetInstance(nullptr,
                          config_->global_topo_mvcc_pool_size);
    property_mvcc_pool_ = OffsetConcurrentMemPool<PropertyMVCC>::GetInstance(nullptr,
                          config_->global_property_mvcc_pool_size);
    MVCCList<TopologyMVCC>::SetGlobalMemoryPool(topology_mvcc_pool_);
    MVCCList<PropertyMVCC>::SetGlobalMemoryPool(property_mvcc_pool_);
    PropertyRowList<EdgePropertyRow>::SetGlobalMemoryPool(ep_row_pool_);
    PropertyRowList<VertexPropertyRow>::SetGlobalMemoryPool(vp_row_pool_);
    TopologyRowList::SetGlobalMemoryPool(ve_row_pool_);

    uint64_t ep_sz = GiB2B(config_->global_edge_property_kv_sz_gb);
    uint64_t vp_sz = GiB2B(config_->global_vertex_property_kv_sz_gb);
    ep_store_ = new MVCCKVStore(nullptr, ep_sz);
    vp_store_ = new MVCCKVStore(nullptr, vp_sz);
    PropertyRowList<EdgePropertyRow>::SetGlobalKVS(ep_store_);
    PropertyRowList<VertexPropertyRow>::SetGlobalKVS(vp_store_);
}

void DataStorage::FillContainer() {
    indexes_ = hdfs_data_loader_->indexes_;

    // access elements in HDFSDataLoader
    auto& v_map = hdfs_data_loader_->vtx_part_map_;
    auto& e_map = hdfs_data_loader_->edge_part_map_;

    for (auto vtx : hdfs_data_loader_->shuffled_vtx_) {
        VertexAccessor v_accessor;
        vertex_map_.insert(v_accessor, vtx.id.value());

        v_accessor->second.label = vtx.label;
        v_accessor->second.vp_row_list = new PropertyRowList<VertexPropertyRow>;
        v_accessor->second.ve_row_list = new TopologyRowList;

        v_accessor->second.ve_row_list->Init(vtx.id);
        v_accessor->second.vp_row_list->Init();

        v_accessor->second.mvcc_list = new MVCCList<TopologyMVCC>;
        v_accessor->second.mvcc_list->AppendVersion(true, 0, 0);

        for (auto in_nb : vtx.in_nbs) {
            eid_t eid = eid_t(vtx.id.vid, in_nb.vid);

            v_accessor->second.ve_row_list->InsertElement(false, in_nb, e_map[eid.value()]->label);
        }

        for (auto out_nb : vtx.out_nbs) {
            eid_t eid = eid_t(out_nb.vid, vtx.id.vid);

            EdgeAccessor e_accessor;
            edge_map_.insert(e_accessor, eid.value());
            e_accessor->second.label = e_map[eid.value()]->label;
            e_accessor->second.ep_row_list = new PropertyRowList<EdgePropertyRow>;
            e_accessor->second.ep_row_list->Init();

            auto* mvcc_list = v_accessor->second.ve_row_list->InsertElement(true, out_nb, e_map[eid.value()]->label);
            e_accessor->second.mvcc_list = mvcc_list;
        }

        for (int i = 0; i < vtx.vp_label_list.size(); i++) {
            v_accessor->second.vp_row_list->InsertElement(vpid_t(vtx.id, vtx.vp_label_list[i]), vtx.vp_value_list[i]);
        }
    }
    node_.Rank0PrintfWithWorkerBarrier("DataStorage::FillContainer() load vtx finished\n");

    // fill EdgePropertyRow
    for (auto edge : hdfs_data_loader_->shuffled_edge_) {
        EdgeConstAccessor e_accessor;
        edge_map_.find(e_accessor, edge.id.value());

        auto& ep_row_list_ref = *e_accessor->second.ep_row_list;

        for (int i = 0; i < edge.ep_label_list.size(); i++) {
            ep_row_list_ref.InsertElement(epid_t(edge.id, edge.ep_label_list[i]), edge.ep_value_list[i]);
        }
    }
    node_.LocalSequentialDebugPrint("ve_row_pool_: " + ve_row_pool_->UsageString());
    node_.LocalSequentialDebugPrint("vp_row_pool_: " + vp_row_pool_->UsageString());
    node_.LocalSequentialDebugPrint("ep_row_pool_: " + ep_row_pool_->UsageString());
    node_.LocalSequentialDebugPrint("topology_mvcc_pool_: " + topology_mvcc_pool_->UsageString());
    node_.LocalSequentialDebugPrint("property_mvcc_pool_: " + property_mvcc_pool_->UsageString());
    node_.Rank0PrintfWithWorkerBarrier("DataStorage::FillContainer() finished\n");
}

void DataStorage::GetVP(const vpid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& ret) {
    vid_t vid = pid.vid;

    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    accessor->second.vp_row_list->ReadProperty(pid, trx_id, begin_time, ret);
}

void DataStorage::GetVP(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time,
                        vector<pair<label_t, value_t>>& ret) {
    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    accessor->second.vp_row_list->ReadAllProperty(trx_id, begin_time, ret);
}

label_t DataStorage::GetVL(const vid_t& vid, const uint64_t& trx_id, const uint64_t& begin_time) {
    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    return accessor->second.label;
}

void DataStorage::GetEP(const epid_t& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& ret) {
    eid_t eid = eid_t(pid.in_vid, pid.out_vid);

    EdgeConstAccessor accessor;
    edge_map_.find(accessor, eid.value());

    accessor->second.ep_row_list->ReadProperty(pid, trx_id, begin_time, ret);
}

void DataStorage::GetEP(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time,
                        vector<pair<label_t, value_t>>& ret) {
    EdgeConstAccessor accessor;
    edge_map_.find(accessor, eid.value());

    accessor->second.ep_row_list->ReadAllProperty(trx_id, begin_time, ret);
}

label_t DataStorage::GetEL(const eid_t& eid, const uint64_t& trx_id, const uint64_t& begin_time) {
    EdgeConstAccessor accessor;
    edge_map_.find(accessor, eid.value());

    return accessor->second.label;
}

void DataStorage::GetConnectedVertexList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                         const uint64_t& trx_id, const uint64_t& begin_time, vector<vid_t>& ret) {
    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    accessor->second.ve_row_list->ReadConnectedVertex(direction, edge_label, trx_id, begin_time, ret);
}

void DataStorage::GetConnectedEdgeList(const vid_t& vid, const label_t& edge_label, const Direction_T& direction,
                                       const uint64_t& trx_id, const uint64_t& begin_time, vector<eid_t>& ret) {
    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    accessor->second.ve_row_list->ReadConnectedEdge(direction, edge_label, trx_id, begin_time, ret);
}

void DataStorage::GetAllVertex(const uint64_t& trx_id, const uint64_t& begin_time, vector<vid_t>& ret) {
    for (auto i = vertex_map_.begin(); i != vertex_map_.end(); i++) {
        ret.emplace_back(vid_t(i->first));
    }
}

void DataStorage::GetAllEdge(const uint64_t& trx_id, const uint64_t& begin_time, vector<eid_t>& ret) {
    // TODO(entityless): Simplify the code by editing eid_t::value()
    for (auto i = edge_map_.begin(); i != edge_map_.end(); i++) {
        uint64_t eid_fetched = i->first;
        eid_t* tmp_eid_p = reinterpret_cast<eid_t*>(&eid_fetched);

        ret.emplace_back(eid_t(tmp_eid_p->out_v, tmp_eid_p->in_v));
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

void DataStorage::PrintLoadedData() {
    node_.LocalSequentialStart();

    if (hdfs_data_loader_->shuffled_vtx_.size() < 20 && hdfs_data_loader_->shuffled_edge_.size() < 40) {
        for (auto vtx : hdfs_data_loader_->shuffled_vtx_) {
            TMPVertex tmp_vtx;
            tmp_vtx.id = vtx.id;

            tmp_vtx.label = GetVL(vtx.id, 0x8000000000000001, 1);

            vector<pair<label_t, value_t>> properties;
            GetVP(vtx.id, 0x8000000000000001, 1, properties);
            for (auto p : properties) {
                tmp_vtx.vp_label_list.push_back(p.first);
                tmp_vtx.vp_value_list.push_back(p.second);
            }

            // for (int i = 0; i < vtx.vp_label_list.size(); i++) {
            //     tmp_vtx.vp_label_list.push_back(vtx.vp_label_list[i]);
            //     value_t val;
            //     GetVP(vpid_t(vtx.id, vtx.vp_label_list[i]), 0x8000000000000001, 1, val);
            //     tmp_vtx.vp_value_list.push_back(val);
            // }

            // vector<vid_t> in_nbs;
            // GetConnectedVertexList(vtx.id, 0, IN, 0x8000000000000001, 1, in_nbs);
            // for (auto in_nb : in_nbs) {
            //     tmp_vtx.in_nbs.push_back(in_nb);
            // }

            // vector<eid_t> in_es;
            // GetConnectedEdgeList(vtx.id, 0, IN, 0x8000000000000001, 1, in_es);
            // for (auto in_e : in_es) {
            //     tmp_vtx.in_nbs.push_back(in_e.out_v);
            // }

            // vector<vid_t> out_nbs;
            // GetConnectedVertexList(vtx.id, 0, OUT, 0x8000000000000001, 1, out_nbs);
            // for (auto out_nb : out_nbs) {
            //     tmp_vtx.out_nbs.push_back(out_nb);
            // }

            // vector<eid_t> out_es;
            // GetConnectedEdgeList(vtx.id, 0, OUT, 0x8000000000000001, 1, out_es);
            // for (auto out_e : out_es) {
            //     tmp_vtx.out_nbs.push_back(out_e.in_v);
            // }

            // vector<vid_t> both_nbs;
            // GetConnectedVertexList(vtx.id, 0, BOTH, 0x8000000000000001, 1, both_nbs);
            // for (auto both_nb : both_nbs) {
            //     tmp_vtx.in_nbs.push_back(both_nb);
            // }

            vector<eid_t> both_es;
            GetConnectedEdgeList(vtx.id, 0, BOTH, 0x8000000000000001, 1, both_es);
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
        GetAllVertex(0x8000000000000001, 1, all_vtx_id);
        printf("all vtx id: [");
        for (auto vtx_id : all_vtx_id) {
            printf("%d ", vtx_id.value());
        }
        printf("]\n");

        fflush(stdout);

        for (auto edge : hdfs_data_loader_->shuffled_edge_) {
            TMPEdge tmp_edge;
            tmp_edge.id = edge.id;

            tmp_edge.label = GetEL(edge.id, 0x8000000000000001, 1);

            vector<pair<label_t, value_t>> properties;
            GetEP(edge.id, 0x8000000000000001, 1, properties);
            for (auto p : properties) {
                tmp_edge.ep_label_list.push_back(p.first);
                tmp_edge.ep_value_list.push_back(p.second);
            }

            // for (int i = 0; i < edge.ep_label_list.size(); i++) {
            //     tmp_edge.ep_label_list.push_back(edge.ep_label_list[i]);
            //     value_t val;
            //     GetEP(epid_t(edge.id, edge.ep_label_list[i]), 0x8000000000000001, 1, val);
            //     tmp_edge.ep_value_list.push_back(val);
            // }

            printf(("   original: " + edge.DebugString() + "\n").c_str());
            printf(("constructed: " + tmp_edge.DebugString() + "\n").c_str());
        }

        vector<eid_t> all_edge_id;
        GetAllEdge(0x8000000000000001, 1, all_edge_id);
        printf("all edge id: [");
        for (auto edge_id : all_edge_id) {
            printf("%d->%d ", edge_id.out_v, edge_id.in_v);
        }
        printf("]\n");

        fflush(stdout);
    }

    node_.LocalSequentialEnd();
}
