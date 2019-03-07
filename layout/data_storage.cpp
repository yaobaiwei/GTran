/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "layout/data_storage.hpp"

// global single instance ptr, can be accessed by other classes
// TODO(entityless): figure out if there is a more elegant way
OffsetConcurrentMemPool<EdgePropertyRow>* global_ep_row_pool = nullptr;
OffsetConcurrentMemPool<VertexEdgeRow>* global_ve_row_pool = nullptr;
OffsetConcurrentMemPool<VertexPropertyRow>* global_vp_row_pool = nullptr;
OffsetConcurrentMemPool<TopoMVCC>* global_topo_mvcc_pool = nullptr;
OffsetConcurrentMemPool<PropertyMVCC>* global_property_mvcc_pool = nullptr;
MVCCKVStore* global_ep_store = nullptr;
MVCCKVStore* global_vp_store = nullptr;

template<class MVCC> OffsetConcurrentMemPool<MVCC>* MVCCList<MVCC>::pool_ptr_ = nullptr;

void DataStorage::Initial() {
    config_ = Config::GetInstance();
    node_ = Node::StaticInstance();

    node_.Rank0Printf("VE_ROW_ITEM_COUNT = %d, sizeof(EdgeHeader) = %d, sizeof(VertexEdgeRow) = %d\n",
                       VE_ROW_ITEM_COUNT, sizeof(EdgeHeader), sizeof(VertexEdgeRow));
    node_.Rank0Printf("VP_ROW_ITEM_COUNT = %d, sizeof(VPHeader) = %d, sizeof(VertexPropertyRow) = %d\n",
                       VP_ROW_ITEM_COUNT, sizeof(VPHeader), sizeof(VertexPropertyRow));
    node_.Rank0Printf("EP_ROW_ITEM_COUNT = %d, sizeof(EPHeader) = %d, sizeof(EdgePropertyRow) = %d\n",
                       EP_ROW_ITEM_COUNT, sizeof(EPHeader), sizeof(EdgePropertyRow));
    node_.Rank0Printf("sizeof(TopoMVCC) = %d, sizeof(PropertyMVCC) = %d\n",
                       sizeof(TopoMVCC), sizeof(PropertyMVCC));

    CreateContainer();
    hdfs_data_loader_ = HDFSDataLoader::GetInstance();
    hdfs_data_loader_->LoadData();
    hdfs_data_loader_->Shuffle();
    FillContainer();
    // Test();
    hdfs_data_loader_->FreeMemory();

    node_.Rank0PrintfWithWorkerBarrier("DataStorage::Initial() all finished\n");
}

void DataStorage::CreateContainer() {
    // TODO(entityless): element_cnt can be given by Config
    ve_row_pool_ = OffsetConcurrentMemPool<VertexEdgeRow>::GetInstance(nullptr, config_->global_ve_row_pool_size);
    vp_row_pool_ = OffsetConcurrentMemPool<VertexPropertyRow>::GetInstance(nullptr, config_->global_vp_row_pool_size);
    ep_row_pool_ = OffsetConcurrentMemPool<EdgePropertyRow>::GetInstance(nullptr, config_->global_ep_row_pool_size);
    topo_mvcc_pool_ = OffsetConcurrentMemPool<TopoMVCC>::GetInstance(nullptr, config_->global_topo_mvcc_pool_size);
    property_mvcc_pool_ = OffsetConcurrentMemPool<PropertyMVCC>::GetInstance(nullptr, config_->global_property_mvcc_pool_size);
    MVCCList<TopoMVCC>::SetGlobalMemoryPool(topo_mvcc_pool_);
    MVCCList<PropertyMVCC>::SetGlobalMemoryPool(property_mvcc_pool_);

    global_ep_row_pool = ep_row_pool_;
    global_ve_row_pool = ve_row_pool_;
    global_vp_row_pool = vp_row_pool_;
    global_topo_mvcc_pool = topo_mvcc_pool_;
    global_property_mvcc_pool = property_mvcc_pool_;

    // // TODO(entityless): initial kvstore
    uint64_t ep_sz = GiB2B(config_->global_edge_property_kv_sz_gb);
    uint64_t vp_sz = GiB2B(config_->global_vertex_property_kv_sz_gb);
    ep_store_ = new MVCCKVStore(nullptr, ep_sz);
    vp_store_ = new MVCCKVStore(nullptr, vp_sz);

    global_vp_store = vp_store_;
    global_ep_store = ep_store_;
}

void DataStorage::FillContainer() {
    indexes_ = hdfs_data_loader_->indexes_;

    // access elements in HDFSDataLoader
    auto& v_map = hdfs_data_loader_->vtx_part_map_;
    auto& e_map = hdfs_data_loader_->edge_part_map_;

    node_.LocalSequentialStart();  // TODO(entityless): remove this debug function

    // construct VertexEdgeRow & VertexPropertyRow, create EdgePropertyRow
    // EdgePropertyRow will be created
    for (auto vtx : hdfs_data_loader_->shuffled_vtx_) {
        VertexEdgeRow* new_ve_row = ve_row_pool_->Get();
        VertexPropertyRow* new_vp_row = vp_row_pool_->Get();

        VertexAccessor accessor;

        vertex_map_.insert(accessor, vtx.id.value());
        accessor->second.label = vtx.label;
        accessor->second.ve_row = new_ve_row;
        accessor->second.vp_row = new_vp_row;

        new_ve_row->Initial();
        new_vp_row->Initial();

        // printf("inserting %s\n", vtx.DebugString().c_str());

        for (auto in_nb : vtx.in_nbs) {
            eid_t eid = eid_t(vtx.id.vid, in_nb.vid);
            // printf("start inserting in_nb %d -> %d, %ld\n", eid.out_v, eid.in_v, eid.value());
            new_ve_row->InsertElement(false, in_nb, e_map[eid.value()]->label, nullptr);
            // printf("stop inserting in_nb %d -> %d, %ld\n", eid.out_v, eid.in_v, eid.value());
        }

        // create EdgePropertyRow, and insert them into edge_map_
        for (auto out_nb : vtx.out_nbs) {
            eid_t eid = eid_t(out_nb.vid, vtx.id.vid);

            EdgePropertyRow* new_ep_row = ep_row_pool_->Get();
            new_ep_row->Initial();

            EdgeAccessor accessor;
            edge_map_.insert(accessor, eid.value());
            // edge_map_.insert(accessor, *((uint64_t*)&eid));
            accessor->second.label = e_map[eid.value()]->label;
            accessor->second.ep_row = new_ep_row;

            // eid_t eid = eid_t(vtx.id.vid, out_nb.vid);
            // printf("start inserting out_nb %d -> %d, %ld\n", eid.out_v, eid.in_v, eid.value());
            new_ve_row->InsertElement(true, out_nb, e_map[eid.value()]->label, new_ep_row);
            // printf("stop inserting out_nb %d -> %d, %ld\n", eid.out_v, eid.in_v, eid.value());
        }

        for (int i = 0; i < vtx.vp_label_list.size(); i++) {
            new_vp_row->InsertElement(vpid_t(vtx.id, vtx.vp_label_list[i]), vtx.vp_value_list[i]);
        }
    }

    // fill EdgePropertyRow
    for (auto edge : hdfs_data_loader_->shuffled_edge_) {
        EdgeConstAccessor accessor;
        edge_map_.find(accessor, edge.id.value());

        auto& ep_row_ref = *accessor->second.ep_row;

        for (int i = 0; i < edge.ep_label_list.size(); i++) {
            ep_row_ref.InsertElement(epid_t(edge.id, edge.ep_label_list[i]), edge.ep_value_list[i]);
        }
    }

    node_.LocalSequentialEnd();  // TODO(entityless): remove this debug function
}

value_t DataStorage::GetVP(vpid_t pid, uint64_t trx_id, uint64_t begin_time) {
    vid_t vid = pid.vid;

    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    auto* vp_row_head = accessor->second.vp_row;

    return vp_row_head->ReadProperty(pid, trx_id, begin_time);
}

vector<pair<label_t, value_t>> DataStorage::GetVP(vid_t vid, uint64_t trx_id, uint64_t begin_time) {
    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    auto* vp_row_head = accessor->second.vp_row;

    return vp_row_head->ReadAllProperty(trx_id, begin_time);
}

label_t DataStorage::GetVL(vid_t vid, uint64_t trx_id, uint64_t begin_time) {
    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    return accessor->second.label;
}

value_t DataStorage::GetEP(epid_t pid, uint64_t trx_id, uint64_t begin_time) {
    eid_t eid = eid_t(pid.in_vid, pid.out_vid);

    EdgeConstAccessor accessor;
    edge_map_.find(accessor, eid.value());

    auto* ep_row_head = accessor->second.ep_row;

    return ep_row_head->ReadProperty(pid, trx_id, begin_time);
}

vector<pair<label_t, value_t>> DataStorage::GetEP(eid_t eid, uint64_t trx_id, uint64_t begin_time) {
    EdgeConstAccessor accessor;
    edge_map_.find(accessor, eid.value());

    auto* ep_row_head = accessor->second.ep_row;

    return ep_row_head->ReadAllProperty(trx_id, begin_time);
}

label_t DataStorage::GetEL(eid_t eid, uint64_t trx_id, uint64_t begin_time) {
    EdgeConstAccessor accessor;
    edge_map_.find(accessor, eid.value());

    return accessor->second.label;
}

vector<vid_t> DataStorage::GetInVertexList(vid_t vid, label_t edge_label, uint64_t trx_id, uint64_t begin_time) {
    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    auto* ve_row_head = accessor->second.ve_row;

    return ve_row_head->ReadInVertex(edge_label, trx_id, begin_time);
}

vector<vid_t> DataStorage::GetOutVertexList(vid_t vid, label_t edge_label, uint64_t trx_id, uint64_t begin_time) {
    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    auto* ve_row_head = accessor->second.ve_row;

    return ve_row_head->ReadOutVertex(edge_label, trx_id, begin_time);
}

vector<vid_t> DataStorage::GetBothVertexList(vid_t vid, label_t edge_label, uint64_t trx_id, uint64_t begin_time) {
    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    auto* ve_row_head = accessor->second.ve_row;

    return ve_row_head->ReadBothVertex(edge_label, trx_id, begin_time);
}

vector<eid_t> DataStorage::GetInEdgeList(vid_t vid, label_t edge_label, uint64_t trx_id, uint64_t begin_time) {
    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    auto* ve_row_head = accessor->second.ve_row;

    return ve_row_head->ReadInEdge(edge_label, vid, trx_id, begin_time);
}

vector<eid_t> DataStorage::GetOutEdgeList(vid_t vid, label_t edge_label, uint64_t trx_id, uint64_t begin_time) {
    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    auto* ve_row_head = accessor->second.ve_row;

    return ve_row_head->ReadOutEdge(edge_label, vid, trx_id, begin_time);
}

vector<eid_t> DataStorage::GetBothEdgeList(vid_t vid, label_t edge_label, uint64_t trx_id, uint64_t begin_time) {
    VertexConstAccessor accessor;
    vertex_map_.find(accessor, vid.value());

    auto* ve_row_head = accessor->second.ve_row;

    return ve_row_head->ReadBothEdge(edge_label, vid, trx_id, begin_time);
}

vector<vid_t> DataStorage::GetAllVertex(uint64_t trx_id, uint64_t begin_time) {
    vector<vid_t> ret;
    for (auto i = vertex_map_.begin(); i != vertex_map_.end(); i++) {
        ret.push_back(vid_t(i->first));
    }
    return ret;
}

vector<eid_t> DataStorage::GetAllEdge(uint64_t trx_id, uint64_t begin_time) {
    vector<eid_t> ret;
    // TODO(entityless): Simplify the code by editing eid_t::value()
    for (auto i = edge_map_.begin(); i != edge_map_.end(); i++) {
        uint64_t eid_fetched = i->first;
        eid_t* tmp_eid_p = (eid_t*)(&eid_fetched);

        ret.push_back(eid_t(tmp_eid_p->out_v, tmp_eid_p->in_v));
    }
    return ret;
}

void DataStorage::Test() {
    node_.LocalSequentialStart();
    for (auto vtx : hdfs_data_loader_->shuffled_vtx_) {
        TMPVertex tmp_vtx;
        tmp_vtx.id = vtx.id;

        tmp_vtx.label = GetVL(vtx.id, 0, 0);

        // auto properties = GetVP(vtx.id, 0, 0);
        // for (auto p : properties) {
        //     tmp_vtx.vp_label_list.push_back(p.first);
        //     tmp_vtx.vp_value_list.push_back(p.second);
        // }

        for (int i = 0; i < vtx.vp_label_list.size(); i++) {
            tmp_vtx.vp_label_list.push_back(vtx.vp_label_list[i]);
            tmp_vtx.vp_value_list.push_back(GetVP(vpid_t(vtx.id, vtx.vp_label_list[i]), 0, 0));
        }

        // auto in_nbs = GetInVertexList(vtx.id, 0, 0, 0);
        // for (auto in_nb : in_nbs) {
        //     tmp_vtx.in_nbs.push_back(in_nb);
        // }

        // auto in_es = GetInEdgeList(vtx.id, 0, 0, 0);
        // for (auto in_e : in_es) {
        //     tmp_vtx.in_nbs.push_back(in_e.out_v);
        // }

        // auto out_nbs = GetOutVertexList(vtx.id, 0, 0, 0);
        // for (auto out_nb : out_nbs) {
        //     tmp_vtx.out_nbs.push_back(out_nb);
        // }

        // auto out_es = GetOutEdgeList(vtx.id, 0, 0, 0);
        // for (auto out_e : out_es) {
        //     tmp_vtx.out_nbs.push_back(out_e.in_v);
        // }

        // auto both_nbs = GetBothVertexList(vtx.id, 0, 0, 0);
        // for (auto both_nb : both_nbs) {
        //     tmp_vtx.in_nbs.push_back(both_nb);
        // }

        auto out_es = GetBothEdgeList(vtx.id, 0, 0, 0);
        for (auto out_e : out_es) {
            if (out_e.in_v == vtx.id)
                tmp_vtx.in_nbs.push_back(out_e.out_v);
            else
                tmp_vtx.out_nbs.push_back(out_e.in_v);
        }

        printf(("   original: " + vtx.DebugString() + "\n").c_str());
        printf(("constructed: " + tmp_vtx.DebugString() + "\n").c_str());
    }

    auto all_vtx_id = GetAllVertex(0, 0);
    printf("all vtx id: [");
    for (auto vtx_id : all_vtx_id) {
        printf("%d ", vtx_id.value());
    }
    printf("]\n");

    fflush(stdout);

    for (auto edge : hdfs_data_loader_->shuffled_edge_) {
        TMPEdge tmp_edge;
        tmp_edge.id = edge.id;

        tmp_edge.label = GetEL(edge.id, 0, 0);

        // auto properties = GetEP(edge.id, 0, 0);
        // for (auto p : properties) {
        //     tmp_edge.ep_label_list.push_back(p.first);
        //     tmp_edge.ep_value_list.push_back(p.second);
        // }

        for (int i = 0; i < edge.ep_label_list.size(); i++) {
            tmp_edge.ep_label_list.push_back(edge.ep_label_list[i]);
            tmp_edge.ep_value_list.push_back(GetEP(epid_t(edge.id, edge.ep_label_list[i]), 0, 0));
        }

        printf(("   original: " + edge.DebugString() + "\n").c_str());
        printf(("constructed: " + tmp_edge.DebugString() + "\n").c_str());
    }

    auto all_edge_id = GetAllEdge(0, 0);
    printf("all edge id: [");
    for (auto edge_id : all_edge_id) {
        printf("%d->%d ", edge_id.out_v, edge_id.in_v);
    }
    printf("]\n");

    fflush(stdout);

    node_.LocalSequentialEnd();
}

void DataStorage::GetNameFromIndex(Index_T type, label_t id, string& str) {
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
