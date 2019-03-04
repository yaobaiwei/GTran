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
    hdfs_data_loader_->FreeMemory();

    node_.Rank0PrintfWithWorkerBarrier("DataStorage::Initial() all finished\n");
}

void DataStorage::CreateContainer() {
    // TODO(entityless): element_cnt can be given by Config
    ep_row_pool_ = OffsetConcurrentMemPool<EdgePropertyRow>::GetInstance(NULL, 10000);
    ve_row_pool_ = OffsetConcurrentMemPool<VertexEdgeRow>::GetInstance(NULL, 10000);
    vp_row_pool_ = OffsetConcurrentMemPool<VertexPropertyRow>::GetInstance(NULL, 10000);
    topo_mvcc_pool_ = OffsetConcurrentMemPool<TopoMVCC>::GetInstance(NULL, 10000);
    property_mvcc_pool_ = OffsetConcurrentMemPool<PropertyMVCC>::GetInstance(NULL, 10000);

    global_ep_row_pool = ep_row_pool_;
    global_ve_row_pool = ve_row_pool_;
    global_vp_row_pool = vp_row_pool_;
    global_topo_mvcc_pool = topo_mvcc_pool_;
    global_property_mvcc_pool = property_mvcc_pool_;

    // // TODO(entityless): initial kvstore
    uint64_t ep_sz = GiB2B(config_->global_edge_property_kv_sz_gb);
    uint64_t vp_sz = GiB2B(config_->global_vertex_property_kv_sz_gb);
    char* ep_mem = config_->kvstore + GiB2B(config_->global_vertex_property_kv_sz_gb);
    char* vp_mem = config_->kvstore;
    ep_store_ = new MVCCKVStore(ep_mem, ep_sz);
    vp_store_ = new MVCCKVStore(vp_mem, vp_sz);

    global_vp_store = vp_store_;
    global_ep_store = ep_store_;
}

void DataStorage::FillContainer() {
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

        // printf("inserting %s\n", vtx.DebugString().c_str());

        for (auto in_nb : vtx.in_nbs) {
            eid_t eid = eid_t(vtx.id.vid, in_nb.vid);
            // printf("start inserting in_nb %d -> %d, %ld\n", eid.out_v, eid.in_v, eid.value());
            new_ve_row->InsertElement(false, in_nb, e_map[eid.value()]->label, NULL);
            // printf("stop inserting in_nb %d -> %d, %ld\n", eid.out_v, eid.in_v, eid.value());
        }

        // create EdgePropertyRow, and insert them into edge_map_
        for (auto out_nb : vtx.out_nbs) {
            eid_t eid = eid_t(out_nb.vid, vtx.id.vid);

            EdgePropertyRow* new_ep_row = ep_row_pool_->Get();
            EdgeAccessor accessor;
            edge_map_.insert(accessor, eid.value());
            accessor->second = new_ep_row;

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

        auto& ep_row_ref = *accessor->second;

        for (int i = 0; i < edge.ep_label_list.size(); i++) {
            ep_row_ref.InsertElement(epid_t(edge.id, edge.ep_label_list[i]), edge.ep_value_list[i]);
        }
    }

    node_.LocalSequentialEnd();  // TODO(entityless): remove this debug function
}

void DataStorage::Test()
{
    //
}
