/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#include "layout/gc_producer.hpp"

void GCProducer::Init() {
    scanner_ = thread(&GCProducer::Scan, this);
    data_storage_ = DataStorage::GetInstance();
}

void GCProducer::Stop() {
    scanner_.join();
}

void GCProducer::Scan() {
    while (true) {
        // Do Scan with DFS
        scan_vertex_map();
    }
}

void GCProducer::scan_vertex_map() {
    // Do Scan to Vertex map
    for (auto v_pair = data_storage_->vertex_map_.begin(); v_pair != data_storage_->vertex_map_.end(); v_pair++) {
        auto& v_item = v_pair->second;
        MVCCList<VertexMVCCItem>* mvcc_list = v_item.mvcc_list;

        if (mvcc_list == nullptr) { continue; }

        VertexMVCCItem* item = mvcc_list->GetHead();
        bool gc_able = false;
        // VertexMVCCList is different with other MVCCList since it only has at most
        // two versions and the second version must be deleted version
        // Therefore, if the first version is unvisible to any transaction
        // (i.e. version->end_time < MINIMUM_ACTIVE_TRANSACTION_BT), the vertex can be GC.
        if (item->GetEndTime() < MINIMUM_ACTIVE_TRANSACTION_BT) {
            // Vertex GCable
            spawn_vertex_map_gctask(v_pair->first);
            // spawn_v_mvcc_gctask(item);
            // Call all other spwan functions
            // spawn_topo_row_list_gctask();
            // spawn_prop_row_list_gctask();
        } else {
            // go deeper, to prop first and then topo
            scan_prop_row_list(v_pair->second.vp_row_list);
            scan_topo_row_list(v_pair->second.ve_row_list);
        }
    }
}

void GCProducer::scan_topo_row_list(TopologyRowList* topo_row_list) {
    VertexEdgeRow* row_ptr = topo_row_list->head_;
    int edge_count_snapshot = topo_row_list->edge_count_;

    int gcable_cell_count = 0;
    for (int i = 0; i < edge_count_snapshot; i++) {
        int cell_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i != 0 && cell_id_in_row == 0) {
            row_ptr = row_ptr->next_;
        }

        MVCCList<EdgeMVCCItem>* cur_edge_mvcc_list = row_ptr->cells_[cell_id_in_row].mvcc_list;  
        if (scan_mvcc_list(cur_edge_mvcc_list)) {
            gcable_cell_count++;
        }
    }

    /*
    if (gcable_cell_count > THRESHOLD) {
        spawn_topo_row_list_defrag_gctask(topo_row_list);
    }*/
}

void GCProducer::scan_ep_row_list(EdgeMVCCItem* edge_version) {
    scan_prop_row_list(edge_version->GetValue().ep_row_list);
}

void GCProducer::spawn_vertex_map_gctask(uint32_t* vid_value) {
    vid_t vid;
    uint2vid_t((*vid_value), vid);
}
