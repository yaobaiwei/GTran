/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include "vtx_edge_row.hpp"

extern OffsetConcurrentMemPool<VertexEdgeRow>* global_ve_row_pool;
extern OffsetConcurrentMemPool<TopoMVCC>* global_topo_mvcc_pool;

void VertexEdgeRow::InsertElement(const bool& is_out, const vid_t& conn_vtx_id, const label_t& label, EdgePropertyRow* ep_row)
{
    int element_id = edge_count_++;
    int element_id_in_row = element_id % VE_ROW_ITEM_COUNT;

    int next_count = (element_id - 1) / VE_ROW_ITEM_COUNT;

    VertexEdgeRow* my_row = this;

    for(int i = 0; i < next_count; i++)
    {
        // TODO(entityless): faster traversal on supernodes
        my_row = my_row->next_;
    }

    if(element_id > 0 && element_id % VE_ROW_ITEM_COUNT == 0)
    {
        my_row->next_ = global_ve_row_pool->Get();
        my_row = my_row->next_;
        my_row->next_ = nullptr;
    }

    TopoMVCC* topo_mvcc = global_topo_mvcc_pool->Get();
    topo_mvcc->begin_time = TopoMVCC::MIN_TIME;
    topo_mvcc->end_time = TopoMVCC::MAX_TIME;
    topo_mvcc->tid = TopoMVCC::INITIAL_TID;
    topo_mvcc->next = nullptr;
    topo_mvcc->action = true;

    my_row->elements_[element_id_in_row].is_out = is_out;
    my_row->elements_[element_id_in_row].conn_vtx_id = conn_vtx_id;
    my_row->elements_[element_id_in_row].label = label;
    my_row->elements_[element_id_in_row].row_head = ep_row;
    my_row->elements_[element_id_in_row].mvcc_head = topo_mvcc;
}
