/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "layout/topology_row_list.hpp"

MVCCList<TopologyMVCC>* TopologyRowList::InsertElement(const bool& is_out, const vid_t& conn_vtx_id,
                                                       const label_t& label) {
    int element_id = edge_count_++;
    int element_id_in_row = element_id % VE_ROW_ITEM_COUNT;

    int next_count = (element_id - 1) / VE_ROW_ITEM_COUNT;

    VertexEdgeRow* my_row = head_;

    for (int i = 0; i < next_count; i++) {
        // TODO(entityless): faster traversal on supernodes
        my_row = my_row->next_;
    }

    if (element_id > 0 && element_id % VE_ROW_ITEM_COUNT == 0) {
        my_row->next_ = pool_ptr_->Get();
        my_row = my_row->next_;
        my_row->next_ = nullptr;
    }

    MVCCList<TopologyMVCC>* mvcc_list = new MVCCList<TopologyMVCC>;
    mvcc_list->AppendVersion(true, 0, 0);

    my_row->elements_[element_id_in_row].is_out = is_out;
    my_row->elements_[element_id_in_row].conn_vtx_id = conn_vtx_id;
    my_row->elements_[element_id_in_row].label = label;
    my_row->elements_[element_id_in_row].mvcc_list = mvcc_list;

    return mvcc_list;
}

void TopologyRowList::ReadConnectedVertex(const Direction_T& direction, const label_t& edge_label,
                                          const uint64_t& trx_id, const uint64_t& begin_time, vector<vid_t>& ret) {
    VertexEdgeRow* current_row = head_;

    for (int i = 0; i < edge_count_; i++) {
        int element_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        // TODO(entityless): optimize this
        if (direction == BOTH ||
            (current_row->elements_[element_id_in_row].is_out && direction == OUT) ||
            (!current_row->elements_[element_id_in_row].is_out && direction == IN)) {
            if (edge_label == 0 || edge_label == current_row->elements_[element_id_in_row].label) {
                ret.emplace_back(current_row->elements_[element_id_in_row].conn_vtx_id);
            }
        }
    }
}

void TopologyRowList::ReadConnectedEdge(const Direction_T& direction, const label_t& edge_label,
                                        const uint64_t& trx_id, const uint64_t& begin_time, vector<eid_t>& ret) {
    VertexEdgeRow* current_row = head_;

    for (int i = 0; i < edge_count_; i++) {
        int element_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        // TODO(entityless): optimize this
        if (direction == BOTH ||
            (current_row->elements_[element_id_in_row].is_out && direction == OUT) ||
            (!current_row->elements_[element_id_in_row].is_out && direction == IN)) {
            if (edge_label == 0 || edge_label == current_row->elements_[element_id_in_row].label) {
                if (current_row->elements_[element_id_in_row].is_out)
                    ret.emplace_back(eid_t(current_row->elements_[element_id_in_row].conn_vtx_id.value(),
                                     my_vid_.value()));
                else
                    ret.emplace_back(eid_t(my_vid_.value(),
                                     current_row->elements_[element_id_in_row].conn_vtx_id.value()));
            }
        }
    }
}
