/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "layout/topology_row_list.hpp"

void TopologyRowList::Init(const vid_t& my_vid) {
    my_vid_ = my_vid;
    head_ = tail_ = mem_pool_->Get();
    edge_count_ = 0;
    pthread_spin_init(&lock_, 0);
}

void TopologyRowList::AllocateCell(const bool& is_out, const vid_t& conn_vtx_id,
                                   MVCCList<EdgeMVCC>* mvcc_list) {
    pthread_spin_lock(&lock_);
    int cell_id = edge_count_;
    int cell_id_in_row = cell_id % VE_ROW_ITEM_COUNT;

    if (cell_id_in_row == 0 && cell_id > 0) {
        tail_->next_ = mem_pool_->Get();
        tail_ = tail_->next_;
    }

    tail_->cells_[cell_id_in_row].is_out = is_out;
    tail_->cells_[cell_id_in_row].conn_vtx_id = conn_vtx_id;
    tail_->cells_[cell_id_in_row].mvcc_list = mvcc_list;

    edge_count_++;
    pthread_spin_unlock(&lock_);
}

MVCCList<EdgeMVCC>* TopologyRowList::InsertInitialCell(const bool& is_out, const vid_t& conn_vtx_id,
                                                       const label_t& label,
                                                       PropertyRowList<EdgePropertyRow>* ep_row_list_ptr) {
    MVCCList<EdgeMVCC>* mvcc_list = new MVCCList<EdgeMVCC>;
    mvcc_list->AppendInitialVersion()[0] = EdgeItem(label, ep_row_list_ptr);

    AllocateCell(is_out, conn_vtx_id, mvcc_list);

    return mvcc_list;
}

READ_STAT TopologyRowList::ReadConnectedVertex(const Direction_T& direction, const label_t& edge_label,
                                               const uint64_t& trx_id, const uint64_t& begin_time,
                                               const bool& read_only, vector<vid_t>& ret) {
    VertexEdgeRow* current_row = head_;
    int current_edge_count = edge_count_;

    for (int i = 0; i < current_edge_count; i++) {
        int cell_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        if (direction == BOTH || (cell_ref.is_out == (direction == OUT))) {
            auto* visible_mvcc = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only);

            if (visible_mvcc == nullptr)
                return READ_STAT::ABORT;

            auto edge_item = visible_mvcc->GetValue();
            if (!edge_item.Exist())
                continue;

            if (edge_label == 0 || edge_label == edge_item.label)
                ret.emplace_back(cell_ref.conn_vtx_id);
        }
    }

    return READ_STAT::SUCCESS;
}

READ_STAT TopologyRowList::ReadConnectedEdge(const Direction_T& direction, const label_t& edge_label,
                                             const uint64_t& trx_id, const uint64_t& begin_time,
                                             const bool& read_only, vector<eid_t>& ret) {
    VertexEdgeRow* current_row = head_;
    int current_edge_count = edge_count_;

    for (int i = 0; i < current_edge_count; i++) {
        int cell_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        if (direction == BOTH || (cell_ref.is_out == (direction == OUT))) {
            auto* visible_mvcc = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only);

            if (visible_mvcc == nullptr)
                return READ_STAT::ABORT;

            auto edge_item = visible_mvcc->GetValue();
            if (!edge_item.Exist())
                continue;

            if (edge_label == 0 || edge_label == edge_item.label) {
                if (cell_ref.is_out)
                    ret.emplace_back(eid_t(cell_ref.conn_vtx_id.value(), my_vid_.value()));
                else
                    ret.emplace_back(eid_t(my_vid_.value(), cell_ref.conn_vtx_id.value()));
            }
        }
    }

    return READ_STAT::SUCCESS;
}

MVCCList<EdgeMVCC>* TopologyRowList::ProcessAddEdge(const bool& is_out, const vid_t& conn_vtx_id,
                                                    const label_t& edge_label,
                                                    PropertyRowList<EdgePropertyRow>* ep_row_list_ptr,
                                                    const uint64_t& trx_id, const uint64_t& begin_time) {
    MVCCList<EdgeMVCC>* mvcc_list = new MVCCList<EdgeMVCC>;
    mvcc_list->AppendVersion(trx_id, begin_time)[0] = EdgeItem(edge_label, ep_row_list_ptr);

    AllocateCell(is_out, conn_vtx_id, mvcc_list);

    return mvcc_list;
}
