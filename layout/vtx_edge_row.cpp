/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include "layout/vtx_edge_row.hpp"

extern OffsetConcurrentMemPool<VertexEdgeRow>* global_ve_row_pool;
extern OffsetConcurrentMemPool<TopoMVCC>* global_topo_mvcc_pool;

void VertexEdgeRow::InsertElement(const bool& is_out, const vid_t& conn_vtx_id,
                                  const label_t& label, EdgePropertyRow* ep_row) {
    int element_id = edge_count_++;
    int element_id_in_row = element_id % VE_ROW_ITEM_COUNT;

    int next_count = (element_id - 1) / VE_ROW_ITEM_COUNT;

    VertexEdgeRow* my_row = this;

    for (int i = 0; i < next_count; i++) {
        // TODO(entityless): faster traversal on supernodes
        my_row = my_row->next_;
    }

    if (element_id > 0 && element_id % VE_ROW_ITEM_COUNT == 0) {
        my_row->next_ = global_ve_row_pool->Get();
        my_row = my_row->next_;
        my_row->next_ = nullptr;
    }

    MVCCList<TopoMVCC>* mvcc_list = new MVCCList<TopoMVCC>;

    mvcc_list->AppendVersion(true, 0, 0);

    my_row->elements_[element_id_in_row].is_out = is_out;
    my_row->elements_[element_id_in_row].conn_vtx_id = conn_vtx_id;
    my_row->elements_[element_id_in_row].label = label;
    my_row->elements_[element_id_in_row].row_head = ep_row;
    my_row->elements_[element_id_in_row].mvcc_list = mvcc_list;
}

vector<vid_t> VertexEdgeRow::ReadInVertex(const label_t& edge_label,
                                          const uint64_t& trx_id, const uint64_t& begin_time) {
    vector<vid_t> ret;

    VertexEdgeRow* current_row = this;

    for (int i = 0; i < edge_count_; i++) {
        int element_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        if (!current_row->elements_[element_id_in_row].is_out) {
            if (edge_label == 0 || edge_label == current_row->elements_[element_id_in_row].label) {
                ret.push_back(current_row->elements_[element_id_in_row].conn_vtx_id);
            }
        }
    }

    return ret;
}

vector<vid_t> VertexEdgeRow::ReadOutVertex(const label_t& edge_label,
                                           const uint64_t& trx_id, const uint64_t& begin_time) {
    vector<vid_t> ret;

    VertexEdgeRow* current_row = this;

    for (int i = 0; i < edge_count_; i++) {
        int element_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        if (current_row->elements_[element_id_in_row].is_out) {
            if (edge_label == 0 || edge_label == current_row->elements_[element_id_in_row].label) {
                ret.push_back(current_row->elements_[element_id_in_row].conn_vtx_id);
            }
        }
    }

    return ret;
}

vector<vid_t> VertexEdgeRow::ReadBothVertex(const label_t& edge_label,
                                            const uint64_t& trx_id, const uint64_t& begin_time) {
    vector<vid_t> ret;

    VertexEdgeRow* current_row = this;

    for (int i = 0; i < edge_count_; i++) {
        int element_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        if (edge_label == 0 || edge_label == current_row->elements_[element_id_in_row].label) {
            ret.push_back(current_row->elements_[element_id_in_row].conn_vtx_id);
        }
    }

    return ret;
}

vector<eid_t> VertexEdgeRow::ReadInEdge(const label_t& edge_label, const vid_t& my_vid,
                                        const uint64_t& trx_id, const uint64_t& begin_time) {
    vector<eid_t> ret;

    VertexEdgeRow* current_row = this;

    for (int i = 0; i < edge_count_; i++) {
        int element_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        if (!current_row->elements_[element_id_in_row].is_out) {
            if (edge_label == 0 || edge_label == current_row->elements_[element_id_in_row].label) {
                ret.push_back(eid_t(my_vid.value(), current_row->elements_[element_id_in_row].conn_vtx_id.value()));
            }
        }
    }

    return ret;
}

vector<eid_t> VertexEdgeRow::ReadOutEdge(const label_t& edge_label, const vid_t& my_vid,
                                         const uint64_t& trx_id, const uint64_t& begin_time) {
    vector<eid_t> ret;

    VertexEdgeRow* current_row = this;

    for (int i = 0; i < edge_count_; i++) {
        int element_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        if (current_row->elements_[element_id_in_row].is_out) {
            if (edge_label == 0 || edge_label == current_row->elements_[element_id_in_row].label) {
                ret.push_back(eid_t(current_row->elements_[element_id_in_row].conn_vtx_id.value(), my_vid.value()));
            }
        }
    }

    return ret;
}

vector<eid_t> VertexEdgeRow::ReadBothEdge(const label_t& edge_label, const vid_t& my_vid,
                                          const uint64_t& trx_id, const uint64_t& begin_time) {
    vector<eid_t> ret;

    VertexEdgeRow* current_row = this;

    for (int i = 0; i < edge_count_; i++) {
        int element_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        if (edge_label == 0 || edge_label == current_row->elements_[element_id_in_row].label) {
            if (current_row->elements_[element_id_in_row].is_out)
                ret.push_back(eid_t(current_row->elements_[element_id_in_row].conn_vtx_id.value(), my_vid.value()));
            else
                ret.push_back(eid_t(my_vid.value(), current_row->elements_[element_id_in_row].conn_vtx_id.value()));
        }
    }

    return ret;
}
