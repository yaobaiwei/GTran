/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "layout/topology_row_list.hpp"

void TopologyRowList::Init(const vid_t& my_vid) {
    my_vid_ = my_vid;
    head_ = tail_ = nullptr;
    edge_count_ = 0;
    pthread_spin_init(&lock_, 0);
}
/* For an specific eid on a vertex, ProcessAddEdge will only be called once.
 * This is guaranteed by DataStorage::ProcessAddE
 * So we do not need to perform cell check like PropertyRowList::AllocateCell
 */
void TopologyRowList::AllocateCell(const bool& is_out, const vid_t& conn_vtx_id,
                                   MVCCList<EdgeMVCCItem>* mvcc_list) {
    pthread_spin_lock(&lock_);
    int cell_id = edge_count_;
    int cell_id_in_row = cell_id % VE_ROW_ITEM_COUNT;

    if (cell_id_in_row == 0) {
        auto* new_row = mem_pool_->Get(TidMapper::GetInstance()->GetTidUnique());
        if (cell_id == 0) {
            head_ = tail_ = new_row;
        } else {
            tail_->next_ = new_row;
            tail_ = tail_->next_;
        }
    }

    tail_->cells_[cell_id_in_row].is_out = is_out;
    tail_->cells_[cell_id_in_row].conn_vtx_id = conn_vtx_id;
    tail_->cells_[cell_id_in_row].mvcc_list = mvcc_list;

    edge_count_++;
    pthread_spin_unlock(&lock_);
}

// This function will only be called when loading data from hdfs
MVCCList<EdgeMVCCItem>* TopologyRowList::InsertInitialCell(const bool& is_out, const vid_t& conn_vtx_id,
                                                           const label_t& label,
                                                           PropertyRowList<EdgePropertyRow>* ep_row_list_ptr) {
    MVCCList<EdgeMVCCItem>* mvcc_list = new MVCCList<EdgeMVCCItem>;
    mvcc_list->AppendInitialVersion()[0] = EdgeVersion(label, ep_row_list_ptr);

    AllocateCell(is_out, conn_vtx_id, mvcc_list);

    return mvcc_list;
}

READ_STAT TopologyRowList::ReadConnectedVertex(const Direction_T& direction, const label_t& edge_label,
                                               const uint64_t& trx_id, const uint64_t& begin_time,
                                               const bool& read_only, vector<vid_t>& ret) {
    ReaderLockGuard reader_lock_guard(gc_rwlock_);
    VertexEdgeRow* current_row = head_;
    if (current_row == nullptr)
        return READ_STAT::SUCCESS;

    int current_edge_count = edge_count_;

    for (int i = 0; i < current_edge_count; i++) {
        int cell_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        if (direction == BOTH || (cell_ref.is_out == (direction == OUT))) {
            EdgeVersion edge_version;
            pair<bool, bool> is_visible = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, edge_version);

            if (!is_visible.first)
                return READ_STAT::ABORT;
            if (!is_visible.second)
                continue;

            if (!edge_version.Exist())
                continue;

            if (edge_label == 0 || edge_label == edge_version.label)
                ret.emplace_back(cell_ref.conn_vtx_id);
        }
    }

    return READ_STAT::SUCCESS;
}

READ_STAT TopologyRowList::ReadConnectedEdge(const Direction_T& direction, const label_t& edge_label,
                                             const uint64_t& trx_id, const uint64_t& begin_time,
                                             const bool& read_only, vector<eid_t>& ret) {
    ReaderLockGuard reader_lock_guard(gc_rwlock_);
    VertexEdgeRow* current_row = head_;

    if (current_row == nullptr)
        return READ_STAT::SUCCESS;

    int current_edge_count = edge_count_;

    for (int i = 0; i < current_edge_count; i++) {
        int cell_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        if (direction == BOTH || (cell_ref.is_out == (direction == OUT))) {
            EdgeVersion edge_version;
            pair<bool, bool> is_visible = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, edge_version);

            if (!is_visible.first)
                return READ_STAT::ABORT;
            if (!is_visible.second)
                continue;

            if (!edge_version.Exist())
                continue;

            if (edge_label == 0 || edge_label == edge_version.label) {
                if (cell_ref.is_out)
                    ret.emplace_back(eid_t(cell_ref.conn_vtx_id.value(), my_vid_.value()));
                else
                    ret.emplace_back(eid_t(my_vid_.value(), cell_ref.conn_vtx_id.value()));
            }
        }
    }

    return READ_STAT::SUCCESS;
}

MVCCList<EdgeMVCCItem>* TopologyRowList::ProcessAddEdge(const bool& is_out, const vid_t& conn_vtx_id,
                                                        const label_t& edge_label,
                                                        PropertyRowList<EdgePropertyRow>* ep_row_list_ptr,
                                                        const uint64_t& trx_id, const uint64_t& begin_time) {
    ReaderLockGuard reader_lock_guard(gc_rwlock_);
    MVCCList<EdgeMVCCItem>* mvcc_list = new MVCCList<EdgeMVCCItem>;
    mvcc_list->AppendVersion(trx_id, begin_time)[0] = EdgeVersion(edge_label, ep_row_list_ptr);

    // This won't fail, guaranteed by DataStorage::ProcessAddE
    AllocateCell(is_out, conn_vtx_id, mvcc_list);

    return mvcc_list;
}

void TopologyRowList::SelfGarbageCollect(const vid_t& origin_vid, vector<pair<eid_t, bool>>* gcable_eids) {
    WriterLockGuard writer_lock_guard(gc_rwlock_);
    VertexEdgeRow* current_row = head_;

    if (current_row == nullptr)
        return;

    int row_count = edge_count_ / VE_ROW_ITEM_COUNT;
    if (row_count * VE_ROW_ITEM_COUNT != edge_count_)
        row_count++;
    if (row_count == 0)
        row_count = 1;

    VertexEdgeRow** row_ptrs = new VertexEdgeRow*[row_count];
    row_ptrs[0] = head_;
    int row_ptr_count = 1;

    for (int i = 0; i < edge_count_; i++) {
        int cell_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
            row_ptrs[row_ptr_count++] = current_row;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];
        // Collect deleted eid for spawning erase_e_map_task
        if (origin_vid == NULL) {
            if (cell_ref.is_out) {
                eid_t eid(cell_ref.conn_vtx_id.value(), origin_vid.value());
                gcable_eids->emplace_back(eid, true);
            } else {
                eid_t eid(origin_vid.value(), cell_ref.conn_vtx_id.value());
                gcable_eids->emplace_back(eid, false);
            }
        }

        EdgeMVCCItem* itr = cell_ref.mvcc_list->GetHead();
        while (itr != nullptr) {
            if (itr->GetValue().ep_row_list == nullptr) {
                itr = itr->GetNext();
                continue;
            }
            itr->GetValue().ep_row_list->SelfGarbageCollect();
            itr = itr->GetNext();
        }

        cell_ref.mvcc_list->SelfGarbageCollect();

        // Do not need to delete mvcc_list, since mvcc_list is still referred by e_map
    }

    for (int i = row_count - 1; i >= 0; i--) {
        mem_pool_->Free(row_ptrs[i], TidMapper::GetInstance()->GetTidUnique());
    }

    head_ = nullptr;
    tail_ = nullptr;

    delete[] row_ptrs;
}

void TopologyRowList::SelfDefragment(const vid_t& origin_vid, vector<pair<eid_t, bool>>* gcable_eids) {
    WriterLockGuard writer_lock_guard(gc_rwlock_);
    VertexEdgeRow* current_row = head_;

    if (current_row == nullptr)
        return;

    int row_count = edge_count_ / VE_ROW_ITEM_COUNT;
    if (row_count * VE_ROW_ITEM_COUNT != edge_count_)
        row_count++;
    if (row_count == 0)
        row_count = 1;

    VertexEdgeRow** row_ptrs = new VertexEdgeRow*[row_count];
    row_ptrs[0] = head_;
    int row_ptr_count = 1;

    queue<pair<int, int>> empty_cell_queue;
    for (int i = 0; i < edge_count_; i++) {
        int cell_id_in_row = i % VE_ROW_ITEM_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
            row_ptrs[row_ptr_count++] = current_row;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];
        if (cell_ref.mvcc_list->GetHead() == nullptr) {
            // Collect deleted eid for spawning erase_e_map_task
            if (cell_ref.is_out) {
                eid_t eid(cell_ref.conn_vtx_id.value(), origin_vid.value());
                gcable_eids->emplace_back(eid, true);
            } else {
                eid_t eid(origin_vid.value(), cell_ref.conn_vtx_id.value());
                gcable_eids->emplace_back(eid, false);
            }

            // gc cell, record to empty cell
            empty_cell_queue.emplace((int)(i / VE_ROW_ITEM_COUNT), cell_id_in_row);

            EdgeMVCCItem* itr = cell_ref.mvcc_list->GetHead();
            while (itr != nullptr) {
                if (itr->GetValue().ep_row_list == nullptr) {
                    itr = itr->GetNext();
                    continue;
                }
                itr->GetValue().ep_row_list->SelfGarbageCollect();
                itr = itr->GetNext();
            }

            cell_ref.mvcc_list->SelfGarbageCollect();
            // Do not need to delete mvcc_list, since mvcc_list is still referred by e_map
        }
    }

    int inverse_cell_index = edge_count_ - 1;
    int cur_edge_count = edge_count_ - empty_cell_queue.size();
    int num_movable_cell = cur_edge_count;
    int inverse_row_index = row_count - 1;

    // Calculate removable row
    int cur_row_count = cur_edge_count / VE_ROW_ITEM_COUNT + 1;
    if (cur_edge_count % VE_ROW_ITEM_COUNT == 0) {
        cur_row_count--;
    }
    int num_gcable_rows = row_count - cur_row_count;

    bool first_iteration = true;
    // move cell from tail to empty cell
    while (true) {
        int cell_id_in_row = inverse_cell_index % VE_ROW_ITEM_COUNT;
        if (cell_id_in_row == VE_ROW_ITEM_COUNT - 1 &&
            inverse_cell_index != edge_count_ - 1) {
            inverse_row_index--;
            CHECK_GE(inverse_row_index, 0);
        }

        auto& cell_ref = row_ptrs[inverse_row_index]->cells_[cell_id_in_row];
        MVCCList<EdgeMVCCItem>* mvcc_list = cell_ref.mvcc_list;

        if (mvcc_list->GetHead() != nullptr) {
            num_movable_cell--;
            pair<int, int> empty_cell = empty_cell_queue.front();
            empty_cell_queue.pop();

            row_ptrs[empty_cell.first]->cells_[empty_cell.second] = cell_ref;
        }

        if (empty_cell_queue.size() == 0 || num_movable_cell == 0) { break; }

        inverse_cell_index--;
        if (inverse_cell_index < 0) { break; }
    }

    // Recycle removable row
    for (int i = row_count - 1; i > cur_row_count - 1; i--) {
        if (i - 1 >= 0) { row_ptrs[i-1]->next_ = nullptr; }
        mem_pool_->Free(row_ptrs[i], TidMapper::GetInstance()->GetTidUnique());
    }

    // new property count
    edge_count_ = cur_edge_count;
    if (cur_row_count == 0) {
        head_ = nullptr;
        tail_ = nullptr;
    } else {
        tail_ = row_ptrs[cur_row_count - 1];
    }
}
