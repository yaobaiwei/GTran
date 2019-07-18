/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

template <class PropertyRow>
void GCProducer::scan_prop_row_list(PropertyRowList<PropertyRow> * prop_row_list) {
    PropertyRow* row_ptr = prop_row_list->head_;
    int property_count_snapshot = prop_row_list->property_count_;

    int gcable_cell_counter = 0;
    typename PropertyRowList<PropertyRow>::MVCCListType* cur_mvcc_list_ptr;
    for (int i = 0; i < property_count_snapshot; i++) {
        int cell_id_in_row = i % PropertyRow::ROW_ITEM_COUNT; 
        if (i != 0 && cell_id_in_row == 0) {
            row_ptr = row_ptr->next_;
        }

        cur_mvcc_list_ptr = row_ptr->cells_[cell_id_in_row].mvcc_list;
        if(scan_mvcc_list(cur_mvcc_list_ptr)) {
            gcable_cell_counter++;
        }
    }

    /*
    if (gcable_cell_counter > THRESHOLD) {
        spawn_vp_row_defrag_gctask(vp_row_list, gcable_cell_counter);
    }*/
}

template <class MVCCItem>
bool GCProducer::scan_mvcc_list(MVCCList<MVCCItem>* mvcc_list) {
    static_assert(is_base_of<AbstractMVCCItem, MVCCItem>::value, "scan_prop_mvcc_list must take property_mvcc_list");

    typename MVCCList<MVCCItem>::SimpleSpinLockGuard lock_guard(&(mvcc_list->lock_));

    MVCCItem *cur_ptr = mvcc_list->head_;
    MVCCItem *iterate_tail = mvcc_list->tail_;

    if (cur_ptr == nullptr) { return true; }
    bool uncommitted_version_exists = false;
    if (iterate_tail->GetTransactionID() != 0) {
        if (cur_ptr == iterate_tail) { return false; }

        iterate_tail = mvcc_list->pre_tail_;
        uncommitted_version_exists = true;
    }

    MVCCItem *gc_checkpoint = nullptr;
    while (true) {
        if (cur_ptr->GetEndTime() < MINIMUM_ACTIVE_TRANSACTION_BT) {
            // This version GCable
            gc_checkpoint = cur_ptr;
        } else {
            if (cur_ptr == iterate_tail) {
                if (cur_ptr->GetValue().IsEmpty()) {
                    gc_checkpoint = cur_ptr;
                    break;
                }
            }

            if (is_base_of<PropertyMVCCItem, MVCCItem>::value) {
                break;
            } else {
                scan_ep_row_list((EdgeMVCCItem*)cur_ptr); 
            }
        }

        if (cur_ptr == iterate_tail) { break; }
        cur_ptr = cur_ptr->next;
    }

    if (gc_checkpoint != nullptr) {
        // spawn_mvcc_list_gctask(mvcc_list->head_);
        mvcc_list->head_ = gc_checkpoint->next;  // Could be nullptr or a uncommitted version
        if (gc_checkpoint == iterate_tail) {  // There is at most one left
            mvcc_list->pre_tail_ = nullptr;
            mvcc_list->tmp_pre_tail_ = nullptr;
            if (!uncommitted_version_exists) {
                mvcc_list->tail_ = nullptr;
                return true;  // Nothing Left, cell could be GC
            }
        } else if (gc_checkpoint->next == iterate_tail) {  // At most two, at least one version left
            mvcc_list->tmp_pre_tail_ = nullptr;
            if (!uncommitted_version_exists) {  // One version left
                mvcc_list->pre_tail_ = nullptr; 
            }
        }

        gc_checkpoint->next = nullptr;
    }

    return false;
}
