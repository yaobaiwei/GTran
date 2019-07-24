/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

template <class PropertyRow>
void GCProducer::scan_prop_row_list(const uint64_t& element_id, PropertyRowList<PropertyRow> * prop_row_list) {
    if (prop_row_list == nullptr) { return; }
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
        if(scan_mvcc_list(element_id, cur_mvcc_list_ptr)) {
            gcable_cell_counter++;
        }
    }

    if (gcable_cell_counter >= property_count_snapshot % prop_row_list->MAP_THRESHOLD) {
        spawn_prop_row_defrag_gctask(prop_row_list, element_id, gcable_cell_counter);
    }
}

template <class MVCCItem>
bool GCProducer::scan_mvcc_list(const uint64_t& element_id, MVCCList<MVCCItem>* mvcc_list) {
    static_assert(is_base_of<AbstractMVCCItem, MVCCItem>::value, "scan_mvcc_list must take MVCCList");
    if (mvcc_list == nullptr) { return true; }

    SimpleSpinLockGuard lock_guard(&(mvcc_list->lock_));

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
    int gc_version_count = 0;
    while (true) {
        if (cur_ptr->GetEndTime() < MINIMUM_ACTIVE_TRANSACTION_BT) {
            // This version GCable
            gc_checkpoint = cur_ptr;
            gc_version_count++;
        } else {
            if (cur_ptr == iterate_tail) {
                if (cur_ptr->GetValue().IsEmpty()) {
                    gc_checkpoint = cur_ptr;
                    gc_version_count++;
                    break;
                }
            }

            if (is_base_of<PropertyMVCCItem, MVCCItem>::value) {
                break;
            } else {
                // scan_ep_row_list((EdgeMVCCItem*)cur_ptr);
                scan_prop_row_list(element_id, ((EdgeMVCCItem*)cur_ptr)->GetValue().ep_row_list);
            }
        }

        if (cur_ptr == iterate_tail) { break; }
        cur_ptr = cur_ptr->next;
    }

    if (gc_checkpoint != nullptr) {
        spawn_mvcc_list_gctask(mvcc_list->head_, element_id, gc_version_count);
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

/* For spawn_prop_row_defrag_gctask and spawn_mvcc_list_gctask, 
 *  We directly Cast pointer to specific class type
 *  which is dangerous but the most easy way to distinguish different types
 *  If our gcc could be updated to 7.x version
 *  we can use 'if constexpr' (c++17 feature but need 7.x gcc to support)
 *  to solve it with code commented in each function
 */
template<class PropertyRow>
void GCProducer::spawn_prop_row_defrag_gctask(PropertyRowList<PropertyRow> * prop_row_list, const uint64_t& element_id, const int& gcable_cell_counter) {
    if (is_same<PropertyRow, VertexPropertyRow>::value) {
        spawn_vp_row_defrag_gctask((PropertyRowList<VertexPropertyRow>*)prop_row_list, element_id, gcable_cell_counter);
    } else if (is_same<PropertyRow, EdgePropertyRow>::value) {
        spawn_ep_row_defrag_gctask((PropertyRowList<EdgePropertyRow>*)prop_row_list, element_id, gcable_cell_counter);
    } else {
        cout << "[GCProducer] Unexpected error in spawn_prop_row_defrag_gctask" << endl;
        CHECK(false);
    }

    /*
    if constexpr (is_same<PropertyRow, VertexPropertyRow>::value) {
        spawn_vp_row_defrag_gctask(prop_row_list, element_id, gcable_cell_counter);
    } else if constexpr (is_same<PropertyRow, EdgePropertyRow>::value) {
        spawn_ep_row_defrag_gctask(prop_row_list, element_id, gcable_cell_counter);
    } else constexpr {
        cout << "[GCProducer] Unexpected error in spawn_prop_row_defrag_gctask" << endl;
        CHECK(false);
    }
    */
}


template<class MVCCItem>
void GCProducer::spawn_mvcc_list_gctask(MVCCItem* gc_header, const uint64_t& element_id, const int& gc_version_count) {
    static_assert(is_base_of<AbstractMVCCItem, MVCCItem>::value, "spawn_mvcc_list_gc_task must take MVCCItem");

    if (is_same<MVCCItem, VPropertyMVCCItem>::value) {
        spawn_vp_mvcc_list_gctask((VPropertyMVCCItem*)gc_header, gc_version_count);
    } else if (is_same<MVCCItem, EPropertyMVCCItem>::value) {
        spawn_ep_mvcc_list_gctask((EPropertyMVCCItem*)gc_header, gc_version_count);
    } else if (is_same<MVCCItem, EdgeMVCCItem>::value) {
        spawn_edge_mvcc_list_gctask((EdgeMVCCItem*)gc_header, element_id, gc_version_count);
    } else {
        cout << "[GCProducer] VertexMVCCListGCTask should not be created in spawn_mvcc_list_gctask or there is a unexpected error" << endl;
        CHECK(false);
    }

    /*
    if constexpr (is_same<MVCCItem, VPropertyMVCCItem>::value) {
        spawn_vp_mvcc_list_gctask(gc_header, gc_version_count);
    } else if constexpr (is_same<MVCCItem, EPropertyMVCCItem>::value) {
        spawn_ep_mvcc_list_gctask(gc_header, gc_version_count);
    } else if constexpr (is_same<MVCCItem, EdgeMVCCItem>::value) {
        spawn_edge_mvcc_list_gctask(gc_header, element_id, gc_version_count);
    } else constexpr {
        cout << "[GCProducer] VertexMVCCListGCTask should not be created in spawn_mvcc_list_gctask or there is a unexpected error" << endl;
        CHECK(false);
    }
    */
}
