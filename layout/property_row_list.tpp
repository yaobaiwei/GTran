/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

template <class PropertyRow>
void PropertyRowList<PropertyRow>::Init() {
    head_ = tail_ = mem_pool_->Get(TidMapper::GetInstance()->GetTidUnique());
    property_count_ = 0;
    cell_map_ = nullptr;
}

// Allocate cell for one property, each pid should occupies only one cell
// property_count_ptr: snapshot of property_count_ from LocateCell
// tail_ptr          : snapshot of tail_ptr from LocateCell
template <class PropertyRow>
typename PropertyRowList<PropertyRow>::CellType* PropertyRowList<PropertyRow>::
        AllocateCell(PidType pid, int* property_count_ptr, PropertyRow** tail_ptr) {
    if (property_count_ptr == nullptr) {
        // Called by InsertInitialCell
        int cell_id = property_count_++;
        int cell_id_in_row = cell_id % PropertyRow::ROW_ITEM_COUNT;

        if (cell_id_in_row == 0 && cell_id > 0) {
            tail_->next_ = mem_pool_->Get(TidMapper::GetInstance()->GetTidUnique());
            tail_ = tail_->next_;
        }

        tail_->cells_[cell_id_in_row].pid = pid;
        return &tail_->cells_[cell_id_in_row];
    }

    // Called by ProcessModifyProperty, thread safety need to be guaranteed.
    WriterLockGuard writer_lock_guard(rwlock_);

    int property_count_snapshot = property_count_;
    int recent_count = property_count_ptr[0];
    CellType* ret = nullptr;
    bool allocated_already = false;

    // Check if new cell allocated by another thread
    if (recent_count != property_count_snapshot) {
        PropertyRow* current_row = tail_ptr[0];
        // Check pid of newly allocated cells
        for (int i = recent_count; i < property_count_snapshot; i++) {
            int cell_id_in_row = i % PropertyRow::ROW_ITEM_COUNT;
            if (i > 0 && cell_id_in_row == 0) {
                current_row = current_row->next_;
            }
            if (current_row->cells_[cell_id_in_row].pid == pid) {
                // A cell with the same pid has already been allocated
                allocated_already = true;
                break;
            }
        }
    }

    if (!allocated_already) {
        // Allocate a new cell
        int cell_id = property_count_snapshot;
        int cell_id_in_row = cell_id % PropertyRow::ROW_ITEM_COUNT;

        if (cell_id_in_row == 0 && cell_id > 0) {
            tail_->next_ = mem_pool_->Get(TidMapper::GetInstance()->GetTidUnique());
            tail_ = tail_->next_;
        }

        tail_->cells_[cell_id_in_row].pid = pid;
        tail_->cells_[cell_id_in_row].mvcc_list = nullptr;

        ret = &tail_->cells_[cell_id_in_row];

        // Use map for fast traversal when #cells > threshold
        if (property_count_snapshot >= MAP_THRESHOLD) {
            if (cell_map_ == nullptr) {
                cell_map_ = new CellMap;
                PropertyRow* current_row = head_;
                for (int i = 0; i < property_count_snapshot + 1; property_count_snapshot++) {
                    int cell_id_in_row = i % PropertyRow::ROW_ITEM_COUNT;
                    if (i > 0 && cell_id_in_row == 0) {
                        current_row = current_row->next_;
                    }

                    auto& cell_ref = current_row->cells_[cell_id_in_row];

                    CellAccessor accessor;
                    cell_map_->insert(accessor, cell_ref.pid.pid);
                    accessor->second = &cell_ref;
                }
            }

            CellAccessor accessor;
            cell_map_->insert(accessor, ret->pid.pid);
            accessor->second = ret;
        }

        // after the cell is initialized, increase the counter
        property_count_++;
    }


    return ret;
}

template <class PropertyRow>
typename PropertyRowList<PropertyRow>::CellType* PropertyRowList<PropertyRow>::
        LocateCell(PidType pid, int* property_count_ptr, PropertyRow** tail_ptr) {
    PropertyRow* current_row = head_;
    int property_count_snapshot;
    CellMap* map_snapshot;

    /* Get the snapshot of the PropertyRowList in critical region
     * Similarly hereinafter.
     */
    if (property_count_ptr != nullptr) {
        ReaderLockGuard reader_lock_guard(rwlock_);
        map_snapshot = cell_map_;
        property_count_snapshot = property_count_ptr[0] = property_count_;
        tail_ptr[0] = tail_;
    } else {
        ReaderLockGuard reader_lock_guard(rwlock_);
        map_snapshot = cell_map_;
        property_count_snapshot = property_count_;
    }

    if (map_snapshot == nullptr) {
        // Traverse the whole PropertyRowList
        for (int i = 0; i < property_count_snapshot; i++) {
            int cell_id_in_row = i % PropertyRow::ROW_ITEM_COUNT;
            if (i > 0 && cell_id_in_row == 0) {
                current_row = current_row->next_;
            }

            auto& cell_ref = current_row->cells_[cell_id_in_row];

            if (cell_ref.pid == pid) {
                return &cell_ref;
            }
        }
    } else {
        CellConstAccessor accessor;
        if (map_snapshot->find(accessor, pid.pid)) {
            return accessor->second;
        }
    }

    return nullptr;
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::InsertInitialCell(const PidType& pid, const value_t& value) {
    auto* cell = AllocateCell(pid);

    MVCCListType* mvcc_list = new MVCCListType;

    mvcc_list->AppendInitialVersion()[0] = value_storage_->InsertValue(value, TidMapper::GetInstance()->GetTidUnique());

    cell->mvcc_list = mvcc_list;
}

template <class PropertyRow>
READ_STAT PropertyRowList<PropertyRow>::
        ReadProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time,
                     const bool& read_only, value_t& ret) {
    auto* cell = LocateCell(pid);
    if (cell == nullptr)
        return READ_STAT::NOTFOUND;

    /* When a cell is visible, it just means that it has been allocated in AllocateCell.
     * However, the mvcc_list will be nullptr before the property has been inserted.
     * We need to fetch the cell->mvcc_list pointer in critical region, as cell->mvcc_list
     * will also be modified in critical region in ProcessModifyProperty.
     * Similarly hereinafter.
     */

    MVCCListType* mvcc_list;
    {
        ReaderLockGuard reader_lock_guard(rwlock_);
        mvcc_list = cell->mvcc_list;
    }
    // Being edited by other transaction.
    if (mvcc_list == nullptr)  // if not read-only, my read set has been modified
        return read_only ? READ_STAT::NOTFOUND : READ_STAT::ABORT;

    MVCCItemType* visible_version;
    ValueHeader storage_header;
    pair<bool, bool> is_visible = mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, storage_header);

    if (!is_visible.first)
        return READ_STAT::ABORT;
    if (!is_visible.second)
        return READ_STAT::NOTFOUND;

    if (!storage_header.IsEmpty()) {
        value_storage_->ReadValue(storage_header, ret);
        return READ_STAT::SUCCESS;
    }

    // Property's storage_header IsEmpty(), means deleted
    return READ_STAT::NOTFOUND;
}

template <class PropertyRow>
READ_STAT PropertyRowList<PropertyRow>::ReadPropertyByPKeyList(const vector<label_t>& p_key, const uint64_t& trx_id,
                                                               const uint64_t& begin_time, const bool& read_only,
                                                               vector<pair<label_t, value_t>>& ret) {
    PropertyRow* current_row = head_;
    int property_count_snapshot = property_count_;
    CellMap* map_snapshot;

    {
        ReaderLockGuard reader_lock_guard(rwlock_);
        CellMap* map_snapshot = cell_map_;
    }
    if (map_snapshot == nullptr) {
        // Traverse the whole PropertyRowList
        set<label_t> pkey_set;
        for (auto p_label : p_key) {
            pkey_set.insert(p_label);
        }

        for (int i = 0; i < property_count_snapshot; i++) {
            if (pkey_set.size() == 0) {
                // All properties has been fetched.
                break;
            }

            int cell_id_in_row = i % PropertyRow::ROW_ITEM_COUNT;
            if (i > 0 && cell_id_in_row == 0) {
                current_row = current_row->next_;
            }

            auto& cell_ref = current_row->cells_[cell_id_in_row];

            if (pkey_set.count(cell_ref.pid.pid) > 0) {
                pkey_set.erase(cell_ref.pid.pid);

                MVCCListType* mvcc_list;

                {
                    ReaderLockGuard reader_lock_guard(rwlock_);
                    mvcc_list = cell_ref.mvcc_list;
                }
                // Being edited by other transaction.
                if (mvcc_list == nullptr) {
                    if (read_only)
                        continue;
                    else  // Read set has been modified
                        return READ_STAT::ABORT;
                }

                ValueHeader storage_header;
                pair<bool, bool> is_visible = mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, storage_header);

                if (!is_visible.first)
                    return READ_STAT::ABORT;
                if (!is_visible.second)
                    continue;


                if (!storage_header.IsEmpty()) {
                    value_t v;
                    label_t label = cell_ref.pid.pid;
                    value_storage_->ReadValue(storage_header, v);
                    ret.emplace_back(make_pair(label, v));
                }
            }
        }
    } else {
        // The index map exists
        for (auto p_label : p_key) {
            CellConstAccessor accessor;
            if (map_snapshot->find(accessor, p_label)) {
                auto& cell_ref = *(accessor->second);

                MVCCListType* mvcc_list;

                {
                    ReaderLockGuard reader_lock_guard(rwlock_);
                    mvcc_list = cell_ref.mvcc_list;
                }
                // Being edited by other transaction
                if (mvcc_list == nullptr) {
                    if (read_only)
                        continue;
                    else
                        return READ_STAT::ABORT;
                }

                ValueHeader storage_header;
                pair<bool, bool> is_visible = mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, storage_header);

                if (!is_visible.first)
                    return READ_STAT::ABORT;
                if (!is_visible.second)
                    continue;

                if (!storage_header.IsEmpty()) {
                    value_t v;
                    label_t label = cell_ref.pid.pid;
                    value_storage_->ReadValue(storage_header, v);
                    ret.emplace_back(make_pair(label, v));
                }
            }
        }
    }

    if (ret.size() > 0)
        return READ_STAT::SUCCESS;
    else
        return READ_STAT::NOTFOUND;
}

template <class PropertyRow>
READ_STAT PropertyRowList<PropertyRow>::
        ReadAllProperty(const uint64_t& trx_id, const uint64_t& begin_time,
                        const bool& read_only, vector<pair<label_t, value_t>>& ret) {
    PropertyRow* current_row = head_;
    int property_count_snapshot = property_count_;

    for (int i = 0; i < property_count_snapshot; i++) {
        int cell_id_in_row = i % PropertyRow::ROW_ITEM_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        MVCCListType* mvcc_list;
        {
            ReaderLockGuard reader_lock_guard(rwlock_);
            mvcc_list = cell_ref.mvcc_list;
        }
        // Being edited by other transaction
        if (mvcc_list == nullptr) {
            if (read_only)
                continue;
            else  // Read set has been modified
                return READ_STAT::ABORT;
        }

        MVCCItemType* visible_version;
        ValueHeader storage_header;
        pair<bool, bool> is_visible = mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, storage_header);

        if (!is_visible.first)
            return READ_STAT::ABORT;
        if (!is_visible.second)
            continue;

        if (!storage_header.IsEmpty()) {
            value_t v;
            label_t label = cell_ref.pid.pid;
            value_storage_->ReadValue(storage_header, v);
            ret.emplace_back(make_pair(label, v));
        }
    }

    return READ_STAT::SUCCESS;
}

template <class PropertyRow>
READ_STAT PropertyRowList<PropertyRow>::
        ReadPidList(const uint64_t& trx_id, const uint64_t& begin_time,
                    const bool& read_only, vector<PidType>& ret) {
    PropertyRow* current_row = head_;
    int property_count_snapshot = property_count_;

    for (int i = 0; i < property_count_snapshot; i++) {
        int cell_id_in_row = i % PropertyRow::ROW_ITEM_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        MVCCListType* mvcc_list;
        {
            ReaderLockGuard reader_lock_guard(rwlock_);
            mvcc_list = cell_ref.mvcc_list;
        }
        // Being edited by other transaction
        if (mvcc_list == nullptr) {
            if (read_only)
                continue;
            else  // Read set has been modified
                return READ_STAT::ABORT;
        }

        MVCCItemType* visible_version;
        ValueHeader storage_header;
        pair<bool, bool> is_visible = mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only, storage_header);

        if (!is_visible.first)
            return READ_STAT::ABORT;
        if (!is_visible.second)
            continue;

        if (!storage_header.IsEmpty())
            ret.emplace_back(cell_ref.pid);
    }

    return READ_STAT::SUCCESS;
}

template <class PropertyRow>
pair<bool, typename PropertyRowList<PropertyRow>::MVCCListType*> PropertyRowList<PropertyRow>::
        ProcessModifyProperty(const PidType& pid, const value_t& value,
                              const uint64_t& trx_id, const uint64_t& begin_time) {
    int tmp_count;
    PropertyRow* tmp_tail;
    auto* cell = LocateCell(pid, &tmp_count, &tmp_tail);
    bool modify_flag = true;  // Will be false if this property does not exists.
    MVCCListType* mvcc_list;

    if (cell == nullptr) {
        cell = AllocateCell(pid, &tmp_count, &tmp_tail);
        modify_flag = false;  // Add property

        if (cell == nullptr)  // Allocate cell failed.
            return make_pair(false, nullptr);

        // cell->mvcc_list = new MVCCListType;
        mvcc_list = new MVCCListType;
    } else {
        ReaderLockGuard reader_lock_guard(rwlock_);
        mvcc_list = cell->mvcc_list;
    }

    // Being edited by other transaction, write-write conflict occurs
    if (mvcc_list == nullptr)
        return make_pair(true, nullptr);

    auto* version_val_ptr = mvcc_list->AppendVersion(trx_id, begin_time);
    if (version_val_ptr == nullptr)  // modify failed
        return make_pair(true, nullptr);

    version_val_ptr[0] = value_storage_->InsertValue(value, TidMapper::GetInstance()->GetTidUnique());

    if (!modify_flag) {
        // For a newly added cell, assign the mvcc_list after it has been initialized.
        WriterLockGuard reader_lock_guard(rwlock_);
        cell->mvcc_list = mvcc_list;
    }

    return make_pair(modify_flag, mvcc_list);
}

template <class PropertyRow>
typename PropertyRowList<PropertyRow>::MVCCListType* PropertyRowList<PropertyRow>::
        ProcessDropProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time) {
    auto* cell = LocateCell(pid);

    // system error; since this function is called by .drop() step, two conditions below won't happens
    assert(cell != nullptr);
    MVCCListType* mvcc_list;
    {
        ReaderLockGuard reader_lock_guard(rwlock_);
        mvcc_list = cell->mvcc_list;
    }
    assert(mvcc_list != nullptr);

    auto* version_val_ptr = mvcc_list->AppendVersion(trx_id, begin_time);
    if (version_val_ptr == nullptr)  // Modify failed, abort
        return nullptr;

    version_val_ptr[0].count = 0;  // IsEmpty() == true

    return mvcc_list;
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::SelfGarbageCollect() {
    // Free all cells
    PropertyRow* current_row = head_;
    int row_count = property_count_ / PropertyRow::ROW_ITEM_COUNT;
    if (row_count * PropertyRow::ROW_ITEM_COUNT != property_count_)
        row_count++;
    if (row_count == 0)
        row_count = 1;

    PropertyRow** row_ptrs = new PropertyRow*[row_count];
    row_ptrs[0] = head_;
    int row_ptr_count = 1;

    for (int i = 0; i < property_count_; i++) {
        int cell_id_in_row = i % PropertyRow::ROW_ITEM_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
            row_ptrs[row_ptr_count++] = current_row;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        cell_ref.mvcc_list->SelfGarbageCollect();
        delete cell_ref.mvcc_list;
    }

    for (int i = row_count - 1; i >= 0; i--) {
        mem_pool_->Free(row_ptrs[i], TidMapper::GetInstance()->GetTidUnique());
    }

    delete[] row_ptrs;
    if (cell_map_ != nullptr)
        delete cell_map_;
}
