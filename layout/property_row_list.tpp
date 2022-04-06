// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

template <class PropertyRow>
void PropertyRowList<PropertyRow>::Init() {
    head_ = tail_ = nullptr;
    property_count_ = 0;
    cell_map_ = nullptr;
}

// Allocate cell for one property, each pid should occupies only one cell
// property_count_ptr: snapshot of property_count_ from LocateCell
// tail_ptr          : snapshot of tail_ptr from LocateCell
template <class PropertyRow>
typename PropertyRowList<PropertyRow>::CellType* PropertyRowList<PropertyRow>::
        AllocateCell(PidType pid, int* property_count_ptr, PropertyRow** tail_ptr) {
    // Called by ProcessModifyProperty, thread safety need to be guaranteed.
    WriterLockGuard writer_lock_guard(rwlock_);

    int property_count_snapshot = property_count_;
    int recent_count = *property_count_ptr;
    CellType* ret = nullptr;
    bool allocated_already = false;

    // Check if new cell allocated by another thread
    if (recent_count != property_count_snapshot) {
        PropertyRow* current_row = *tail_ptr;
        if (current_row == nullptr)
            current_row = head_;
        // Check pid of newly allocated cells
        for (int i = recent_count; i < property_count_snapshot; i++) {
            int cell_id_in_row = i % PropertyRow::ROW_CELL_COUNT;
            if (i > 0 && cell_id_in_row == 0) {
                current_row = current_row->next_;
            }
            if (current_row->cells_[cell_id_in_row].pid == pid) {
                // A cell with the same pid has already been allocated after LocateCell is called
                allocated_already = true;
                break;
            }
        }
    }

    if (!allocated_already) {
        // Allocate a new cell
        int cell_id = property_count_snapshot;
        int cell_id_in_row = cell_id % PropertyRow::ROW_CELL_COUNT;

        if (cell_id_in_row == 0) {
            auto* new_row = mem_pool_->Get(TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));
            if (cell_id == 0) {
                head_ = tail_ = new_row;
            } else {
                tail_->next_ = new_row;
                tail_ = tail_->next_;
            }
        }

        tail_->cells_[cell_id_in_row].pid = pid;
        tail_->cells_[cell_id_in_row].mvcc_list = nullptr;

        ret = &tail_->cells_[cell_id_in_row];

        // Use map for fast traversal when #cells > threshold
        if (property_count_snapshot >= MAP_THRESHOLD) {
            if (cell_map_ == nullptr) {
                cell_map_ = new CellMap;
                PropertyRow* current_row = head_;
                for (int i = 0; i < property_count_snapshot + 1; i++) {
                    int cell_id_in_row = i % PropertyRow::ROW_CELL_COUNT;
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
    PropertyRow* current_row;
    int property_count_snapshot;
    CellMap* map_snapshot;

    {
        ReaderLockGuard reader_lock_guard(rwlock_);

        map_snapshot = cell_map_;
        property_count_snapshot = property_count_;
        current_row = head_;

        if (property_count_ptr != nullptr) {
            // called by AllocateCell, need to return the snapshot of property_count_ and tail_ via pointers
            *property_count_ptr = property_count_snapshot;
            *tail_ptr = tail_;
        }
    }

    if (map_snapshot == nullptr) {
        // Traverse the whole PropertyRowList
        for (int i = 0; i < property_count_snapshot; i++) {
            int cell_id_in_row = i % PropertyRow::ROW_CELL_COUNT;
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
    int cell_id = property_count_++;
    int cell_id_in_row = cell_id % PropertyRow::ROW_CELL_COUNT;

    if (cell_id_in_row == 0) {
        auto* new_row = mem_pool_->Get(TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));
        if (cell_id == 0) {
            head_ = tail_ = new_row;
        } else {
            tail_->next_ = new_row;
            tail_ = tail_->next_;
        }
    }

    tail_->cells_[cell_id_in_row].pid = pid;

    MVCCListType* mvcc_list = new MVCCListType;
    *(mvcc_list->AppendInitialVersion()) = value_store_->InsertValue(value, TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));
    tail_->cells_[cell_id_in_row].mvcc_list = mvcc_list;
}

template <class PropertyRow>
READ_STAT PropertyRowList<PropertyRow>::
        ReadProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time,
                     const bool& read_only, value_t& ret) {
    ReaderLockGuard reader_lock_guard(gc_rwlock_);
    auto* cell = LocateCell(pid);
    if (cell == nullptr)
        return READ_STAT::NOTFOUND;

    MVCCListType* mvcc_list = cell->mvcc_list;
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
        value_store_->ReadValue(storage_header, ret);
        return READ_STAT::SUCCESS;
    } else {
        // this property was deleted
        READ_STAT::NOTFOUND;
    }
}

template <class PropertyRow>
READ_STAT PropertyRowList<PropertyRow>::ReadPropertyByPKeyList(const vector<label_t>& p_key, const uint64_t& trx_id,
                                                               const uint64_t& begin_time, const bool& read_only,
                                                               vector<pair<label_t, value_t>>& ret) {
    ReaderLockGuard reader_lock_guard(gc_rwlock_);
    PropertyRow* current_row;
    int property_count_snapshot;
    CellMap* map_snapshot;

    {
        ReaderLockGuard reader_lock_guard(rwlock_);
        map_snapshot = cell_map_;
        property_count_snapshot = property_count_;
        current_row = head_;
    }
    if (current_row == nullptr)
        return READ_STAT::NOTFOUND;

    if (map_snapshot == nullptr) {
        // Traverse the whole PropertyRowList
        set<label_t> pkey_set;
        for (auto p_label : p_key) {
            pkey_set.insert(p_label);
        }

        for (int i = 0; i < property_count_snapshot; i++) {
            if (pkey_set.size() == 0) {
                // All needed properties has been fetched.
                break;
            }

            int cell_id_in_row = i % PropertyRow::ROW_CELL_COUNT;
            if (i > 0 && cell_id_in_row == 0) {
                current_row = current_row->next_;
            }

            auto& cell_ref = current_row->cells_[cell_id_in_row];

            if (pkey_set.count(cell_ref.pid.pid) > 0) {
                pkey_set.erase(cell_ref.pid.pid);

                MVCCListType* mvcc_list = cell_ref.mvcc_list;

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
                    value_store_->ReadValue(storage_header, v);
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

                MVCCListType* mvcc_list = cell_ref.mvcc_list;

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
                    value_store_->ReadValue(storage_header, v);
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
    ReaderLockGuard reader_lock_guard(gc_rwlock_);
    PropertyRow* current_row;
    int property_count_snapshot;

    {
        ReaderLockGuard reader_lock_guard(rwlock_);
        current_row = head_;
        property_count_snapshot = property_count_;
    }

    for (int i = 0; i < property_count_snapshot; i++) {
        int cell_id_in_row = i % PropertyRow::ROW_CELL_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        MVCCListType* mvcc_list = cell_ref.mvcc_list;
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
            value_store_->ReadValue(storage_header, v);
            ret.emplace_back(make_pair(label, v));
        }
    }

    return READ_STAT::SUCCESS;
}

template <class PropertyRow>
READ_STAT PropertyRowList<PropertyRow>::
        ReadPidList(const uint64_t& trx_id, const uint64_t& begin_time,
                    const bool& read_only, vector<PidType>& ret) {
    ReaderLockGuard reader_lock_guard(gc_rwlock_);

    PropertyRow* current_row;
    int property_count_snapshot;

    {
        ReaderLockGuard reader_lock_guard(rwlock_);
        current_row = head_;
        property_count_snapshot = property_count_;
    }

    for (int i = 0; i < property_count_snapshot; i++) {
        int cell_id_in_row = i % PropertyRow::ROW_CELL_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        MVCCListType* mvcc_list = cell_ref.mvcc_list;

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
        ProcessModifyProperty(const PidType& pid, const value_t& value, value_t& old_val,
                              const uint64_t& trx_id, const uint64_t& begin_time) {
    ReaderLockGuard reader_lock_guard(gc_rwlock_);
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
        mvcc_list = cell->mvcc_list;
    }

    // Being edited by other transaction, write-write conflict occurs
    if (mvcc_list == nullptr)
        return make_pair(true, nullptr);

    bool old_val_exists = true;
    ValueHeader old_val_header;
    auto* version_val_ptr = mvcc_list->AppendVersion(trx_id, begin_time, &old_val_header, &old_val_exists);
    if (old_val_exists) {
        // Get old value
        value_store_->ReadValue(old_val_header, old_val);
    }

    if (version_val_ptr == nullptr)  // modify failed
        return make_pair(true, nullptr);

    *version_val_ptr = value_store_->InsertValue(value, TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));

    if (!modify_flag) {
        // For a newly added cell, assign the mvcc_list after it has been initialized.
        cell->mvcc_list = mvcc_list;
    }

    return make_pair(modify_flag, mvcc_list);
}

template <class PropertyRow>
typename PropertyRowList<PropertyRow>::MVCCListType* PropertyRowList<PropertyRow>::
        ProcessDropProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t & old_val) {
    ReaderLockGuard reader_lock_guard(gc_rwlock_);
    auto* cell = LocateCell(pid);

    // system error; since this function is called by .drop() step, two conditions below won't happens
    CHECK(cell != nullptr);
    MVCCListType* mvcc_list = cell->mvcc_list;

    CHECK(mvcc_list != nullptr);

    bool old_val_exists = true;
    ValueHeader old_val_header;
    auto* version_val_ptr = mvcc_list->AppendVersion(trx_id, begin_time, &old_val_header, &old_val_exists);
    if (old_val_exists) {
        // Get old Value
        value_store_->ReadValue(old_val_header, old_val);
    }

    if (version_val_ptr == nullptr)  // modify failed
        return nullptr;

    version_val_ptr->byte_count = 0;  // IsEmpty() == true, as a "drop" version

    return mvcc_list;
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::SelfGarbageCollect() {
    // Free all cells
    WriterLockGuard writer_lock_guard(gc_rwlock_);
    PropertyRow* current_row = head_;
    if (current_row == nullptr)
        return;

    int row_count = property_count_ / PropertyRow::ROW_CELL_COUNT;
    if (row_count * PropertyRow::ROW_CELL_COUNT != property_count_)
        row_count++;
    if (row_count == 0)
        row_count = 1;

    PropertyRow** row_ptrs = new PropertyRow*[row_count];
    row_ptrs[0] = head_;
    int row_ptr_count = 1;

    for (int i = 0; i < property_count_; i++) {
        int cell_id_in_row = i % PropertyRow::ROW_CELL_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
            row_ptrs[row_ptr_count++] = current_row;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];
        MVCCListType* mvcc_list = cell_ref.mvcc_list;

        mvcc_list->SelfGarbageCollect();
        delete mvcc_list;
    }

    for (int i = row_count - 1; i >= 0; i--) {
        mem_pool_->Free(row_ptrs[i], TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));
    }

    head_ = nullptr;
    tail_ = nullptr;

    delete[] row_ptrs;
    if (cell_map_ != nullptr)
        delete cell_map_;
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::SelfDefragment() {
    // Scan and collect cell
    WriterLockGuard writer_lock_guard(gc_rwlock_);
    PropertyRow* current_row = head_;
    if (current_row == nullptr)
        return;

    int row_count = property_count_ / PropertyRow::ROW_CELL_COUNT;
    if (row_count * PropertyRow::ROW_CELL_COUNT != property_count_)
        row_count++;
    if (row_count == 0)
        row_count = 1;

    PropertyRow** row_ptrs = new PropertyRow*[row_count];
    row_ptrs[0] = head_;
    int row_ptr_count = 1;

    queue<pair<int, int>> empty_cell_queue;
    // Collect empty cell info
    for (int i = 0; i < property_count_; i++) {
        int cell_id_in_row = i % PropertyRow::ROW_CELL_COUNT;
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
            row_ptrs[row_ptr_count++] = current_row;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];
        MVCCListType* mvcc_list = cell_ref.mvcc_list;

        CHECK(mvcc_list != nullptr);

        // mark the gcable cell's mvcc_list as nullptr
        if (mvcc_list->GetHead() == nullptr) {
            // gc cell, record to empty cell
            empty_cell_queue.emplace((int) (i / PropertyRow::ROW_CELL_COUNT), cell_id_in_row);
            mvcc_list->SelfGarbageCollect();
            delete mvcc_list;
            cell_ref.mvcc_list = nullptr;

            // erase from the index map
            if (cell_map_ != nullptr)
                cell_map_->erase(cell_ref.pid.pid);
        }
    }

    if (empty_cell_queue.empty())
        return;

    int inverse_cell_index = property_count_ - 1;
    int cur_property_count = property_count_ - empty_cell_queue.size();
    int num_movable_cell = cur_property_count;
    int inverse_row_index = row_count - 1;

    // Calculate removable row
    int cur_row_count = cur_property_count / PropertyRow::ROW_CELL_COUNT + 1;
    if (cur_property_count % PropertyRow::ROW_CELL_COUNT == 0) {
        cur_row_count--;
    }

    // move cell from tail to empty cell
    while (true) {
        int cell_id_in_row = inverse_cell_index % PropertyRow::ROW_CELL_COUNT;
        if (cell_id_in_row == PropertyRow::ROW_CELL_COUNT - 1 &&
            inverse_cell_index != property_count_ - 1) {
            inverse_row_index--;
            CHECK_GE(inverse_row_index, 0);
        }

        auto& cell_ref = row_ptrs[inverse_row_index]->cells_[cell_id_in_row];
        MVCCListType* mvcc_list = cell_ref.mvcc_list;

        // move a non-empty cell to an empty cell
        if (mvcc_list != nullptr) {
            CHECK(mvcc_list->GetHead() != nullptr);
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
        if (i-1 >= 0) { row_ptrs[i-1]->next_ = nullptr; }
        mem_pool_->Free(row_ptrs[i], TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));
    }

    if (cur_row_count == 0) {
        head_ = nullptr;
        tail_ = nullptr;
    } else {
        tail_ = row_ptrs[cur_row_count - 1];
    }

    property_count_ = cur_property_count;

    delete[] row_ptrs;

    if (cell_map_ == nullptr)
        return;

    // reindex the cell_map_ for the remaining properties
    current_row = head_;
    for (int i = 0; i < property_count_; i++) {
        int cell_id_in_row = i % PropertyRow::ROW_CELL_COUNT;
        if (i > 0 && cell_id_in_row == 0)
            current_row = current_row->next_;

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        CellAccessor accessor;
        CHECK(cell_map_->find(accessor, cell_ref.pid.pid));

        accessor->second = &cell_ref;
    }
}