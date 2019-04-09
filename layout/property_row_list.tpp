/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

template <class PropertyRow>
void PropertyRowList<PropertyRow>::Init() {
    head_ = tail_ = mem_pool_->Get();
    property_count_ = 0;
    pthread_spin_init(&lock_, 0);
}

template <class PropertyRow>
typename PropertyRowList<PropertyRow>::CellType* PropertyRowList<PropertyRow>::
        AllocateCell(PidType pid, int* property_count_ptr, PropertyRow** tail_ptr) {
    if (property_count_ptr == nullptr) {
        // called by InsertInitialCell
        int cell_id = property_count_++;
        int cell_id_in_row = cell_id % PropertyRow::RowItemCount();

        if (cell_id_in_row == 0 && cell_id > 0) {
            tail_->next_ = mem_pool_->Get();
            tail_ = tail_->next_;
        }

        tail_->cells_[cell_id_in_row].pid = pid;
        return &tail_->cells_[cell_id_in_row];
    }

    // thread safe needed
    pthread_spin_lock(&lock_);
    // check if pid already exists
    int current_property_count = property_count_;
    int recent_count = property_count_ptr[0];
    CellType* ret = nullptr;
    bool found = false;
    if (recent_count != current_property_count) {
        PropertyRow* recent_row = tail_ptr[0];
        PropertyRow* current_row = recent_row;

        for (int i = recent_count; i < current_property_count; i++) {
            int cell_id_in_row = i % PropertyRow::RowItemCount();
            if (i > 0 && cell_id_in_row == 0) {
                current_row = current_row->next_;
            }
            if (current_row->cells_[cell_id_in_row].pid == pid) {
                // a cell with the same pid has already been allocated
                // abort
                found = true;
                break;
            }
        }
    }

    if (!found) {
        // allocate a new cell
        int cell_id = current_property_count;
        int cell_id_in_row = cell_id % PropertyRow::RowItemCount();

        if (cell_id_in_row == 0 && cell_id > 0) {
            tail_->next_ = mem_pool_->Get();
            tail_ = tail_->next_;
        }

        tail_->cells_[cell_id_in_row].pid = pid;
        tail_->cells_[cell_id_in_row].mvcc_list = nullptr;

        ret = &tail_->cells_[cell_id_in_row];

        // after the cell is initialized, increase the counter
        property_count_++;
    }

    pthread_spin_unlock(&lock_);

    return ret;
}

template <class PropertyRow>
typename PropertyRowList<PropertyRow>::CellType* PropertyRowList<PropertyRow>::
        LocateCell(PidType pid, int* property_count_ptr, PropertyRow** tail_ptr) {
    PropertyRow* current_row = head_;
    int current_property_count;

    if (property_count_ptr != nullptr) {
        pthread_spin_lock(&lock_);
        current_property_count = property_count_ptr[0] = property_count_;
        tail_ptr[0] = tail_;
        pthread_spin_unlock(&lock_);
    } else {
        current_property_count = property_count_;
    }

    for (int i = 0; i < current_property_count; i++) {
        int cell_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        if (cell_ref.pid == pid) {
            return &cell_ref;
        }
    }

    return nullptr;
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::InsertInitialCell(const PidType& pid, const value_t& value) {
    auto* cell = AllocateCell(pid);

    MVCCListType* mvcc_list = new MVCCListType;

    mvcc_list->AppendInitialVersion()[0] = value_storage_->InsertValue(value);

    cell->mvcc_list = mvcc_list;
}

template <class PropertyRow>
READ_STAT PropertyRowList<PropertyRow>::
        ReadProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time,
                     const bool& read_only, value_t& ret) {
    auto* cell = LocateCell(pid);
    if (cell == nullptr)
        return READ_STAT::NOTFOUND;

    // being edited by other transaction
    // TODO(entityless): double check this in the future
    if (cell->mvcc_list == nullptr)
        return READ_STAT::NOTFOUND;

    auto* visible_mvcc = cell->mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only);

    if (visible_mvcc == nullptr)
        return READ_STAT::ABORT;

    auto storage_header = visible_mvcc->GetValue();
    if (!storage_header.IsEmpty()) {
        value_storage_->GetValue(storage_header, ret);
        return READ_STAT::SUCCESS;
    }

    return READ_STAT::NOTFOUND;
}

template <class PropertyRow>
READ_STAT PropertyRowList<PropertyRow>::ReadPropertyByPKeyList(const vector<label_t>& p_key, const uint64_t& trx_id,
                                                               const uint64_t& begin_time, const bool& read_only,
                                                               vector<pair<label_t, value_t>>& ret) {
    PropertyRow* current_row = head_;
    int current_property_count = property_count_;

    set<label_t> pkey_set;
    for (auto p_label : p_key) {
        pkey_set.insert(p_label);
    }

    for (int i = 0; i < current_property_count; i++) {
        if (pkey_set.size() == 0)
            break;

        int cell_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        if (pkey_set.count(cell_ref.pid.pid) > 0) {
            pkey_set.erase(cell_ref.pid.pid);

            // being edited by other transaction
            // TODO(entityless): double check this in the future
            if (cell_ref.mvcc_list == nullptr)
                continue;

            auto* visible_mvcc = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only);

            if (visible_mvcc == nullptr)
                return READ_STAT::ABORT;

            auto storage_header = visible_mvcc->GetValue();

            if (!storage_header.IsEmpty()) {
                value_t v;
                label_t label = cell_ref.pid.pid;
                value_storage_->GetValue(storage_header, v);
                ret.emplace_back(make_pair(label, v));
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
    int current_property_count = property_count_;

    for (int i = 0; i < current_property_count; i++) {
        int cell_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        // being edited by other transaction
        // TODO(entityless): double check this in the future
        if (cell_ref.mvcc_list == nullptr)
            continue;

        auto* visible_mvcc = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only);

        if (visible_mvcc == nullptr)
            return READ_STAT::ABORT;

        auto storage_header = visible_mvcc->GetValue();

        if (!storage_header.IsEmpty()) {
            value_t v;
            label_t label = cell_ref.pid.pid;
            value_storage_->GetValue(storage_header, v);
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
    int current_property_count = property_count_;

    for (int i = 0; i < current_property_count; i++) {
        int cell_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        // being edited by other transaction
        // TODO(entityless): double check this in the future
        if (cell_ref.mvcc_list == nullptr)
            continue;

        auto* visible_mvcc = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only);

        if (visible_mvcc == nullptr)
            return READ_STAT::ABORT;

        auto storage_header = visible_mvcc->GetValue();

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
    bool modify_flag = true;

    if (cell == nullptr) {
        cell = AllocateCell(pid, &tmp_count, &tmp_tail);
        modify_flag = false;  // Add property

        if (cell == nullptr)  // add failed
            return make_pair(false, nullptr);

        cell->mvcc_list = new MVCCListType;
    }

    // being edited by other transaction
    // TODO(entityless): double check this in the future
    if (cell->mvcc_list == nullptr)
        return make_pair(true, nullptr);

    auto* version_val_ptr = cell->mvcc_list->AppendVersion(trx_id, begin_time);
    if (version_val_ptr == nullptr)  // modify failed
        return make_pair(true, nullptr);

    version_val_ptr[0] = value_storage_->InsertValue(value);
    return make_pair(modify_flag, cell->mvcc_list);
}

template <class PropertyRow>
typename PropertyRowList<PropertyRow>::MVCCListType* PropertyRowList<PropertyRow>::
        ProcessDropProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time) {
    auto* cell = LocateCell(pid);

    // system error; since this function is called by .drop() step, two conditions below won't happens
    assert(cell != nullptr);
    assert(cell->mvcc_list != nullptr);

    auto* version_val_ptr = cell->mvcc_list->AppendVersion(trx_id, begin_time);
    if (version_val_ptr == nullptr)  // modify failed
        return nullptr;

    version_val_ptr[0].count = 0;  // then IsEmpty() == true

    return cell->mvcc_list;
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::SelfGarbageCollect() {
    // free all cells
    PropertyRow* current_row = head_;
    int row_count = property_count_ / PropertyRow::RowItemCount();
    if (row_count * PropertyRow::RowItemCount() != property_count_)
        row_count++;
    if (row_count == 0)
        row_count = 1;

    PropertyRow** row_ptrs = new PropertyRow*[row_count];
    row_ptrs[0] = head_;
    int row_ptr_count = 1;

    for (int i = 0; i < property_count_; i++) {
        int cell_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
            row_ptrs[row_ptr_count++] = current_row;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        cell_ref.mvcc_list->SelfGarbageCollect();
        delete cell_ref.mvcc_list;
    }

    for (int i = row_count - 1; i >= 0; i--) {
        mem_pool_->Free(row_ptrs[i]);
    }

    delete[] row_ptrs;
}
