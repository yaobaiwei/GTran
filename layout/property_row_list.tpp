/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

template <class PropertyRow>
void PropertyRowList<PropertyRow>::Init() {
    head_ = tail_ = mem_pool_->Get();
    property_count_ = 0;
}

template <class PropertyRow>
typename PropertyRowList<PropertyRow>::CellType* PropertyRowList<PropertyRow>::AllocateCell() {
    int cell_id = property_count_++;
    int cell_id_in_row = cell_id % PropertyRow::RowItemCount();

    if (cell_id_in_row == 0 && cell_id > 0) {
        tail_->next_ = mem_pool_->Get();
        tail_ = tail_->next_;
    }

    return &tail_->cells_[cell_id_in_row];
}

template <class PropertyRow>
typename PropertyRowList<PropertyRow>::CellType* PropertyRowList<PropertyRow>::LocateCell(PidType pid) {
    PropertyRow* current_row = head_;

    for (int i = 0; i < property_count_; i++) {
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
    auto* cell = AllocateCell();

    MVCCListType* mvcc_list = new MVCCListType;

    mvcc_list->AppendInitialVersion()[0] = value_storage_->InsertValue(value);

    cell->pid = pid;
    cell->mvcc_list = mvcc_list;
}

template <class PropertyRow>
bool PropertyRowList<PropertyRow>::ReadProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time, const bool& read_only, value_t& ret) {
    PropertyRow* current_row = head_;

    auto* cell = LocateCell(pid);
    if (cell == nullptr)
        return false;

    auto storage_header = cell->mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only)->GetValue();
    if (!storage_header.IsEmpty()) {
        value_storage_->GetValue(storage_header, ret);
        return true;
    }
    return false;
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::ReadPropertyByPKeyList(const vector<label_t>& p_key, const uint64_t& trx_id,
                                                          const uint64_t& begin_time, const bool& read_only,
                                                          vector<pair<label_t, value_t>>& ret) {
    PropertyRow* current_row = head_;

    set<label_t> pkey_set;
    for (auto p_label : p_key) {
        pkey_set.insert(p_label);
    }

    for (int i = 0; i < property_count_; i++) {
        if (pkey_set.size() == 0)
            break;

        int cell_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        if (pkey_set.count(cell_ref.pid.pid) > 0) {
            pkey_set.erase(cell_ref.pid.pid);

            auto storage_header = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only)->GetValue();

            if(!storage_header.IsEmpty()){
                value_t v;
                label_t label = cell_ref.pid.pid;
                value_storage_->GetValue(storage_header, v);
                ret.emplace_back(make_pair(label, v));
            }
        }
    }
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::ReadAllProperty(const uint64_t& trx_id, const uint64_t& begin_time, const bool& read_only, vector<pair<label_t, value_t>>& ret) {
    PropertyRow* current_row = head_;

    for (int i = 0; i < property_count_; i++) {
        int cell_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        auto storage_header = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only)->GetValue();

        if(!storage_header.IsEmpty()){
            value_t v;
            label_t label = cell_ref.pid.pid;
            value_storage_->GetValue(storage_header, v);
            ret.emplace_back(make_pair(label, v));
        }
    }
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::ReadPidList(const uint64_t& trx_id, const uint64_t& begin_time, const bool& read_only, vector<PidType>& ret) {
    PropertyRow* current_row = head_;

    for (int i = 0; i < property_count_; i++) {
        int cell_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && cell_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[cell_id_in_row];

        auto storage_header = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time, read_only)->GetValue();

        if(!storage_header.IsEmpty())
            ret.emplace_back(cell_ref.pid);
    }
}

template <class PropertyRow>
pair<bool, typename PropertyRowList<PropertyRow>::MVCCListType*> PropertyRowList<PropertyRow>::ProcessModifyProperty(const PidType& pid, const value_t& value, const uint64_t& trx_id, const uint64_t& begin_time) {
    auto* cell = LocateCell(pid);
    bool modify_flag = true;

    if (cell == nullptr) {
        cell = AllocateCell();
        modify_flag = false;  // Add property

        cell->pid = pid;
        cell->mvcc_list = new MVCCListType;
    }

    auto* version_val_ptr = cell->mvcc_list->AppendVersion(trx_id, begin_time);
    if (version_val_ptr == nullptr)  // modify failed
        return make_pair(true, nullptr);

    version_val_ptr[0] = value_storage_->InsertValue(value);
    return make_pair(modify_flag, cell->mvcc_list);
}

template <class PropertyRow>
typename PropertyRowList<PropertyRow>::MVCCListType* PropertyRowList<PropertyRow>::ProcessDropProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time) {
    auto* cell = LocateCell(pid);  // must not be nullptr

    auto* version_val_ptr = cell->mvcc_list->AppendVersion(trx_id, begin_time);
    if (version_val_ptr == nullptr)  // modify failed
        return nullptr;

    version_val_ptr[0].count = 0;  // then IsEmpty() == true

    return cell->mvcc_list;
}
