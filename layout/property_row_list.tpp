/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

template <class PropertyRow>
void PropertyRowList<PropertyRow>::Init() {
    head_ = mem_pool_->Get();
    property_count_ = 0;
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::InsertInitialElement(const PidType& pid, const value_t& value) {
    int element_id = property_count_++;
    int element_id_in_row = element_id % PropertyRow::RowItemCount();

    int next_count = (element_id - 1) / PropertyRow::RowItemCount();

    PropertyRow* current_row = head_;

    for (int i = 0; i < next_count; i++) {
        current_row = current_row->next_;
    }

    if (element_id > 0 && element_id % PropertyRow::RowItemCount() == 0) {
        current_row->next_ = mem_pool_->Get();
        current_row = current_row->next_;
        current_row->next_ = nullptr;
    }

    MVCCListType* mvcc_list = new MVCCListType;

    mvcc_list->AppendInitialVersion()[0] = value_storage_->InsertValue(value);

    current_row->cells_[element_id_in_row].pid = pid;
    current_row->cells_[element_id_in_row].mvcc_list = mvcc_list;
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::ReadProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& ret) {

    PropertyRow* current_row = head_;

    for (int i = 0; i < property_count_; i++) {
        int element_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[element_id_in_row];

        if (cell_ref.pid == pid) {
            auto storage_header = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time)->GetValue();
            if (!storage_header.IsEmpty()) {
                value_storage_->GetValue(storage_header, ret);
            }
            break;
        }
    }
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::ReadAllProperty(const uint64_t& trx_id, const uint64_t& begin_time, vector<pair<label_t, value_t>>& ret) {
    PropertyRow* current_row = head_;

    for (int i = 0; i < property_count_; i++) {
        int element_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[element_id_in_row];

        auto storage_header = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time)->GetValue();

        if(!storage_header.IsEmpty()){
            value_t v;
            label_t label = cell_ref.pid.pid;
            value_storage_->GetValue(storage_header, v);
            ret.emplace_back(make_pair(label, v));
        }
    }
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::ReadPidList(const uint64_t& trx_id, const uint64_t& begin_time, vector<PidType>& ret) {
    PropertyRow* current_row = head_;

    for (int i = 0; i < property_count_; i++) {
        int element_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[element_id_in_row];

        auto storage_header = cell_ref.mvcc_list->GetVisibleVersion(trx_id, begin_time)->GetValue();

        if(!storage_header.IsEmpty())
            ret.emplace_back(cell_ref.pid);
    }
}

template <class PropertyRow>
pair<bool, typename PropertyRowList<PropertyRow>::MVCCListType*> PropertyRowList<PropertyRow>::ProcessModifyProperty(const PidType& pid, const value_t& value, const uint64_t& trx_id, const uint64_t& begin_time) {
    PropertyRow* current_row = head_;

    for (int i = 0; i < property_count_; i++) {
        int element_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& cell_ref = current_row->cells_[element_id_in_row];

        if (cell_ref.pid == pid) {
            auto* version_val_ptr = cell_ref.mvcc_list->AppendVersion(trx_id, begin_time);
            if (version_val_ptr == nullptr)
                return make_pair(true, nullptr);

            version_val_ptr[0] = value_storage_->InsertValue(value);
            return make_pair(true, cell_ref.mvcc_list);
        }
    }

    // Add property
    int element_id = property_count_++;
    int element_id_in_row = element_id % PropertyRow::RowItemCount();
    if (element_id > 0 && element_id_in_row == 0) {
        current_row = current_row->next_;
    }

    auto& cell_ref = current_row->cells_[element_id_in_row];
    cell_ref.pid = pid;
    cell_ref.mvcc_list = new MVCCListType;

    // this won't be nullptr
    auto* version_val_ptr = cell_ref.mvcc_list->AppendVersion(trx_id, begin_time);
    version_val_ptr[0] = value_storage_->InsertValue(value);

    return make_pair(false, cell_ref.mvcc_list);
}
