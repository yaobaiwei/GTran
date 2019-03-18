/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

template <class PropertyRow>
void PropertyRowList<PropertyRow>::Init() {
    head_ = pool_ptr_->Get();
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
        current_row->next_ = pool_ptr_->Get();
        current_row = current_row->next_;
        current_row->next_ = nullptr;
    }

    MVCCList<PropertyMVCC>* mvcc_list = new MVCCList<PropertyMVCC>;

    mvcc_list->AppendInitialVersion()[0] = value_storage_ptr_->Insert(value);

    current_row->elements_[element_id_in_row].pid = pid;
    current_row->elements_[element_id_in_row].mvcc_list = mvcc_list;
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::ReadProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& ret) {

    PropertyRow* current_row = head_;

    for (int i = 0; i < property_count_; i++) {
        int element_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& element_ref = current_row->elements_[element_id_in_row];

        if (element_ref.pid == pid) {
            auto storage_header = element_ref.mvcc_list->GetCurrentVersion(trx_id, begin_time)->GetValue();
            if (!storage_header.IsEmpty()) {
                value_storage_ptr_->GetValue(storage_header, ret);
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

        auto& element_ref = current_row->elements_[element_id_in_row];

        auto storage_header = element_ref.mvcc_list->GetCurrentVersion(trx_id, begin_time)->GetValue();

        if(!storage_header.IsEmpty()){
            value_t v;
            label_t label = element_ref.pid.pid;
            value_storage_ptr_->GetValue(storage_header, v);
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

        auto& element_ref = current_row->elements_[element_id_in_row];

        auto storage_header = element_ref.mvcc_list->GetCurrentVersion(trx_id, begin_time)->GetValue();

        if(!storage_header.IsEmpty())
            ret.emplace_back(element_ref.pid);
    }
}

template <class PropertyRow>
bool PropertyRowList<PropertyRow>::ProcessModifyProperty(const PidType& pid, const value_t& value, const uint64_t& trx_id, const uint64_t& begin_time) {
    PropertyRow* current_row = head_;

    for (int i = 0; i < property_count_; i++) {
        int element_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& element_ref = current_row->elements_[element_id_in_row];

        if (element_ref.pid == pid) {
            auto* version_val_ptr = element_ref.mvcc_list->AppendVersion(trx_id, begin_time);
            if (version_val_ptr == nullptr)
                return false;

            version_val_ptr[0] = value_storage_ptr_->Insert(value);
            return true;
        }
    }

    // TODO(entityless): insert property?
    int element_id = property_count_++;

    return true;
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::Commit(const PidType& pid, const uint64_t& trx_id, const uint64_t& commit_time) {
    PropertyRow* current_row = head_;

    for (int i = 0; i < property_count_; i++) {
        int element_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& element_ref = current_row->elements_[element_id_in_row];

        if (element_ref.pid == pid) {
            element_ref.mvcc_list->CommitVersion(trx_id, commit_time);
        }
    }
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::Abort(const PidType& pid, const uint64_t& trx_id) {
    PropertyRow* current_row = head_;

    for (int i = 0; i < property_count_; i++) {
        int element_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        auto& element_ref = current_row->elements_[element_id_in_row];

        if (element_ref.pid == pid) {
            auto header_to_free = element_ref.mvcc_list->AbortVersion(trx_id);
            value_storage_ptr_->FreeValue(header_to_free);
        }
    }
}
