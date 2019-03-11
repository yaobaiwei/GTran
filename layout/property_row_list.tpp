/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

template <class PropertyRow>
void PropertyRowList<PropertyRow>::InsertElement(const PidType& pid, const value_t& value) {
    int element_id = property_count_++;
    int element_id_in_row = element_id % PropertyRow::RowItemCount();

    int next_count = (element_id - 1) / PropertyRow::RowItemCount();

    PropertyRow* my_row = head_;

    for (int i = 0; i < next_count; i++) {
        my_row = my_row->next_;
    }

    if (element_id > 0 && element_id % PropertyRow::RowItemCount() == 0) {
        my_row->next_ = pool_ptr_->Get();
        my_row = my_row->next_;
        my_row->next_ = nullptr;
    }

    MVCCList<PropertyMVCC>* mvcc_list = new MVCCList<PropertyMVCC>;

    mvcc_list->AppendVersion(kvs_ptr_->Insert(MVCCHeader(0, pid.value()), value), 0, 0);

    my_row->elements_[element_id_in_row].pid = pid;
    my_row->elements_[element_id_in_row].mvcc_list = mvcc_list;
}

template <class PropertyRow>
void PropertyRowList<PropertyRow>::ReadProperty(const PidType& pid, const uint64_t& trx_id, const uint64_t& begin_time, value_t& ret) {

    PropertyRow* current_row = head_;

    for (int i = 0; i < property_count_; i++) {
        int element_id_in_row = i % PropertyRow::RowItemCount();
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }
        if (current_row->elements_[element_id_in_row].pid == pid) {
            kvs_ptr_->Get(current_row->elements_[element_id_in_row].
                          mvcc_list->GetCurrentVersion(trx_id, begin_time)->GetValue(), ret);
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

        value_t v;
        label_t label = current_row->elements_[element_id_in_row].pid.pid;
        kvs_ptr_->Get(current_row->elements_[element_id_in_row].
                      mvcc_list->GetCurrentVersion(trx_id, begin_time)->GetValue(), v);
        ret.emplace_back(make_pair(label, v));
    }
}
