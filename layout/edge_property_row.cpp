/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include "layout/edge_property_row.hpp"

extern OffsetConcurrentMemPool<EdgePropertyRow>* global_vp_row_pool;
extern OffsetConcurrentMemPool<PropertyMVCC>* global_property_mvcc_pool;
extern MVCCKVStore* global_ep_store;

void EdgePropertyRow::InsertElement(const epid_t& pid, const value_t& value) {
    int element_id = property_count_++;
    int element_id_in_row = element_id % EP_ROW_ITEM_COUNT;

    int next_count = (element_id - 1) / EP_ROW_ITEM_COUNT;

    EdgePropertyRow* my_row = this;

    for (int i = 0; i < next_count; i++) {
        my_row = my_row->next_;
    }

    if (element_id > 0 && element_id % EP_ROW_ITEM_COUNT == 0) {
        my_row->next_ = global_vp_row_pool->Get();
        my_row = my_row->next_;
        my_row->next_ = nullptr;
    }

    MVCCList<PropertyMVCC>* mvcc_list = new MVCCList<PropertyMVCC>;

    mvcc_list->AppendVersion(global_ep_store->Insert(MVCCHeader(0, pid.value()), value), 0, 0);

    my_row->elements_[element_id_in_row].pid = pid;
    my_row->elements_[element_id_in_row].mvcc_list = mvcc_list;
}

value_t EdgePropertyRow::ReadProperty(epid_t pid, const uint64_t& trx_id, const uint64_t& begin_time) {
    value_t ret;

    EdgePropertyRow* current_row = this;

    for (int i = 0; i < property_count_; i++) {
        int element_id_in_row = i % EP_ROW_ITEM_COUNT;
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }
        if (current_row->elements_[element_id_in_row].pid == pid) {
            global_ep_store->Get(current_row->elements_[element_id_in_row].
                                 mvcc_list->GetCurrentVersion(trx_id, begin_time)->GetValue(), ret);
            break;
        }
    }

    return ret;
}

vector<pair<label_t, value_t>> EdgePropertyRow::ReadAllProperty(const uint64_t& trx_id, const uint64_t& begin_time) {
    vector<pair<label_t, value_t>> ret;

    EdgePropertyRow* current_row = this;

    for (int i = 0; i < property_count_; i++) {
        int element_id_in_row = i % EP_ROW_ITEM_COUNT;
        if (i > 0 && element_id_in_row == 0) {
            current_row = current_row->next_;
        }

        value_t v;
        label_t label = current_row->elements_[element_id_in_row].pid.pid;
        global_ep_store->Get(current_row->elements_[element_id_in_row].
                             mvcc_list->GetCurrentVersion(trx_id, begin_time)->GetValue(), v);
        ret.push_back(make_pair(label, v));
    }

    return ret;
}
