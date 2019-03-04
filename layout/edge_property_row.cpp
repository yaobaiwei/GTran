/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include "edge_property_row.hpp"

extern OffsetConcurrentMemPool<EdgePropertyRow>* global_vp_row_pool;
extern OffsetConcurrentMemPool<PropertyMVCC>* global_property_mvcc_pool;

void EdgePropertyRow::InsertElement(const epid_t& pid, const value_t& value)
{
    int element_id = property_count_++;
    int element_id_in_row = element_id % EP_ROW_ITEM_COUNT;

    int next_count = (element_id - 1) / EP_ROW_ITEM_COUNT;

    EdgePropertyRow* my_row = this;

    for(int i = 0; i < next_count; i++)
    {
        my_row = my_row->next_;
    }

    if(element_id > 0 && element_id % EP_ROW_ITEM_COUNT == 0)
    {
        my_row->next_ = global_vp_row_pool->Get();
        my_row = my_row->next_;
        my_row->next_ = nullptr;
    }

    PropertyMVCC* property_mvcc = global_property_mvcc_pool->Get();
    property_mvcc->begin_time = PropertyMVCC::MIN_TIME;
    property_mvcc->end_time = PropertyMVCC::MAX_TIME;
    property_mvcc->tid = PropertyMVCC::INITIAL_TID;
    property_mvcc->next = nullptr;
    property_mvcc->kv_ptr = nullptr;  //TODO(entityless): insert kvs

    my_row->elements_[element_id_in_row].pid = pid;
    my_row->elements_[element_id_in_row].mvcc_head = property_mvcc;
}
