/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdio>

#include "layout/mvcc_definition.hpp"
#include "layout/row_definition.hpp"
#include "layout/property_row_list.hpp"

// including row_definition.hpp in mvcc_definition.hpp will cause failure

// note: this file is only for implementing InTransactionGC()

void VPropertyMVCCItem::InTransactionGC() {
    value_store->FreeValue(val, TidMapper::GetInstance()->GetTidUnique());
}

void EPropertyMVCCItem::InTransactionGC() {
    value_store->FreeValue(val, TidMapper::GetInstance()->GetTidUnique());
}

void VertexMVCCItem::InTransactionGC() {}

void EdgeMVCCItem::InTransactionGC() {
    PropertyRowList<EdgePropertyRow>* ep_row_list = val.ep_row_list;
    ep_row_list->SelfGarbageCollect();
    delete ep_row_list;
}
