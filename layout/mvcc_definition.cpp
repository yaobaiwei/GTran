/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdio>

#include "layout/mvcc_definition.hpp"
#include "layout/layout_type.hpp"

// note: this file is only for implementing ValueGC()

void VPropertyMVCCItem::ValueGC() {
    value_store->FreeValue(val, TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));
}

void EPropertyMVCCItem::ValueGC() {
    value_store->FreeValue(val, TidPoolManager::GetInstance()->GetTid(TID_TYPE::CONTAINER));
}

void VertexMVCCItem::ValueGC() {}

void EdgeMVCCItem::ValueGC() {
    if (val.ep_row_list != nullptr) {
        val.ep_row_list->SelfGarbageCollect();
        delete val.ep_row_list;
        val.ep_row_list = nullptr;
    }
}
