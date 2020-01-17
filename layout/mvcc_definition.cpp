/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdio>

#include "layout/mvcc_definition.hpp"
#include "layout/property_row_list.hpp"

// note: this file is only for implementing ValueGC()

void VPropertyMVCCItem::ValueGC() {
    value_store->FreeValue(val, TidMapper::GetInstance()->GetTidUnique());
}

void EPropertyMVCCItem::ValueGC() {
    value_store->FreeValue(val, TidMapper::GetInstance()->GetTidUnique());
}

void VertexMVCCItem::ValueGC() {}

void EdgeMVCCItem::ValueGC() {
    val.ep_row_list = nullptr;
}
