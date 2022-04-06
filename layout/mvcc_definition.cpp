// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
