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

#include <vector>
#include "base/type.hpp"
#include "glog/logging.h"
#include "utils/tool.hpp"

bool operator ==(const value_t& v1, const value_t& v2);

bool operator !=(const value_t& v1, const value_t& v2);

bool operator <(const value_t& v1, const value_t& v2);

bool operator >(const value_t& v1, const value_t& v2);

bool operator <=(const value_t& v1, const value_t& v2);

bool operator >=(const value_t& v1, const value_t& v2);

struct PredicateValue {
    Predicate_T pred_type;
    vector<value_t> values;

    PredicateValue(Predicate_T _pred_type, vector<value_t> _values) : pred_type(_pred_type), values(_values) {}

    PredicateValue(Predicate_T _pred_type, value_t value) : pred_type(_pred_type) {
        Tool::value_t2vec(value, values);
    }
};

struct PredicateHistory {
    Predicate_T pred_type;
    vector<int> history_step_labels;

    PredicateHistory(Predicate_T _pred_type, vector<int> _step_labels) :
        pred_type(_pred_type), history_step_labels(_step_labels) {}
};

bool Evaluate(PredicateValue & pv, const value_t *value = NULL);
bool Evaluate(Predicate_T pred_type, value_t & val1, value_t & val2);
