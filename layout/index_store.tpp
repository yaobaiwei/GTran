// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
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

template <class Iterator, class T>
void build_range(map<value_t, T>& m, PredicateValue& pred, Iterator& low, Iterator& high) {
    low = m.begin();
    high = m.end();

    // get lower bound
    switch (pred.pred_type) {
      case Predicate_T::GT:
      case Predicate_T::GTE:
      case Predicate_T::INSIDE:
      case Predicate_T::BETWEEN:
        low = m.lower_bound(pred.values[0]);
    }

    // remove "EQ"
    switch (pred.pred_type) {
      case Predicate_T::GT:
      case Predicate_T::INSIDE:
        if (low != m.end() && low->first == pred.values[0]) {
            low++;
        }
    }

    int param = 1;
    // get upper_bound
    switch (pred.pred_type) {
      case Predicate_T::LT:
      case Predicate_T::LTE:
        param = 0;
      case Predicate_T::INSIDE:
      case Predicate_T::BETWEEN:
        high = m.upper_bound(pred.values[param]);
    }

    // remove "EQ"
    switch (pred.pred_type) {
      case Predicate_T::LT:
      case Predicate_T::INSIDE:
        // exclude last one if match
        if (high != low) {
            high--;
            if (high->first != pred.values[param]) {
                high++;
            }
        }
    }
}
