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

template<class T1, class T2>
size_t MemSize(const pair<T1, T2>& p) {
    size_t s = MemSize(p.first);
    s += MemSize(p.second);
    return s;
}

template<class T>
size_t MemSize(const vector<T>& data) {
    size_t s = sizeof(size_t);
    for (auto& t : data) {
        s += MemSize(t);
    }
    return s;
}
