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


#pragma once

#include <mpi.h>
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <set>
#include <string>
#include <vector>

#include "base/serialization.hpp"

class MPISnapshotManager {
 private:
    string path_;

    set<string> successfully_read_key_set_;

    map<string, string> config_info_map_;

    bool read_enabled_ = true;
    bool write_enabled_ = true;

    MPISnapshotManager() {}

    /* MPISnapshotManager has three status:
     *   1. ROOT_NOT_SET: Before setting the root path
     *   2. APPENDING_CONFIG: Adding configs to the config_info_map_
     *   3. CONFIG_CONFIRMED: The config is fixed. According to the config,
     *                        the folder name is generated.
     *
     *       SetRootPath       ConfirmConfig
     *   1 ---------------> 2 ---------------> 3
     */
    enum class Status {
        ROOT_NOT_SET,
        APPENDING_CONFIG,
        CONFIG_CONFIRMED
    };

    Status status_ = Status::ROOT_NOT_SET;

 public:
    static MPISnapshotManager* GetInstance() {
        static MPISnapshotManager* snapshot_manager_single_instance = nullptr;
        if (snapshot_manager_single_instance == nullptr) {
            snapshot_manager_single_instance = new MPISnapshotManager;
        }

        return snapshot_manager_single_instance;
    }

    void SetRootPath(string root_path);
    void AppendConfig(string key, string str_val);
    void SetComm(MPI_Comm comm);
    void ConfirmConfig();

    // use this if you want to overwrite snapshot (e.g., the file is damaged.)
    bool EnableRead(bool enabled) {read_enabled_ = enabled;}
    // use this if you do not want to write snapshot
    bool EnableWrite(bool enabled) {write_enabled_ = enabled;}

    template<class T>
    bool ReadData(string key, T& data);
    template<class T>
    bool WriteData(string key, T& data, bool force_write = false);  // if force_write == true, the snapshot will be written even after successfully reading
};

template<class T>
bool MPISnapshotManager::ReadData(string key, T& data) {
    if (!read_enabled_ || status_ != Status::CONFIG_CONFIRMED)
        return false;
    if (successfully_read_key_set_.count(key) > 0)
        return true;
    ifstream in_f(path_ + key, ios::binary);
    if (!in_f.is_open())
        return false;

    uint64_t sz;
    in_f.read(reinterpret_cast<char*>(&sz), sizeof(uint64_t));

    char* tmp_buf = new char[sz];
    in_f.read(tmp_buf, sz);
    in_f.close();

    obinstream m;
    m.assign(tmp_buf, sz, 0);

    m >> data;

    successfully_read_key_set_.insert(key);
    return true;
}

template<class T>
bool MPISnapshotManager::WriteData(string key, T& data, bool force_write) {
    if (!write_enabled_ || status_ != Status::CONFIG_CONFIRMED)
        return false;
    if (successfully_read_key_set_.count(key) > 0 && !force_write)
        return true;
    ofstream out_f(path_ + key, ios::binary);
    if (!out_f.is_open())
        return false;

    ibinstream m;
    m << data;

    uint64_t buf_sz = m.size();
    out_f.write(reinterpret_cast<char*>(&buf_sz), sizeof(uint64_t));
    out_f.write(m.get_buf(), m.size());
    out_f.close();

    return true;
}
