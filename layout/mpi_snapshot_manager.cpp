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

#include "layout/mpi_snapshot_manager.hpp"

void MPISnapshotManager::SetRootPath(string root_path) {
    path_ = root_path;
    status_ = Status::APPENDING_CONFIG;
}

void MPISnapshotManager::AppendConfig(string key, string value) {
    if (status_ != Status::APPENDING_CONFIG)
        return;
    config_info_map_[key] = value;
}

void MPISnapshotManager::SetComm(MPI_Comm comm) {
    int my_rank, comm_sz;
    MPI_Comm_rank(comm, &my_rank);
    MPI_Comm_size(comm, &comm_sz);
    char hostname[MPI_MAX_PROCESSOR_NAME];
    int hostname_len;
    MPI_Get_processor_name(hostname, &hostname_len);
    hostname_len++;

    int* hostname_lens = new int[comm_sz];
    int* hostname_offs = new int[comm_sz];

    MPI_Allgather(&hostname_len, 1, MPI_INT, hostname_lens, 1, MPI_INT, comm);

    int total_len = hostname_lens[0];
    hostname_offs[0] = 0;

    for (int i = 1; i < comm_sz; i++) {
        hostname_offs[i] = total_len;
        total_len += hostname_lens[i];
    }

    char* all_hostnames = new char[total_len + 1];
    MPI_Allgatherv(hostname, hostname_len, MPI_CHAR, all_hostnames, hostname_lens, hostname_offs, MPI_CHAR, comm);
    all_hostnames[total_len] = 0;
    for (int i = 1; i < comm_sz; i++) {
        all_hostnames[hostname_offs[i] - 1] = ' ';
    }

    AppendConfig("comm_hosts", string(all_hostnames));
    AppendConfig("comm_size", to_string(comm_sz));
    AppendConfig("comm_my_hostname", hostname);
    AppendConfig("comm_my_rank", to_string(my_rank));

    delete hostname_lens;
    delete hostname_offs;
    delete all_hostnames;
}

void MPISnapshotManager::ConfirmConfig() {
    // generate config content
    string config_content;
    for (auto kv : config_info_map_) {
        config_content += "[" + kv.first + " : " + kv.second + "]\n";
    }

    // generate snapshot directory
    string folder_name = to_string(hash<string>()(config_content));
    path_ = path_ + "/" + folder_name + "/";
    string cmd = "mkdir -p " + path_;
    system(cmd.c_str());

    string info_path = path_ + "snapshot_info.log";
    FILE* info_file = fopen(info_path.c_str(), "w");
    if (info_file) {
        fprintf(info_file, config_content.c_str());
        fclose(info_file);
    }

    status_ = Status::CONFIG_CONFIRMED;
}
