/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "layout/mpi_snapshot_manager.hpp"

using std::to_string;

bool MPISnapshotManager::TestRead(string key) {
    if (read_map_.count(key) == 0)
        return false;  // not found
    return read_map_[key];
}

bool MPISnapshotManager::TestWrite(string key) {
    if (write_map_.count(key) == 0)
        return false;  // not found
    return write_map_[key];
}

void MPISnapshotManager::SetRootPath(string root_path) {
    path_ = root_path;
    status_ = APPENDING_CONFIG;
}

void MPISnapshotManager::AppendConfig(string key, string str_val) {
    if (!status_ == APPENDING_CONFIG)
        return;
    config_info_map_[key] = str_val;
}

// optional
void MPISnapshotManager::SetComm(MPI_Comm comm) {
    int my_rank, comm_sz;
    MPI_Comm_rank(comm, &my_rank);
    MPI_Comm_size(comm, &comm_sz);
    char tmp_hn[MPI_MAX_PROCESSOR_NAME];
    int hn_len;
    MPI_Get_processor_name(tmp_hn, &hn_len);
    hn_len++;

    int* hn_lens = new int[comm_sz];
    int* hn_displs = new int[comm_sz];

    MPI_Allgather(&hn_len, 1, MPI_INT, hn_lens, 1, MPI_INT, comm);

    int total_len = hn_lens[0];
    hn_displs[0] = 0;

    for (int i = 1; i < comm_sz; i++) {
        hn_displs[i] = total_len;
        total_len += hn_lens[i];
    }

    char* tmp_hn_cat = new char[total_len + 1];
    MPI_Allgatherv(tmp_hn, hn_len, MPI_CHAR, tmp_hn_cat, hn_lens, hn_displs, MPI_CHAR, comm);
    tmp_hn_cat[total_len] = 0;
    for (int i = 1; i < comm_sz; i++) {
        tmp_hn_cat[hn_displs[i] - 1] = ' ';
    }

    AppendConfig("comm_hosts", string(tmp_hn_cat));
    AppendConfig("comm_size", to_string(comm_sz));
    AppendConfig("comm_my_hostname", tmp_hn);
    AppendConfig("comm_my_rank", to_string(my_rank));

    delete hn_lens;
    delete hn_displs;
    delete tmp_hn_cat;
}

void MPISnapshotManager::ConfirmConfig() {
    // generate config content
    string config_content;
    for (auto kv : config_info_map_) {
        config_content += "[" + kv.first + " : " + kv.second + "]\n";
    }

    // generate final directory
    hash<string> str_hash;
    string folder_name = to_string(str_hash(config_content));
    path_ = path_ + "/" + folder_name + "/";
    string cmd = "mkdir -p " + path_;
    system(cmd.c_str());

    // write snapshot_info.md (consists of all configurations of the MPISnapshotManager)
    string info_path = path_ + "snapshot_info.md";
    FILE* info_file = fopen(info_path.c_str(), "w");
    if (info_file) {
        fprintf(info_file, config_content.c_str());
        fclose(info_file);
    }

    status_ = CONFIG_CONFIRMED;
}
