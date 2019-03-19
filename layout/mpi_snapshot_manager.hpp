/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <mpi.h>
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <map>
#include <string>
#include <vector>

using std::string;
using std::hash;
using std::map;
using std::ifstream;
using std::ofstream;
using std::ios;

class MPISnapshotManager {
 private:
    string path_;

    map<string, bool> read_map_;
    map<string, bool> write_map_;

    map<string, string> config_info_map_;

    bool read_enabled_ = true;
    bool write_enabled_ = true;

    MPISnapshotManager() {}

    enum Status{
        ROOT_NOT_SET,
        APPENDING_CONFIG,
        CONFIG_CONFIRMED
    };

    Status status_ = ROOT_NOT_SET;

 public:
    static MPISnapshotManager* GetInstance() {
        static MPISnapshotManager snapshot_manager_single_instance;
        return &snapshot_manager_single_instance;
    }

    void SetRootPath(string root_path);
    void AppendConfig(string key, string str_val);
    void SetComm(MPI_Comm comm);  // optional; calls AppendConfig
    void ConfirmConfig();

    // use this if you want to overwrite snapshot (e.g., the file is damaged.)
    bool EnableRead(bool enabled) {read_enabled_ = enabled;}
    // you can use this if you are test on a tiny dataset to avoid write snapshot
    bool EnableWrite(bool enabled) {write_enabled_ = enabled;}

    bool TestRead(string key);
    bool TestWrite(string key);
    template<class T>
    bool ReadData(string key, T& data, void(ReadFunction)(ifstream&, T&), bool data_const = true);
    template<class T>
    bool WriteData(string key, T& data, void(WriteFunction)(ofstream&, T&));
};

template<class T>
bool MPISnapshotManager::ReadData(string key, T& data, void(ReadFunction)(ifstream&, T&), bool data_const) {
    if (!read_enabled_ || status_ != CONFIG_CONFIRMED) return false;
    if (TestRead(key)) return true;
    ifstream in_f(path_ + key, ios::binary);
    if (!in_f.is_open()) return false;
    ReadFunction(in_f, data);
    in_f.close();
    read_map_[key] = true;
    if (data_const) write_map_[key] = read_map_[key];
    return read_map_[key];
}

template<class T>
bool MPISnapshotManager::WriteData(string key, T& data, void(WriteFunction)(ofstream&, T&)) {
    if (!write_enabled_ || status_ != CONFIG_CONFIRMED) return false;
    if (TestWrite(key)) return true;
    ofstream out_f(path_ + key, ios::binary);
    if (!out_f.is_open()) return false;
    WriteFunction(out_f, data);
    out_f.close();
    write_map_[key] = true;
    return write_map_[key];
}
