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

    /* MPISnapshotManager has three status:
     *   1. ROOT_NOT_SET: Before setting the root path
     *   2. APPENDING_CONFIG: Adding config to the config_info_map_
     *   3. CONFIG_CONFIRMED: The config is fixed. According to the config,
     *                        the folder name is generated.
     *
     *       SetRootPath       ConfirmConfig
     *   1 ---------------> 2 ---------------> 3
     */
    enum Status{
        ROOT_NOT_SET,
        APPENDING_CONFIG,
        CONFIG_CONFIRMED
    };

    Status status_ = ROOT_NOT_SET;

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
    void SetComm(MPI_Comm comm);  // optional; calls AppendConfig
    void ConfirmConfig();

    // use this if you want to overwrite snapshot (e.g., the file is damaged.)
    bool EnableRead(bool enabled) {read_enabled_ = enabled;}
    // you can use this if you are test on a tiny dataset to avoid write snapshot
    bool EnableWrite(bool enabled) {write_enabled_ = enabled;}

    // test if a snapshoted data has been read / written
    bool TestRead(string key);
    bool TestWrite(string key);

    /* Since MPISnapshotManager is designed to manage where to read and write the snapshot,
     * the function of serializing and deserializing a specific type of data need to be
     * passed to WriteData and ReadData via parameter.
     */
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
