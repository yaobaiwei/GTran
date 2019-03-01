/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

// this file is designed to be isolated from MPISnapshot
// the function below is to be used as a parameter of MPISnapShot::WriteData and MPISnapShot::ReadData

#include <ext/hash_map>

#include <hash_map>
#include <tuple>
#include <string>
#include <vector>

#include "base/serialization.hpp"
#include "core/abstract_mailbox.hpp"

// namespace std is used there
using __gnu_cxx::hash_map;

template<typename T>
static inline bool WriteSerImpl(string fn, T& data) {
    ofstream out_f(fn, ios::binary);

    if (!out_f.is_open()) {
        return false;
    }

    ibinstream m;
    m << data;

    uint64_t buf_sz = m.size();
    out_f.write(reinterpret_cast<char*>(&buf_sz), sizeof(uint64_t));

    out_f.write(m.get_buf(), m.size());

    out_f.close();

    return true;
}

template<typename T>
static inline bool ReadSerImpl(string fn, T& data) {
    ifstream in_f(fn, ios::binary);

    if (!in_f.is_open()) {
        return false;
    }

    uint64_t sz;
    in_f.read(reinterpret_cast<char*>(&sz), sizeof(uint64_t));

    char* tmp_buf = new char[sz];
    in_f.read(tmp_buf, sz);
    in_f.close();

    obinstream m;
    m.assign(tmp_buf, sz, 0);

    m >> data;

    return true;
}


template<typename T1, typename T2>
static inline bool WriteHashMapSerImpl(string fn, hash_map<T1, T2*>& data) {
    ofstream out_f(fn, ios::binary);

    if (!out_f.is_open()) {
        return false;
    }

    // write data to the instream
    ibinstream m;

    for (auto kv : data) {
        // notice that kv.second is a pointer
        // and the content of the pointer need to be written
        m << kv.first;  // the key
        m << *kv.second;  // the value is a pointer
    }

    uint64_t data_sz = data.size(), buf_sz = m.size();

    // out_f << data_sz;
    // out_f << buf_sz;
    out_f.write(reinterpret_cast<char*>(&data_sz), sizeof(uint64_t));
    out_f.write(reinterpret_cast<char*>(&buf_sz), sizeof(uint64_t));
    out_f.write(m.get_buf(), m.size());

    out_f.close();

    return true;
}

template<typename T1, typename T2>
static inline bool ReadHashMapSerImpl(string fn, hash_map<T1, T2*>& data) {
    ifstream in_f(fn, ios::binary);

    if (!in_f.is_open()) {
        return false;
    }

    uint64_t buf_sz, data_sz;
    // in_f >> data_sz;
    // in_f >> buf_sz;
    in_f.read(reinterpret_cast<char*>(&data_sz), sizeof(uint64_t));
    in_f.read(reinterpret_cast<char*>(&buf_sz), sizeof(uint64_t));


    char* tmp_buf = new char[buf_sz];
    in_f.read(tmp_buf, buf_sz);
    in_f.close();

    obinstream m;
    m.assign(tmp_buf, buf_sz, 0);
    // do not free

    for (uint64_t i = 0; i < data_sz; i++) {
        T1 key;
        T2* value = new T2;

        m >> key >> *value;

        data[key] = value;
    }

    return true;
}

static inline bool WriteKVStoreImpl(string fn, tuple<uint64_t, uint64_t, char*>& data) {
    ofstream out_f(fn, ios::binary);

    if (!out_f.is_open()) {
        return false;
    }

    uint64_t last_entry = get<0>(data), mem_sz = get<1>(data);
    char* mem = get<2>(data);


    out_f.write(reinterpret_cast<char*>(&last_entry), sizeof(uint64_t));
    out_f.write(reinterpret_cast<char*>(&mem_sz), sizeof(uint64_t));
    out_f.write(mem, mem_sz);

    out_f.close();

    return true;
}

static inline bool ReadKVStoreImpl(string fn, tuple<uint64_t, uint64_t, char*>& data) {
    ifstream in_f(fn, ios::binary);

    if (!in_f.is_open()) {
        return false;
    }

    char* mem = get<2>(data);

    uint64_t last_entry, mem_sz;

    in_f.read(reinterpret_cast<char*>(&last_entry), sizeof(uint64_t));
    in_f.read(reinterpret_cast<char*>(&mem_sz), sizeof(uint64_t));

    in_f.read(mem, mem_sz);

    in_f.close();

    data = make_tuple(last_entry, mem_sz, mem);

    return true;
}

// for initMsg
// struct mailbox_data_t {
//     ibinstream stream;
//     int dst_nid;
//     int dst_tid;
// };
static inline bool WriteMailboxDataImpl(string fn, vector<AbstractMailbox::mailbox_data_t>& data) {
    ofstream out_f(fn, ios::binary);

    if (!out_f.is_open()) {
        return false;
    }

    // write data to the instream

    uint64_t vec_len = data.size();
    out_f.write(reinterpret_cast<char*>(&vec_len), sizeof(uint64_t));

    for (auto data_val : data) {
        ibinstream& m = data_val.stream;

        uint64_t buf_sz = m.size();

        out_f.write(reinterpret_cast<char*>(&buf_sz), sizeof(uint64_t));
        out_f.write(m.get_buf(), m.size());
        out_f.write(reinterpret_cast<char*>(&data_val.dst_nid), sizeof(int));
        out_f.write(reinterpret_cast<char*>(&data_val.dst_tid), sizeof(int));
    }

    out_f.close();

    return true;
}

static inline bool ReadMailboxDataImpl(string fn, vector<AbstractMailbox::mailbox_data_t>& data) {
    ifstream in_f(fn, ios::binary);

    if (!in_f.is_open()) {
        return false;
    }

    // write data to the instream

    uint64_t vec_len;

    in_f.read(reinterpret_cast<char*>(&vec_len), sizeof(uint64_t));
    data.resize(vec_len);

    for (int i = 0; i < vec_len; i++) {
        auto& data_val = data[i];
        ibinstream& m = data_val.stream;

        uint64_t buf_sz = m.size();

        in_f.read(reinterpret_cast<char*>(&buf_sz), sizeof(uint64_t));

        char* tmp_buf = new char[buf_sz];
        in_f.read(tmp_buf, buf_sz);
        m.raw_bytes(tmp_buf, buf_sz);
        delete[] tmp_buf;

        in_f.read(reinterpret_cast<char*>(&data_val.dst_nid), sizeof(int));
        in_f.read(reinterpret_cast<char*>(&data_val.dst_tid), sizeof(int));
    }

    in_f.close();

    return true;
}
