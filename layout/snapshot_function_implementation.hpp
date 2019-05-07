/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <ext/hash_map>
#include <hash_map>
#include <string>
#include <tuple>
#include <vector>

#include "base/serialization.hpp"
#include "core/abstract_mailbox.hpp"
#include "layout/layout_type.hpp"

/* In MPISnapshotManager:
 *      template<class T>
 *      bool ReadData(string key, T& data, void(ReadFunction)(ifstream&, T&), bool data_const = true);
 *      template<class T>
 *      bool WriteData(string key, T& data, void(WriteFunction)(ofstream&, T&));
 * This .hpp file is dedicated to implement ReadFunction and WriteFunction.
 */

template<typename T>
static inline void WriteBySerialization(ofstream& out_f, T& data) {
    ibinstream m;
    m << data;

    uint64_t buf_sz = m.size();
    out_f.write(reinterpret_cast<char*>(&buf_sz), sizeof(uint64_t));

    out_f.write(m.get_buf(), m.size());
}

template<typename T>
static inline void ReadBySerialization(ifstream& in_f, T& data) {
    uint64_t sz;
    in_f.read(reinterpret_cast<char*>(&sz), sizeof(uint64_t));

    char* tmp_buf = new char[sz];
    in_f.read(tmp_buf, sz);
    in_f.close();

    obinstream m;
    m.assign(tmp_buf, sz, 0);

    m >> data;
}
