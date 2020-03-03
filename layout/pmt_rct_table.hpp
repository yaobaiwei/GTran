/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#pragma once

#include <ext/hash_map>
#include <ext/hash_set>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_unordered_set.h>

#include <set>
#include <unordered_map>
#include <vector>

#include "base/type.hpp"
#include "glog/logging.h"

class PrimitiveRCTTable {
 public:
    PrimitiveRCTTable() {}

    void Init() {
        for (int p = 0; p < static_cast<int>(Primitive_T::COUNT); p++) {
            rct_map.emplace((Primitive_T) p, rct_type());
        }
    }

    // Validation : Get RCT data
    void GetRecentActionSet(Primitive_T p, const vector<uint64_t> & trxIDList,
                            unordered_map<uint64_t, vector<rct_extract_data_t>> & trx_rct_map);
    void InsertRecentActionSet(Primitive_T p, uint64_t trxID, const vector<uint64_t> & data);
    void EraseRecentActionSet(const vector<uint64_t>& trx_ids);

    static PrimitiveRCTTable* GetInstance() {
        static PrimitiveRCTTable* pmt_rct_table_ptr = nullptr;

        if (pmt_rct_table_ptr == nullptr) {
            pmt_rct_table_ptr = new PrimitiveRCTTable();
        }

        return pmt_rct_table_ptr;
    }

 private:
    // Insert V/E, Delete V/E (4 tables)
    // Insert/Modify/Delete VP/EP (6 tables)
    // TrxID --> ObjectList (impl by tbb::concurrent_unordered_set<uint64_t>)
    //     Insert -> multi-thread
    //     Read -> single-thread
    typedef tbb::concurrent_hash_map<uint64_t, tbb::concurrent_unordered_set<uint64_t>> rct_type;
    typedef rct_type::accessor rct_accessor;
    typedef rct_type::const_accessor rct_const_accessor;

    // Primitive_T -> rct
    unordered_map<Primitive_T, rct_type, PrimitiveEnumClassHash> rct_map;
};
