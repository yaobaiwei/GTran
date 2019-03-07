/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#include "layout/pmt_rct_table.hpp"

// p : Primitive
void PrimitiveRCTTable::GetRecentActionSet(Primitive_T p, const vector<uint64_t> & trxIDList, unordered_map<uint64_t, vector<uint64_t>> & trx_rct_map) {
    for (auto & trxID : trxIDList) {
        rct_const_accessor rctca;
        vector<uint64_t> rct_content;
        if (rct_map.at(p).find(rctca, trxID)) {
            for (auto & item : rctca->second) {
                rct_content.emplace_back(item);
            }
            trx_rct_map.emplace(trxID, rct_content);
        }
    }
}

// p : Primitive
void PrimitiveRCTTable::InsertRecentActionSet(Primitive_T p, uint64_t trxID, const vector<uint64_t> & data) {
    rct_accessor rcta;
    rct_map.at(p).insert(rcta, trxID);

    for (auto & val : data) {
        rcta->second.emplace_back(val);
    }
}
