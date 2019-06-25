/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)
*/

#include "layout/pmt_rct_table.hpp"

// p : Primitive
void PrimitiveRCTTable::GetRecentActionSet(Primitive_T p, const vector<uint64_t> & trxIDList,
                                           unordered_map<uint64_t, vector<rct_extract_data_t>> & trx_rct_map) {
    for (auto & trxID : trxIDList) {
        rct_const_accessor rctca;
        vector<rct_extract_data_t> rct_content;
        if (rct_map.at(p).find(rctca, trxID)) {
            for (auto & item : rctca->second) {
                uint64_t id;
                int pid;
                switch (p) {
                  case Primitive_T::IV: case Primitive_T::DV:
                    rct_content.emplace_back(make_tuple(item, -1, Element_T::VERTEX));
                    break;
                  case Primitive_T::IE: case Primitive_T::DE:
                    rct_content.emplace_back(make_tuple(item, -1, Element_T::EDGE));
                    break;
                  case Primitive_T::IVP: case Primitive_T::MVP: case Primitive_T::DVP:
                    id = (item >> PID_BITS);
                    pid = item - (id << PID_BITS);
                    rct_content.emplace_back(make_tuple(id, pid, Element_T::VERTEX));
                    break;
                  case Primitive_T::IEP: case Primitive_T::MEP: case Primitive_T::DEP:
                    id = (item >> PID_BITS);
                    pid = item - (id << PID_BITS);
                    rct_content.emplace_back(make_tuple(id, pid, Element_T::EDGE));
                    break;
                  default:
                    cout << "[Error] Unexpected Primitive Type" << endl;
                    return;
                }
            }
            trx_rct_map.emplace(trxID, move(rct_content));
        }
    }
}

// p : Primitive
void PrimitiveRCTTable::InsertRecentActionSet(Primitive_T p, uint64_t trxID, const vector<uint64_t> & data) {
    CHECK((int)p >= 0 && p < Primitive_T::COUNT);
    if (data.size() == 0) { return; }

    rct_accessor rcta;
    rct_map.at(p).insert(rcta, trxID);

    rcta->second.insert(data.begin(), data.end());
}
