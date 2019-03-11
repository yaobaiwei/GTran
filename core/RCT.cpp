/* Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#include "core/RCT.hpp"

RCTable * RCTable::rct_ = nullptr;

RCTable::RCTable() {
     /* Init b+tree_ */
    bpt_config_.order = 7;
    bpt_config_.entries = 10;

    tree_ = bplus_tree_init(bpt_config_.order, bpt_config_.entries);

    if (tree_ == nullptr) {
        std::cerr << "RCT Error: Init Failure!" << std::endl;
        CHECK(false);
    }
}

int RCTable::trxid2int(uint64_t trx_id) {
    return (trx_id << 1) >> 1;
}

uint64_t RCTable::int2trxid(int trx_id) {
    return trx_id | (1 << 63);
}

int RCTable::ct2int(uint64_t ct) {
    return (int) ct;
}

bool RCTable::insertTransaction(uint64_t ct, uint64_t trx_id) {
    CHECK(is_valid_trx_id(trx_id));
    CHECK(is_valid_time(ct));

    int inner_trx_id = trxid2int(trx_id);
    int inner_ct = ct2int(ct);
    DLOG(INFO) << "[RCT] insertTransaction: " << inner_ct << " trx_id " << inner_trx_id << " inserted" << endl;

    bplus_tree_put(tree_, inner_ct, inner_trx_id);
    return true;
}

bool RCTable::queryTransactions(uint64_t bt, uint64_t ct, std::set<uint64_t>& trx_ids) {
    CHECK(is_valid_time(ct));
    CHECK(is_valid_time(bt));

    for (uint64_t n = bt; n <= ct; n++) {
        int trx_id = bplus_tree_get(tree_, n);
        if (trx_id > 0)
            trx_ids.insert(int2trxid(trx_id));
    }
    return true;
}

bool RCTable::delete_single_transaction(uint64_t ct) {
    CHECK(is_valid_time(ct));

    bplus_tree_put(tree_, ct, 0);
    return true;
}

