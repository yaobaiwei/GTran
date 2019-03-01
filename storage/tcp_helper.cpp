/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji Li (cjli@cse.cuhk.edu.hk)
*/

#pragma once

#include "storage/tcp_helper.hpp"

TCPHelper::TCPHelper(VKVStore * vpstore, EKVStore * epstore, bool use_rdma) :
    vpstore_(vpstore), epstore_(epstore), use_rdma_(use_rdma) {}

TCPHelper::~TCPHelper() {
    delete vpstore_;
    delete epstore_;
}

bool TCPHelper::GetPropertyForVertex(uint64_t vp_id_v, value_t & val) {
    assert(!use_rdma_);
    vpstore_->get_property_local(vp_id_v, val);

    if (val.content.size()) return true;
    return false;
}

bool TCPHelper::GetPropertyForEdge(uint64_t ep_id_v, value_t & val) {
    assert(!use_rdma_);
    epstore_->get_property_local(ep_id_v, val);

    if (val.content.size()) return true;
    return false;
}
