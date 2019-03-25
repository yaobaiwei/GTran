/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#include "core/trx_table_stub_zmq.hpp"
#include "core/trx_table_stub_rdma.hpp"

class TrxTableStubFactory{
 public:
    static TrxTableStub * GetTrxTableStub(){
        if(Config::GetInstance()->global_use_rdma) {
            return RDMATrxTableStub::GetInstance();
        } else {
            return TcpTrxTableStub::GetInstance();
        }
    }
};