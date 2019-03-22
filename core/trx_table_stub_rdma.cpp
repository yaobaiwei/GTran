/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#include "core/trx_table_stub_rdma.hpp"

RDMATrxTableStub * RDMATrxTableStub::instance_ = nullptr;

bool RDMATrxTableStub::update_status(uint64_t trx_id, TRX_STAT new_status, std::vector<uint64_t> * trx_ids = nullptr) {
    CHECK((new_status == TRX_STAT::VALIDATING && trx_ids != nullptr) || (new_status != TRX_STAT::VALIDATING && trx_ids == nullptr)) << "[RDMATrxTableStub] update_status: new_status should correspond to trx_ids";

    ibinstream in;
    int status_i = int(new_status);
    in << node_.get_local_rank() << trx_id << status_i;
    mailbox_ ->Send_Notify(config_->global_num_workers, in);

    if (new_status == TRX_STAT::VALIDATING) {
        obinstream out;
        mailbox_ -> Recv_Notify(out);
        out >> *trx_ids;
    }
    return true;
}

bool RDMATrxTableStub::read_status(uint64_t trx_id, TRX_STAT &status) {
    CHECK(is_valid_trx_id(trx_id));

    int t_id = TidMapper::GetInstance()->GetTid();
    uint64_t bucket_id = trx_id % trx_num_main_buckets_;
    DLOG(INFO) << "[RDMATrxTableStub] read_status： t_id = " << t_id << "; bucket_id = " << bucket_id;

    while (true) {
        char * send_buffer = buf_->GetSendBuf(t_id);
        uint64_t off = bucket_id * ASSOCIATIVITY_ * sizeof(TidStatus);
        uint64_t sz = ASSOCIATIVITY_ * sizeof(TidStatus);

        RDMA &rdma = RDMA::get_rdma();

        int master_id = config_->global_num_workers;
        
        rdma.dev->RdmaRead(t_id, master_id, send_buffer, sz, off);

        TidStatus *trx_status = (TidStatus *)(send_buffer);

        for (int i = 0; i < ASSOCIATIVITY_; ++i) {
            if (i < ASSOCIATIVITY_ - 1) {
                if (trx_status[i].trx_id == trx_id) {
                    status = trx_status[i].getState();
                    return true;
                }
            }
            else {
                if (trx_status[i].isEmpty()) {
                    return false;
                }
                else {
                    bucket_id = trx_status[i].trx_id;
                    break;
                }
            }
        }
    }
}