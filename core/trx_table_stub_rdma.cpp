/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#include "core/trx_table_stub_rdma.hpp"

RDMATrxTableStub * RDMATrxTableStub::instance_ = nullptr;

bool RDMATrxTableStub::update_status(uint64_t trx_id, TRX_STAT new_status, bool is_read_only) {
    CHECK(new_status != TRX_STAT::VALIDATING);

    int worker_id = coordinator_->GetWorkerFromTrxID(trx_id);

    if (worker_id == node_.get_local_rank()) {
        // directly append the request to the local queue
        UpdateTrxStatusReq req{node_.get_local_rank(), trx_id, new_status, is_read_only};
        pending_trx_updates_->Push(req);
    } else {
        // send the request to remote worker
        ibinstream in;
        int status_i = int(new_status);
        in << (int)(NOTIFICATION_TYPE::UPDATE_STATUS) << node_.get_local_rank() << trx_id << status_i << is_read_only;

        mailbox_ ->SendNotification(worker_id, in);
    }

    return true;
}

bool RDMATrxTableStub::read_status(uint64_t trx_id, TRX_STAT &status) {
    CHECK(IS_VALID_TRX_ID(trx_id));
    int worker_id = coordinator_->GetWorkerFromTrxID(trx_id);

    if (worker_id == node_.get_local_rank()) {
        return trx_table_->query_status(trx_id, status);
    }

    int t_id = TidMapper::GetInstance()->GetTid();
    uint64_t bucket_id = trx_id % trx_num_main_buckets_;
    DLOG(INFO) << "[RDMATrxTableStub] read_status: t_id = " << t_id << "; bucket_id = " << bucket_id;

    while (true) {
        char * send_buffer = buf_->GetSendBuf(t_id);
        uint64_t off = bucket_id * ASSOCIATIVITY_ * sizeof(TidStatus);
        uint64_t sz = ASSOCIATIVITY_ * sizeof(TidStatus);

        RDMA &rdma = RDMA::get_rdma();

        off = bucket_id * ASSOCIATIVITY_ * sizeof(TidStatus) + config_->trx_table_offset;
        rdma.dev->RdmaRead(t_id, worker_id, send_buffer, sz, off);

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

bool RDMATrxTableStub::read_ct(uint64_t trx_id, TRX_STAT & status, uint64_t & ct) {
    CHECK(IS_VALID_TRX_ID(trx_id));
    int worker_id = coordinator_->GetWorkerFromTrxID(trx_id);

    if (worker_id == node_.get_local_rank()) {
        bool query_status_ret = trx_table_->query_status(trx_id, status);
        bool query_ct_ret = trx_table_->query_ct(trx_id, ct);
        return query_status_ret && query_ct_ret;
    }

    int t_id = TidMapper::GetInstance()->GetTid();
    uint64_t bucket_id = trx_id % trx_num_main_buckets_;
    DLOG(INFO) << "[RDMATrxTableStub] read_status: t_id = " << t_id << "; bucket_id = " << bucket_id;

    while (true) {
        char * send_buffer = buf_->GetSendBuf(t_id);
        uint64_t off = bucket_id * ASSOCIATIVITY_ * sizeof(TidStatus);
        uint64_t sz = ASSOCIATIVITY_ * sizeof(TidStatus);

        RDMA &rdma = RDMA::get_rdma();

        off = bucket_id * ASSOCIATIVITY_ * sizeof(TidStatus) + config_->trx_table_offset;
        rdma.dev->RdmaRead(t_id, worker_id, send_buffer, sz, off);

        TidStatus *trx_status = (TidStatus *)(send_buffer);

        for (int i = 0; i < ASSOCIATIVITY_; ++i) {
            if (i < ASSOCIATIVITY_ - 1) {
                if (trx_status[i].trx_id == trx_id) {
                    status = trx_status[i].getState();
                    // Only get CT when trx is commited or validating
                    if (status == TRX_STAT::COMMITTED || status == TRX_STAT::VALIDATING) {
                        ct = trx_status[i].getCT();
                    } else {
                        ct = 0;
                    }
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
