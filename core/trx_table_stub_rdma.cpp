/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#include "core/trx_table_stub_rdma.hpp"

RDMATrxTableStub * RDMATrxTableStub::instance_ = nullptr;

bool RDMATrxTableStub::update_status(uint64_t trx_id, TRX_STAT new_status, bool is_read_only) {
    ibinstream in;
    int status_i = int(new_status);
    in << (int)(NOTIFICATION_TYPE::UPDATE_STATUS) << node_.get_local_rank() << trx_id << status_i << is_read_only;

    unique_lock<mutex> lk(update_mutex_);
    mailbox_ ->SendNotification(config_->global_num_workers, in);

    // // TMP: also send to worker
    int worker_id = coordinator_->GetWorkerFromTrxID(trx_id);
    // should accept notification of self
    mailbox_ ->SendNotification(worker_id, in);

    return true;
}

bool RDMATrxTableStub::read_status(uint64_t trx_id, TRX_STAT &status) {
    CHECK(IS_VALID_TRX_ID(trx_id));

    int t_id = TidMapper::GetInstance()->GetTid();
    uint64_t bucket_id = trx_id % trx_num_main_buckets_;
    DLOG(INFO) << "[RDMATrxTableStub] read_status: t_id = " << t_id << "; bucket_id = " << bucket_id;

    while (true) {
        char * send_buffer = buf_->GetSendBuf(t_id);
        uint64_t off = bucket_id * ASSOCIATIVITY_ * sizeof(TidStatus);
        uint64_t sz = ASSOCIATIVITY_ * sizeof(TidStatus);

        RDMA &rdma = RDMA::get_rdma();

        int worker_id = coordinator_->GetWorkerFromTrxID(trx_id);
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

    int t_id = TidMapper::GetInstance()->GetTid();
    uint64_t bucket_id = trx_id % trx_num_main_buckets_;
    DLOG(INFO) << "[RDMATrxTableStub] read_status: t_id = " << t_id << "; bucket_id = " << bucket_id;

    while (true) {
        char * send_buffer = buf_->GetSendBuf(t_id);
        uint64_t off = bucket_id * ASSOCIATIVITY_ * sizeof(TidStatus);
        uint64_t sz = ASSOCIATIVITY_ * sizeof(TidStatus);

        RDMA &rdma = RDMA::get_rdma();

        int worker_id = coordinator_->GetWorkerFromTrxID(trx_id);
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

uint64_t RDMATrxTableStub::read_min_bt() {
    uint64_t ret;
    while (true) {
        char* min_bt_recv_buffer = buf_->GetMinBTBuf();
        RDMA &rdma = RDMA::get_rdma();

        int master_id = config_->global_num_workers;
        int off = config_->trx_table_sz;
        int t_id = config_->global_num_threads + 1;

        rdma.dev->RdmaRead(t_id, master_id, min_bt_recv_buffer, 64, off);

        bool ok = ((MinBTCLine*)min_bt_recv_buffer)->GetMinBT(ret);
        if (ok)
            break;
    }

    return ret;
}
