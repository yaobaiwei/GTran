/**
 * Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 *          Modified by Changji LI (cjli@cse.cuhk.edu.hk)
 * 
 * This file defines part of common classes of master and workers, including the format  * of communication messages, and interfaces
 */

#pragma once

#include <stdint.h>
#include "base/type.hpp"
#include "glog/logging.h"

#define IS_VALID_TRX_ID(trx_id) (trx_id & 0x8000000000000000)

/*
 * trx_id : status : if empty
 * Item type in the table
 * possible state transition: 
 * 1. enter P
 * 2. P->V
 * 3. V->A
 * 4. V->C
 * */
struct TidStatus {
    uint64_t trx_id : 64;
    uint8_t P : 1;
    uint8_t V : 1;
    uint8_t C : 1;
    uint8_t A : 1;
    uint8_t occupied : 1;
    uint64_t ct;  // Commit Time

    // enter P
    void enterProcessState(uint64_t trx_id) {
        CHECK(trx_id != 0 && P == 0 && V == 0 && C == 0 && A == 0 && occupied == 0);
        this->trx_id = trx_id;
        this->P = 1;
        this->occupied = 1;
    }

    // P->V
    void enterValidationState() {
        CHECK(P == 1 && V == 0 && C == 0 && A == 0 && occupied == 1);
        this -> V = 1;
    }

    // V->A
    void enterAbortState() {
        CHECK(P == 1 && C == 0 && occupied == 1);
        this -> A = 1;
    }

    // V->C
    void enterCommitState() {
        CHECK(P == 1 && V == 1 && C == 0 && A == 0 && occupied == 1);
        this -> C = 1;
    }

    void enterCommitTime(uint64_t ct_) {
        CHECK(P == 1 && V == 0 && C == 0 && A == 0 && occupied == 1);
        this->ct = ct_;
    }

    TRX_STAT getState() {
        CHECK(!(A == 1 && C == 1));
        if (A == 1) return TRX_STAT::ABORT;
        if (C == 1) return TRX_STAT::COMMITTED;
        if (V == 1) return TRX_STAT::VALIDATING;
        if (P == 1) return TRX_STAT::PROCESSING;
    }

    uint64_t getCT() {
        CHECK(P == 1 && V == 1 && occupied == 1);
        return ct;
    }

    bool isEmpty() {
        return (occupied == 0) ? true : false;
    }

    bool setEmpty() {
        P = 0; V = 0; A = 0; C = 0;
        occupied = 0;
        return true;
    }

    string DebugString() {
        std::stringstream ss;

        ss << "trx_id=" << trx_id
            << "; P=" << std::to_string(P)
            << "; V=" << std::to_string(V)
            << "; C=" << std::to_string(C)
            << "; A=" << std::to_string(A)
            << "; occupied=" << std::to_string(occupied)
            << "; commit_time=" << ct;

        return ss.str();
    }
} __attribute__((packed));

struct UpdateTrxStatusReq{
    int n_id;
    uint64_t trx_id;
    TRX_STAT new_status;
    bool is_read_only;
    uint64_t ct;
};

struct ReadTrxStatusReq{
    int n_id;
    int t_id;
    uint64_t trx_id;
    bool read_ct;

    string DebugString(){
        std::stringstream ss;
        ss << "trx_id: " << trx_id << "; ";
        ss << "n_id: " << n_id << "; ";
        ss << "t_id: " << t_id << "; Da";
        ss << "t_id: " << read_ct << "\n";
        return ss.str();
    }
};
