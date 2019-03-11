/* This file defines part of common classes of master and workers, including the format  * of communication messages, and interfaces
 *
 * */
#pragma once

#include <stdint.h>
#include <base/type.hpp>
#include "glog/logging.h"


bool is_valid_trx_id(uint64_t trx_id);
bool is_valid_time(uint64_t t);

/*
 * trx_id : status : if empty
 * Item type in the table
 * possible state transition: 
 * 1. enter P
 * 2. P->V
 * 3. V->A
 * 4. V->C
 * */
struct TidStatus{
    uint64_t trx_id : 64;
    uint8_t P : 1;
    uint8_t V : 1 ;
    uint8_t C : 1 ;
    uint8_t A : 1 ;
    uint8_t occupied : 1;

    // enter P
    void enterProcessState(uint64_t trx_id){
        CHECK(trx_id != 0 && P==0 && V==0 && C==0 && A==0 && occupied==0);
        this->trx_id = trx_id;
        this->P = 1;
        this->occupied = 1;
    }
    
    // P->V
    void enterValidationState(){
        CHECK(P==1 && V==0 && C==0 && A==0 && occupied==1);
        this->V=1;
    }

    // V->A

    void enterAbortState(){
        CHECK(P==1 && V==1 && C==0 && A==0 && occupied==1);
        this->A=1;
    }

    // V->C
    void enterCommitState(){
        CHECK(P==1 && V==1 && C==0 && A==0 && occupied==1);
        this->C=1;
    }

    TRX_STAT getState(){
        CHECK (!(A==1&&C==1));
        if(A==1) return TRX_STAT::ABORT;
        if(C==1) return TRX_STAT::COMMITTED;
        if(V==1) return TRX_STAT::VALIDATING;
        if(P==1) return TRX_STAT::PROCESSING;
    }

    bool isEmpty(){
        return (occupied == 0) ? true : false; 
    }

    bool setEmpty(){
        occupied = 0;
        return true;
    }

    string DebugString(){
        std::stringstream ss;

        ss << "trx_id=" << trx_id << "; P=" << std::to_string(P) << "; V=" << std::to_string(V) << "; C=" << std::to_string(C) << "; A=" << std::to_string(A) << "; occupied=" << std::to_string(occupied);
        return ss.str();
    }
};
