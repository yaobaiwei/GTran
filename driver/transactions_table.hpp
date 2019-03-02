/* Copyright 2019 Husky Data Lab, CUHK
 * Authors: Created by Jian Zhang (jzhang@cse.cuhk.edu.hk)
 */

#include<map>
#include<base/type.hpp>
#include <pthread.h>

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
    uint64_t trx_id;
    uint8_t P : 1;
    uint8_t V : 1 ;
    uint8_t C : 1 ;
    uint8_t A : 1 ;
    uint8_t occupied : 1;

    // enter P
    void enterProcessState(uint64_t trx_id){
        assert(trx_id != 0 && P==0 && V==0 && C==0 && A==0 && occupied==0)
        this->trx_id = trx_id;
        this->P = 1;
        this->occupied = 1;
    }
    // P->V
    void enterValidationState(){
        assert(P==1 && V==0 && C==0 && A==0 && occupied==1)
        this->P=1
    }
    // V->A
    void enterAbortState(){
        assert(P==1 && V==1 && C==0 && A==0 && occupied==1)
        this->A=1;
    }
    // V->C
    void enterCommitState(){
        assert(P==1 && V==1 && C==0 && A==0 && occupied==1)
        this->C=1
    }
    TRANSACTION_STATUS_T getState(){
        assert (!(A==1&&C==1))
        if(A==1) return TRANSACTION_STATUS_T.Abort;
        if(C==1) return TRANSACTION_STATUS_T.Committed;
        if(V==1) return TRANSACTION_STATUS_T.Validating;
        if(P==1) return TRANSACTION_STATUS_T.Processing;
    }

    bool isEmpty(){
        return (occupied == 1) ? false : true; 
    }
};

/*
 * a table of transactions
 * columns:  trx_id, status, bt
 * 
 * the actual memory region: buffer
 * This class is responsible for managering this region and provide public interfaces   * to access this memory region
 */

class Transactions{
private:
    static Transactions * t_table;

    Transactions();
    ~Transactions();

    /* core fields */
    uint64_t next_trx_id;

    // bt table is separated from status region since we must keep status region in RDMA
    std::map<uint64_t, uint64_t> bt_table;
    pthread_spinlock_t bt_lock;
    uint64_t next_bt;

    char * buffer;  
    uint64_t buffer_sz;
    // arrays corresponding to buffer
    TidStatus * table;
    uint64_t next_ct;
    std::map<uint64_t, uint64_t> ct_table;
    pthread_spinlock_t ct_lock;

    const uint64_t ASSOCIATIVITY = 8;
    const double MI_RATIO = 0.8; // the ratio of main buckets vs indirect buckets
    uint64_t num_total_buckets;
    uint64_t num_main_buckets;
    uint64_t num_indirect_buckets;
    uint64_t num_slots;

    // the next available bucket. 
    // table[last_ext]
    uint64_t last_ext;

    /* secondary fields: used to operate on external objects and the objects above*/ 
    Config * config;
    // when P->V, this field will be used to insert a new Transaction to RCT
    RecentTransactions * rct;
    // lock
    pthread_spinlock_t spinlock;

public:
    Transactions* GetInstance_p();
    bool insert_single_transaction(uint64_t trx_id, TRANSACTION_STATUS_T status, uint64_t bt);
    bool modify_status(uint64_t trx_id, TRANSACTION_STATUS_T new_status);
    // allocate bt and trx_id
    // Note that trx_id must begin with 1, not 0
    bool allocate_bt(uint64_t& bt, uint64& trx_id);
    // assign a ct. can only be called for once for some specific Transsaction
    // undefined behavior if some transaction call it over once
    bool allocate_ct(uint64_t ct, uint64& trx_id);
    // this function is not needed any more
    // bool read_status(uint64_t trx_id, TRANSACTION_STATUS_T * status_p);
};