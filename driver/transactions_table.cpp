#include <map>
#include <driver/transactions_table.hpp>

Transactions::Transactions(){
    pthread_spin_init(&bt_lock, PTHREAD_PROCESS_PRIVATE);
    pthread_spin_init(&ct_lock, PTHREAD_PROCESS_PRIVATE);
    
    config = Config.GetInstance();
    buffer = config->config ->transaction_table;
    buff_sz = config->transaction_table_sz;
    rct = RecentTransactions.GetInstance_p();
    table = (TidStatus*) buffer;

    num_total_buckets = buffer_sz / (ASSOCIATIVITY * sizeof(TidStatus));
    num_main_buckets = uint64_t (num_total_buckets * MI_RATIO);
    num_indirect_buckets = num_total_buckets - num_main_buckets;
    num_slots = num_total_buckets * ASSOCIATIVITY;
    last_ext = 0;

    next_bt = 0;
    next_ct = 0;
    next_trx_id = 0;
}

Transactions::~Transactions(){
    pthread_spin_destroy(&bt_lock);
    // TODO
    // whether should delete rct?
}

Transactions* Transactions::GetInstance_p(){
    if(t_table == null)
        t_table = new Transactions();
    return t_table;
}

bool Transactions::insert_single_transaction(uint64_t trx_id, TRANSACTION_STATUS_T status, uint64_t bt){
    bool op_succ = false;
 
    // pthread_spinlock_lock (&spinlock);
    uint64_t bucket_id = trx_id % num_main_buckets;
    uint64_t slot_id = bucket_id * ASSOCIATIVITY;// used as cursor to traverse the target bucket
    while(slot_id < num_slots){
        for(int i = 0; i < ASSOCIATIVITY -1; ++ i, ++ slot_id){
            if(table[slot_id].trx_id == trx_id){
                // already exists
                cerr << "Transaction Status Table Error: already exists" << endl;
                assert(false);
            }

            if(table[slot_id].isEmpty()){
                table[slot_id].enterProcessState(trx_id);
                goto done;
            } 
        }

        if(!table[slot_id].trx_id == 0){
            slot_id = table[slot_id].trx_id *ASSOCIATIVITY;
            continue;
        }

        // allocate a new bucket
        if(last_ext >= num_indirect_buckets){
            cerr << "Transaction Status Table Error: out of indirect-header region." << endl;
        }
        table[slot_id].trx_id = num_main_buckets + last_ext;
        ++ last_ext;
        slot_id = table[slot_id].trx_id * ASSOCIATIVITY;
        table[slot_id].enterProcessState(trx_id);
    }

done:
    assert(slot_id < num_slots);
    assert(table[slot_id].trx_id == trx_id);
    
    bt_table[trx_id] = bt;
    op_succ = true;

    return op_succ;
}

// maybe need to check the situation of not existing
bool Transactions::modify_status(uint64_t trx_id, TRANSACTION_STATUS_T new_status){
    bool op_succ = true;

    uint64_t bucket_id = trx_id % num_buckets;

    while(true){
        for(int i = 0;i < ASSOCIATIVITY; ++ i){
            uint64_t slot_id = bucket_id * ASSOCIATIVITY + i;
            if(i < ASSOCIATIVITY - 1){
                if(table[slot_id].trx_id == trx_id){
                    // found it
                    switch (new_status)
                    {
                        case TRANSACTION_STATUS_T.Validating:
                            table[slot_id].enterValidationState();
                            return op_succ;
                        case TRANSACTION_STATUS_T.Abort:
                            table[slot_id].enterAbortState();
                            return op_succ;
                        case TRANSACTION_STATUS_T.Committed:
                            table[slot_id].enterCommitState();
                            return op_succ;
                        default:
                            cerr << "Transaction Status Table Error: modify_status" << endl;
                            assert(false);
                            break;
                    }
                } else{
                    if(table[slot_id].isEmpty())
                        return;
                    bucket_id = keys[slot_id].trx_id;
                    break;
                }
                    
            }
        }
    }

    return false;
}

bool Transactions::allocate_bt(uint64_t& bt, uint64& trx_id){
    trx_id = next_trx_id ++;
    bt = next_bt ++;
    assert(bt_table.find(trx_id)==bt_table.end())

    // register the bt of the new transaction
    pthread_spinlock_lock (&bt_lock);
    bt_table[trx_id] = bt;
    pthread_spinlock_unlock (&bt_lock);
    return true;
}

bool Transactions::allocate_ct(uint64_t ct, uint64& trx_id){
    ct = next_ct ++;
    assert(ct_table.find(trx_id)==ct_table.end())
    pthread_spinlock_lock (&ct_lock);
    ct_table[trx_id] = ct;
    pthread_spinlock_unlock (&ct_lock);
    return true;
}
