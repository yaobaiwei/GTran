#include "core/common.hpp"

bool is_valid_trx_id(uint64_t trx_id){
    // printf("valid_trx_id: trx_id = %llx\n; 1<<63=%lld", (long long) trx_id, 1<<63);
    bool is_trx_id = trx_id & 0x8000000000000000 ;
    bool non_over_flow = ((trx_id >> 32) & 0x7FFFFFFF) == 0;
    // cout << is_trx_id << "\t" << non_over_flow << endl;
    return is_trx_id && non_over_flow;
}

bool is_valid_time(uint64_t t){
    // printf("valid_trx_id: t = %llx\n", (long long) t);

    bool non_over_flow = ((t & 0xFFFFFFFF00000000) == 0);
    return non_over_flow;
}
