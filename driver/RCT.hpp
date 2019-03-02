#include <stdint.h>
#include <set>
class RecentTransactions{
private:
    // TODO a B+ tree
public:
    RecentTransactions();
    ~RecentTransactions();
    bool insertTransaction(uint64_t ct, uint64_t t_id);
    bool queryTransactions(uint64_t bt, uint64_t ct, std::set<uint64_t> trx_ids);

};