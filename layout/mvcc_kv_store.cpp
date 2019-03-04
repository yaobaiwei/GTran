/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include "layout/mvcc_kv_store.hpp"

MVCCKVStore::MVCCKVStore(char* mem, uint64_t mem_sz) {
    config_ = Config::GetInstance();

    mem_ = mem;
    mem_sz_ = mem_sz;

    HD_RATIO_ = config_->key_value_ratio_in_rdma;

    // size for header and entry
    uint64_t header_sz = mem_sz_ * HD_RATIO_ / 100;
    uint64_t entry_sz = mem_sz_ - header_sz;

    // header region
    num_slots_ = header_sz / sizeof(KVKey);
    num_buckets_ = mymath::hash_prime_u64((num_slots_ / ASSOCIATIVITY) * MHD_RATIO / 100);
    num_buckets_ext_ = (num_slots_ / ASSOCIATIVITY) - num_buckets_;
    last_ext_ = 0;

    // entry region
    num_entries_ = entry_sz;
    last_entry_ = 0;

    // Header
    keys_ = (KVKey*)(mem_);
    // Entry
    values_ = (char *)(mem_ + num_slots_ * sizeof(KVKey));

    pthread_spin_init(&entry_lock_, 0);
    pthread_spin_init(&bucket_ext_lock_, 0);
    for (int i = 0; i < NUM_LOCKS; i++)
        pthread_spin_init(&bucket_locks_[i], 0);
}

ptr_t MVCCKVStore::Insert(const MVCCHeader& key, const value_t& value) {
    // insert key and get slot_id
    int slot_id = InsertId(key);

    // get length of centent
    uint64_t length = value.content.size();

    // allocate for values_ in entry_region
    uint64_t off = SyncFetchAndAllocValues(length + 1);

    // insert ptr
    ptr_t ptr = ptr_t(length + 1, off);
    keys_[slot_id].ptr = ptr;

    // insert type of value first
    values_[off++] = (char)value.type;

    // insert value
    memcpy(&values_[off], &value.content[0], length);

    return ptr;
}

uint64_t MVCCKVStore::SyncFetchAndAllocValues(uint64_t n) {
    uint64_t orig;
    pthread_spin_lock(&entry_lock_);
    orig = last_entry_;
    last_entry_ += n;
    if (last_entry_ >= num_entries_) {
        cout << "MVCCKVStore ERROR: out of entry region." << endl;
        assert(last_entry_ < num_entries_);
    }
    pthread_spin_unlock(&entry_lock_);
    return orig;
}

uint64_t MVCCKVStore::InsertId(const MVCCHeader& _mvcc_header) {
    // mvcc_header is not hashed
    uint64_t bucket_id = _mvcc_header.HashToUint64() % num_buckets_;
    uint64_t slot_id = bucket_id * ASSOCIATIVITY;
    uint64_t lock_id = bucket_id % NUM_LOCKS;

    bool found = false;
    pthread_spin_lock(&bucket_locks_[lock_id]);
    while (slot_id < num_slots_) {
        // the last slot of each bucket is reserved for pointer to indirect header
        /// key.mvcc_header is used to store the bucket_id of indirect header
        for (int i = 0; i < ASSOCIATIVITY - 1; i++, slot_id++) {
            //assert(vertices[slot_id].key != key); // no duplicate key
            if (keys_[slot_id].mvcc_header == _mvcc_header) {
                // Cannot get the original mvcc_header
                cout << "MVCCKVStore ERROR: conflict at slot["
                     << slot_id << "] of bucket["
                     << bucket_id << "]" << endl;
                assert(false);
            }

            // insert to an empty slot
            if (keys_[slot_id].mvcc_header.pid == 0) {
                keys_[slot_id].mvcc_header = _mvcc_header;
                goto done;
            }
        }

        // whether the bucket_ext (indirect-header region) is used
        if (!keys_[slot_id].is_empty()) {
            slot_id = keys_[slot_id].mvcc_header.pid * ASSOCIATIVITY;
            continue; // continue and jump to next bucket
        }

        // allocate and link a new indirect header
        pthread_spin_lock(&bucket_ext_lock_);
        if (last_ext_ >= num_buckets_ext_) {
            cout << "MVCCKVStore ERROR: out of indirect-header region." << endl;
            assert(last_ext_ < num_buckets_ext_);
        }
        keys_[slot_id].mvcc_header.pid = num_buckets_ + (last_ext_++);  // ??????
        pthread_spin_unlock(&bucket_ext_lock_);

        slot_id = keys_[slot_id].mvcc_header.pid * ASSOCIATIVITY; // move to a new bucket_ext
        keys_[slot_id].mvcc_header = _mvcc_header; // insert to the first slot
        goto done;
    }

done:
    pthread_spin_unlock(&bucket_locks_[lock_id]);
    assert(slot_id < num_slots_);
    assert(keys_[slot_id].mvcc_header == _mvcc_header);
    return slot_id;
}

void MVCCKVStore::Get(ptr_t ptr, value_t& val) {
    uint64_t off = ptr.off;
    uint64_t size = ptr.size - 1;

    val.type = values_[off++];
    val.content.resize(size);

    char* ctt = &(values_[off]);
    std::copy(ctt, ctt + size, val.content.begin());
}
