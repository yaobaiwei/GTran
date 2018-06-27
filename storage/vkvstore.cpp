/*
 * vkvstore.cpp
 *
 *  Created on: Jun 1, 2018
 *      Author: Aaron Li, Hongzhi Chen
 */

#include <assert.h>
#include "storage/vkvstore.hpp"

using namespace std;

// ==================VKVStore=======================
uint64_t VKVStore::insert_id(uint64_t _pid) {
    // pid is already hashed
    uint64_t bucket_id = _pid % num_buckets;
    uint64_t slot_id = bucket_id * ASSOCIATIVITY;
    uint64_t lock_id = bucket_id % NUM_LOCKS;

    bool found = false;
    pthread_spin_lock(&bucket_locks[lock_id]);
    while (slot_id < num_slots) {
        // the last slot of each bucket is reserved for pointer to indirect header
        /// key.pid is used to store the bucket_id of indirect header
        for (int i = 0; i < ASSOCIATIVITY - 1; i++, slot_id++) {
            //assert(vertices[slot_id].key != key); // no duplicate key
            if (keys[slot_id].pid == _pid) {
                // Cannot get the original pid
                cout << "VKVStore ERROR: conflict at slot["
                     << slot_id << "] of bucket["
                     << bucket_id << "]" << endl;
                assert(false);
            }

            // insert to an empty slot
            if (keys[slot_id].pid == 0) {
                keys[slot_id].pid = _pid;
                goto done;
            }
        }

        // whether the bucket_ext (indirect-header region) is used
        if (!keys[slot_id].is_empty()) {
            slot_id = keys[slot_id].pid * ASSOCIATIVITY;
            continue; // continue and jump to next bucket
        }


        // allocate and link a new indirect header
        pthread_spin_lock(&bucket_ext_lock);
        if (last_ext >= num_buckets_ext) {
            cout << "VKVStore ERROR: out of indirect-header region." << endl;
            assert(last_ext < num_buckets_ext);
        }
        keys[slot_id].pid = num_buckets + (last_ext++);
        pthread_spin_unlock(&bucket_ext_lock);

        slot_id = keys[slot_id].pid * ASSOCIATIVITY; // move to a new bucket_ext
        keys[slot_id].pid = _pid; // insert to the first slot
        goto done;
    }

done:
    pthread_spin_unlock(&bucket_locks[lock_id]);
    assert(slot_id < num_slots);
    assert(keys[slot_id].pid == _pid);
    return slot_id;
}

// Insert all properties for one vertex
void VKVStore::insert_single_vertex_property(VProperty* vp) {
	vpid_t key(vp->id, 0);
	string str = to_string(vp->label);

	int slot_id = insert_id(key.hash());
	uint64_t length = sizeof(label_t);
	uint64_t off = sync_fetch_and_alloc_values(length);

	// insert ptr
	ptr_t ptr = ptr_t(length, off);
	keys[slot_id].ptr = ptr;

	// insert value
	strncpy(&values[off], str.c_str(), length);

    // Every <vpid_t, value_t>
    for (int i = 0; i < vp->plist.size(); i++) {
        V_KVpair v_kv = vp->plist[i];
        // insert key and get slot_id
        int slot_id = insert_id(v_kv.key.hash());

        // get length of centent
        uint64_t length = v_kv.value.content.size();

        // allocate for values in entry_region
        uint64_t off = sync_fetch_and_alloc_values(length + 1);

        // insert ptr
        ptr_t ptr = ptr_t(length + 1, off);
        keys[slot_id].ptr = ptr;

        // insert type of value first
        values[off++] = (char)v_kv.value.type;

        // insert value
        strncpy(&values[off], &v_kv.value.content[0], length);
    }
}

// Get current available memory location and reserve enough space for current kv
uint64_t VKVStore::sync_fetch_and_alloc_values(uint64_t n) {
    uint64_t orig;
    pthread_spin_lock(&entry_lock);
    orig = last_entry;
    last_entry += n;
    if (last_entry >= num_entries) {
        cout << "VKVStore ERROR: out of entry region." << endl;
        assert(last_entry < num_entries);
    }
    pthread_spin_unlock(&entry_lock);
    return orig;
}

// Get ikey_t by vpid or epid
void VKVStore::get_key_local(uint64_t pid, ikey_t & key) {
    uint64_t bucket_id = pid % num_buckets;
    while (true) {
        for (int i = 0; i < ASSOCIATIVITY; i++) {
            uint64_t slot_id = bucket_id * ASSOCIATIVITY + i;
            if (i < ASSOCIATIVITY - 1) {
                //data part
                if (keys[slot_id].pid == pid) {
                    //we found it
                    key = keys[slot_id];
                }
            } else {
                if (keys[slot_id].is_empty())
                    return;

                bucket_id = keys[slot_id].pid; // move to next bucket
                break; // break for-loop
            }
        }
    }
}

// Get key by key remotely
void VKVStore::get_key_remote(int tid, int dst_nid, uint64_t pid, ikey_t & key) {
    uint64_t bucket_id = pid % num_buckets;

    while (true) {
        char * buffer = buf_->GetSendBuf(tid);
        uint64_t off = offset + bucket_id * ASSOCIATIVITY * sizeof(ikey_t);
        uint64_t sz = ASSOCIATIVITY * sizeof(ikey_t);

        RDMA &rdma = RDMA::get_rdma();
        rdma.dev->RdmaRead(dst_nid, buffer, sz, off);

        ikey_t * keys = (ikey_t *)buffer;
        for (int i = 0; i < ASSOCIATIVITY; i++) {
            if (i < ASSOCIATIVITY - 1) {
                if (keys[i].pid == pid) {
                    key = keys[i];
                }
            } else {
                if (keys[i].is_empty())
                    return; // not found

                bucket_id = keys[i].pid; // move to next bucket
                break; // break for-loop
            }
        }
    }
}

VKVStore::VKVStore(Config * config, Buffer * buf) : config_(config), buf_(buf)
{
	mem = config_->kvstore;
	mem_sz = GiB2B(config_->global_vertex_property_kv_sz_gb);
    offset = config_->kvstore_offset;

    // size for header and entry
    uint64_t header_sz = mem_sz * HD_RATIO / 100;
    uint64_t entry_sz = mem_sz - header_sz;

    // header region
    num_slots = header_sz / sizeof(ikey_t);
    num_buckets = mymath::hash_prime_u64((num_slots / ASSOCIATIVITY) * MHD_RATIO / 100);
    num_buckets_ext = (num_slots / ASSOCIATIVITY) - num_buckets;
    last_ext = 0;

    // entry region
    num_entries = entry_sz;
    last_entry = 0;

    cout << "INFO: vkvstore = " << header_sz + entry_sz << " bytes " << std::endl
         << "      header region: " << num_slots << " slots"
         << " (main = " << num_buckets << ", indirect = " << num_buckets_ext << ")" << std::endl
         << "      entry region: " << num_entries << " entries" << std::endl;

    // Header
    keys = (ikey_t *)(mem);
    // Entry
    values = (char *)(mem + num_slots * sizeof(ikey_t));

    pthread_spin_init(&entry_lock, 0);
    pthread_spin_init(&bucket_ext_lock, 0);
    for (int i = 0; i < NUM_LOCKS; i++)
        pthread_spin_init(&bucket_locks[i], 0);
}

void VKVStore::init() {
    // initiate keys to 0 which means empty key
    for (uint64_t i = 0; i < num_slots; i++) {
        keys[i] = ikey_t();
    }
}

// Insert a list of Vertex properties
void VKVStore::insert_vertex_properties(vector<VProperty*> & vplist) {
    for (int i = 0; i < vplist.size(); i++){
    	insert_single_vertex_property(vplist.at(i));
    }
}

// Get properties by key locally
void VKVStore::get_property_local(uint64_t pid, value_t & val) {
    ikey_t key;
    get_key_local(mymath::hash_u64(pid), key);

    if (key.is_empty()) {
        val.content.resize(0);
        return;
    }

    uint64_t off = key.ptr.off;
    uint64_t size = key.ptr.size - 1;

    // type : char to uint8_t
    val.type = values[off++];
	val.content.resize(size);

	char * ctt = &(values[off]);
	std::copy(ctt, ctt+size, val.content.begin());
}

// Get properties by key remotely
void VKVStore::get_property_remote(int tid, int dst_nid, uint64_t pid, value_t & val) {
	ikey_t key;
	get_key_remote(tid, dst_nid, mymath::hash_u64(pid), key);
	if (key.is_empty()) {
		val.content.resize(0);
		return;
	}

	char * buffer = buf_->GetSendBuf(tid);
	uint64_t r_off = offset + num_slots * sizeof(ikey_t) + key.ptr.off;
	uint64_t r_sz = key.ptr.size;

	RDMA &rdma = RDMA::get_rdma();
	rdma.dev->RdmaRead(dst_nid, buffer, r_sz, r_off);

	// type : char to uint8_t
	val.type = buffer[0];
	val.content.resize(r_sz-1);

	char * ctt = &(buffer[1]);
	std::copy(ctt, ctt + r_sz-1, val.content.begin());
}

void VKVStore::get_label_local(uint64_t pid, label_t & label){
    ikey_t key;
    get_key_local(mymath::hash_u64(pid), key);

	if (key.is_empty()) return;

    uint64_t off = key.ptr.off;

    label = *reinterpret_cast<label_t *>(&(values[off]));
}


void VKVStore::get_label_remote(int tid, int dst_nid, uint64_t pid, label_t & label){
	ikey_t key;
	get_key_remote(tid, dst_nid, mymath::hash_u64(pid), key);

	if (key.is_empty()) return;

	char * buffer = buf_->GetSendBuf(tid);
	uint64_t r_off = offset + num_slots * sizeof(ikey_t) + key.ptr.off;
	uint64_t r_sz = key.ptr.size;

	RDMA &rdma = RDMA::get_rdma();
	rdma.dev->RdmaRead(dst_nid, buffer, r_sz, r_off);

	label = *reinterpret_cast<label_t *>(buffer);
}

// analysis
void VKVStore::print_mem_usage() {
    uint64_t used_slots = 0;
    for (uint64_t x = 0; x < num_buckets; x++) {
        uint64_t slot_id = x * ASSOCIATIVITY;
        for (int y = 0; y < ASSOCIATIVITY - 1; y++, slot_id++) {
            if (keys[slot_id].is_empty())
                continue;
            used_slots++;
        }
    }

    cout << "VKVStore main header: " << B2MiB(num_buckets * ASSOCIATIVITY * sizeof(ikey_t))
         << " MB (" << num_buckets * ASSOCIATIVITY << " slots)" << endl;
    cout << "\tused: " << 100.0 * used_slots / (num_buckets * ASSOCIATIVITY)
         << " % (" << used_slots << " slots)" << endl;
    cout << "\tchain: " << 100.0 * num_buckets / (num_buckets * ASSOCIATIVITY)
         << " % (" << num_buckets << " slots)" << endl;

    used_slots = 0;
    for (uint64_t x = num_buckets; x < num_buckets + last_ext; x++) {
        uint64_t slot_id = x * ASSOCIATIVITY;
        for (int y = 0; y < ASSOCIATIVITY - 1; y++, slot_id++) {
            if (keys[slot_id].is_empty())
                continue;
            used_slots++;
        }
    }

    cout << "indirect header: " << B2MiB(num_buckets_ext * ASSOCIATIVITY * sizeof(ikey_t))
         << " MB (" << num_buckets_ext * ASSOCIATIVITY << " slots)" << endl;
    cout << "\talloced: " << 100.0 * last_ext / num_buckets_ext
         << " % (" << last_ext << " buckets)" << endl;
    cout << "\tused: " << 100.0 * used_slots / (num_buckets_ext * ASSOCIATIVITY)
         << " % (" << used_slots << " slots)" << endl;

    cout << "entry: " << B2MiB(num_entries * sizeof(char))
         << " MB (" << num_entries << " entries)" << endl;
    cout << "\tused: " << 100.0 * last_entry / num_entries
         << " % (" << last_entry << " entries)" << endl;
}
