/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Nick Fang (jcfang6@cse.cuhk.edu.hk)
         Modified by Aaron LI (cjli@cse.cuhk.edu.hk)
*/

#include <unordered_map>
#include <string>
#include <algorithm>
#include <utility>
#include <vector>
#include <set>
#include <map>
#include <tbb/concurrent_vector.h>

#include "base/type.hpp"
#include "base/predicate.hpp"
#include "core/message.hpp"
#include "utils/config.hpp"
#include "layout/data_storage.hpp"

#pragma once

#define INDEX_THRESHOLD_RATIO 0.2

/**
 * IndexStore is used to store all index related data to accelerate processing,
 * including 1.Topology Index (i.e. Init Message) 2.Property Index
 */
class IndexStore {
 public:
    IndexStore() {
        config_ = Config::GetInstance();
        data_storage_ = DataStorage::GetInstance();
    }

    static IndexStore* GetInstance() {
        static IndexStore* index_store_ptr = nullptr;
        if (index_store_ptr == nullptr) {
            index_store_ptr = new IndexStore();
        }
        return index_store_ptr;
    }

    void Init();

    bool IsIndexEnabled(Element_T type, int pid, PredicateValue* pred = NULL, uint64_t* count = NULL);
    bool SetIndexMap(Element_T type, int pid, map<value_t, vector<value_t>>& index_map, vector<value_t>& no_key_vec);
    bool SetIndexMapEnable(Element_T type, int pid, bool inverse = false);
    void GetElements(Element_T type, vector<pair<int, PredicateValue>>& pred_chain, vector<value_t>& data);
    bool GetRandomValue(Element_T type, int pid, int rand_seed, string& value_str);

    /* ----------Topology Index------------- */
    void InsertToUpdateBuffer(const uint64_t & trx_id, vector<uint64_t>& val, ID_T type, bool isAdd, TRX_STAT stat);
    void InsertToUpdateRegion(vector<uint64_t>& val, ID_T type, bool isAdd, TRX_STAT stat);
    void InsertToUpdateRegion(update_element & elem, ID_T type);
    void MoveBufferToRegion(const uint64_t & trx_id);

    void ReadVtxTopoIndex(const uint64_t & trx_id, const uint64_t & begin_time, const bool & read_only, vector<vid_t> & data);
    void ReadEdgeTopoIndex(const uint64_t & trx_id, const uint64_t & begin_time, const bool & read_only, vector<eid_t> & data);
    /* ----------Topology Index------------- */

 private:
    Config * config_;
    DataStorage * data_storage_;

    mutex thread_mutex_;
    struct index_{
        bool isEnabled;
        uint64_t total;
        map<value_t, set<value_t>> index_map;
        set<value_t> no_key;
        map<value_t, uint64_t> count_map;
        vector<const value_t *> values;
    };

    unordered_map<int, index_> vtx_index;
    unordered_map<int, index_> edge_index;

    void get_elements_by_predicate(Element_T type, int pid, PredicateValue& pred, bool need_sort, vector<value_t>& vec);
    uint64_t get_count_by_predicate(Element_T type, int pid, PredicateValue& pred);
    void build_range_count(map<value_t, uint64_t>& m, PredicateValue& pred, uint64_t& count);
    void build_range_elements(map<value_t, set<value_t>>& m, PredicateValue& pred, vector<value_t>& vec, int& num_set);

    /* ----------Topology Index (init data)------------- */
    // Original init data
    vector<vid_t> topo_vtx_data;
    vector<eid_t> topo_edge_data;
    // Updata region
    tbb::concurrent_vector<update_element> vtx_update_list;
    tbb::concurrent_vector<update_element> edge_update_list;

    // Buffer for storing update info during processing
    // and will be inserted into update region when validation begins
    //  TrxID -> ElementIDList (V, E, VP, EP)
    tbb::concurrent_hash_map<uint64_t, vector<update_element>> vtx_update_buffers;  // Topo
    tbb::concurrent_hash_map<uint64_t, vector<update_element>> edge_update_buffers;  // Topo
    tbb::concurrent_hash_map<uint64_t, vector<update_element>> vp_update_buffers;  // Prop
    tbb::concurrent_hash_map<uint64_t, vector<update_element>> ep_update_buffers;  // Prop
    typedef tbb::concurrent_hash_map<uint64_t, vector<update_element>>::accessor up_buf_accessor;
    typedef tbb::concurrent_hash_map<uint64_t, vector<update_element>>::const_accessor up_buf_const_accessor;

    void build_topo_data();
    /* ----------Topology Index (init data)------------- */
};

#include "index_store.tpp"
