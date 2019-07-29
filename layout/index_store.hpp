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
#include "core/factory.hpp"
#include "layout/data_storage.hpp"
#include "utils/config.hpp"
#include "utils/tool.hpp"
#include "utils/write_prior_rwlock.hpp"

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
        trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();
    }

    static IndexStore* GetInstance() {
        static IndexStore* index_store_ptr = nullptr;
        if (index_store_ptr == nullptr) {
            index_store_ptr = new IndexStore();
        }
        return index_store_ptr;
    }

    enum PropertyUpdateT { NONE, ADD, MODIFY, DROP };
    // Add/Drop V/E ---> check isAdd
    //      true --> add
    //      false --> delete
    // Add/Drop VP/EP ---> check update_type
    //      PropertyUpdateT::ADD --> add
    //      PropertyUpdateT::DROP --> drop
    // Modify VP/EP ---> check update_type with isAdd
    //      PropertyUpdateT::MODIFY & true --> modifyToNew
    //      PropertyUpdateT::MODIFY & false --> modifyFromOld
    //      e.g. v4: "age" from 8 to 9
    //          ("age", 8) : PropertyUpdateT::MODIFY & false
    //          ("age", 9) : PropertyUpdateT::MODIFY & true
    struct update_element {  // For Update Region
        uint64_t element_id;  // vid, eid, vpid or epid
        bool isAdd;  // 1 for ADD; 0 for DELETE
        uint64_t trxid;  // id of related trx
        uint64_t ct;  // Commit time of transaction, assigned when MoveBufferToRegin

        // For Property Modify
        value_t value;
        PropertyUpdateT update_type = PropertyUpdateT::NONE;

        update_element(uint64_t element_id_, bool isAdd_, uint64_t trxid_) :
            element_id(element_id_), isAdd(isAdd_), trxid(trxid_) { ct = 0; }

        void set_modify_value(value_t value_, PropertyUpdateT update_type_) {
            value = value_;
            update_type = update_type_;
        }

        void set_ct(const uint64_t& ct_) { ct = ct_; }

        void Print() {
            cout << "[UpdateElement] " << element_id << ", " << (isAdd ? "ADD" : "DELETE") << endl;
            if (!value.isEmpty()) {
                cout << "\tvalue : " << Tool::DebugString(value) << " with type " << update_type << endl;
            }
        }
    };

    void Init();

    // Write Update Data
    void InsertToUpdateBuffer(const uint64_t & trx_id, vector<uint64_t>& ids, ID_T type, bool isAdd,
                            value_t* new_val = NULL, vector<value_t>* old_vals = NULL);
    void MoveTopoBufferToRegion(const uint64_t & trx_id, const uint64_t & ct);  // Invoke when validation begins
    void MovePropBufferToRegion(const uint64_t & trx_id, const uint64_t & ct);  // Invoke when commit successfully

    // Prop Index Related
    bool IsIndexEnabled(Element_T type, int pid, PredicateValue* pred = NULL, uint64_t* count = NULL);
    bool SetIndexMap(Element_T type, int pid, map<value_t, vector<value_t>>& index_map, vector<value_t>& no_key_vec);
    bool SetIndexMapEnable(Element_T type, int pid, bool inverse = false);

    // Read Index
    void ReadVtxTopoIndex(const uint64_t & trx_id, const uint64_t & begin_time, const bool & read_only, vector<vid_t> & data);
    void ReadEdgeTopoIndex(const uint64_t & trx_id, const uint64_t & begin_time, const bool & read_only, vector<eid_t> & data);
    void ReadPropIndex(Element_T type, vector<pair<int, PredicateValue>>& pred_chain, vector<value_t>& data);  // For Prop
    bool GetRandomValue(Element_T type, int pid, int rand_seed, string& value_str);

    // GC
    void VtxSelfGarbageCollect(const uint64_t& threshold);
    void EdgeSelfGarbageCollect(const uint64_t& threshold);
    void PropSelfGarbageCollect(const uint64_t& threshold, const int& pid, Element_T type);

 private:
    Config * config_;
    DataStorage * data_storage_;
    TrxTableStub * trx_table_stub_;

    mutex thread_mutex_;
    struct index_{  // One Index for One PropertyKey (e.g. age)
        bool isEnabled;
        uint64_t total;  // Number of objs in total {i.e. all vertices or edges}
        map<value_t, set<value_t>> index_map;  // Map for (value, set<element>) (e.g. (13 -> [v1, v2]) {g.V().has("age", 13)}
        set<value_t> no_key; // Set for all elements that does not have propertyKey; {g.V().hasNot("age")}
        map<value_t, uint64_t> count_map; // Number of each 'age' values (e.g. (13 -> 2)) {g.V().has("age", 13).count()}
        vector<const value_t *> values;  // Used when creating random values; Not for normal index

        string DebugString() {
            string ret = "\n";
            ret += "\tisEnabled: " + (string)(isEnabled ? "True" : "False") + "\n";
            ret += "\ttotal: " + to_string(total) + "\n";
            ret += "\tsize of index_map: " + to_string(index_map.size()) + "\n";
            ret += "\tsize of no_key:" + to_string(no_key.size()) + "\n";
            ret += "\tsize of count_map: " + to_string(count_map.size()) + "\n";
            ret += "\tsize of values: " + to_string(values.size()) + "\n";

            return ret;
        }
    };

    // Original init data
    //  Read Only except during GC
    vector<vid_t> topo_vtx_data;
    vector<eid_t> topo_edge_data;
    unordered_map<int, index_> vtx_prop_index;  // key: PropertyKey
    unordered_map<int, index_> edge_prop_index;  // key: PropertyKey

    // Updata region
    tbb::concurrent_vector<update_element> vtx_update_list;
    tbb::concurrent_vector<update_element> edge_update_list;
    tbb::concurrent_hash_map<int, map<value_t, vector<update_element>>> vp_update_map;  // key: PropertyKey
    tbb::concurrent_hash_map<int, map<value_t, vector<update_element>>> ep_update_map;  // key: PropertyKey
    typedef tbb::concurrent_hash_map<int, map<value_t, vector<update_element>>>::accessor prop_up_map_accessor;
    typedef tbb::concurrent_hash_map<int, map<value_t, vector<update_element>>>::const_accessor prop_up_map_const_accessor;

    // Buffer for storing update info during processing
    // and will be inserted into update region when validation begins
    //  TrxID -> ElementIDList (V, E, VP, EP)
    tbb::concurrent_hash_map<uint64_t, vector<update_element>> vtx_update_buffers;  // Topo
    tbb::concurrent_hash_map<uint64_t, vector<update_element>> edge_update_buffers;  // Topo
    tbb::concurrent_hash_map<uint64_t, vector<update_element>> vp_update_buffers;  // Prop
    tbb::concurrent_hash_map<uint64_t, vector<update_element>> ep_update_buffers;  // Prop
    typedef tbb::concurrent_hash_map<uint64_t, vector<update_element>>::accessor up_buf_accessor;
    typedef tbb::concurrent_hash_map<uint64_t, vector<update_element>>::const_accessor up_buf_const_accessor;

    // GC Lock
    WritePriorRWLock vtx_topo_gc_rwlock_;
    WritePriorRWLock edge_topo_gc_rwlock_;
    WritePriorRWLock vtx_prop_gc_rwlock_;
    WritePriorRWLock edge_prop_gc_rwlock_;

    void build_topo_data();

    void get_elements_by_predicate(Element_T type, int pid, PredicateValue& pred, bool need_sort, vector<value_t>& vec);
    uint64_t get_count_by_predicate(Element_T type, int pid, PredicateValue& pred);
    void read_prop_update_data(const update_element & up_elem, vector<value_t> & vec);

    void build_range_count(map<value_t, uint64_t>& m, PredicateValue& pred, uint64_t& count);
    void build_range_elements(map<value_t, set<value_t>>& m, PredicateValue& pred, vector<value_t>& vec, int& num_set);

    void modify_index(index_ * idx, value_t& id_value_t, value_t& val_value_t, bool isAdd);
};

#include "index_store.tpp"
