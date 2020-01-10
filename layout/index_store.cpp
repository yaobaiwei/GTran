/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron LI (cjli@cse.cuhk.edu.hk)
*/

#include "layout/index_store.hpp"

void IndexStore::Init() {
    build_topo_data();
}

bool IndexStore::IsIndexEnabled(Element_T type, int pid, PredicateValue* pred = NULL, uint64_t* count = NULL) {
    if (config_->global_enable_indexing) {
        WritePriorRWLock * rw_lock;
        unordered_map<int, index_>* m;
        if (type == Element_T::VERTEX) {
            m = &vtx_prop_index;
            rw_lock = &vtx_prop_gc_rwlock_;
        } else {
            m = &edge_prop_index;
            rw_lock = &edge_prop_gc_rwlock_;
        }

        ReaderLockGuard reader_guard_lock(*rw_lock);

        thread_mutex_.lock();
        // check property key
        auto itr = m->find(pid);
        if (itr == m->end()) {
            thread_mutex_.unlock();
            return false;
        }
        thread_mutex_.unlock();

        index_ &idx = itr->second;
        if (idx.isEnabled) {
            if (pred != NULL) {
                uint64_t threshold = idx.total * INDEX_THRESHOLD_RATIO;
                *count = get_count_by_predicate(type, pid, *pred);
                if (*count >= threshold) {
                    return false;
                } else {
                    return true;
                }
            } else {
                return true;
            }
        } else {
            return false;
        }
    }
    return false;
}

//    type:             VERTEX / EDGE
//    property_key:     key
//    index_map:        alreay constructed index map
//    no_key_vec:       vector of elements that have no provided key
bool IndexStore::SetIndexMap(Element_T type, int pid, map<value_t, vector<uint64_t>>& index_map, vector<uint64_t>& no_key_vec) {
    if (config_->global_enable_indexing) {
        WritePriorRWLock * rw_lock;
        unordered_map<int, index_>* m;
        if (type == Element_T::VERTEX) {
            m = &vtx_prop_index;
            rw_lock = &vtx_prop_gc_rwlock_;
        } else {
            m = &edge_prop_index;
            rw_lock = &edge_prop_gc_rwlock_;
        }

        ReaderLockGuard reader_guard_lock(*rw_lock);
        index_ &idx = (*m)[pid];

        uint64_t sum = 0;
        // sort each vector for better searching performance
        for (auto& item : index_map) {
            set<uint64_t> temp(std::make_move_iterator(item.second.begin()),
                            std::make_move_iterator(item.second.end()));
            uint64_t count = temp.size();
            sum += count;
            auto itr = idx.index_map.insert(idx.index_map.end(), {move(item.first), move(temp)});
            idx.count_map[item.first] = count;
            idx.values.push_back(&(itr->first));
        }

        idx.no_key = set<uint64_t>(make_move_iterator(no_key_vec.begin()), make_move_iterator(no_key_vec.end()));
        sum += idx.no_key.size();
        idx.total = sum;

        return true;
    }
    return false;
}

bool IndexStore::SetIndexMapEnable(Element_T type, int pid, bool inverse = false) {
    if (config_->global_enable_indexing) {
        WritePriorRWLock * rw_lock;
        unordered_map<int, index_>* m;
        if (type == Element_T::VERTEX) {
            m = &vtx_prop_index;
            rw_lock = &vtx_prop_gc_rwlock_;
        } else {
            m = &edge_prop_index;
            rw_lock = &edge_prop_gc_rwlock_;
        }

        ReaderLockGuard reader_guard_lock(*rw_lock);
        index_ &idx = (*m)[pid];

        thread_mutex_.lock();
        if (inverse) {
            idx.isEnabled = !idx.isEnabled;
        } else {
            idx.isEnabled = true;
        }
        thread_mutex_.unlock();
        return idx.isEnabled;
    }
    return false;
}

void IndexStore::ReadPropIndex(Element_T type, vector<pair<int, PredicateValue>>& pred_chain, vector<value_t>& data) {
    bool is_first = true;
    bool need_sort = pred_chain.size() != 1;
    vector<uint64_t> tmp_data;

    for (auto& pred_pair : pred_chain) {
        vector<uint64_t> vec;
        // get sorted vector of all elements satisfying current predicate
        get_elements_by_predicate(type, pred_pair.first, pred_pair.second, need_sort, vec);

        if (is_first) {
            tmp_data.swap(vec);
            is_first = false;
        } else {
            vector<uint64_t> temp;
            // do intersection with previous result
            // temp is sorted after intersection
            set_intersection(make_move_iterator(tmp_data.begin()), make_move_iterator(tmp_data.end()),
                            make_move_iterator(vec.begin()), make_move_iterator(vec.end()),
                            back_inserter(temp));
            tmp_data.swap(temp);
        }
    }

    // Convert all ids from uint64_t to value_t, required by InitActor::InitWithIndex()
    data.resize(tmp_data.size());
    for (int i = 0; i < tmp_data.size(); i++) {
        Tool::uint64_t2value_t(tmp_data[i], data[i]);
    }
}

bool IndexStore::GetRandomValue(Element_T type, int pid, string& value_str, const bool& is_update) {
    WritePriorRWLock * rw_lock;
    unordered_map<int, index_>* m;
    unordered_map<int, unordered_set<int>>* r_map;
    if (type == Element_T::VERTEX) {
        m = &vtx_prop_index;
        rw_lock = &vtx_prop_gc_rwlock_;
        r_map = &vtx_rand_count;
    } else {
        m = &edge_prop_index;
        rw_lock = &edge_prop_gc_rwlock_;
        r_map = &edge_rand_count;
    }
    ReaderLockGuard reader_guard_lock(*rw_lock);

    auto itr = m->find(pid);
    if (itr == m->end()) {
        return false;
    }

    index_ &idx = itr->second;
    int size = idx.values.size();
    if (size == 0) {
        return false;
    }

    auto r_itr = r_map->emplace(pid, unordered_set<int>()).first;
    unordered_set<int>& r_count = r_itr->second;

    int rand_index;
    uint64_t start_time = timer::get_usec();
    while (true) {
        rand_index = rand() % size;
        if (!is_update) {
            // get value string
            value_t v = *idx.values[rand_index];
            value_str = v.DebugString();
            if (value_str.find(";") != string::npos ||
                value_str.find("=") != string::npos ||
                value_str.find("(") != string::npos ||
                value_str.find(")") != string::npos) {
                continue;
            }

            break;
        }

        if (r_count.find(rand_index) == r_count.end()) {
            r_count.emplace(rand_index);

            // get value string
            value_t v = *idx.values[rand_index];
            value_str = v.DebugString();
            if (value_str.find(";") != string::npos ||
                value_str.find("=") != string::npos ||
                value_str.find("(") != string::npos ||
                value_str.find(")") != string::npos) {
                continue;
            }

            break;
        } else {
            uint64_t end_time = timer::get_usec();
            if (start_time - end_time > TIMEOUT) {
                r_count.clear();
            }
        }
    }

    return true;
}

void IndexStore::CleanRandomCount() {
    for (auto & pair : vtx_rand_count) {
        pair.second.clear();
    }
    vtx_rand_count.clear();

    for (auto & pair : edge_rand_count) {
        pair.second.clear();
    }
    edge_rand_count.clear();
} 

void IndexStore::VtxSelfGarbageCollect(const uint64_t& threshold) {
    WriterLockGuard writer_lock_guard(vtx_topo_gc_rwlock_);

    vector<vid_t> addV_vec;
    set<vid_t> delV_set;
    // tbb::concurrent_vector does NOT support erase store
    // un-mergable elements into new vector and swap
    tbb::concurrent_vector<update_element> new_update_list;
    for (auto & up_elem : vtx_update_list) {
        if (up_elem.ct < threshold && up_elem.ct != 0) {
            // Mergable element
            TRX_STAT trx_stat;
            trx_sc_accessor tac;
            if (trx_status_and_count_table.find(tac, up_elem.trxid)) {
                trx_stat = tac->second.first;
            } else {
                cout << "[IndexStore] Zombie Update Element Exists in IndexStore" << endl;
                continue;
            }

            vid_t vid;
            uint2vid_t(up_elem.element_id, vid);
            if (trx_stat == TRX_STAT::COMMITTED) {
                if (up_elem.isAdd) {
                    addV_vec.emplace_back(vid);
                } else {
                    delV_set.emplace(vid);
                }

                tac->second.second--;
            } else {
                if (trx_stat != TRX_STAT::ABORT) {
                    // For Abort Trx, Erase element from update list
                    new_update_list.emplace_back(up_elem);
                } else {
                    tac->second.second--;
                }
            }

            if (tac->second.second == 0) { trx_status_and_count_table.erase(tac); }
        } else {
            new_update_list.emplace_back(up_elem);
        }
    }

    auto checkFunc = [&](vid_t& vid) {
        if (delV_set.find(vid) != delV_set.end()) {
            // Need to delete, erase
            return true;
        }
        return false;
    };

    if (delV_set.size() != 0) {
        topo_vtx_data.erase(remove_if(topo_vtx_data.begin(), topo_vtx_data.end(), checkFunc), topo_vtx_data.end());
    }
    topo_vtx_data.insert(topo_vtx_data.end(), addV_vec.begin(), addV_vec.end());
    vtx_update_list.swap(new_update_list);
}

void IndexStore::EdgeSelfGarbageCollect(const uint64_t& threshold) {
    WriterLockGuard writer_lock_guard(edge_topo_gc_rwlock_);

    set<eid_t> addE_set;
    set<eid_t> delE_set;
    // tbb::concurrent_vector does NOT support erase store
    // un-mergable elements into new vector and swap
    tbb::concurrent_vector<update_element> new_update_list;
    for (auto & up_elem : edge_update_list) {
        if (up_elem.ct < threshold && up_elem.ct != 0) {
            // Mergable element
            TRX_STAT trx_stat;
            trx_sc_accessor tac;
            if (trx_status_and_count_table.find(tac, up_elem.trxid)) {
                trx_stat = tac->second.first;
            } else {
                cout << "[IndexStore] Zombie Update Element Exists in IndexStore" << endl;
                continue;
            }

            eid_t eid;
            uint2eid_t(up_elem.element_id, eid);
            if (trx_stat == TRX_STAT::COMMITTED) {
                if (up_elem.isAdd) {
                    addE_set.emplace(eid);
                } else {
                    delE_set.emplace(eid);
                }
            } else {
                if (trx_stat != TRX_STAT::ABORT) {
                    // For Abort Trx, Erase element from update list
                    new_update_list.emplace_back(up_elem);
                }
            }
        } else {
            new_update_list.emplace_back(up_elem);
        }
    }

    auto checkFunc = [&](eid_t& eid) {
        if (addE_set.find(eid) != addE_set.end()) {
            addE_set.erase(eid);
        }

        if (delE_set.find(eid) != delE_set.end()) {
            // Need to delete, erase
            return true;
        }
        return false;
    };

    if (delE_set.size() != 0) {
        topo_edge_data.erase(remove_if(topo_edge_data.begin(), topo_edge_data.end(), checkFunc), topo_edge_data.end());
    }
    topo_edge_data.insert(topo_edge_data.end(), addE_set.begin(), addE_set.end());
    edge_update_list.swap(new_update_list);
}

void IndexStore::PropSelfGarbageCollect(const uint64_t& threshold, const int& pid, Element_T type) {
    WritePriorRWLock* rw_lock;
    unordered_map<int, index_>* m;
    tbb::concurrent_hash_map<int, map<value_t, vector<update_element>>>* up_region;
    if (type == Element_T::VERTEX) {
        m = &vtx_prop_index;
        up_region = &vp_update_map;
        rw_lock = &vtx_prop_gc_rwlock_;
    } else {
        m = &edge_prop_index;
        up_region = &ep_update_map;
        rw_lock = &edge_prop_gc_rwlock_;
    }

    // Lock Here, Lock outside is hard
    WriterLockGuard writer_lock_guard(*rw_lock);

    if (m->find(pid) == m->end()) {
        // There is no such key in index
        cout << "[IndexStore] Unexpected PropertyKey when try to gc" << endl;
        return;
    }
    index_ * cur_index = &(m->at(pid));

    prop_up_map_accessor pac;
    if (!up_region->find(pac, pid)) {
        // No update for pid on this machine
        return;
    }

    for (auto & pair : pac->second) {
        auto up_elem_itr = pair.second.begin();
        while (up_elem_itr != pair.second.end()) {
            if (up_elem_itr->ct < threshold && up_elem_itr->ct != 0) {
                uint64_t eid_value = up_elem_itr->element_id;

                if (up_elem_itr->update_type == PropertyUpdateT::ADD) {
                    // Add a new property for this vertex
                    // Need to modify index_map, no_key and count_map
                    // index_.no_key
                    auto itr = cur_index->no_key.find(eid_value);
                    if (itr != cur_index->no_key.end()) { cur_index->no_key.erase(itr); }

                    modify_index(cur_index, eid_value, up_elem_itr->value, true);
                } else if (up_elem_itr->update_type == PropertyUpdateT::DROP) {
                    // property dropped from this vertex
                    // Need to modify index_map, no_key and count_map

                    // index_.no_key
                    cur_index->no_key.emplace(eid_value);

                    modify_index(cur_index, eid_value, up_elem_itr->value, false);
                } else if (up_elem_itr->update_type == PropertyUpdateT::MODIFY) {
                    // Property value changed from this vertex
                    // Need to modify index_map, count_map
                    modify_index(cur_index, eid_value, up_elem_itr->value, up_elem_itr->isAdd);
                }

                up_elem_itr = pair.second.erase(up_elem_itr);
                continue;
            }

            up_elem_itr++;
        }
    }
}

void IndexStore::InsertToUpdateBuffer(const uint64_t& trx_id, vector<uint64_t>& ids, ID_T type, bool isAdd,
                                      value_t* new_val = NULL, vector<value_t>* old_vals = NULL) {
    vector<update_element> up_list;
    if (old_vals != NULL) {
        CHECK(ids.size() == old_vals->size());
    }

    for (int i = 0; i < ids.size(); i++) {
        update_element up_elem(ids.at(i), isAdd, trx_id);
        if (new_val != NULL && old_vals != NULL) {  // ModifyToNew V/EP
            up_elem.isAdd = true;
            up_elem.set_modify_value(*new_val, PropertyUpdateT::MODIFY);
            up_list.emplace_back(up_elem);

            if (!old_vals->at(i).isEmpty()) {  // ModifyFromOld V/EP
                up_elem.isAdd = false;
                up_elem.set_modify_value(old_vals->at(i), PropertyUpdateT::MODIFY);
                up_list.emplace_back(up_elem);
            } else {  // Add V/EP
                up_elem.set_modify_value(old_vals->at(i), PropertyUpdateT::ADD);
                up_list.emplace_back(up_elem);
            }
        } else if (new_val != NULL && old_vals == NULL) {  // Add V/EP
            up_elem.set_modify_value(*new_val, PropertyUpdateT::ADD);
            up_list.emplace_back(up_elem);
        } else if (new_val == NULL && old_vals != NULL) {  // Drop V/EP
            up_elem.set_modify_value(old_vals->at(i), PropertyUpdateT::DROP);
            up_list.emplace_back(up_elem);
        } else {  // V/E update
            up_list.emplace_back(up_elem);
        }
    }

    up_buf_accessor ac;
    bool found = false;
    if (type == ID_T::VID) {
        vtx_update_buffers.insert(ac, trx_id);
    } else if (type == ID_T::EID) {
        edge_update_buffers.insert(ac, trx_id);
    } else if (type == ID_T::VPID) {
        vp_update_buffers.insert(ac, trx_id);
    } else if (type == ID_T::EPID) {
        ep_update_buffers.insert(ac, trx_id);
    } else {
        cout << "[Index Store] Unexpected element type in InsertToUpdateBuffer()" << endl;
    }

    // Append new data
    ac->second.insert(ac->second.end(), up_list.begin(), up_list.end());
}

void IndexStore::MoveTopoBufferToRegion(const uint64_t & trx_id, const uint64_t & ct) {
    up_buf_const_accessor cac;
    // V
    if (vtx_update_buffers.find(cac, trx_id)) {
        for (auto & up_elem : cac->second) {
            up_elem.set_ct(ct);
            vtx_update_list.emplace_back(up_elem);

            trx_sc_accessor tac;
            if (trx_status_and_count_table.insert(tac, trx_id)) {
                tac->second = make_pair(TRX_STAT::VALIDATING, 1);
            } else {
                tac->second.second++;
            }
        }
        vtx_update_buffers.erase(cac);
    }

    // E
    if (edge_update_buffers.find(cac, trx_id)) {
        for (auto & up_elem : cac->second) {
            up_elem.set_ct(ct);
            edge_update_list.emplace_back(up_elem);

            trx_sc_accessor tac;
            if (trx_status_and_count_table.insert(tac, trx_id)) {
                tac->second = make_pair(TRX_STAT::VALIDATING, 1);
            } else {
                tac->second.second++;
            }
        }
        edge_update_buffers.erase(cac);
    }
}

void IndexStore::MovePropBufferToRegion(const uint64_t & trx_id, const uint64_t & ct) {
    up_buf_accessor ac;
    // VP
    if (vp_update_buffers.find(ac, trx_id)) {
        prop_up_map_accessor pac;
        for (auto & up_elem : ac->second) {
            vpid_t vpid;
            uint2vpid_t(up_elem.element_id, vpid);
            up_elem.set_ct(ct);
            up_elem.element_id = vpid.vid;

            vp_update_map.insert(pac, vpid.pid);

            if (pac->second.find(up_elem.value) != pac->second.end()) {
                pac->second.at(up_elem.value).emplace_back(up_elem);
            } else {
                pac->second.emplace(up_elem.value, vector<update_element>{up_elem});
            }
        }
        vp_update_buffers.erase(ac);
    }

    // EP
    if (ep_update_buffers.find(ac, trx_id)) {
        prop_up_map_accessor pac;
        for (auto & up_elem : ac->second) {
            uint64_t eid = up_elem.element_id >> PID_BITS;
            uint64_t pid = up_elem.element_id - (eid << PID_BITS);
            up_elem.set_ct(ct);
            up_elem.element_id = eid;

            ep_update_map.insert(pac, pid);

            if (pac->second.find(up_elem.value) != pac->second.end()) {
                pac->second.at(up_elem.value).emplace_back(up_elem);
            } else {
                pac->second.emplace(up_elem.value, vector<update_element>{up_elem});
            }
        }
        ep_update_buffers.erase(ac);
    }
}

void IndexStore::UpdateTrxStatus(const uint64_t & trx_id, TRX_STAT stat) {
    trx_sc_accessor tac;
    if (trx_status_and_count_table.find(tac, trx_id)) {
        tac->second.first = stat;
    }
}

void IndexStore::ReadVtxTopoIndex(const uint64_t & trx_id, const uint64_t & begin_time,
                                  const bool & read_only, vector<vid_t> & data) {
    vector<vid_t> addV_vec;
    set<vid_t> delV_set;
    for (int i = 0; i < vtx_update_list.size(); i++) {
        // Risk Here : what if the iteration cannot stop
        update_element up_elem = vtx_update_list[i];
        if (up_elem.element_id == 0) { continue; }
        vid_t vid;
        uint2vid_t(up_elem.element_id, vid);
        if (data_storage_->CheckVertexVisibilityWithVid(trx_id, begin_time, read_only, vid)) {
            // Visible (For Add)
            if (up_elem.isAdd) {
                addV_vec.emplace_back(vid);
            }
        } else {
            // Invisible (For Del)
            if (!up_elem.isAdd) {
                delV_set.emplace(vid);
            }
        }
    }

    auto checkFunc = [&](vid_t& vid) {
        if (delV_set.find(vid) != delV_set.end()) {
            // Need to delete, erase
            return true;
        }
        return false;
    };

    ReaderLockGuard reader_lock_guard(vtx_topo_gc_rwlock_);
    data = topo_vtx_data;
    if (delV_set.size() != 0) {
        data.erase(remove_if(data.begin(), data.end(), checkFunc), data.end());
    }
    data.insert(data.end(), addV_vec.begin(), addV_vec.end());

    // Check Update Buffer to Read self-updated data
    up_buf_const_accessor cac;
    if (vtx_update_buffers.find(cac, trx_id)) {
        for (auto & up_elem : cac->second) {
            vid_t vid;
            uint2vid_t(up_elem.element_id, vid);
            data.emplace_back(vid);
        }
    }
}

void IndexStore::ReadEdgeTopoIndex(const uint64_t & trx_id, const uint64_t & begin_time,
                                   const bool & read_only, vector<eid_t> & data) {
    set<eid_t> addE_set;
    set<eid_t> delE_set;
    for (int i = 0; i < edge_update_list.size(); i++) {
        // Risk Here : what if the iteration cannot stop
        update_element up_elem = edge_update_list[i];
        if (up_elem.element_id == 0) { continue; }
        eid_t eid;
        uint2eid_t(up_elem.element_id, eid);

        if (data_storage_->CheckEdgeVisibilityWithEid(trx_id, begin_time, read_only, eid)) {
            // Visible (For Add)
            if (up_elem.isAdd) {
                addE_set.emplace(eid);
            }
        } else {
            // Invisible (For Del)
            if (!up_elem.isAdd) {
                delE_set.emplace(eid);
            }
        }
    }

    auto checkFunc = [&](eid_t& eid) {
        if (addE_set.find(eid) != addE_set.end()) {
            addE_set.erase(eid);
        }

        if (delE_set.find(eid) != delE_set.end()) {
            // Need to delete, erase
            return true;
        }
        return false;
    };

    ReaderLockGuard reader_lock_guard(edge_topo_gc_rwlock_);
    data = topo_edge_data;
    if (delE_set.size() != 0) {
        data.erase(remove_if(data.begin(), data.end(), checkFunc), data.end());
    }
    data.insert(data.end(), addE_set.begin(), addE_set.end());

    // Check Update Buffer to Read self-updated data
    up_buf_const_accessor cac;
    if (edge_update_buffers.find(cac, trx_id)) {
        for (auto & up_elem : cac->second) {
            eid_t eid;
            uint2eid_t(up_elem.element_id, eid);
            data.emplace_back(eid);
        }
    }
}

void IndexStore::get_elements_by_predicate(Element_T type, int pid,
        PredicateValue& pred, bool need_sort, vector<uint64_t>& vec) {
    unordered_map<int, index_>* m;
    tbb::concurrent_hash_map<int, map<value_t, vector<update_element>>>* up_region;
    WritePriorRWLock * rw_lock;
    if (type == Element_T::VERTEX) {
        m = &vtx_prop_index;
        up_region = &vp_update_map;
        rw_lock = &vtx_prop_gc_rwlock_;
    } else {
        m = &edge_prop_index;
        up_region = &ep_update_map;
        rw_lock = &edge_prop_gc_rwlock_;
    }

    ReaderLockGuard reader_guard_lock(*rw_lock);
    index_ &idx = (*m)[pid];

    auto &index_map = idx.index_map;
    map<value_t, set<uint64_t>>::iterator itr;
    int num_set = 0;

    // Updated Region
    prop_up_map_const_accessor pcac;
    bool isUpdated = up_region->find(pcac, pid);

    switch (pred.pred_type) {
      case Predicate_T::ANY:
        // Get All Values
        for (auto& item : index_map) {
            vec.insert(vec.end(), item.second.begin(), item.second.end());
            num_set++;
        }

        if (isUpdated) {
            for (auto & pair : pcac->second) {
                for (auto & up_elem : pair.second) {
                    read_prop_update_data(up_elem, vec);
                }
            }
        }
        break;
      case Predicate_T::NEQ:
      case Predicate_T::WITHOUT:
        // Search though whole index map to find matched values
        for (auto& item : index_map) {
            if (Evaluate(pred, &item.first)) {
                vec.insert(vec.end(), item.second.begin(), item.second.end());
                num_set++;
            }
        }

        if (isUpdated) {
            for (auto & pair : pcac->second) {
                if (Evaluate(pred, &pair.first)) {
                    for (auto & up_elem : pair.second) {
                        read_prop_update_data(up_elem, vec);
                    }
                }
            }
        }
        break;
      case Predicate_T::EQ:
        // Get elements with single value
        itr = index_map.find(pred.values[0]);
        if (itr != index_map.end()) {
            vec.assign(itr->second.begin(), itr->second.end());
            num_set++;
        }

        if (isUpdated) {
            if (pcac->second.find(pred.values[0]) != pcac->second.end()) {
                for (auto& up_elem : pcac->second.at(pred.values[0])) {
                    read_prop_update_data(up_elem, vec);
                }
            }
        }
        break;
      case Predicate_T::WITHIN:
        // Get elements with given values
        for (auto& val : pred.values) {
            itr = index_map.find(val);
            if (itr != index_map.end()) {
                vec.insert(vec.end(), itr->second.begin(), itr->second.end());
                num_set++;
            }
        }

        if (isUpdated) {
            for (auto & given_val : pred.values) {
                if (pcac->second.find(given_val) != pcac->second.end()) {
                    for (auto& up_elem : pcac->second.at(given_val)) {
                        read_prop_update_data(up_elem, vec);
                    }
                }
            }
        }
        break;
      case Predicate_T::NONE:
        // Get elements from no_key_store
        vec.assign(idx.no_key.begin(), idx.no_key.end());

        if (isUpdated) {
            for (auto & pair : pcac->second) {
                for (auto & up_elem : pair.second) {
                    uint64_t id = up_elem.element_id;
                    if (up_elem.update_type == PropertyUpdateT::DROP) {
                        vec.emplace_back(id);
                    } else if (up_elem.update_type == PropertyUpdateT::ADD) {
                        vector<uint64_t>::iterator itr = find(vec.begin(), vec.end(), id);
                        if (itr != vec.end()) {
                            vec.erase(itr);
                        }
                    }
                }
            }
        }
        num_set++;
        break;

      case Predicate_T::OUTSIDE:
        // find less than
        pred.pred_type = Predicate_T::LT;
        build_range_elements(index_map, pred, vec, num_set);
        // find greater than
        pred.pred_type = Predicate_T::GT;
        swap(pred.values[0], pred.values[1]);
        build_range_elements(index_map, pred, vec, num_set);
        break;
      default:
        // LT, LTE, GT, GTE, BETWEEN, INSIDE
        build_range_elements(index_map, pred, vec, num_set);
        break;
    }

    // TODO: OLTP Situation always needs sort
    if (need_sort && num_set > 1) {
        sort(vec.begin(), vec.end());
    }
}

void IndexStore::read_prop_update_data(const update_element & up_elem, vector<uint64_t> & vec) {
    if (up_elem.element_id == 0) { return; }
    vector<uint64_t>::iterator itr = find(vec.begin(), vec.end(), up_elem.element_id);
    if (up_elem.isAdd) {
        if (itr == vec.end()) {
            vec.emplace_back(up_elem.element_id);
        }
    } else {
        if (itr != vec.end()) {
            vec.erase(itr);
        }
    }
}

uint64_t IndexStore::get_count_by_predicate(Element_T type, int pid, PredicateValue& pred) {
    unordered_map<int, index_>* m;
    if (type == Element_T::VERTEX) {
        m = &vtx_prop_index;
    } else {
        m = &edge_prop_index;
    }

    index_ &idx = (*m)[pid];
    auto& count_map = idx.count_map;
    uint64_t count = 0;

    map<value_t, uint64_t>::iterator itr;
    switch (pred.pred_type) {
      case Predicate_T::ANY:
        count = idx.total - idx.no_key.size();
        break;
      case Predicate_T::NEQ:
      case Predicate_T::WITHOUT:
        // Search though whole index map to find matched values
        for (auto& item : count_map) {
            if (Evaluate(pred, &item.first)) {
                count += item.second;
            }
        }
        break;
      case Predicate_T::EQ:
        // Get elements with single value
        itr = count_map.find(pred.values[0]);
        if (itr != count_map.end()) {
            count += itr->second;
        }
        break;
      case Predicate_T::WITHIN:
        // Get elements with given values
        for (auto& val : pred.values) {
            itr = count_map.find(val);
            if (itr != count_map.end()) {
                count += itr->second;
            }
        }
        break;
      case Predicate_T::NONE:
        // Get elements from no_key_store
        count += idx.no_key.size();
        break;

      case Predicate_T::OUTSIDE:
        // find less than
        pred.pred_type = Predicate_T::LT;
        build_range_count(count_map, pred, count);
        // find greater than
        pred.pred_type = Predicate_T::GT;
        swap(pred.values[0], pred.values[1]);
        build_range_count(count_map, pred, count);
        break;
      default:
        // LT, LTE, GT, GTE, BETWEEN, INSIDE
        build_range_count(count_map, pred, count);
        break;
    }
    return count;
}

void IndexStore::build_range_count(map<value_t, uint64_t>& m, PredicateValue& pred, uint64_t& count) {
    map<value_t, uint64_t>::iterator itr_low;
    map<value_t, uint64_t>::iterator itr_high;

    build_range(m, pred, itr_low, itr_high);

    for (auto itr = itr_low; itr != itr_high; itr++) {
        count += itr->second;
    }
}

void IndexStore::build_range_elements(map<value_t, set<uint64_t>>& m, PredicateValue& pred,
        vector<uint64_t>& vec, int& num_set) {
    map<value_t, set<uint64_t>>::iterator itr_low;
    map<value_t, set<uint64_t>>::iterator itr_high;

    build_range(m, pred, itr_low, itr_high);

    for (auto itr = itr_low; itr != itr_high; itr++) {
        vec.insert(vec.end(), itr->second.begin(), itr->second.end());
        num_set++;
    }
}

void IndexStore::build_topo_data() {
    uint64_t start_t, end_t;

    start_t = timer::get_usec();
    // Build Vertex Init Data
    data_storage_->GetAllVertices(0, 0, true, topo_vtx_data);
    end_t = timer::get_usec();
    cout << "[InitData] Got Vertex with size " << topo_vtx_data.size() << endl;
    cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for Building InitVData in init_actor" << endl;

    start_t = timer::get_usec();
    // Build Edge Init Data
    data_storage_->GetAllEdges(0, 0, true, topo_edge_data);
    end_t = timer::get_usec();
    cout << "[InitData] Got Egde with size " << topo_edge_data.size() << endl;
    cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for Building InitEData in init_actor" << endl;
}

// id: uint64_t(vid), uint64_t(eid)
// val_value_t: value_t(property_value)
void IndexStore::modify_index(index_ * idx, uint64_t& id, value_t& val_value_t, bool isAdd) {
    if (isAdd) {
        // index_.index_map
        if (idx->index_map.find(val_value_t) != idx->index_map.end()) {
            idx->index_map.at(val_value_t).emplace(id);
        } else {
            idx->index_map.emplace(val_value_t, set<uint64_t>{id});
        }

        // index_.count_map
        if (idx->count_map.find(val_value_t) != idx->count_map.end()) {
            idx->count_map.at(val_value_t)++;
        } else {
            idx->count_map.emplace(val_value_t, 1);
        }
    } else {
        bool exists = false;
        // index_.index_map
        if (idx->index_map.find(val_value_t) != idx->index_map.end()) {
            idx->index_map.at(val_value_t).erase(id);
        }

        // index_.count_map
        if (idx->count_map.find(val_value_t) != idx->count_map.end() && exists) {
            idx->count_map.at(val_value_t)--;
        }
    }
}
