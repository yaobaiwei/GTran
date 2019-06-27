/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron LI (cjli@cse.cuhk.edu.hk)
*/

#include "layout/index_store.hpp"

void IndexStore::Init() {
    build_topo_data();
}

bool IndexStore::IsIndexEnabled(Element_T type, int pid, PredicateValue* pred = NULL, uint64_t* count = NULL) {
    if (config_->global_enable_indexing) {
        unordered_map<int, index_>* m;
        if (type == Element_T::VERTEX) {
            m = &vtx_prop_index;
        } else {
            m = &edge_prop_index;
        }

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
//  no_key_vec:        vector of elements that have no provided key
bool IndexStore::SetIndexMap(Element_T type, int pid, map<value_t, vector<value_t>>& index_map, vector<value_t>& no_key_vec) {
    if (config_->global_enable_indexing) {
        unordered_map<int, index_>* m;
        if (type == Element_T::VERTEX) {
            m = &vtx_prop_index;
        } else {
            m = &edge_prop_index;
        }

        index_ &idx = (*m)[pid];

        uint64_t sum = 0;
        // sort each vector for better searching performance
        for (auto& item : index_map) {
            set<value_t> temp(std::make_move_iterator(item.second.begin()),
                            std::make_move_iterator(item.second.end()));
            uint64_t count = temp.size();
            sum += count;
            auto itr = idx.index_map.insert(idx.index_map.end(), {move(item.first), move(temp)});
            idx.count_map[item.first] = count;
            idx.values.push_back(&(itr->first));
        }

        idx.no_key = set<value_t>(make_move_iterator(no_key_vec.begin()), make_move_iterator(no_key_vec.end()));
        sum += idx.no_key.size();
        idx.total = sum;
        return true;
    }
    return false;
}

bool IndexStore::SetIndexMapEnable(Element_T type, int pid, bool inverse = false) {
    if (config_->global_enable_indexing) {
        unordered_map<int, index_>* m;
        if (type == Element_T::VERTEX) {
            m = &vtx_prop_index;
        } else {
            m = &edge_prop_index;
        }

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

void IndexStore::GetElements(Element_T type, vector<pair<int, PredicateValue>>& pred_chain, vector<value_t>& data) {
    bool is_first = true;
    bool need_sort = pred_chain.size() != 1;
    for (auto& pred_pair : pred_chain) {
        vector<value_t> vec;
        // get sorted vector of all elements satisfying current predicate
        get_elements_by_predicate(type, pred_pair.first, pred_pair.second, need_sort, vec);

        if (is_first) {
            data.swap(vec);
            is_first = false;
        } else {
            vector<value_t> temp;
            // do intersection with previous result
            // temp is sorted after intersection
            set_intersection(make_move_iterator(data.begin()), make_move_iterator(data.end()),
                            make_move_iterator(vec.begin()), make_move_iterator(vec.end()),
                            back_inserter(temp));
            data.swap(temp);
        }
    }
}

bool IndexStore::GetRandomValue(Element_T type, int pid, int rand_seed, string& value_str) {
    unordered_map<int, index_>* m;
    if (type == Element_T::VERTEX) {
        m = &vtx_prop_index;
    } else {
        m = &edge_prop_index;
    }

    auto itr = m->find(pid);
    if (itr == m->end()) {
        return false;
    }

    index_ &idx = itr->second;
    int size = idx.values.size();
    if (size == 0) {
        return false;
    }

    // get value string
    value_t v = *idx.values[rand_seed % size];
    value_str = Tool::DebugString(v);
    return true;
}

void IndexStore::InsertToUpdateBuffer(const uint64_t& trx_id, vector<uint64_t>& val_list, ID_T type, bool isAdd, TRX_STAT stat) {
    vector<update_element> up_list;
    for (auto & val : val_list) {
        up_list.emplace_back(val, isAdd, stat);
    }

    up_buf_accessor ac;
    bool found = false;
    if (type == ID_T::VID) {
        if(!vtx_update_buffers.find(ac, trx_id)) { vtx_update_buffers.insert(ac, trx_id); }
    } else if (type == ID_T::EID) {
        if(!edge_update_buffers.find(ac, trx_id)) { edge_update_buffers.insert(ac, trx_id); }
    } else if (type == ID_T::VPID) {
        if(!vp_update_buffers.find(ac, trx_id)) { vp_update_buffers.insert(ac, trx_id); }
    } else if (type == ID_T::EPID) {
        if(!ep_update_buffers.find(ac, trx_id)) { ep_update_buffers.insert(ac, trx_id); }
    } else {
        cout << "[Index Store] Unexpected element type in InsertToBuffer()" << endl;
    }

    // Append new data
    ac->second.insert(ac->second.end(), up_list.begin(), up_list.end());
}

void IndexStore::InsertToUpdateRegion(vector<uint64_t>& val_list, ID_T type, bool isAdd, TRX_STAT stat) {
    for (auto & val : val_list) {
        update_element up_elem(val, isAdd, stat);
        insert_to_update_region(up_elem, type);
    }
}

void IndexStore::MoveBufferToRegion(const uint64_t & trx_id) {
    up_buf_const_accessor cac;
    // V
    if(vtx_update_buffers.find(cac, trx_id)) {
        for (auto & up_elem : cac->second) {
            vtx_update_list.emplace_back(up_elem);
        }
        vtx_update_buffers.erase(cac);
    }

    // E
    if(edge_update_buffers.find(cac, trx_id)) {
        for (auto & up_elem : cac->second) {
            edge_update_list.emplace_back(up_elem);
        }
        edge_update_buffers.erase(cac);
    }

    // VP
    if(vtx_update_buffers.find(cac, trx_id)) {
        for (auto & up_elem : cac->second) {
            // TODO
        }
    }

    // EP
    if(vtx_update_buffers.find(cac, trx_id)) {
        for (auto & up_elem : cac->second) {
            // TODO
        }
    }
}

void IndexStore::ReadVtxTopoIndex(const uint64_t & trx_id, const uint64_t & begin_time,
                                  const bool & read_only, vector<vid_t> & data) {
    vector<vid_t> addV_vec;
    set<vid_t> delV_set;
    for (int i = 0; i < vtx_update_list.size(); i++) {
        // Risk Here : what if the iteration cannot stop
        update_element up_elem = vtx_update_list[i];
        vid_t vid;
        uint2vid_t(up_elem.element_id, vid);
        if (data_storage_->CheckVertexVisibility(trx_id, begin_time, read_only, vid)) {
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

    data = topo_vtx_data;
    if (delV_set.size() != 0) {
        data.erase(remove_if(data.begin(), data.end(), checkFunc), data.end());
    }
    data.insert(data.end(), addV_vec.begin(), addV_vec.end());

    // Check Update Buffer to Read self-updated data
    up_buf_const_accessor cac;
    if(vtx_update_buffers.find(cac, trx_id)) {
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
        eid_t eid;
        uint2eid_t(up_elem.element_id, eid);
        if (data_storage_->CheckEdgeVisibility(trx_id, begin_time, read_only, eid)) {
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

    data = topo_edge_data;
    if (delE_set.size() != 0) {
        data.erase(remove_if(data.begin(), data.end(), checkFunc), data.end());
    }
    data.insert(data.end(), addE_set.begin(), addE_set.end());

    // Check Update Buffer to Read self-updated data
    up_buf_const_accessor cac;
    if(edge_update_buffers.find(cac, trx_id)) {
        for (auto & up_elem : cac->second) {
            eid_t eid;
            uint2eid_t(up_elem.element_id, eid);
            data.emplace_back(eid);
        }
    }
}

void IndexStore::insert_to_update_region(update_element & up_elem, ID_T type) {
    if (type == ID_T::VID) {
        vtx_update_list.emplace_back(up_elem); 
    } else if (type == ID_T::EID) {
        edge_update_list.emplace_back(up_elem); 
    } else if (type == ID_T::VPID) {
        // TODO
    } else if (type == ID_T::EPID) {
        // TODO
    } else {
        cout << "[Index Store] Unexpected element type in InsertUpdatedData()" << endl;
    }
}

void IndexStore::get_elements_by_predicate(Element_T type, int pid, PredicateValue& pred, bool need_sort, vector<value_t>& vec) {
    unordered_map<int, index_>* m;
    if (type == Element_T::VERTEX) {
        m = &vtx_prop_index;
    } else {
        m = &edge_prop_index;
    }

    index_ &idx = (*m)[pid];

    auto &index_map = idx.index_map;
    map<value_t, set<value_t>>::iterator itr;
    int num_set = 0;

    switch (pred.pred_type) {
      case Predicate_T::ANY:
        // Search though whole index map
        for (auto& item : index_map) {
            vec.insert(vec.end(), item.second.begin(), item.second.end());
            num_set++;
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
        break;
      case Predicate_T::EQ:
        // Get elements with single value
        itr = index_map.find(pred.values[0]);
        if (itr != index_map.end()) {
            vec.assign(itr->second.begin(), itr->second.end());
            num_set++;
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
        break;
      case Predicate_T::NONE:
        // Get elements from no_key_store
        vec.assign(idx.no_key.begin(), idx.no_key.end());
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

    if (need_sort && num_set > 1) {
        sort(vec.begin(), vec.end());
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

void IndexStore::build_range_elements(map<value_t, set<value_t>>& m, PredicateValue& pred, vector<value_t>& vec, int& num_set) {
    map<value_t, set<value_t>>::iterator itr_low;
    map<value_t, set<value_t>>::iterator itr_high;

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
