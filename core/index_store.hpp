/*
 * index_store.hpp
 *
 *  Created on: Aug 27, 2018
 *      Author: Nick Fang
 */
#include <unordered_map>

#include "base/type.hpp"
#include "base/predicate.hpp"
#include "core/message.hpp"
#include "utils/config.hpp"

#pragma once

class IndexStore {
public:
	IndexStore(Config * config) : config_(config){}

	bool IsIndexEnabled(Element_T type, int property_key){
		if(config_->global_enable_indexing){
			unordered_map<int, bool>* m;
			if(type == Element_T::VERTEX){
				m = &vtx_index_enable;
			}else{
				m = &edge_index_enable;
			}

			thread_mutex_.lock();
			// check property key
			auto itr = m->find(property_key);
			if(itr == m->end()){
				itr = m->insert(itr, {property_key, false});
			}
			thread_mutex_.unlock();
			return itr->second;
		}
		return false;
	}

	//	type: 			VERTEX / EDGE
	//	property_key: 	key
	//	index_map:		alreay constructed index map
	//  no_key_vec:		vector of elements that have no provided key
	bool SetIndexMap(Element_T type, int property_key, map<value_t, vector<value_t>>& index_map, vector<value_t>& no_key_vec){
		if(config_->global_enable_indexing){
			map<value_t, set<value_t>>* m;
			set<value_t>* s;
			if(type == Element_T::VERTEX){
				m = &vtx_index_store[property_key];
				s = &vtx_index_store_no_key[property_key];
			}else{
				m = &edge_index_store[property_key];
				s = &edge_index_store_no_key[property_key];
			}

			// sort each vector for better searching performance
			for(auto& item : index_map){
				set<value_t> temp(std::make_move_iterator(item.second.begin()), std::make_move_iterator(item.second.end()));
				(*m)[item.first] = move(temp);
			}

			*s = set<value_t>(make_move_iterator(no_key_vec.begin()),make_move_iterator(no_key_vec.end()));
			return true;
		}
		return false;
	}

	bool SetIndexMapEnable(Element_T type, int property_key){
		if(config_->global_enable_indexing){
			thread_mutex_.lock();
			if(type == Element_T::VERTEX){
				vtx_index_enable[property_key] = true;
			}else{
				edge_index_enable[property_key] = true;
			}
			thread_mutex_.unlock();
			return true;
		}
		return false;
	}

	void GetElements(Element_T type, vector<pair<int, PredicateValue>>& pred_chain, vector<value_t>& data){
		bool is_first = true;
		bool need_sort = pred_chain.size() != 1;
		for(auto& pred_pair : pred_chain){
			vector<value_t> vec;
			// get sorted vector of all elements satisfying current predicate
			get_by_predicate(type, pred_pair.first, pred_pair.second, need_sort, vec);

			if(is_first){
				data.swap(vec);
				is_first = false;
			}else{
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

private:
	Config * config_;

	mutex thread_mutex_;

	unordered_map<int, bool> vtx_index_enable;
	unordered_map<int, bool> edge_index_enable;

	unordered_map<int, map<value_t, set<value_t>>> vtx_index_store;
	unordered_map<int, map<value_t, set<value_t>>> edge_index_store;

	unordered_map<int, set<value_t>> vtx_index_store_no_key;
	unordered_map<int, set<value_t>> edge_index_store_no_key;

	void get_by_predicate(Element_T type, int pid, PredicateValue& pred, bool need_sort, vector<value_t>& vec){
		map<value_t, set<value_t>>* m;
		set<value_t>* s;

		if(type == Element_T::VERTEX){
			m = &vtx_index_store[pid];
			s = &vtx_index_store_no_key[pid];
		}else{
			m = &edge_index_store[pid];
			s = &edge_index_store_no_key[pid];
		}

		map<value_t, set<value_t>>::iterator itr;
		int num_set = 0;

		switch (pred.pred_type) {
		case Predicate_T::ANY:
			// Search though whole index map
			for(auto& item : *m){
				vec.insert(vec.end(), item.second.begin(), item.second.end());
				num_set++;
			}
			break;
		case Predicate_T::NEQ:
		case Predicate_T::WITHOUT:
			// Search though whole index map to find matched values
			for(auto& item : *m){
				if(Evaluate(pred, &item.first)){
					vec.insert(vec.end(), item.second.begin(), item.second.end());
					num_set++;
				}
			}
			break;
		case Predicate_T::EQ:
			// Get elements with single value
			itr = m->find(pred.values[0]);
			if(itr != m->end()){
				vec.assign(itr->second.begin(), itr->second.end());
				num_set ++;
			}
			break;
		case Predicate_T::WITHIN:
			// Get elements with given values
			for(auto& val : pred.values){
				itr = m->find(val);
				if(itr != m->end()){
					vec.insert(vec.end(), itr->second.begin(), itr->second.end());
					num_set++;
				}
			}
			break;
		case Predicate_T::NONE:
			// Get elements from no_key_store
			vec.assign(s->begin(), s->end());
			num_set++;
			break;

		case Predicate_T::OUTSIDE:
			// find less than
			pred.pred_type = Predicate_T::LT;
			build_range(m, pred, vec, num_set);
			// find greater than
			pred.pred_type = Predicate_T::GT;
			swap(pred.values[0], pred.values[1]);
			build_range(m, pred, vec, num_set);
			break;
		default:
			// LT, LTE, GT, GTE, BETWEEN, INSIDE
			build_range(m, pred, vec, num_set);
			break;
		}

		if(need_sort && num_set > 1){
			sort(vec.begin(),vec.end());
		}
	}

	void build_range(map<value_t, set<value_t>>* m, PredicateValue& pred, vector<value_t>& vec, int& num_set){
		map<value_t, set<value_t>>::iterator itr_low = m->begin();
		map<value_t, set<value_t>>::iterator itr_high = m->end();

		// get lower bound
		switch (pred.pred_type) {
		case Predicate_T::GT:
		case Predicate_T::GTE:
		case Predicate_T::INSIDE:
		case Predicate_T::BETWEEN:
			itr_low = m->lower_bound(pred.values[0]);
		}

		// remove "EQ"
		switch (pred.pred_type) {
		case Predicate_T::GT:
		case Predicate_T::INSIDE:
			if(itr_low != m->end() && itr_low->first == pred.values[0]){
				itr_low ++;
			}
		}

		int param = 1;
		// get upper_bound
		switch (pred.pred_type) {
		case Predicate_T::LT:
		case Predicate_T::LTE:
			param = 0;
		case Predicate_T::INSIDE:
		case Predicate_T::BETWEEN:
			itr_high = m->upper_bound(pred.values[param]);
		}

		// remove "EQ"
		switch (pred.pred_type) {
		case Predicate_T::LT:
		case Predicate_T::INSIDE:
			// exclude last one if match
			if(itr_high != itr_low){
				itr_high --;
				if(itr_high->first != pred.values[param]){
					itr_high ++;
				}
			}
		}

		for(auto itr = itr_low; itr != itr_high; itr ++){
			vec.insert(vec.end(), itr->second.begin(), itr->second.end());
			num_set ++;
		}
	}
};
