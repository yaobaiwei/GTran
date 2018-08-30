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

	//	type: 		VERTEX / EDGE
	//	labels:		labels to extract
	//	isInit:		If isInit, return all results from index map
	//				Else do intersection between input and result
	// 	vec:		If isInit, vec is Empty
	//				Else vec contains vtx/edge id list
	bool GetElementByLabel(Element_T type, vector<value_t>& labels, bool& isInit, vector<pair<history_t, vector<value_t>>>& vec){
		if(IsIndexEnabled(type, 0)){
			map<value_t, set<value_t>>* m;

			// get index map according to element type
			if(type == Element_T::VERTEX){
				m = &vtx_index_store[0];
			}else{
				m = &edge_index_store[0];
			}

			// get all elements with provided labels
			vector<value_t> temp;
			for(auto& item : labels){
				auto itr = m->find(item);
				if(itr != m->end()){
					temp.insert(temp.end(), itr->second.begin(), itr->second.end());
				}
			}

			if(isInit){
				vec.clear();
				vec.emplace_back(history_t(), move(temp));
				isInit = false;
			}else{
				sort(temp.begin(),temp.end());
				for(auto& data_pair : vec){
					sort(data_pair.second.begin(), data_pair.second.end());
					vector<value_t> temp_store;
					// TODO: should not use intersection as it will do dedup
					set_intersection(make_move_iterator(data_pair.second.begin()), make_move_iterator(data_pair.second.end()),
									make_move_iterator(temp.begin()), make_move_iterator(temp.end()),
									std::back_inserter(temp_store));

					data_pair.second.swap(temp_store);
				}
			}

			return true;
		}
		return false;
	}

	//	type: 			VERTEX / EDGE
	//	property_key:	key
	// 	pred:			PredicateValue, defines behaviour of searching index map
	//	isInit:			If isInit, return all results from index map
	//					Else do intersection between input and result
	// 	vec:			If isInit, vec is Empty
	//					Else vec contains vtx/edge id list
	bool GetElementByProperty(Element_T type, int property_key, PredicateValue& pred, bool& isInit, vector<pair<history_t, vector<value_t>>>& vec){
		if(IsIndexEnabled(type, property_key)){
			map<value_t, set<value_t>>* m;
			set<value_t>* s;

			// get index map and elements without provided key according to element type
			if(type == Element_T::VERTEX){
				m = &vtx_index_store[property_key];
				s = &vtx_index_store_no_key[property_key];
			}else{
				m = &edge_index_store[property_key];
				s = &edge_index_store_no_key[property_key];
			}

			Predicate_T real_type = pred.pred_type;
			if(!isInit){
				// For follwing predicate, switch to opposite type and do difference
				switch(real_type){
				case Predicate_T::ANY:
					pred.pred_type = Predicate_T::NONE;
				case Predicate_T::NEQ:
					pred.pred_type = Predicate_T::EQ;
				case Predicate_T::OUTSIDE:
					pred.pred_type = Predicate_T::BETWEEN;
				case Predicate_T::WITHOUT:
					pred.pred_type = Predicate_T::WITHIN;
				}
			}

			vector<value_t> temp;
			map<value_t, set<value_t>>::iterator itr;
			switch (pred.pred_type) {
			case Predicate_T::EQ:
				// Get elements with single value
				itr = m->find(pred.values[0]);
				if(itr != m->end()){
					temp.assign(itr->second.begin(), itr->second.end());
				}
				break;
			case Predicate_T::WITHIN:
				// Get elements with given values
				for(auto& val : pred.values){
					itr = m->find(val);
					if(itr != m->end()){
						temp.insert(temp.end(), itr->second.begin(), itr->second.end());
					}
				}
				break;
			case Predicate_T::NONE:
				// Get elements from no_key_store
				temp.assign(s->begin(), s->end());
				break;
			case Predicate_T::ANY:
			case Predicate_T::NEQ:
			case Predicate_T::WITHOUT:
				// Search though whole index map to find matched values
				for(auto& item : *m){
					if(Evaluate(pred, &item.first)){
						temp.insert(temp.end(), item.second.begin(), item.second.end());
					}
				}
				break;
			case Predicate_T::OUTSIDE:
				// find less than
				pred.pred_type = Predicate_T::LT;
				build_range(m, pred, temp);
				// find greater than
				pred.pred_type = Predicate_T::GT;
				swap(pred.values[0], pred.values[1]);
				build_range(m, pred, temp);
				break;
			default:
				// LT, LTE, GT, GTE, BETWEEN, INSIDE
				build_range(m, pred, temp);
				break;
			}

			if(isInit){
				vec.clear();
				vec.emplace_back(history_t(), move(temp));
				isInit = false;
			}else{
				sort(temp.begin(),temp.end());
				for(auto& data_pair : vec){
					sort(data_pair.second.begin(), data_pair.second.end());
					vector<value_t> temp_store;
					if(pred.pred_type == real_type){
						// TODO: should not use intersection as it will do dedup
						set_intersection(make_move_iterator(data_pair.second.begin()), make_move_iterator(data_pair.second.end()),
										make_move_iterator(temp.begin()), make_move_iterator(temp.end()),
										std::back_inserter(temp_store));
					}else{
						set_difference(make_move_iterator(data_pair.second.begin()), make_move_iterator(data_pair.second.end()),
										make_move_iterator(temp.begin()), make_move_iterator(temp.end()),
										std::back_inserter(temp_store));
					}
					data_pair.second.swap(temp_store);
				}
			}
			return true;
		}
		return false;
	}

	// Get predicate priority for parser re-ordering
	int PredicatePriority(Predicate_T type, bool isInit){
		switch(type){
		// single known value
		case Predicate_T::EQ:
			return 1;
		case Predicate_T::NEQ:
			return isInit ? 9 : 1;
		// mulitple known values
		case Predicate_T::WITHIN:
			return 2;
		case Predicate_T::WITHOUT:
			return isInit ? 8 : 2;
		// single set
		case Predicate_T::NONE:
			return 3;
		case Predicate_T::ANY:
			return isInit ? 7 : 3;
		// bounded range
		case Predicate_T::INSIDE:
		case Predicate_T::BETWEEN:
			return 4;
		case Predicate_T::OUTSIDE:
			return isInit ? 6 : 4;
		// unbounded range
		case Predicate_T::LT:
		case Predicate_T::LTE:
		case Predicate_T::GT:
		case Predicate_T::GTE:
			return 5;
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

	void build_range(map<value_t, set<value_t>>* m, PredicateValue& pred, vector<value_t>& vec){
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
		}
	}
};
