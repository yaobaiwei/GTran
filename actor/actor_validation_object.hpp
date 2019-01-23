/*
 * actor_validation_object.hpp
 *
 *  Created on: Dec 17, 2018
 *      Author: Aaron LI 
 */
#ifndef ACTOR_VALID_HPP_
#define ACTOR_VALID_HPP_

#include <string>
#include <vector>
#include <type_traits>
#include <tbb/concurrent_hash_map.h>

#include "base/type.hpp"

class ActorValidationObject {

public : 
	ActorValidationObject(){}

	void RecordInputSet(uint64_t TransactionID, int step_num, short data_type, vector<uint64_t> & input_set, bool recordALL){
		// Get TrxStepKey
		validation_record_key_t key(TransactionID, step_num);

		// Insert Trx2Step map
		{
			trx2step_accessor tac;
			trx_to_step_table.insert(tac, TransactionID);
			tac->second.emplace_back(step_num);
		}

		// Insert data
		data_accessor dac;
		validation_data.insert( dac, key );
		dac->second.isAll = recordALL;
		if (!recordALL)
			dac->second.data.insert(dac->second.data.end(), input_set.begin(), input_set.end());
	}

	// Return Value : 
	// 	True --> no conflict
	// 	False --> conflict, do dependency check
	bool Validate(uint64_t TransactionID, int step_num, vector<uint64_t> & recv_set){
		validation_record_key_t key(TransactionID, step_num);
		for (auto & item : recv_set) {
			uint64_t inv = item >> VID_BITS;
			uint64_t outv = item - (inv << VID_BITS);

			{
				data_const_accessor daca;
				if (validation_data.find(daca, key)) {
					return true; // Did not find input_set --> validation success
				}

				if (daca->second.isAll) {
					return false; // Definitely in input_set
				}

				// Once found match, conflict 
				for (auto & record_item : daca->second.data) {
					if (item == record_item || inv == record_item || outv == record_item) {
						return false;
					}
				}
			}
		}
		return true;
	}

	void DeleteInputSet(uint64_t TransactionID){
		// Actually if delete, there is definitely no other access and insert.
		trx2step_const_accessor tcac;
		if (trx_to_step_table.find(tcac, TransactionID)) {
			for (auto & step : tcac->second) {
				validation_record_key_t key( TransactionID, step );	
				validation_data.erase(key);	
			}

			trx_to_step_table.erase(TransactionID);
		}
	}

private : 

	// TransactionID -> <step>
	// <TrxID, step> -> <value>
	//
	struct validation_record_key_t {
		uint64_t trxID;	
		int step_num;

		validation_record_key_t() : trxID(0), step_num(0) {}
		validation_record_key_t(uint64_t _trxID, int _step_num) : trxID(_trxID), step_num(_step_num) {}

		bool operator==(const validation_record_key_t & key) const {
			if ((trxID == key.trxID) && (step_num == key.step_num)) {
				return true;
			}
			return false;
		}

	};

	struct validation_record_hash_compare {
		static size_t hash (const validation_record_key_t& key) {
			uint64_t k1 = (uint64_t)key.trxID;
			int k2 = (int)key.step_num;
			size_t seed = 0;
			mymath::hash_combine(seed, k1);
			mymath::hash_combine(seed, k2);
			return seed;
		}

		static bool equal (const validation_record_key_t& key1, const validation_record_key_t& key2) {
			return (key1 == key2);
		}
	};

	struct validation_record_val_t {
		// If isAll = true; data will be empty;
		vector<uint64_t> data;
		bool isAll;

		validation_record_val_t() {}
	};

	tbb::concurrent_hash_map<uint64_t, vector<int>> trx_to_step_table;
	typedef tbb::concurrent_hash_map<uint64_t, vector<int>>::accessor trx2step_accessor;
	typedef tbb::concurrent_hash_map<uint64_t, vector<int>>::const_accessor trx2step_const_accessor;

	tbb::concurrent_hash_map<validation_record_key_t, validation_record_val_t, validation_record_hash_compare> validation_data;
	typedef tbb::concurrent_hash_map<validation_record_key_t, validation_record_val_t, validation_record_hash_compare>::accessor data_accessor;
	typedef tbb::concurrent_hash_map<validation_record_key_t, validation_record_val_t, validation_record_hash_compare>::const_accessor data_const_accessor;

};

#endif /* ACTOR_VALID_HPP_ */
