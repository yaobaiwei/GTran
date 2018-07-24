/*
 * actor_cache.hpp
 *
 *  Created on: July 23, 2018
 *      Author: Aaron LI 
 */
#ifndef ACTOR_CACHE_HPP_
#define ACTOR_CACHE_HPP_

#include <string>
#include <vector>
#include <type_traits>
#include <pthread.h>

#include "core/message.hpp"

class ActorCache {

public:

	bool get_label_from_cache(uint64_t id, label_t & label) {
		value_t val;
		if (!lookup(id, val)) {
			return false;
		}

		label = Tool::value_t2int(val);
		return true;
	}

	bool get_property_from_cache(uint64_t id, value_t & val) {
		if (!lookup(id, val)) {
			return false;
		}
		return true;
	}

	void insert_properties(uint64_t id, value_t & val) {
		insert(id, val);
	}

	void insert_label(uint64_t id, label_t & label) {
		value_t val;
		Tool::str2int(to_string(label), val);

		insert(id, val);
	}

private:

	struct CacheItem {
		pthread_spinlock_t lock;
		uint64_t id; // epid_t, vpid_t, eid_t, vid_t
		value_t value; // properties or labels

		CacheItem() {
			pthread_spin_init(&lock, 0);
		}
	};

	static const int NUM_CACHE = 10000;
	CacheItem items[NUM_CACHE];

	bool lookup(uint64_t id, value_t & val) {
		bool isFound = false;

		int key = mymath::hash_u64(id) % NUM_CACHE;

		pthread_spin_lock(&(items[key].lock));
		if (items[key].id == id) {
			val = items[key].value;
			isFound = true;
		}
		pthread_spin_unlock(&(items[key].lock));

		return isFound;
	}

	void insert(uint64_t id, value_t & val) {
		int key = mymath::hash_u64(id) % NUM_CACHE;
		pthread_spin_lock(&(items[key].lock));
		items[key].id = id;
		items[key].value = val;
		pthread_spin_unlock(&(items[key].lock));
	}

};

#endif /* ACTOR_CACHE_HPP_ */
