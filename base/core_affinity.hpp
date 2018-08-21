#ifndef CORE_AFFINITY_HPP_
#define CORE_AFFINITY_HPP_

#include <fstream>
#include <algorithm>

#include <hwloc.h>
#include <math.h>
#include <boost/algorithm/string/predicate.hpp>

#include "base/type.hpp"
#include "utils/global.hpp"

using namespace std;

/*
 * $numactl --hardware
 * available: 2 nodes (0-1)
 * node 0 cpus: 0 2 4 6 8 10 12 14 16 18 20 22
 * node 0 size: 24530 MB
 * node 0 free: 15586 MB
 * node 1 cpus: 1 3 5 7 9 11 13 15 17 19 21 23
 * node 1 size: 24576 MB
 * node 1 free: 20081 MB
 *
 * node distances:
 * node   0   1 
 *   0:  10  20 
 *   1:  20  10
 */

class CoreAffinity {
public:

	CoreAffinity (int world_rank) : world_rank_(world_rank) {}

	void Init(int num_thread) {
		load_node_topo();
		// Calculate Number of threads for each thread division
		get_num_thread_for_each_division(num_thread);

		// Assign thread id to each division according to last function
		get_core_id_for_each_division();

		// Pair Actor Type and ActorDivisionType 
		load_actor_division();

		// Pair CoreId and ThreadId
		load_core_to_thread_map();

		// Assign stealing list to each thread
		load_steal_list();

		// Test 
		if (world_rank_ == 1) {
			cout << "HAS : " << GetThreadIdForActor(ACTOR_T::HAS) << endl;
			cout << "HAS : " << GetThreadIdForActor(ACTOR_T::HAS) << endl;
			cout << "HAS : " << GetThreadIdForActor(ACTOR_T::HAS) << endl;
			cout << "HAS : " << GetThreadIdForActor(ACTOR_T::HAS) << endl;
			cout << "HAS : " << GetThreadIdForActor(ACTOR_T::HAS) << endl;

			cout << "GROUP : " << GetThreadIdForActor(ACTOR_T::GROUP) << endl;
			cout << "GROUP : " << GetThreadIdForActor(ACTOR_T::GROUP) << endl;
			cout << "GROUP : " << GetThreadIdForActor(ACTOR_T::GROUP) << endl;
			cout << "GROUP : " << GetThreadIdForActor(ACTOR_T::GROUP) << endl;
			cout << "GROUP : " << GetThreadIdForActor(ACTOR_T::GROUP) << endl;
			cout << "GROUP : " << GetThreadIdForActor(ACTOR_T::GROUP) << endl;
			cout << "GROUP : " << GetThreadIdForActor(ACTOR_T::GROUP) << endl;
		}
	}

	/* 
	 * For Master : 
	 * 	   Thread Listener : bind to thread 0;
	 * 	   Thread Process bind to thread 1;
	 * 	   Main Thread : No need to bind;
	 * 
	 * For Worker :
	 * 	   Main Thread : bind to thread [offset + num_thread]
	 * 	   Thread RecvReq : bind to thread [offset + num_thread]
	 * 	   Thread SendQueryMsg : bind to thread [offset + num_thread]
	 * 	   Thread Monitor::ProgressReport : bind to thread [offset + num_thread]
	 * 	   Thread pool for actors : bind to thread [0, num_threads]
	 */
	bool BindToCore(int tid)
	{
		size_t core = (size_t)thread_to_core_map[tid];
		if (world_rank_ == 1) cout << to_string(core) << endl;
		cpu_set_t mask;
		CPU_ZERO(&mask);
		CPU_SET(core, &mask);
		if (sched_setaffinity(0, sizeof(mask), &mask) != 0)
			cout << "Failed to set affinity (core: " << core << ")" << endl;
	}

	void GetStealList (int tid, vector<int> & list) {
		int core_id = thread_to_core_map[tid];
		for (auto itr = stealing_table[core_id].begin(); itr != stealing_table[core_id].end(); itr++) {
			list.push_back(core_to_thread_map[*itr]);
		}
	}

	int GetThreadIdForActor(ACTOR_T type) {
		return core_to_thread_map[get_core_id_by_actor(type)];
	}

private:

	int world_rank_;

	vector<vector<int>> cpu_topo;
	int num_cores = 0;

	// bool enable_binding = false;
	vector<int> core_bindings;

	// Full core_id for each division
	// use once
	int PotentialCorePool[6][6] = {
		{0, 6, 1, 7, 2, 8},
		{3, 9, 4, 10},
		{5, 11},
		{12, 18, 13, 19},
		{14, 15, 20, 21},
		{16, 17, 22, 23}
	};

	// stealing list for each thread
	map<int, vector<int>> stealing_table;

	// order eq priority
	int num_threads[NUM_THREAD_DIVISION];
	map<ACTOR_T, ActorDivisionType> actor_division;

	map<int, vector<int>> core_pool_table;
	map<int, vector<int>::iterator> core_pool_table_itr;
	pthread_spinlock_t lock_table[NUM_THREAD_DIVISION];

	map<int, int> core_to_thread_map;
	map<int, int> thread_to_core_map;

	void load_actor_division()
	{
		// CacheSeq
		actor_division[ACTOR_T::LABEL] = ActorDivisionType::CACHE_SEQ;
		actor_division[ACTOR_T::HASLABEL] = ActorDivisionType::CACHE_SEQ;
		actor_division[ACTOR_T::PROPERTY] = ActorDivisionType::CACHE_SEQ;
		actor_division[ACTOR_T::VALUES] = ActorDivisionType::CACHE_SEQ;
		actor_division[ACTOR_T::HAS] = ActorDivisionType::CACHE_SEQ;
		actor_division[ACTOR_T::KEY] = ActorDivisionType::CACHE_SEQ;

		// CacheBarrier
		actor_division[ACTOR_T::GROUP] = ActorDivisionType::CACHE_BARR;
		actor_division[ACTOR_T::ORDER] = ActorDivisionType::CACHE_BARR;

		// Traversal
		actor_division[ACTOR_T::TRAVERSAL] = ActorDivisionType::TRAVERSAL;
		
		// NormalBarrier
		actor_division[ACTOR_T::COUNT] = ActorDivisionType::NORMAL_BARR;
		actor_division[ACTOR_T::AGGREGATE] = ActorDivisionType::NORMAL_BARR;
		actor_division[ACTOR_T::CAP] = ActorDivisionType::NORMAL_BARR;
		actor_division[ACTOR_T::DEDUP] = ActorDivisionType::NORMAL_BARR;
		actor_division[ACTOR_T::MATH] = ActorDivisionType::NORMAL_BARR;

		// NormalBranch
		actor_division[ACTOR_T::RANGE] = ActorDivisionType::NORMAL_BRANCH;
		actor_division[ACTOR_T::BRANCH] = ActorDivisionType::NORMAL_BRANCH;
		actor_division[ACTOR_T::BRANCHFILTER] = ActorDivisionType::NORMAL_BRANCH;

		// NormalSeq
		actor_division[ACTOR_T::AS] = ActorDivisionType::NORMAL_SEQ;
		actor_division[ACTOR_T::SELECT] = ActorDivisionType::NORMAL_SEQ;
		actor_division[ACTOR_T::WHERE] = ActorDivisionType::NORMAL_SEQ;
		actor_division[ACTOR_T::IS] = ActorDivisionType::NORMAL_SEQ;
	}

	void dump_node_topo(vector<vector<int>> topo)
	{
		cout << "TOPO: " << topo.size() << " nodes:" << endl;
		for (int nid = 0; nid < topo.size(); nid++) {
			cout << "node " << nid << " cores: ";
			for (int cid = 0; cid < topo[nid].size(); cid++)
				cout << topo[nid][cid] << " ";
			cout << endl;
		}
		cout << endl;
	}

	void load_node_topo()
	{
		hwloc_topology_t topology;

		hwloc_topology_init(&topology);
		hwloc_topology_load(topology);

		// Currently, nnodes may return 0 while the NUMANODEs in cpulist is 1
		// (hwloc think there is actually no numa-node).
		// Fortunately, it can detect the number of processing units (PU) correctly
		// when MT processing is on, the number of PU will be twice as #cores
		int nnodes = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_NUMANODE);
		if (nnodes != 0) {
			cpu_topo.resize(nnodes);
			for (int i = 0; i < nnodes; i++) {
				hwloc_obj_t obj = hwloc_get_obj_by_type(topology, HWLOC_OBJ_NUMANODE, i);
				hwloc_cpuset_t cpuset = hwloc_bitmap_dup(obj->cpuset);

				unsigned int core = 0;
				hwloc_bitmap_foreach_begin(core, cpuset);
				cpu_topo[i].push_back(core);
				core_bindings.push_back(core);
				hwloc_bitmap_foreach_end();

				hwloc_bitmap_free(cpuset);
			}
		} else {
			cpu_topo.resize(1);
			int nPUs = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
			for (int i = 0; i < nPUs; i++) {
				hwloc_obj_t obj = hwloc_get_obj_by_type(topology, HWLOC_OBJ_PU, i);
				hwloc_cpuset_t cpuset = hwloc_bitmap_dup(obj->cpuset);

				unsigned int core = 0;
				hwloc_bitmap_foreach_begin(core, cpuset);
				cpu_topo[0].push_back(core);
				core_bindings.push_back(core);
				hwloc_bitmap_foreach_end();

				hwloc_bitmap_free(cpuset);
			}
		}

		num_cores = core_bindings.size();

		// dump_node_topo(cpu_topo);
	}

	/*
	 * Create Thread Division by number of threads given
	 *
	 */
	void get_num_thread_for_each_division(int num_thread) {
		// For better performance, dont allow more than num_cores threads exist 
		assert(num_thread > 0 && num_thread <= num_cores - NUM_RESIDENT_THREAD); 

		int division_level = num_thread / NUM_THREAD_DIVISION;
		int free_to_assign = num_thread % NUM_THREAD_DIVISION;

		if (division_level == 0) {
			cout << "At least 6 threads to support thread division" << endl;
			return;
		} else {
			num_threads[ActorDivisionType::CACHE_SEQ] = multi_floor(division_level, 3.0 / 2);
			num_threads[ActorDivisionType::CACHE_BARR] = division_level;
			num_threads[ActorDivisionType::TRAVERSAL] = multi_ceil(division_level, 1.0 / 2);
			num_threads[ActorDivisionType::NORMAL_BARR] = division_level;
			num_threads[ActorDivisionType::NORMAL_BRANCH] = division_level;
			num_threads[ActorDivisionType::NORMAL_SEQ] = division_level;
		}

		// Assign rest of threads by priority
		for (int i = 0; i < free_to_assign; i++) {
			int id = i % 6;
			if (id == ActorDivisionType::TRAVERSAL) {
				// Traversal Max NumOfThreads == 2
				if (num_threads[id] == 2) {
					free_to_assign++;
					continue;
				}
			}
			num_threads[id]++;
		}

		// Debug
		int sum = 0;
		for (int i = 0; i < NUM_THREAD_DIVISION; i++) {
			if (world_rank_ == 1)
				cout << DebugString(i) << " : " << num_threads[i] << endl;
			sum += num_threads[i];
		}
		assert(sum == num_thread);
	}

	void get_core_id_for_each_division() {
		for (int i = 0; i < NUM_THREAD_DIVISION; i++) {
			for (int j = 0; j < num_threads[i]; j++) {
				core_pool_table[i].push_back(PotentialCorePool[i][j]);
			}
		}

		// Init Iterator and lock
		for (int i = 0; i < NUM_THREAD_DIVISION; i++) {
			core_pool_table_itr[i] = core_pool_table[i].begin();
			pthread_spin_init(&lock_table[i], 0);
		}

		// debug
		for (int i = 0; i < NUM_THREAD_DIVISION; i++) {
			if (world_rank_ == 1)
				cout << i << ":";
			for (auto & item : core_pool_table[i]) {
				if (world_rank_ == 1)
					cout << item << ", ";
			}
			if (world_rank_ == 1)
				cout << endl;
		}
	}

	void load_core_to_thread_map() {
		int thread_id = 0;
		for (int i = 0; i < NUM_THREAD_DIVISION; i++) {
			for(auto & core_id : core_pool_table[i]) {
				core_to_thread_map[core_id] = thread_id;
				thread_to_core_map[thread_id] = core_id;
				thread_id++;
			}
		}
	}

	void load_steal_list() {
		for (int i = 0; i < NUM_THREAD_DIVISION; i++) {
			for(auto & id : core_pool_table[i]) {
				// Add self division
				stealing_table[id].insert(stealing_table[id].end(), core_pool_table[i].begin(), core_pool_table[i].end());

				// Erase itself
				stealing_table[id].erase(remove(stealing_table[id].begin(), stealing_table[id].end(), id), stealing_table[id].end());

				// Add other division
				if (id / (num_cores / 2) == 0) {
					for (int j = 0; j < NUM_THREAD_DIVISION; j++) {
						if (j != i) {
							stealing_table[id].insert(stealing_table[id].end(),
									core_pool_table[j].begin(), core_pool_table[j].end());
						}
					}
				} else {
					for (int j = NUM_THREAD_DIVISION - 1; j >= 0; j--) {
						if (j != i) {
							stealing_table[id].insert(stealing_table[id].end(),
									core_pool_table[j].begin(), core_pool_table[j].end());
						}
					}
				}
			}
		}


		// Test
		if (world_rank_ == 1) {
			for (auto itr = stealing_table.begin(); itr != stealing_table.end();) {
				cout << "Thread " << itr->first << " :" << endl;
				for (auto & nbr : itr->second) {
					cout << nbr << " ";
				}
				cout << endl;
				itr++;
			}
		}
	}

	int get_core_id_by_actor(ACTOR_T type) {
		// TODO: TEST Concurrency
		ActorDivisionType att = actor_division[type];

		pthread_spin_lock(&(lock_table[att]));
		int cur_tid = *core_pool_table_itr[att];
		core_pool_table_itr[att]++;
		if (core_pool_table_itr[att] == core_pool_table[att].end()) {
			core_pool_table_itr[att] = core_pool_table[att].begin();
		}
		pthread_spin_unlock(&(lock_table[att]));

		return cur_tid;
	}

	string DebugString(int type_) {
		string str;
		switch(type_) {
			case 0:
				str = "CacheSEQ";
				break;
			case 1:
				str = "CacheBarr";
				break;
			case 2:
				str = "Traversal";
				break;
			case 3:
				str = "NormalBarr";
				break;
			case 4:
				str = "NormalBranch";
				break;
			case 5:
				str = "NormalSEQ";
				break;
			default:
				str = "NotDeclare";
		}

		return str;
	}

	/*
	 * Bind the current thread to a special core (core number)
	 */
	/*
	void bind_to_core(size_t core)
	{
		cpu_set_t mask;
		CPU_ZERO(&mask);
		CPU_SET(core, &mask);
		if (sched_setaffinity(0, sizeof(mask), &mask) != 0)
			cout << "Failed to set affinity (core: " << core << ")" << endl;
	}*/

	/*
	 * Bind the current thread to special cores (mask)
	 */
	/*
	void bind_to_core(cpu_set_t mask)
	{
		if (sched_setaffinity(0, sizeof(mask), &mask) != 0)
			cout << "Fail to set affinity!" << endl;
	}*/

	/*
	 * Bind the current thread to all of cores
	 * It would like unbind to special cores
	 * and not return the previous core binding
	 */
	/*
	void bind_to_all()
	{
		cpu_set_t mask;
		CPU_ZERO(&mask);
		for (int i = 0; i < default_bindings.size(); i++)
			CPU_SET(default_bindings[i], &mask);

		if (sched_setaffinity(0, sizeof(mask), &mask) != 0)
			cout << "Fail to set affinity" << endl;
	}*/

	/*
	 * Return the mask of the current core binding
	 */
	/*
	cpu_set_t get_core_binding()
	{
		cpu_set_t mask;
		CPU_ZERO(&mask);
		if (sched_getaffinity(0, sizeof(mask), &mask) != 0)
			cout << "Fail to get affinity" << endl;
		return mask;
	}*/

	/*
	 * Unbind the current thread to special cores
	 * and return the preivous core binding
	 */
	/*
	cpu_set_t unbind_to_core()
	{
		cpu_set_t mask;
		mask = get_core_binding(); // record the current core binding

		bind_to_all();
		return mask;
	}*/

	int multi_floor (int a, double b) {
		return (int)floor(a * b);
	}

	int multi_ceil (int a, double b) {
		return (int)ceil(a * b);
	}

};

#endif /* CORE_AFFINITY_HPP_ */
