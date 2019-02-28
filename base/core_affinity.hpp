/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
         Modified by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#ifndef CORE_AFFINITY_HPP_
#define CORE_AFFINITY_HPP_

#include <fstream>
#include <algorithm>
#include <mutex>

#include <hwloc.h>
#include <math.h>
#include <boost/algorithm/string/predicate.hpp>

#include "base/type.hpp"
#include "utils/config.hpp"
#include "utils/global.hpp"
#include "utils/timer.hpp"

#include "base/cpuinfo_util.hpp"

#include "base/node.hpp"

#include "utils/simple_thread_safe_map.hpp"

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

    CoreAffinity ()
    {
        config_ = Config::GetInstance();
    }

    Node node_;


    void Init() {
        node_ = Node::StaticInstance();
        //
        init_potential_core_pool();

        load_node_topo();
        // Calculate Number of threads for each thread division
        // Fail when number of threads < 6
        if (!get_num_thread_for_each_division(config_->global_num_threads)) {
            config_->global_enable_actor_division = false;
        }

        // Assign thread id to each division according to last function
        get_core_id_for_each_division();

        // Pair Actor Type and ActorDivisionType 
        load_actor_division();

        // Pair CoreId and ThreadId
        load_core_to_thread_map();

        // Assign stealing list to each thread
        load_steal_list();
    }

    bool BindToCore(int tid)
    {
        //notice that this function is called in 
        //void ThreadExecutor(int tid) {
        //    if (config_->global_enable_core_binding) {
        //        core_affinity_->BindToCore(tid);

        size_t core = (size_t)thread_to_core_map[tid];
        cpu_set_t mask;
        CPU_ZERO(&mask);
        CPU_SET(core, &mask);
        if (sched_setaffinity(0, sizeof(mask), &mask) != 0)
            cout << "Failed to set affinity (core: " << core << ")" << endl;
        else
        {
            int global_core = cpuinfo_->GetCoreInGlobalMappingVector(core);

            vector<int> counter = core_counter_.GetAndLock(global_core);
            int counter_sz = counter.size();

            // if you are worry that "two thread on the same physical core" reduces the performance, you can uncomment the code below
            // if(counter_sz != 0)
            // {
            //     if(node_.get_local_rank() == 0)
            //     {
            //         printf("core %d already binded, with tid cnt = %d, tid[0] = %d\n", global_core, counter_sz, counter[0]);
            //     }
            // }
            // else
            // {
            //     if(node_.get_local_rank() == 0)
            //     {
            //         printf("bind fine, t = %d, c = %d\n", core, global_core);
            //     }
            // }

            counter.push_back(core);//not global core

            core_counter_.SetAndUnlock(global_core, counter);
        }

        //to insert into a map
        // core_counter_
    }

    void GetStealList (int tid, vector<int> & list) {
        int core_id = thread_to_core_map[tid];
        for (auto itr = stealing_table[core_id].begin(); itr != stealing_table[core_id].end(); itr++) {
            list.push_back(core_to_thread_map[*itr]);
        }
    }

    int GetThreadIdForActor(ACTOR_T type) {
        if (config_->global_enable_actor_division)
            return core_to_thread_map[get_core_id_by_actor(type)];
        else {
            counter++;
            return counter % config_->global_num_threads;
        }
    }

private:

    // Config
    Config * config_;
    int counter = 0;

    vector<vector<int>> cpu_topo;
    int num_cores = 0;

    // bool enable_binding = false;
    vector<int> core_bindings;

    CPUInfoUtil* cpuinfo_ = CPUInfoUtil::GetInstance();

    // Full core_id for each division
    //on 2 * 8 core
    /*
    int potential_thread_pool_[6][6] = {
        {0, 12, 2, 14, 4, 16},
        {6, 18, 8, 20},
        {10, 22},
        {1, 13, 3, 15},
        {5, 17, 7, 19},
        {9, 21, 11, 23}
    };*/

    vector<int> potential_core_pool_[NUM_THREAD_DIVISION];
    vector<int> potential_thread_pool_[NUM_THREAD_DIVISION];
    //this will be initialed via cpuinfo_

    //this is a const array, implemented from the last line of type.hpp

    //{the sum of numerator} / {denominator} == 1
    const int division_numerator_[NUM_THREAD_DIVISION] = {3, 2, 1, 2, 2, 2};
    const int division_denominator_ = 12;

    //on 2 * 8 core
    // int potential_thread_pool_[6][8] = {
    //     {0, 16, 2, 18, 4, 20, 6, 22},
    //     {8, 24, 10, 26},
    //     {12, 28, 14, 30},
    //     {1, 17, 3, 19, 5, 21},
    //     {7, 23, 9, 25, 11, 27},
    //     {13, 29, 15, 31}
    // };

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

    SimpleThreadSafeMap<int, vector<int>> core_counter_;//this is implemented in case of bad affinity implementation

    //to do:
    //    to consider NUMA when assigning cores
    //    thread level assignment if core count is insufficient
    void init_potential_core_pool()
    {
        //first, try to divide via core
        int cur_step = 0;
        int last_div = 0;//last division core

        int core_cnt = cpuinfo_->GetTotalCoreCount();

        for(int i = 0; i < core_cnt; i++)
        {
            core_counter_.Set(i, vector<int>());//initial
        }

        //init potential pool via cpuinfo
        for(int i = 0; i < NUM_THREAD_DIVISION; i++)
        {
            cur_step += division_numerator_[i] * core_cnt;

            int cur_div = cur_step / division_denominator_;
            int cur_core_cnt = cur_div - last_div;

            //guarantee that at least one core will be given
            if(cur_core_cnt == 0)
            {
                cur_step = (last_div + 1) * core_cnt;
                cur_div = cur_step / core_cnt;
            }

            for(int j = last_div; j < cur_div; j++)
                potential_core_pool_[i].push_back(j);

            last_div = cur_div;
        }
        
        //the thread pool will be initialed after the core pool initialized
        //the loop sequence must be so.
        for(int j = 0; j < cpuinfo_->GetThreadPerCore(); j++)
        for(int i = 0; i < NUM_THREAD_DIVISION; i++)
        for(auto core_id : potential_core_pool_[i])
        {
            potential_thread_pool_[i].push_back(cpuinfo_->GetCoreThreads(core_id)[j]);
        }

        //local_rank_ == 0  node 0: cores: 
        //[ 0 1 2 3], 
        //[ 4 5], 
        //[ 6 7], 
        //[ 8 9], 
        //[ 10 11 12], 
        //[ 13 14 15],  
        //threads: 
        //[ 0 2 4 6 16 18 20 22], 
        //[ 8 10 24 26], 
        //[ 12 14 28 30], 
        //[ 1 3 17 19], 
        //[ 5 7 9 21 23 25], 
        //[ 11 13 15 27 29 31], 


        //debug; would be necessarty if run on nodes with different CPU configuration
        std::stringstream ss;
        ss<<"node "<<node_.get_local_rank()<<": cores: ";
        for(auto cores : potential_core_pool_)
        {
            ss<<"[";
            for(auto core : cores)
            {
                ss<<" "<<core;
            }
            ss<<"], ";
        }
        ss<<" threads: ";
        for(auto threads : potential_thread_pool_)
        {
            ss<<"[";
            for(auto thread : threads)
            {
                ss<<" "<<thread;
            }
            ss<<"], ";
        }
        node_.LocalSequentialDebugPrint(ss.str());
    }

    void load_actor_division()
    {
        // CacheSeq
        actor_division[ACTOR_T::LABEL] = ActorDivisionType::CACHE_SEQ;
        actor_division[ACTOR_T::HASLABEL] = ActorDivisionType::CACHE_SEQ;
        actor_division[ACTOR_T::PROPERTIES] = ActorDivisionType::CACHE_SEQ;
        actor_division[ACTOR_T::VALUES] = ActorDivisionType::CACHE_SEQ;
        actor_division[ACTOR_T::HAS] = ActorDivisionType::CACHE_SEQ;
        actor_division[ACTOR_T::KEY] = ActorDivisionType::CACHE_SEQ;

        // CacheBarrier
        actor_division[ACTOR_T::GROUP] = ActorDivisionType::CACHE_BARR;
        actor_division[ACTOR_T::ORDER] = ActorDivisionType::CACHE_BARR;

        // Traversal
        actor_division[ACTOR_T::TRAVERSAL] = ActorDivisionType::TRAVERSAL;
        actor_division[ACTOR_T::INIT] = ActorDivisionType::TRAVERSAL;
        actor_division[ACTOR_T::INDEX] = ActorDivisionType::TRAVERSAL;

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
    bool get_num_thread_for_each_division(int num_thread) {
        // For better performance, dont allow more than num_cores threads exist 
        assert(num_thread > 0 && num_thread <= num_cores - NUM_RESIDENT_THREAD); 

        int division_level = num_thread / NUM_THREAD_DIVISION;
        int free_to_assign = num_thread % NUM_THREAD_DIVISION;

        if (division_level == 0) {
            cout << "[Warning] At least 6 threads to support thread division" << endl;
            cout << "[Warning] Automatically set ENABLE_CORE_BIND to False" << endl;
            return false;
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

        int sum = 0;
        for (int i = 0; i < NUM_THREAD_DIVISION; i++) {
            sum += num_threads[i];
        }
        assert(sum == num_thread);
        return true;
    }

    void get_core_id_for_each_division() {
        for (int i = 0; i < NUM_THREAD_DIVISION; i++) {
            //it will crash if not enough logical core to assign
            for (int j = 0; j < num_threads[i]; j++) {
                core_pool_table[i].push_back(potential_thread_pool_[i][j]);
            }
        }

        // Init Iterator and lock
        for (int i = 0; i < NUM_THREAD_DIVISION; i++) {
            core_pool_table_itr[i] = core_pool_table[i].begin();
            pthread_spin_init(&lock_table[i], 0);
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
                if (id % 2 == 0) {
                    for (int j = 0; j < NUM_THREAD_DIVISION; j++) {
                        if (j != i) {
                            stealing_table[id].insert(stealing_table[id].end(),
                                    core_pool_table[j].begin(), core_pool_table[j].end());
                        }
                    }
                } else {
                    for (int j = NUM_THREAD_DIVISION / 2; j < NUM_THREAD_DIVISION; j++) {
                        if (j != i) {
                            stealing_table[id].insert(stealing_table[id].end(),
                                    core_pool_table[j].begin(), core_pool_table[j].end());
                        }
                    }
                    for (int j = 0; j < NUM_THREAD_DIVISION / 2; j++) {
                        stealing_table[id].insert(stealing_table[id].end(),
                                core_pool_table[j].begin(), core_pool_table[j].end());
                    }
                }
            }
        }
    }

    int get_core_id_by_actor(ACTOR_T type) {
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

    int multi_floor (int a, double b) {
        return (int)floor(a * b);
    }

    int multi_ceil (int a, double b) {
        return (int)ceil(a * b);
    }

};

#endif /* CORE_AFFINITY_HPP_ */
