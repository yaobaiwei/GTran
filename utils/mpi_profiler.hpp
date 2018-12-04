/*-----------------------------------------------------

       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-11
  Author(s) : Chenghuan Huang(entityless@gmail.com)
:)
-----------------------------------------------------*/

#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <vector>
#include <map>
#include <string.h>
#include <iostream>
#include <assert.h>
#include <fstream>
#include <mpi.h>

#if defined (__i386__)
#define CC_TYPE unsigned long long
#define CCPS 2900000000 //need to be changed according to the processor info
static __inline__ unsigned long long GetCycleCount()
{
        unsigned long long int x;
        __asm__ volatile("rdtsc":"=A"(x));
        return x;
}
#elif defined (__x86_64__)
#define CC_TYPE unsigned long long
#define CCPS 2900000000 //need to be changed according to the processor info
static __inline__ unsigned long long GetCycleCount()
{
        unsigned hi,lo;
        __asm__ volatile("rdtsc":"=a"(lo),"=d"(hi));
        return ((unsigned long long)lo)|(((unsigned long long)hi)<<32);
}
#elif (defined SW2) || (defined SW5)
#define CC_TYPE unsigned long
#define CCPS 1450000000
//does Sunway Taihulight support C++???
//so suspicious.
static __inline__ unsigned long GetCycleCount()
{
    unsigned long time;
    asm("rtc %0": "=r" (time) : );
    return time;
}
#endif

//too lazy to std::, so make myself be std
//suppose to use it on shared file system

////profile type: CC or wtime
//use macro instead if-else branch to significantly improve performance
#define MPI_PF_WTIME
// #define MPI_PF_CC

////profile policy: append or aggregate
// #define MPI_PF_APPEND
// #define MPI_PF_AGGREGATE
//todo

namespace std
{
    //support conditional static instance
    //to do: support multithread inside MPI
    //to do: support variable collect.
    //to do: support MPI_PF_APPEND

    class MPIProfiler
    {
    private:
        vector<string> labels_;//the header of csv

        #ifdef MPI_PF_WTIME
        typedef double pf_type;
        using pf_func_type = pf_type(*)();
        pf_func_type pf_func = &MPI_Wtime;
        #elif defined MPI_PF_CC
        typedef CC_TYPE pf_type;
        using pf_func_type = pf_type(*)();
        pf_func_type pf_func = &GetCycleCount;
        #else
        static_assert((1) == (2), "At least one of MPI_PF_WTIME or MPI_PF_CC should be defined");
        #endif

        map<string, int> pf_map_;
        vector<string> pf_label_;
        vector<pf_type> pfs_;
        vector<pf_type> tmp_pfs_;
        string instance_name_;

        int my_rank_, comm_sz_;
        MPI_Comm comm_;

        MPIProfiler(string instance_name, MPI_Comm comm);
        MPIProfiler(const MPIProfiler&);//not to def
        MPIProfiler& operator=(const MPIProfiler&);//not to def
        ~MPIProfiler();//will ctrl+c in MPI program supports -> exit(0)?

    public:

        //profile related
        //only in CC mode do we care about the extreme efficiency
        inline void STPF(int pos){tmp_pfs_[pos] = (*pf_func)();};
        inline void EDPF(int pos){pf_type pf_result = (*pf_func)() - tmp_pfs_[pos]; pfs_[pos] += pf_result;};

        inline void STPF(string label){int pos = pf_map_[label]; STPF(pos);};
        inline void EDPF(string label){int pos = pf_map_[label]; EDPF(pos);};
        //inline function's decl and impl cannot be seperated??

        void InsertLabel(string label);//necessary.
        //in the first version, the label can be inserted when doing STPF
        //now, this feature has been disabled


        //print summary
        void PrintSummary();//a csv file on shared file system

        //get instance via a map
        //be careful with the MPI_COMM
        //not thread safe
        static MPIProfiler* GetInstance(string instance_name, MPI_Comm comm = MPI_COMM_WORLD)
        {
            static map<string, MPIProfiler*> instance_map;

            if(instance_map.count(instance_name) == 0)
            {
                MPIProfiler* p = new MPIProfiler(instance_name, comm);
                instance_map.insert(make_pair(instance_name, p));
            }

            return instance_map[instance_name];
        }
    };
};
