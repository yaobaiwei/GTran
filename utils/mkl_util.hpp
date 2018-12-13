/*-----------------------------------------------------
       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-11
  Author(s) : Chenghuan Huang(entityless@gmail.com)
:)
-----------------------------------------------------*/
#pragma once



#include <cstdio>
#include <string>
#include <cstdlib>
#include <termios.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include <list>
#include <memory.h>
#include <signal.h>
#include <mkl.h>

#include "tid_mapper.hpp"
#include "ugly_thread_safe_map.hpp"

namespace std
{
//one instance for one tid
//before using this, the thread must have registed in TidMapper
class MKLUtil
{
private:

    MKLUtil(const MKLUtil&);//not to def
    MKLUtil& operator=(const MKLUtil&);//not to def
    ~MKLUtil()
    {
        printf("MKLUtil::~MKLUtil()\n");
    }
    MKLUtil(int tid);
    

    //get instance from the map with key tid
    static MKLUtil* GetInstanceActual(int tid)
    {
        static UglyThreadSafeMap<int, MKLUtil*> instance_map;

        if(instance_map.Count(tid) == 0)
        {
            MKLUtil* p = new MKLUtil(tid);
            instance_map.Set(tid, p);
        }

        return instance_map.Get(tid);
    }

    int tid_;

    //RNG related
    VSLStreamStatePtr rng_stream_;

public:

    //currently, assuming that each tid has only one instance of MKLUtil.
    //maybe in the future, a "thread instance" will be able to use a pool of threads.
    //but not now
    static MKLUtil* GetInstance()
    {
        auto tid = TidMapper::GetInstance().GetTidUnique();

        return GetInstanceActual(tid);
    }

    void Test();

    //
    void UniformRNGI4(int* dst, int len, int min, int max);
    void UniformRNGF4(float* dst, int len, float min, float max);
    void UniformRNGF8(double* dst, int len, double min, double max);

    // void DGEMM();
    // void SGEMM();
};

}



