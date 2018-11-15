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
#include <pthread.h>

//the only usage of this class is to record the numerical tid of any thread (from 0)
//suppose that any thread is supported by pthread
namespace std
{
    class TidMapper
    {
    private:
        TidMapper(const TidMapper&);//not to def
        TidMapper& operator=(const TidMapper&);//not to def
        ~TidMapper(){};

        //never buffer this in cache.
        // volatile map<pthread_t, int> tid_map_;
        map<pthread_t, int> manual_tid_map_;
        map<pthread_t, int> unique_tid_map_;

        pthread_spinlock_t lock_;

    public:

        static TidMapper& GetInstance()
        {
            static TidMapper thread_mapper_single_instance;
            return thread_mapper_single_instance;
        }

        void Register(int tid);
        int GetTid();
        int GetTidUnique();

    private:
        TidMapper();
    };
};