/*-----------------------------------------------------

       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-12
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

//this is a poor performance map implementation for multi-thread usage.
namespace std
{
    //not to use mutex in this map
    //because assessing the map should be a lightweight operation
    //do not try to implement a single instance map here
    //this is just a abstract implementation
    template <typename __KEY_T, typename __VALUE_T>
    class UglyThreadSafeMap
    {
    private:
        map<__KEY_T, pair<pthread_spinlock_t, __VALUE_T>> base_map_;
        pthread_spinlock_t comm_lock_;

    public:
        UglyThreadSafeMap(){pthread_spin_init(&comm_lock_, 0);};
        ~UglyThreadSafeMap(){};


        //this can only guarantee that 
        __VALUE_T Get(__KEY_T key)//ignore key error, just do it
        {
            __VALUE_T ret_val;           
            pthread_spin_lock(&comm_lock_);
            ret_val = base_map_[key].second;
            pthread_spin_unlock(&comm_lock_);
            return ret_val;
        }

        void Set(__KEY_T key, const __VALUE_T& value)
        {
            pthread_spin_lock(&comm_lock_);

            base_map_[key] = make_pair(pthread_spinlock_t(), value);
            //init lock
            pthread_spin_init(&base_map_[key].first, 0);

            pthread_spin_unlock(&comm_lock_);
        }

        int Count(__KEY_T key)
        {
            int ret_val;
            pthread_spin_lock(&comm_lock_);
            ret_val = base_map_.count(key);
            pthread_spin_unlock(&comm_lock_);
            return ret_val;
        }

        __VALUE_T GetAndLock(__KEY_T key)
        {
            //get the value in a specific slot, and lock it
            __VALUE_T ret_val;
            pthread_spin_lock(&comm_lock_);//this should be locked first in case of dead lock
            pthread_spin_lock(&base_map_[key].first);
            ret_val = base_map_[key].second;
            pthread_spin_unlock(&comm_lock_);

            return ret_val;
        }

        void SetAndUnlock(__KEY_T key, const __VALUE_T& value)
        {
            //assuming that the key exists.
            pthread_spin_lock(&comm_lock_);

            base_map_[key].second = value;

            pthread_spin_unlock(&base_map_[key].first);
            pthread_spin_unlock(&comm_lock_);
        }

    };

    class UglyInstanceManager
    {
    private:
        // static pthread_spinlock_t lock_;//super super low performance
        //not needed at all, actually.

        // UglyInstanceRefManager(){pthread_spin_init(&lock_, 0);};

        // template <typename T>
        // UglyThreadSafeMap<string, T> doge;

        UglyInstanceManager();

    public:
        //to utilize the insane property of template

        // ugly_thread_safe_map.o: In function `_GLOBAL__sub_I_(int0_t, l, long, int0_t, int0_t)':
        // ugly_thread_safe_map.cpp:(.text.startup+0x25): undefined reference to `std::UglyInstanceManager::lock_'
        // collect2: error: ld returned 1 exit status
        // static int InitialLock()
        // {
        //     pthread_spin_init(&UglyInstanceManager::lock_, 0);//
        //     printf("UglyInstanceManager::InitialLock\n");
        //     return 0;
        // }

        template <typename T>
        static T* GetInstanceP(string instance_name, T* ptr_outer = nullptr)
        {
            static UglyThreadSafeMap<string, T*> instance_map;

            if(ptr_outer != nullptr)
            {
                //replace the value
                instance_map.Set(instance_name, ptr_outer);
            }

            T* ret_val;

            if(instance_map.Count(instance_name) == 0)
            {
                ret_val = nullptr;
            }
            else
            {
                ret_val = instance_map.Get(instance_name);
            }

            return ret_val;
        }

        template <typename T>
        static void PrintInstance(string instance_name)
        {
            T* ptr = GetInstanceP<T>(instance_name);

            if(ptr == nullptr)
            {
                printf("PrintInstance nullptr\n");
            }
            else
            {
                cout << "key is "<< instance_name << ", value is " << *ptr << endl;
            }
        }
    };
};



