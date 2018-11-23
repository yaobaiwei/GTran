/*-----------------------------------------------------
       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-11
  Author(s) : Chenghuan Huang(entityless@gmail.com)
:)
-----------------------------------------------------*/
#pragma once

//this file is designed to be isolated from MPISnapshot

#include "base/serialization.hpp"
#include <ext/hash_map>

using __gnu_cxx::hash_map;

//the most simple

template<typename T>
static inline bool WriteSerImpl(string fn, T& data)
{
    ofstream doge(fn, ios::binary);

    if(!doge.is_open())
    {
        return false;
    }

    ibinstream m;
    m << data;

    int buf_sz = m.size();
    doge.write((char*)&buf_sz, sizeof(int));
    // doge << m.size();

    doge.write(m.get_buf(), m.size());

    doge.close();

    return true;
}

template<typename T>
static inline bool ReadSerImpl(string fn, T& data)
{
    ifstream doge(fn, ios::binary);

    if(!doge.is_open())
    {
        return false;
    }

    int sz;
    // doge >> sz;
    doge.read((char*)&sz, sizeof(int));

    char* tmp_buf = new char[sz];
    doge.read(tmp_buf, sz);
    doge.close();

    obinstream m;
    m.assign(tmp_buf, sz, 0);

    m >> data;

    return true;
}


template<typename T1,typename T2>
static inline bool WriteHashMapSerImpl(string fn, hash_map<T1, T2*>& data)
{
    ofstream doge(fn, ios::binary);

    if(!doge.is_open())
    {
        return false;
    }

    //write data to the instream
    ibinstream m;

    for(auto kv : data)
    {
        //notice that kv.second is a pointer
        //and the content of the pointer need to be written

        m << kv.first;//the key
        m << *kv.second;//the value is a pointer
    }

    int data_sz = data.size(), buf_sz = m.size();
    printf("WriteHashMapSerImpl data_sz = %d, buf_sz = %d\n", data_sz, buf_sz);

    // doge << data_sz;
    // doge << buf_sz;
    doge.write((char*)&data_sz, sizeof(int));
    doge.write((char*)&buf_sz, sizeof(int));
    doge.write(m.get_buf(), m.size());

    doge.close();

    return true;
}

template<typename T1,typename T2>
static inline bool ReadHashMapSerImpl(string fn, hash_map<T1, T2*>& data)
{
    ifstream doge(fn, ios::binary);

    if(!doge.is_open())
    {
        return false;
    }

    int buf_sz, data_sz;
    // doge >> data_sz;
    // doge >> buf_sz;
    doge.read((char*)&data_sz, sizeof(int));
    doge.read((char*)&buf_sz, sizeof(int));

    printf("ReadHashMapSerImpl data_sz = %d, buf_sz = %d\n", data_sz, buf_sz);

    char* tmp_buf = new char[buf_sz];
    doge.read(tmp_buf, buf_sz);
    doge.close();

    obinstream m;
    m.assign(tmp_buf, buf_sz, 0);

    for(int i = 0; i < data_sz; i++)
    {
        T1 key;
        T2* value = new T2;

        m >> key >> *value;

        data[key] = value;
    }

    return true;
}




