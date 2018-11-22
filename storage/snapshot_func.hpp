/*-----------------------------------------------------
       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-11
  Author(s) : Chenghuan Huang(entityless@gmail.com)
:)
-----------------------------------------------------*/
#pragma once

//this file is designed to be isolated from MPISnapshot

#include "base/serialization.hpp"

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

    doge << m.size();
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
    doge >> sz;
    char* tmp_buf = new char[sz];
    doge.read(tmp_buf, sz);
    doge.close();

    obinstream m;
    m.assign(tmp_buf, sz, 0);

    m >> data;

    return true;
}


// template<typename T1,typename T2>
// static inline bool WriteMapSerImpl(string fn, map<T1, T2>& data)
// {
//     ofstream doge(fn, ios::binary);

//     if(!doge.is_open())
//     {
//         return false;
//     }

//     ibinstream m;
//     m << data;

//     doge << m.size();
//     doge.write(m.get_buf(), m.size());

//     doge.close();

//     return true;
// }

// template<typename T1,typename T2>
// static inline bool ReadMapSerImpl(string fn, map<T1, T2>& data)
// {
//     ifstream doge(fn, ios::binary);

//     if(!doge.is_open())
//     {
//         return false;
//     }

//     int sz;
//     doge >> sz;
//     char* tmp_buf = new char[sz];
//     doge.read(tmp_buf, sz);
//     doge.close();

//     obinstream m;
//     m.assign(tmp_buf, sz, 0);

//     m >> data;

//     return true;
// }




