/*-----------------------------------------------------
       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-11
  Author(s) : Chenghuan Huang(entityless@gmail.com)
:)
-----------------------------------------------------*/
#pragma once

#include "utils/mpi_config_namer.hpp"
#include <unistd.h>
#include <fstream>
#include "base/serialization.hpp"

//make sure that the snapshot path exists
//when this instance is called, make sure that MPIConfigNamer is initialed.

namespace std
{
class MPISnapshot
{
public:
    //to do: better use const reference.

    template<typename T>
    bool WriteData(string key, T& data, bool(write_func)(string, T&))
    {
        //hash the key
        string fn = path_ + "/" + n_->ultos(n_->GetHash(key));//dirty code

        write_func(fn, data);

        write_map_[key] = true;

        return true;
    }

    template<typename T>
    bool ReadData(string key, T& data, bool(read_func)(string, T&))
    {
        //hash the key
        string fn = path_ + "/" + n_->ultos(n_->GetHash(key));//dirty code

        read_map_[key] = read_func(fn, data);

        return read_map_[key];
    }

    //after read data of a specific key, this function will return true (for global usage)
    bool TestRead(string key)
    {
        if(read_map_.count(key) == 0)
            return false;//not found
        if(read_map_[key])
            return true;
        return false;
    }

    bool TestWrite(string key)
    {
        if(write_map_.count(key) == 0)
            return false;//not found
        if(write_map_[key])
            return true;
        return false;
    }


private:
    //
    MPIConfigNamer* n_;//bad name
    string path_;

    map<string, bool> read_map_;
    map<string, bool> write_map_;

    MPISnapshot(string path);


public:
    //Watashi Wa Tensai ⑨
    static MPISnapshot* GetInstanceP(string path = "")
    {
        static MPISnapshot* snapshot_single_instance = NULL;

        if(snapshot_single_instance == NULL)
        {
            snapshot_single_instance = new MPISnapshot(path);
        }

        return snapshot_single_instance;
    }
};
};
