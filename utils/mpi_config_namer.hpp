/*-----------------------------------------------------
       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-11
  Author(s) : Chenghuan Huang(entityless@gmail.com)
:)
-----------------------------------------------------*/
#pragma once


#include <string>
#include <mpi.h>

class MPIConfigNamer
{
private:
    //the concat of hostnames
    void GetHostsStr();

    MPIConfigNamer(MPI_Comm comm)
    {
        comm_ = comm;
        MPI_Comm_rank(comm, &my_rank_);
        MPI_Comm_size(comm, &comm_sz_);

        GetHostsStr();

        AppendHash(hn_cat_);
    }

    MPI_Comm comm_;
    int my_rank_;
    int comm_sz_;

    std::string hn_cat_;

    unsigned long GetHash(std::string s);
    std::string ultos(unsigned long ul);

    std::string hashed_str_;

public:
    static MPIConfigNamer* GetInstanceP(MPI_Comm comm = MPI_COMM_WORLD)
    {
        static MPIConfigNamer* config_namer_single_instance = NULL;

        if(config_namer_single_instance == NULL)
        {
            config_namer_single_instance = new MPIConfigNamer(comm);
        }

        return config_namer_single_instance;
    }

    void AppendHash(std::string to_append);//extend the file name
    std::string ExtractHash();
};