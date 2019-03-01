/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include <memory.h>
#include <signal.h>
#include <termios.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include <list>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <functional>
#include "mpi_unique_namer.hpp"

void MPIUniqueNamer::GetHostsStr() {
    char tmp_hn[1000];
    int hn_len;
    MPI_Get_processor_name(tmp_hn, &hn_len);

    string hn(tmp_hn);

    int* hn_lens = new int[comm_sz_];
    int* hn_displs = new int[comm_sz_];

    MPI_Allgather(&hn_len, 1, MPI_INT, hn_lens, 1, MPI_INT, comm_);

    // after gathered this, call gatherv to merge hostname

    int total_len = hn_lens[0];
    hn_displs[0] = 0;


    for (int i = 1; i < comm_sz_; i++) {
        hn_displs[i] = total_len;
        total_len += hn_lens[i];
    }

    char* tmp_hn_cat = new char[total_len + 1];

    MPI_Allgatherv(tmp_hn, hn_len, MPI_CHAR, tmp_hn_cat, hn_lens, hn_displs, MPI_CHAR, comm_);

    tmp_hn_cat[total_len] = 0;

    string hn_cat(tmp_hn_cat);
    string rank_str = ultos(my_rank_);

    hn_cat_ = rank_str + hn_cat;  // make sure that different host has different dir name, which enables debug on NFS

    delete hn_lens;
    delete hn_displs;
    delete tmp_hn_cat;
}

unsigned long MPIUniqueNamer::GetHash(string s) {
    hash<string> str_hash;
    return str_hash(s);
}

string MPIUniqueNamer::ultos(unsigned long ul) {
    char c[50];
    snprintf(c, sizeof(c), "%lu", ul);
    return string(c);
}

void MPIUniqueNamer::AppendHash(string to_append) {
    if (hashed_str_.size() != 0)
        hashed_str_ = hashed_str_ + "_";

    hashed_str_ += ultos(GetHash(to_append));
}

string MPIUniqueNamer::ExtractHash() {
    // //debug
    // for(int i = 0; i < comm_sz_; i++)
    // {
    //     if(i == my_rank_)
    //     {
    //         cout << hn_cat_ << "  " << hashed_str_ << endl;
    //     }
    //     MPI_Barrier(comm_);
    // }
    return hashed_str_;
}
