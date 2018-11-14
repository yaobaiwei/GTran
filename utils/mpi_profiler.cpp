/*-----------------------------------------------------

       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-11
  Author(s) : Chenghuan Huang(entityless@gmail.com)
:)
-----------------------------------------------------*/

#include "mpi_profiler.hpp"

using namespace std;

MPIProfiler::MPIProfiler(string instance_name, MPI_Comm comm)
{
    comm_ = comm;
    MPI_Comm_rank(comm, &my_rank_);
    MPI_Comm_size(comm, &comm_sz_);
    instance_name_ = instance_name;
}

MPIProfiler::~MPIProfiler()
{

}

void MPIProfiler::InsertLabel(string label)
{
    pf_map_.insert(std::make_pair(label, pfs_.size()));
    pfs_.emplace_back(0);
    tmp_pfs_.emplace_back(0);
}

void MPIProfiler::PrintSummary()
{
    string header = "my_rank_";
    for(auto lb_kv : pf_map_)
    {
        header += ", " + lb_kv.first;
    }

    //print the header
    //file name?
    string fn = instance_name_ + "_pf_result.csv";
    ofstream of_header(fn);
    of_header << header << endl;
    of_header.close();

    for(int i = 0; i < comm_sz_; i++)
    {
        if(i == my_rank_)
        {
            ofstream of(fn, ofstream::app);
            of << i;
            for(auto pf : pfs_)
            {
                of << ", " << pf;
            }
            of << endl;
            of.close();
        }
        MPI_Barrier(comm_);
    }
}