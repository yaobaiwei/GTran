/*-----------------------------------------------------
       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-11
  Author(s) : Chenghuan Huang(entityless@gmail.com)
:)
-----------------------------------------------------*/

#include "mpi_snapshot.hpp"

using namespace std;

MPISnapshot::MPISnapshot(string path)
{
    n_ = MPIConfigNamer::GetInstanceP();

    path_ = path + "/" + n_->ExtractHash();

    //mkdir
    string cmd = "mkdir -p " + path_;
    // printf("%s\n", cmd.c_str());

    system(cmd.c_str());
}