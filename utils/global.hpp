//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen


#ifndef GLOBAL_HPP_
#define GLOBAL_HPP_

#include <mpi.h>

#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stddef.h>
#include <string.h>
#include <iostream>

#include "utils/config.hpp"

extern "C" {
#include "utils/iniparser/iniparser.h"
}

using namespace std;

#define MASTER_RANK 0
//============================

extern int _my_rank;
extern int _num_nodes;

inline int get_node_id()
{
	return _my_rank;
}

inline int get_num_nodes()
{
	return _num_nodes;
}

void init_worker(int* argc, char*** argv);
void worker_finalize();
void worker_barrier();

//============================

void mk_dir(const char *dir);
void rm_dir(string path);
void check_dir(string path, bool force_write);
//=========================================================

void load_config(Config & config);

//=====================================================================


#endif /* GLOBAL_HPP_ */
