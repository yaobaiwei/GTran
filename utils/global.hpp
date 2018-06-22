/*
 * global.hpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */
//

#ifndef GLOBAL_HPP_
#define GLOBAL_HPP_

#include <mpi.h>

#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stddef.h>
#include <string.h>
#include <iostream>

#include "base/node.hpp"

using namespace std;

#define MASTER_RANK 0
#define COMMUN_CHANNEL 200
//============================

void InitMPIComm(int* argc, char*** argv, Node & node);
void node_finalize(Node & node);
void node_barrier(Node & node);
void worker_barrier(Node & node);
//============================

void mk_dir(const char *dir);
void rm_dir(string path);
void check_dir(string path, bool force_write);
//=========================================================

#endif /* GLOBAL_HPP_ */
