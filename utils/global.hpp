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

enum MSG
{
	START = 0,
	TERMINATE = 1,
	REPORT = 2,
	DONE = 3
};

#define MASTER_RANK 0

const int COMMUN_CHANNEL=200;
const int MONITOR_CHANNEL=201;
const int MSCOMMUN_CHANNEL=202;
const int COMMUN_TIME=1;

//============================

void InitMPIComm(int* argc, char*** argv, Node & node);
void worker_finalize(Node & node);
void worker_barrier(Node & node);
void node_finalize();
void node_barrier();

//============================

void mk_dir(const char *dir);
void rm_dir(string path);
void check_dir(string path, bool force_write);
//=========================================================

#endif /* GLOBAL_HPP_ */
