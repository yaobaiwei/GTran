/*
 * gquery.cpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */

#include "base/node.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"
#include "driver/master.hpp"
#include "driver/worker.hpp"

#include "glog/logging.h"

//prog node-config-fname_path host-fname_path
int main(int argc, char* argv[])
{
	google::InitGoogleLogging(argv[0]);

	Node my_node;
	InitMPIComm(&argc, &argv, my_node);
	cout << my_node.DebugString();

	string host_fname = argv[1];
	CHECK(!host_fname.empty());

	Config * config = new Config();
	config->Init();
	cout  << "DONE -> Config->Init()" << endl;

	if(my_node.get_world_rank() == MASTER_RANK){
		Master m(my_node, config);
		m.Start();
	}else{
		Worker w(my_node, config, host_fname);
		w.Start();

		worker_barrier(my_node);
		worker_finalize(my_node);
	}

	void node_barrier();
	void node_finalize();

	return 0;
}




