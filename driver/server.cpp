/*
 * server.cpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */

#include "base/node.hpp"
#include "base/node_util.hpp"
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

	string cfg_fname = argv[1];
	CHECK(!cfg_fname.empty());

	vector<Node> nodes = ParseFile(cfg_fname);
	CHECK(CheckUniquePort(nodes));
	Node node = GetNodeById(nodes, my_node.get_world_rank());
	CHECK_EQ(node.hostname, my_node.hostname);

	my_node.tcp_port = node.tcp_port;

	Config * config = new Config();
	config->Init();
	cout  << "DONE -> Config->Init()" << endl;

	if(my_node.get_world_rank() == MASTER_RANK){
		Master master(my_node, config);
		master.Init();

		master.Start();
	}else{
		Worker worker(my_node, config, nodes);
		worker.Start();

		worker_barrier(my_node);
		worker_finalize(my_node);
	}

	void node_barrier();
	void node_finalize();

	return 0;
}




