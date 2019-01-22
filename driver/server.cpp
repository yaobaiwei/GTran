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

	string cfg_fname = argv[1];
	CHECK(!cfg_fname.empty());

	vector<Node> nodes = ParseFile(cfg_fname);
	CHECK(CheckUniquePort(nodes));
	Node & node = GetNodeById(nodes, my_node.get_world_rank());

	my_node.ibname = node.ibname;
	my_node.tcp_port = node.tcp_port;
	my_node.rdma_port = node.rdma_port;
	cout << my_node.DebugString();
	nodes.erase(nodes.begin()); //delete the master info in nodes (array) for rdma init

	//set my_node as the shared static Node instance

	my_node.InitialLocalWtime();

	//set this as 
	Node::StaticInstance(&my_node);

	// Config * config = new Config();
	// config->Init();

	Config* config = Config::GetInstance();
	config->Init();

	cout  << "DONE -> Config->Init()" << endl;

	if(my_node.get_world_rank() == MASTER_RANK){
		Master master(my_node);
		master.Init();

		master.Start();
	}else{
		Worker worker(my_node, nodes);
		worker.Init();
		worker.Start();

		worker_barrier(my_node);
		worker_finalize(my_node);
	}

	void node_barrier();
	void node_finalize();

	return 0;
}




