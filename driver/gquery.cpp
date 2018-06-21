/*
 * gquery.cpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */

#include "base/node.hpp"
#include "base/node_util.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"
#include "driver/hoster.hpp"
#include "driver/worker.hpp"

#include "glog/logging.h"


//prog node-config-fname_path host-fname_path
int main(int argc, char* argv[])
{
	google::InitGoogleLogging(argv[0]);
	init_worker(&argc, &argv);

	string node_config_fname = argv[1];
	string host_fname = argv[2];

	CHECK(!node_config_fname.empty());
	CHECK(!host_fname.empty());

	//get nodes from config file
	std::vector<Node> nodes = ParseFile(node_config_fname);
	CHECK(CheckValidNodeIds(nodes));
	CHECK(CheckUniquePort(nodes));
	CHECK(CheckConsecutiveIds(nodes));
	Node my_node = GetNodeById(nodes, get_node_id());

	cout << my_node.DebugString();

	Config * config = new Config();
	config->Init();
	LOG(INFO) << "DONE -> Config->Init()" << endl;

	if(get_node_id() == HOSTER_RANK){
		Hoster m(my_node, config);
		m.Start();
	}else{
		Worker w(my_node, config, host_fname);
		w.Start();
	}

	worker_finalize();
	return 0;
}




