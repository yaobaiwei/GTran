/*
 * client.cpp
 *
 *  Created on: Jun 24, 2018
 *      Author: Hongzhi Chen
 */

#include "base/node.hpp"
#include "base/node_util.hpp"

#include "glog/logging.h"

class Client{
public:
	Client(string cfg_fname): fname_(cfg_fname){}
	void Init(){
		nodes_ = ParseFile(fname_);
		CHECK(CheckUniquePort(nodes_));
		master_ = GetNodeById(nodes_, 0);
	}


private:
	string fname_;
	vector<Node> nodes_;
	Node master_;
};

//prog node-config-fname_path host-fname_path
int main(int argc, char* argv[])
{
	google::InitGoogleLogging(argv[0]);
	string cfg_fname = argv[1];
	CHECK(!cfg_fname.empty());

	Client client(cfg_fname);
	client.Init();



	Config * config = new Config();
	config->Init();
	cout  << "DONE -> Config->Init()" << endl;

	if(my_node.get_world_rank() == MASTER_RANK){
		Master master(my_node, config);
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
