/*
 * gquery.cpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */


#include "utils/global.hpp"
#include "utils/config.hpp"
#include "core/id_mapper.hpp"
#include "core/buffer.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/actors_adapter.hpp"
#include "utils/hdfs_core.hpp"
#include "storage/layout.hpp"
#include "base/node.hpp"
#include "base/node_util.hpp"

#include "glog/logging.h"


//prog node-config-fname_path host-fname_path
int main(int argc, char* argv[])
{
	init_worker(&argc, &argv);

	string node_config_fname = argv[1];
	string host_fname = argv[2];

	CHECK(!node_config_fname.empty());
	CHECK(!host_fname.empty());
	VLOG(1) << node_config_fname << " " << host_fname;

	//get nodes from config file
	std::vector<Node> nodes = ParseFile(node_config_fname);
	CHECK(CheckValidNodeIds(nodes));
	CHECK(CheckUniquePort(nodes));
	CHECK(CheckConsecutiveIds(nodes));
	Node my_node = GetNodeById(nodes, get_node_id());
	LOG(INFO) << my_node.DebugString();

	Config * config = new Config();
	config->Init();

	NaiveIdMapper * id_mapper = new NaiveIdMapper(config, my_node);

	//set the in-memory layout for RDMA buf
	Buffer * buf = new Buffer(config);
	buf->Init();

	//init the rdma mailbox
	RdmaMailbox * mailbox = RdmaMailbox(config, id_mapper, buf);
	mailbox->Init(host_fname);

	DataStore * datastore = new DataStore(config, id_mapper, buf);
	datastore->Init();

	datastore->LoadDataFromHDFS();
	//=======data shuffle==========
	datastore->Shuffle();
	worker_barrier();
	//=======data shuffle==========

	datastore->DataConverter();


	//actor driver starts
	ActorAdapter * actor_adapter = new ActorAdapter(config, my_node, mailbox);
	actor_adapter->Start();

	worker_finalize();
	return 0;
}




