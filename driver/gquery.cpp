/*
 * gquery.cpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */
#include "gflags/gflags.h"
#include "glog/logging.h"

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


DEFINE_string(node_config_fname, "", "The node config file path");
DEFINE_string(host_fname,"", "The host file path");


int main(int argc, char* argv[])
{
	gflags::ParseCommandLineFlags(&argc, &argv, true);
	google::InitGoogleLogging(argv[0]);

	CHECK(!FLAGS_node_config_fname.empty());
	CHECK(!FLAGS_host_fname.empty());
	VLOG(1) << FLAGS_node_config_fname << " " << FLAGS_host_fname;

	init_worker(&argc, &argv);

	//get nodes from config file
	std::vector<Node> nodes = ParseFile(FLAGS_node_config_fname);
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
	mailbox->Init(FLAGS_host_fname);

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




