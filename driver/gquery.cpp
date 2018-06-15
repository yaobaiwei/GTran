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
#include "utils/hdfs_core.hpp"
#include "core/id_mapper.hpp"
#include "core/buffer.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/actors_adapter.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"

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
	VLOG(1) << node_config_fname << " " << host_fname;

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

	NaiveIdMapper * id_mapper = new NaiveIdMapper(config, my_node);
	id_mapper->Init();

	LOG(INFO) <<  "DONE -> NaiveIdMapper->Init()" << endl;

	//set the in-memory layout for RDMA buf
	Buffer * buf = new Buffer(config);
	buf->Init();

	LOG(INFO) << "DONE -> Buffer->Init()" << endl;

	//init the rdma mailbox
	RdmaMailbox * mailbox = new RdmaMailbox(config, id_mapper, buf);
	mailbox->Init(host_fname);

	LOG(INFO) << "DONE -> RdmaMailbox->Init()" << endl;

	DataStore * datastore = new DataStore(config, id_mapper, buf);
	datastore->Init();

	LOG(INFO) << "DONE -> DataStore->Init()" << endl;

	datastore->LoadDataFromHDFS();
	worker_barrier();

	//=======data shuffle==========
	datastore->Shuffle();
	//=======data shuffle==========

	datastore->DataConverter();
	worker_barrier();
	LOG(INFO) << "DONE -> Datastore->DataConverter()" << endl;

	//TEST
	for(int i = 0 ; i < get_num_nodes(); i++){
		MSG_T type = MSG_T::FEED;
		int qid = i;
		int step = 0;
		int sender = get_node_id();
		int recver = i;
		vector<ACTOR_T> chains;
		chains.push_back(ACTOR_T::HW);
		SArray<char> data;
		data.push_back(48+get_node_id());
		data.push_back(48+i);
		Message msg = CreateMessage(type, qid, step, sender, recver,chains, data);
		mailbox->Send(0,msg);
	}

	//actor driver starts
	ActorAdapter * actor_adapter = new ActorAdapter(config, my_node, mailbox);
	actor_adapter->Start();

	actor_adapter->Stop();

	worker_finalize();
	return 0;
}




