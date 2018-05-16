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
#include "comm/rdma.hpp"

int main(int argc, char* argv[])
{
	init_worker(&argc, &argv);

	Config config;
	load_config(config);
	config.set_nodes_config(); //TODO UNFINISHED
	config.set_more();

	string host_fname = std::string(argv[2]);

	NaiveIdMapper * id_mapper = new NaiveIdMapper(config);

	//set the in-memory layout for buffer
	Buffer<int> * buf = new Buffer<int>(&config, id_mapper);
	buf->InitBuf();
	buf->SetStorage();
	buf->SetBuf();

	//init the rdma mailbox
	RdmaMailbox<int> * mailbox = RdmaMailbox<int>(&config, id_mapper, buf);
	mailbox->Init(host_fname);

	//load the data from HDFS

	worker_finalize();
	return 0;
}




