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
#include "core/data_loader.hpp"
#include "utils/hdfs_core.hpp"
#include "storage/layout.hpp"


int main(int argc, char* argv[])
{
	init_worker(&argc, &argv);

	Config * config = new Config();
	load_config(*config);
	config->set_nodes_config(); //TODO UNFINISHED
	config->set_more();

	string host_fname = std::string(argv[2]);

	NaiveIdMapper * id_mapper = new NaiveIdMapper(config);

	//set the in-memory layout for buffer
	Buffer<int> * buf = new Buffer<int>(config, id_mapper);
	buf->InitBuf();
	buf->SetStorage();
	buf->SetBuf();

	//init the rdma mailbox
	RdmaMailbox<int> * mailbox = RdmaMailbox<int>(config, id_mapper, buf);
	mailbox->Init(host_fname);

	//load the index and data from HDFS
	string_index indexes;
	vector<Vertex*> vertices;

	DataLoader * data_loader = new DataLoader(config);
	data_loader->get_string_indexes(indexes);
	data_loader->get_vertices(vertices);

	worker_finalize();
	return 0;
}




