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
#include "utils/hdfs_core.hpp"
#include "storage/layout.hpp"

//TODO
//parse the string content in HDFS
//fill into vertex
//at the same time, fill in the string server
Vertex* to_vertex(char* line)
{
	Vertex * v = new Vertex;
	char * pch;
	pch = strtok(line, "\t");
	v->id = atoi(pch);
	pch = strtok(NULL, "\t");
	int num_in_nbs = atoi(pch);
	for(int i = 0 ; i < num_in_nbs; i++){
		pch = strtok(NULL, " ");
		v->in_nbs.push_back(atoi(pch));
	}
	pch = strtok(NULL, "\t");
	int num_out_nbs = atoi(pch);
	for(int i = 0 ; i < num_out_nbs; i++){
		pch = strtok(NULL, " ");
		v->out_nbs.push_back(atoi(pch));
	}
	//fill in label field
	return v;
}

void load_graph(const char* inpath, vector<Vertex> & vtxs)
{
	hdfsFS fs = get_hdfs_fs();
	hdfsFile in = get_r_handle(inpath, fs);
	LineReader reader(fs, in);
	while(true)
	{
		reader.read_line();
		if (!reader.eof())
		{
			VertexT * v = to_vertex(reader.get_line());
			local_table_.set(v->id, v);
		}
		else
			break;
	}
	hdfsCloseFile(fs, in);
	hdfsDisconnect(fs);
}

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

	vector<Vertex> vtxs;
	string rank = to_string(get_node_id());
	string myfile = config.HDFS_INPUT_PATH + "/part_" + rank;
	load_graph(myfile.c_str(), vtxs);

	worker_finalize();
	return 0;
}




