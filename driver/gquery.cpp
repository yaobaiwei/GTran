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
#include "core/data_loader.hpp"
#include "core/actors_adapter.hpp"
#include "utils/hdfs_core.hpp"
#include "storage/layout.hpp"
#include "base/node.hpp"
#include "base/node_util.hpp"


void shuffle(vector<Vertex*> & vertices, vector<Edge*> & edges, vector<VProperty*> & vplist, vector<EProperty*> & eplist){
	//vertices
	vector<vector<Vertex*>> vtx_parts;
	vtx_parts.resize((get_num_nodes()));
	for (int i = 0; i < vertices.size(); i++)
	{
		Vertex* v = vertices[i];
		vtx_parts[mymath::hash_mod(v->id.hash(), get_num_nodes())].push_back(v);
	}
	all_to_all(vtx_parts);
	vertices.clear();

	for (int i = 0; i < get_num_nodes(); i++)
	{
		vertices.insert(vertices.end(), vtx_parts[i].begin(), vtx_parts[i].end());
	}
	vtx_parts.clear();

	//edges
	vector<vector<Edge*>> edges_parts;
	edges_parts.resize((get_num_nodes()));
	for (int i = 0; i < edges.size(); i++)
	{
		Edge* e = edges[i];
		edges_parts[mymath::hash_mod(e->id.hash(), get_num_nodes())].push_back(e);
	}

	all_to_all(edges_parts);

	edges.clear();
	for (int i = 0; i < get_num_nodes(); i++)
	{
		edges.insert(edges.end(), edges_parts[i].begin(), edges_parts[i].end());
	}
	edges_parts.clear();

	//VProperty
	vector<vector<VProperty*>> vp_parts;
	vp_parts.resize((get_num_nodes()));
	for (int i = 0; i < vplist.size(); i++)
	{
		VProperty* vp = vplist[i];
		vp_parts[mymath::hash_mod(vp->id.hash(), get_num_nodes())].push_back(vp);
	}
	all_to_all(vp_parts);
	vplist.clear();

	for (int i = 0; i < get_num_nodes(); i++)
	{
		vplist.insert(vplist.end(), vp_parts[i].begin(), vp_parts[i].end());
	}
	vp_parts.clear();

	//EProperty
	vector<vector<EProperty*>> ep_parts;
	ep_parts.resize((get_num_nodes()));
	for (int i = 0; i < eplist.size(); i++)
	{
		EProperty* ep = eplist[i];
		ep_parts[mymath::hash_mod(ep->id.hash(), get_num_nodes())].push_back(ep);
	}

	all_to_all(ep_parts);
	eplist.clear();

	for (int i = 0; i < get_num_nodes(); i++)
	{
		eplist.insert(eplist.end(), ep_parts[i].begin(), ep_parts[i].end());
	}
	ep_parts.clear();
}

DEFINE_string(node_config_fname, "", "The node config file path");
DEFINE_string(host_fname,"", "The host file path");

//
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

	//set the in-memory layout for buffer
	Buffer<int> * buf = new Buffer<int>(config, id_mapper);
	buf->InitBuf();
	buf->SetStorage();
	buf->SetBuf();

	//init the rdma mailbox
	string host_fname = std::string(argv[2]);
	RdmaMailbox<int> * mailbox = RdmaMailbox<int>(config, id_mapper, buf);
	mailbox->Init(host_fname);

	//load the index and data from HDFS
	string_index indexes; //index is global, no need to shuffle

	vector<Vertex*> vertices;
	vector<Edge*> edges;
	vector<VProperty*> vplist;
	vector<EProperty*> eplist;

	DataLoader * data_loader = new DataLoader(config);
	data_loader->get_string_indexes(indexes);
	data_loader->get_vertices(vertices);
	data_loader->get_edges(edges);
	data_loader->get_vplist(vplist, vertices);
	data_loader->get_eplist(eplist, edges);

	//=======data shuffle==========
	shuffle(vertices, edges, vplist, eplist);

	//barrier for data loading
	worker_barrier();
	//=====end of data shuffle======

	//=====load vp_list & ep_list to kv-store =====


	//TODO init node and nodes

	//actor driver starts
	ActorAdapter<int> * actor_adapter = new ActorAdapter<int>(config, my_node, mailbox);
	actor_adapter->Start();

	worker_finalize();
	return 0;
}




