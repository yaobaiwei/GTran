/*
 * gquery.cpp
 *
 *  Created on: May 29, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include <string.h>
#include <stdlib.h>
#include <ext/hash_map>
#include <ext/hash_set>

#include <hdfs.h>

#include "utils/hdfs_core.hpp"
#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "utils/type.hpp"
#include "utils/tool.hpp"
#include "utils/global.hpp"
#include "core/id_mapper.hpp"
#include "core/buffer.hpp"
#include "base/communication.hpp"
#include "storage/vkvstore.hpp"
#include "storage/ekvstore.hpp"

using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;

class DataStore {
public:
	DataStore(Config * config, AbstractIdMapper * id_mapper, Buffer * buf): config_(config), id_mapper_(id_mapper), buffer_(buf){
		vtx_count = 0;
		edge_count = 0;
		vpstore_ = NULL;
		epstore_ = NULL;
	}

	~DataStore(){
		delete vpstore_;
		delete epstore_;
	}

	void Init(){
    	vpstore_ = new VKVStore(config_, buffer_);
    	epstore_ = new EKVStore(config_, buffer_);
    	vpstore_->init();
    	epstore_->init();
	}
	//index format
	//string \t index [int]
	/*
	 * 	unordered_map<string, label_t> str2el; //map to edge_label
	 * 	unordered_map<label_t, string> el2str;
	 *	unordered_map<string, label_t> str2epk; //map to edge's property key
	 *	unordered_map<label_t, string> epk2str;
	 *	unordered_map<string, label_t> str2vl; //map to vtx_label
	 *	unordered_map<label_t, string> vl2str;
	 *	unordered_map<string, label_t> str2vpk; //map to vtx's property key
	 *	unordered_map<label_t, string> vpk2str;
	 */

	void LoadDataFromHDFS(){
		get_string_indexes();
		get_vertices();
		get_edges();
		get_vplist();
		get_eplist();
	}

	void Shuffle()
	{
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


	void DataConverter(){

		for(int i = 0 ; i < vertices.size(); i++){
			v_table[vertices[i]->id] = vertices[i];
		}
		vector<Vertex*>().swap(vertices);

		for(int i = 0 ; i < edges.size(); i++){
			e_table[edges[i]->id] = edges[i];
		}
		vector<Edge*>().swap(edges);

		vpstore_->insert_vertex_properties(vplist);
		//clean the vp_list
		for(int i = 0 ; i < vplist.size(); i++) delete vplist[i];
		vector<VProperty*>().swap(vplist);

		epstore_->insert_edge_properties(eplist);
		//clean the ep_list
		for(int i = 0 ; i < eplist.size(); i++) delete eplist[i];
		vector<EProperty*>().swap(eplist);
	}

	Vertex* GetVertex(vid_t v_id){
		CHECK(id_mapper_->IsVertexLocal(v_id));
		return v_table[v_id];
	}

	Edge* GetEdge(eid_t e_id){
		CHECK(id_mapper_->IsEdgeLocal(e_id));
		return e_table[e_id];
	}


	void GetPropertyForVertex(int tid, vpid_t vp_id, value_t & val){
		elem_t em;
		if(id_mapper_->IsVPropertyLocal(vp_id)){ 	//locally
			vpstore_->get_property_local(vp_id.value(), em);
		}else{										//remotely
			vpstore_->get_property_remote(tid, id_mapper_->GetMachineIdForVProperty(vp_id), vp_id.value(), em);
		}

		if(em.sz > 0){
			val.type = em.type;
			val.content.resize(em.sz);
			std::copy(em.content, em.content+em.sz, val.content.begin());
		}else{
			fprintf(stderr,"ERROR: Failed to get property based on vp_id\n");
		}
	}


	void GetPropertyForEdge(int tid, epid_t ep_id, value_t & val){
		elem_t em;
		if(id_mapper_->IsEPropertyLocal(ep_id)){ 	//locally
			epstore_->get_property_local(ep_id.value(), em);
		}else{										//remotely
			epstore_->get_property_remote(tid, id_mapper_->GetMachineIdForEProperty(ep_id), ep_id.value(), em);
		}

		if(em.sz > 0){
			val.type = em.type;
			val.content.resize(em.sz);
			std::copy(em.content, em.content+em.sz, val.content.begin());
		}else{
			fprintf(stderr,"ERROR: Failed to get property based on ep_id\n");
		}
	}

private:

	Buffer * buffer_;
	AbstractIdMapper* id_mapper_;
	Config* config_;

	//load the index and data from HDFS
	string_index indexes; //index is global, no need to shuffle
	hash_map<vid_t, Vertex*> v_table;
	hash_map <eid_t, Edge*> e_table;

    VKVStore * vpstore_;
    EKVStore * epstore_;
	//=========tmp usage=========

	vector<Vertex*> vertices;
	vector<Edge*> edges;
	vector<VProperty*> vplist;
	vector<EProperty*> eplist;

	hash_map<vid_t, uint32_t> vtx_offset_map;
	hash_map<eid_t, uint32_t> edge_offset_map;

	uint32_t vtx_count;
	uint32_t edge_count;

	//==========tmp usage=========

	void get_string_indexes()
	{
		hdfsFS fs = get_hdfs_fs();

		string el_path = config_->HDFS_INDEX_PATH + "./edge_label";
		hdfsFile el_file = get_r_handle(el_path.c_str(), fs);
		LineReader el_reader(fs, el_file);
		while(true)
		{
			el_reader.read_line();
			if (!el_reader.eof())
			{
				char * line = el_reader.get_line();
				char * pch;
				pch = strtok(line, "\t");
				string key(pch);
				pch = strtok(NULL, "\t");
				label_t id = atoi(pch);

				// both string and ID are unique
				assert(indexes.str2el.find(key) == indexes.str2el.end());
				assert(indexes.el2str.find(id) == indexes.el2str.end());

				indexes.str2el[key] = id;
				indexes.el2str[id] = key;
			}
			else
				break;
		}
		hdfsCloseFile(fs, el_file);

		string epk_path = config_->HDFS_INDEX_PATH + "./edge_property_index";
		hdfsFile epk_file = get_r_handle(epk_path.c_str(), fs);
		LineReader epk_reader(fs, epk_file);
		while(true)
		{
			epk_reader.read_line();
			if (!epk_reader.eof())
			{
				char * line = epk_reader.get_line();
				char * pch;
				pch = strtok(line, "\t");
				string key(pch);
				pch = strtok(NULL, "\t");
				label_t id = atoi(pch);

				// both string and ID are unique
				assert(indexes.str2epk.find(key) == indexes.str2epk.end());
				assert(indexes.epk2str.find(id) == indexes.epk2str.end());

				indexes.str2epk[key] = id;
				indexes.epk2str[id] = key;
			}
			else
				break;
		}
		hdfsCloseFile(fs, epk_file);

		string vl_path = config_->HDFS_INDEX_PATH + "./vtx_label";
		hdfsFile vl_file = get_r_handle(vl_path.c_str(), fs);
		LineReader vl_reader(fs, vl_file);
		while(true)
		{
			vl_reader.read_line();
			if (!vl_reader.eof())
			{
				char * line = vl_reader.get_line();
				char * pch;
				pch = strtok(line, "\t");
				string key(pch);
				pch = strtok(NULL, "\t");
				label_t id = atoi(pch);

				// both string and ID are unique
				assert(indexes.str2vl.find(key) == indexes.str2vl.end());
				assert(indexes.vl2str.find(id) == indexes.vl2str.end());

				indexes.str2vl[key] = id;
				indexes.vl2str[id] = key;
			}
			else
				break;
		}
		hdfsCloseFile(fs, vl_file);

		string vpk_path = config_->HDFS_INDEX_PATH + "./edge_label";
		hdfsFile vpk_file = get_r_handle(vpk_path.c_str(), fs);
		LineReader vpk_reader(fs, vpk_file);
		while(true)
		{
			vpk_reader.read_line();
			if (!vpk_reader.eof())
			{
				char * line = vpk_reader.get_line();
				char * pch;
				pch = strtok(line, "\t");
				string key(pch);
				pch = strtok(NULL, "\t");
				label_t id = atoi(pch);

				// both string and ID are unique
				assert(indexes.str2vpk.find(key) == indexes.str2vpk.end());
				assert(indexes.vpk2str.find(id) == indexes.vpk2str.end());

				indexes.str2vpk[key] = id;
				indexes.vpk2str[id] = key;
			}
			else
				break;
		}
		hdfsCloseFile(fs, vpk_file);
		hdfsDisconnect(fs);
	}

	void get_vertices()
	{
		//check path + arrangement
		const char * indir = (config_->HDFS_INPUT_PATH + config_->HDFS_VTX_SUBFOLDER).c_str();

		if (_my_rank == MASTER_RANK)
		{
			if(dir_check(indir) == -1)
				exit(-1);
		}

		vector<vector<string> >* arrangement;
		if (_my_rank == MASTER_RANK)
		{
			arrangement = dispatch_locality(indir);
			master_scatter(*arrangement);
			vector<string>& assigned_splits = (*arrangement)[0];
			//reading assigned splits (map)
			for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
				load_vertices(it->c_str());
			delete arrangement;
		}
		else
		{
			vector<string> assigned_splits;
			slave_scatter(assigned_splits);
			//reading assigned splits (map)
			for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
				load_vertices(it->c_str());
		}
	}

	void load_vertices(const char* inpath)
	{
		hdfsFS fs = get_hdfs_fs();
		hdfsFile in = get_r_handle(inpath, fs);
		LineReader reader(fs, in);
		while (true)
		{
			reader.read_line();
			if (!reader.eof()){
				Vertex * v = to_vertex(reader.get_line());
				vertices.push_back(v);
				vtx_offset_map[v->id] = vtx_count;
				vtx_count++;
			}else{
				break;
			}
		}
		hdfsCloseFile(fs, in);
		hdfsDisconnect(fs);
	}

	Vertex* to_vertex(char* line)
	{
		Vertex * v = new Vertex;
		char * pch;
		pch = strtok(line, "\t");
		v->id = atoi(pch);
		pch = strtok(NULL, "\t");
		v->label = (label_t)atoi(pch);
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
		return v;
	}

	void get_edges()
	{
		//check path + arrangement
		const char * indir = (config_->HDFS_INPUT_PATH + config_->HDFS_EDGE_SUBFOLDER).c_str();
		if (_my_rank == MASTER_RANK)
		{
			if(dir_check(indir) == -1)
				exit(-1);
		}

		vector<vector<string> >* arrangement;
		if (_my_rank == MASTER_RANK)
		{
			arrangement = dispatch_locality(indir);
			master_scatter(*arrangement);
			vector<string>& assigned_splits = (*arrangement)[0];
			//reading assigned splits (map)
			for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
				load_edges(it->c_str());
			delete arrangement;
		}
		else
		{
			vector<string> assigned_splits;
			slave_scatter(assigned_splits);
			//reading assigned splits (map)
			for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
				load_edges(it->c_str());
		}
	}

	void load_edges(const char* inpath)
	{
		hdfsFS fs = get_hdfs_fs();
		hdfsFile in = get_r_handle(inpath, fs);
		LineReader reader(fs, in);
		while (true)
		{
			reader.read_line();
			if (!reader.eof()){
				Edge * e = to_edge(reader.get_line());
				edges.push_back(e);
				edge_offset_map[e->id] = edge_count;
				edge_count++;
			}else{
				break;
			}
		}
		hdfsCloseFile(fs, in);
		hdfsDisconnect(fs);
	}

	Edge* to_edge(char* line)
	{
		Edge * e = new Edge;
		char * pch;
		pch = strtok(line, "\t");
		int v_1 = atoi(pch);
		pch = strtok(NULL, "\t");
		int v_2 = atoi(pch);

		eid_t eid(v_1, v_2);
		e->id = eid;
		pch = strtok(NULL, "\t");
		e->label = (label_t)atoi(pch);
		return e;
	}

	void get_vplist()
	{
		//check path + arrangement
		const char * indir = (config_->HDFS_INPUT_PATH + config_->HDFS_VP_SUBFOLDER).c_str();
		if (_my_rank == MASTER_RANK)
		{
			if(dir_check(indir) == -1)
				exit(-1);
		}

		vector<vector<string> >* arrangement;
		if (_my_rank == MASTER_RANK)
		{
			arrangement = dispatch_locality(indir);
			master_scatter(*arrangement);
			vector<string>& assigned_splits = (*arrangement)[0];
			//reading assigned splits (map)
			for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
				load_vplist(it->c_str());
			delete arrangement;
		}
		else
		{
			vector<string> assigned_splits;
			slave_scatter(assigned_splits);
			//reading assigned splits (map)
			for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
				load_vplist(it->c_str());
		}
		//clear map
		vtx_offset_map.clear();
	}

	void load_vplist(const char* inpath)
	{
		hdfsFS fs = get_hdfs_fs();
		hdfsFile in = get_r_handle(inpath, fs);
		LineReader reader(fs, in);
		while (true)
		{
			reader.read_line();
			if (!reader.eof())
				vplist.push_back(to_vp(reader.get_line()));
			else
				break;
		}
		hdfsCloseFile(fs, in);
		hdfsDisconnect(fs);
	}

	VProperty* to_vp(char* line)
	{
		VProperty * vp = new VProperty;
		char * pch;
		pch = strtok(line, "\t");
		int vid = atoi(pch);
		vp->id = vid;
		pch = strtok(NULL, "\t");
		string s(pch);

		int offset = vtx_offset_map[vp->id];
		Vertex * v = vertices[offset];

		vector<string> kvpairs = Tool::split(s, "\[\],");
		for(int i = 0 ; i < kvpairs.size(); i++){
			kv_pair p;
			Tool::get_kvpair(kvpairs[i], p);

			V_KVpair v_pair;
			v_pair.key = vpid_t(vid, p.key);
			v_pair.value = p.value;
			vp->plist.push_back(v_pair);

			v->vp_list.push_back((label_t)p.key);
		}

		//sort p_list in vertex
		sort(v->vp_list.begin(), v->vp_list.end());
		return vp;
	}

	void get_eplist()
	{
		//check path + arrangement
		const char * indir = (config_->HDFS_INPUT_PATH + config_->HDFS_EP_SUBFOLDER).c_str();
		if (_my_rank == MASTER_RANK)
		{
			if(dir_check(indir) == -1)
				exit(-1);
		}

		vector<vector<string> >* arrangement;
		if (_my_rank == MASTER_RANK)
		{
			arrangement = dispatch_locality(indir);
			master_scatter(*arrangement);
			vector<string>& assigned_splits = (*arrangement)[0];
			//reading assigned splits (map)
			for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
				load_eplist(it->c_str());
			delete arrangement;
		}
		else
		{
			vector<string> assigned_splits;
			slave_scatter(assigned_splits);
			//reading assigned splits (map)
			for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
				load_eplist(it->c_str());
		}
		//clear map
		edge_offset_map.clear();
	}

	void load_eplist(const char* inpath)
	{
		hdfsFS fs = get_hdfs_fs();
		hdfsFile in = get_r_handle(inpath, fs);
		LineReader reader(fs, in);
		while (true)
		{
			reader.read_line();
			if (!reader.eof())
				eplist.push_back(to_ep(reader.get_line()));
			else
				break;
		}
		hdfsCloseFile(fs, in);
		hdfsDisconnect(fs);
	}

	EProperty* to_ep(char* line)
	{
		EProperty * ep = new EProperty;
		char * pch;
		pch = strtok(line, "\t");
		int in_v = atoi(pch);
		pch = strtok(NULL, "\t");
		int out_v = atoi(pch);
		ep->id = eid_t(in_v, out_v);

		pch = strtok(NULL, "\t");
		string s(pch);

		int offset = edge_offset_map[ep->id];
		Edge * e = edges[offset];

		vector<string> kvpairs = Tool::split(s, "\[\],");
		for(int i = 0 ; i < kvpairs.size(); i++){
			kv_pair p;
			Tool::get_kvpair(kvpairs[i], p);

			E_KVpair e_pair;
			e_pair.key = epid_t(in_v, out_v, p.key);
			e_pair.value = p.value;
			ep->plist.push_back(e_pair);

			e->ep_list.push_back((label_t)p.key);
		}

		//sort p_list in vertex
		sort(e->ep_list.begin(), e->ep_list.end());

		return ep;
	}

};
