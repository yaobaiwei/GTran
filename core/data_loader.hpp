/*
 * data_loader.hpp
 *
 *  Created on: May 21, 2018
 *      Author: Hongzhi Chen
 */

#ifndef DATA_LOADER_HPP_
#define DATA_LOADER_HPP_

#include <string.h>
#include <stdlib.h>
#include <ext/hash_map>
#include <ext/hash_set>

#include <hdfs.h>
#include "utils/hdfs_core.hpp"
#include "utils/type.hpp"
#include "utils/tool.hpp"
#include "utils/global.hpp"
#include "base/communication.hpp"

using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;

class DataLoader {
public:
	DataLoader(Config * config) : config_(config){
		vtx_count = 0;
		edge_count = 0;
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
	void get_string_indexes(string_index & indexes){
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

	void get_vertices(vector<Vertex*> & vertices){
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
				load_vertices(it->c_str(), vertices);
			delete arrangement;
		}
		else
		{
			vector<string> assigned_splits;
			slave_scatter(assigned_splits);
			//reading assigned splits (map)
			for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
				load_vertices(it->c_str(), vertices);
		}
	}

	void load_vertices(const char* inpath, vector<Vertex*> & vertices)
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

	void get_edges(vector<Edge*> & edges){
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
				load_edges(it->c_str(), edges);
			delete arrangement;
		}
		else
		{
			vector<string> assigned_splits;
			slave_scatter(assigned_splits);
			//reading assigned splits (map)
			for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
				load_edges(it->c_str(), edges);
		}
	}

	void load_edges(const char* inpath, vector<Edge*> & edges)
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

	void get_vplist(vector<VProperty*> & vplist, vector<Vertex*> & vertices){
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
				load_vplist(it->c_str(), vplist, vertices);
			delete arrangement;
		}
		else
		{
			vector<string> assigned_splits;
			slave_scatter(assigned_splits);
			//reading assigned splits (map)
			for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
				load_vplist(it->c_str(), vplist, vertices);
		}
		//clear map
		vtx_offset_map.clear();
	}

	void load_vplist(const char* inpath, vector<VProperty*> & vplist, vector<Vertex*> & vertices)
	{
		hdfsFS fs = get_hdfs_fs();
		hdfsFile in = get_r_handle(inpath, fs);
		LineReader reader(fs, in);
		while (true)
		{
			reader.read_line();
			if (!reader.eof())
				vplist.push_back(to_vp(reader.get_line(), vertices));
			else
				break;
		}
		hdfsCloseFile(fs, in);
		hdfsDisconnect(fs);
	}

	VProperty* to_vp(char* line, vector<Vertex*> & vertices)
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

	void get_eplist(vector<EProperty*> & eplist, vector<Edge*> & edges){
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
				load_eplist(it->c_str(), eplist, edges);
			delete arrangement;
		}
		else
		{
			vector<string> assigned_splits;
			slave_scatter(assigned_splits);
			//reading assigned splits (map)
			for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
				load_eplist(it->c_str(), eplist, edges);
		}
		//clear map
		edge_offset_map.clear();
	}

	void load_eplist(const char* inpath, vector<EProperty*> & eplist, vector<Edge*> & edges)
	{
		hdfsFS fs = get_hdfs_fs();
		hdfsFile in = get_r_handle(inpath, fs);
		LineReader reader(fs, in);
		while (true)
		{
			reader.read_line();
			if (!reader.eof())
				eplist.push_back(to_ep(reader.get_line(), edges));
			else
				break;
		}
		hdfsCloseFile(fs, in);
		hdfsDisconnect(fs);
	}

	EProperty* to_ep(char* line, vector<Edge*> & edges)
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

private:
	Config * config_;
	int vtx_count;
	int edge_count;
	hash_map<vid_t, int> vtx_offset_map;
	hash_map<eid_t, int> edge_offset_map;
};



#endif /* DATA_LOADER_HPP_ */
