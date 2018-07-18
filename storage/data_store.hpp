/*
 * gquery.cpp
 *
 *  Created on: May 29, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include <string>
#include <stdlib.h>
#include <ext/hash_map>
#include <ext/hash_set>

#include <hdfs.h>
#include "glog/logging.h"

#include "storage/vkvstore.hpp"
#include "storage/ekvstore.hpp"
#include "core/id_mapper.hpp"
#include "core/buffer.hpp"
#include "utils/hdfs_core.hpp"
#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "utils/tool.hpp"
#include "utils/global.hpp"
#include "base/type.hpp"
#include "base/communication.hpp"


using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;

class DataStore {
public:
	DataStore(Node & node, Config * config, AbstractIdMapper * id_mapper, Buffer * buf);

	~DataStore();

	void Init();

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

	void LoadDataFromHDFS();
	void Shuffle();
	void DataConverter();


	Vertex* GetVertex(vid_t v_id);

	Edge* GetEdge(eid_t e_id);

	void GetAllVertices(vector<vid_t> & vid_list);

	void GetAllEdges(vector<eid_t> & eid_list);

	bool GetPropertyForVertex(int tid, vpid_t vp_id, value_t & val);

	bool GetPropertyForEdge(int tid, epid_t ep_id, value_t & val);

	bool GetLabelForVertex(int tid, vid_t vid, label_t & label);

	bool GetLabelForEdge(int tid, eid_t eid, label_t & label);

	int GetMachineIdForVertex(vid_t v_id);

	int GetMachineIdForEdge(eid_t e_id);

private:

	Buffer * buffer_;
	AbstractIdMapper* id_mapper_;
	Config* config_;
	Node & node_;

	//load the index and data from HDFS
	string_index indexes; //index is global, no need to shuffle
	hash_map<vid_t, Vertex*> v_table;
	hash_map<eid_t, Edge*> e_table;

    VKVStore * vpstore_;
    EKVStore * epstore_;

	//=========tmp usage=========
	vector<Vertex*> vertices;  //x
	vector<Edge*> edges; //x
	vector<VProperty*> vplist; //x
	vector<EProperty*> eplist; //x
	vector<vp_list*> vp_buf; //x

	typedef hash_map<uint32_t, uint8_t> type_map;
	typedef hash_map<uint32_t, uint8_t>::iterator type_map_itr;
	type_map vtx_pty_key_to_type; //x
	type_map edge_pty_key_to_type; //x

	//==========tmp usage=========

	void get_string_indexes();
	void get_vertices();
	void load_vertices(const char* inpath);
	Vertex* to_vertex(char* line);

	void get_vplist();
	void load_vplist(const char* inpath);
	void to_vp(char* line, vector<VProperty*> & vplist, vector<vp_list*> & vp_buf);

	void get_eplist();
	void load_eplist(const char* inpath);
	void to_ep(char* line, vector<EProperty*> & eplist);

	void upload_pty_types();
};
