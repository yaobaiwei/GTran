/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

#include <algorithm>
#include <mutex>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <ext/hash_map>
#include <ext/hash_set>

#include <hdfs.h>
#include "glog/logging.h"

#include "storage/vkvstore.hpp"
#include "storage/ekvstore.hpp"
#include "storage/tcp_helper.hpp"
#include "core/id_mapper.hpp"
#include "core/buffer.hpp"
#include "utils/hdfs_core.hpp"
#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "utils/tool.hpp"
#include "utils/global.hpp"
#include "base/type.hpp"
#include "base/node_util.hpp"
#include "base/communication.hpp"


using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;

class DataStore {
public:
	DataStore(Node & node, AbstractIdMapper * id_mapper, Buffer * buf);

	~DataStore();

	void Init(vector<Node> & nodes);

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

	void ReadSnapshot();
	void WriteSnapshot();

	Vertex* GetVertex(vid_t v_id);
	Edge* GetEdge(eid_t e_id);

	void GetAllVertices(vector<vid_t> & vid_list);
	void GetAllEdges(vector<eid_t> & eid_list);

	bool VPKeyIsLocal(vpid_t vp_id);
	bool EPKeyIsLocal(epid_t ep_id);

	bool GetPropertyForVertex(int tid, vpid_t vp_id, value_t & val);
	bool GetPropertyForEdge(int tid, epid_t ep_id, value_t & val);

	bool GetLabelForVertex(int tid, vid_t vid, label_t & label);
	bool GetLabelForEdge(int tid, eid_t eid, label_t & label);

	int GetMachineIdForVertex(vid_t v_id);
	int GetMachineIdForEdge(eid_t e_id);

	void GetNameFromIndex(Index_T type, label_t label, string & str);

	void InsertAggData(agg_t key, vector<value_t> & data);
	void GetAggData(agg_t key, vector<value_t> & data);
	void DeleteAggData(agg_t key);

	// Validation : Get RCT data
	void GetRecentlyCommittedTable (vector<Premitive_T> premitiveList,
									vector<uint64_t> & committedTimeList,
									vector<int> & propertyKeyList,
									vector<uint64_t> & data);

	// For TCP use
	TCPHelper * tcp_helper;

	//single ptr instance
	//diff from Node
	static DataStore* StaticInstanceP(DataStore* p = NULL)
	{
		static DataStore* static_instance_p_ = NULL;
		if(p)
		{
			// if(static_instance_p_)
			// 	delete static_instance_p_;
			// static_instance_p_ = new DataStore;
			static_instance_p_ = p;
		}
			
		assert(static_instance_p_ != NULL);
		return static_instance_p_;
	}

private:

	Buffer * buffer_;
	AbstractIdMapper* id_mapper_;
	Config* config_;
	Node & node_;

	//load the index and data from HDFS
	string_index indexes; //index is global, no need to shuffle
	hash_map<vid_t, Vertex*> v_table;
	hash_map<eid_t, Edge*> e_table;

	unordered_map<agg_t, vector<value_t>> agg_data_table;
	mutex agg_mutex;

	VKVStore * vpstore_;
	EKVStore * epstore_;

	// Validation Use
	// 	Insert V/E, Delete V/E (4 tables)
	// 	Insert/Modify/Delete VP/EP (3/6 tables)
	// 	TrxID --> ObjectList
	// 	One thread access one Transaction at a time.
	hash_map<uint64_t, vector<vid_t>> rct_IV;
	hash_map<uint64_t, vector<eid_t>> rct_IE;
	hash_map<uint64_t, vector<vid_t>> rct_DV;
	hash_map<uint64_t, vector<eid_t>> rct_DE;
	hash_map<uint64_t, vector<vpid_t>> rct_IVP;
	hash_map<uint64_t, vector<epid_t>> rct_IEP;
	hash_map<uint64_t, vector<vpid_t>> rct_MVP;
	hash_map<uint64_t, vector<epid_t>> rct_MEP;
	hash_map<uint64_t, vector<vpid_t>> rct_DVP;
	hash_map<uint64_t, vector<epid_t>> rct_DEP;

	//=========tmp usage=========
	vector<Vertex*> vertices;  //x
	vector<Edge*> edges; //x
	vector<VProperty*> vplist; //x
	vector<EProperty*> eplist; //x
	vector<vp_list*> vp_buf; //x

	typedef map<string, uint8_t> type_map;
	typedef map<string, uint8_t>::iterator type_map_itr;
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
	
	// Validation use
	void get_rct_all(vector<uint64_t> & trxIDList, vector<int> & propertyKeyList, vector<uint64_t> & data); 

	template <class T>
	void get_rct_topo(hash_map<uint64_t, vector<T>> & rct, vector<uint64_t> & trxIDList, vector<int> & propertyKeyList, vector<uint64_t> & data);
	template <class T>
	void get_rct_prop(hash_map<uint64_t, vector<T>> & rct, vector<uint64_t> & trxIDList, vector<int> & propertyKeyList, vector<uint64_t> & data);
};
