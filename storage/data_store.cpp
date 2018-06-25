/*
 * data_store.cpp
 *
 *  Created on: Jun 7, 2018
 *      Author: Hongzhi Chen
 */

#include "storage/data_store.hpp"

DataStore::DataStore(Node & node, Config * config, AbstractIdMapper * id_mapper, Buffer * buf): node_(node), config_(config), id_mapper_(id_mapper), buffer_(buf){
	vpstore_ = NULL;
	epstore_ = NULL;
}

DataStore::~DataStore(){
	delete vpstore_;
	delete epstore_;
}

void DataStore::Init(){
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

void DataStore::LoadDataFromHDFS(){
	get_string_indexes();
	get_vertices();
	get_edges();
	get_vplist();
	get_eplist();
	upload_pty_types();
}

void DataStore::Shuffle()
{
	//vertices
	vector<vector<Vertex*>> vtx_parts;
	vtx_parts.resize(node_.get_local_size());
	for (int i = 0; i < vertices.size(); i++)
	{
		Vertex* v = vertices[i];
		vtx_parts[mymath::hash_mod(v->id.hash(), node_.get_local_size())].push_back(v);
	}
	all_to_all(node_, false, vtx_parts);
	vertices.clear();

	for (int i = 0; i < node_.get_local_size(); i++)
	{
		vertices.insert(vertices.end(), vtx_parts[i].begin(), vtx_parts[i].end());
	}
	vtx_parts.clear();

	//edges
	vector<vector<Edge*>> edges_parts;
	edges_parts.resize(node_.get_local_size());
	for (int i = 0; i < edges.size(); i++)
	{
		Edge* e = edges[i];
		edges_parts[mymath::hash_mod(e->id.hash(), node_.get_local_size())].push_back(e);
	}

	all_to_all(node_, false, edges_parts);

	edges.clear();
	for (int i = 0; i < node_.get_local_size(); i++)
	{
		edges.insert(edges.end(), edges_parts[i].begin(), edges_parts[i].end());
	}
	edges_parts.clear();

	//VProperty
	vector<vector<VProperty*>> vp_parts;
	vp_parts.resize(node_.get_local_size());
	for (int i = 0; i < vplist.size(); i++)
	{
		VProperty* vp = vplist[i];
		vp_parts[mymath::hash_mod(vp->id.hash(), node_.get_local_size())].push_back(vp);
	}
	all_to_all(node_, false, vp_parts);
	vplist.clear();

	for (int i = 0; i < node_.get_local_size(); i++)
	{
		vplist.insert(vplist.end(), vp_parts[i].begin(), vp_parts[i].end());
	}
	vp_parts.clear();

	//EProperty
	vector<vector<EProperty*>> ep_parts;
	ep_parts.resize(node_.get_local_size());
	for (int i = 0; i < eplist.size(); i++)
	{
		EProperty* ep = eplist[i];
		ep_parts[mymath::hash_mod(ep->id.hash(), node_.get_local_size())].push_back(ep);
	}

	all_to_all(node_, false, ep_parts);
	eplist.clear();

	for (int i = 0; i < node_.get_local_size(); i++)
	{
		eplist.insert(eplist.end(), ep_parts[i].begin(), ep_parts[i].end());
	}
	ep_parts.clear();

	//vp_lists
	vector<vector<vp_list*>> vpl_parts;
	vpl_parts.resize(node_.get_local_size());
	for (int i = 0; i < vp_buf.size(); i++)
	{
		vp_list* vp = vp_buf[i];
		vpl_parts[mymath::hash_mod(vp->vid.hash(), node_.get_local_size())].push_back(vp);
	}
	all_to_all(node_, false, vpl_parts);
	vp_buf.clear();

	for (int i = 0; i < node_.get_local_size(); i++)
	{
		vp_buf.insert(vp_buf.end(), vpl_parts[i].begin(), vpl_parts[i].end());
	}
	vpl_parts.clear();

	//ep_lists
	vector<vector<ep_list*>> epl_parts;
	epl_parts.resize(node_.get_local_size());
	for (int i = 0; i < ep_buf.size(); i++)
	{
		ep_list* ep = ep_buf[i];
		epl_parts[mymath::hash_mod(ep->eid.hash(), node_.get_local_size())].push_back(ep);
	}
	all_to_all(node_, false, epl_parts);
	ep_buf.clear();

	for (int i = 0; i < node_.get_local_size(); i++)
	{
		ep_buf.insert(ep_buf.end(), epl_parts[i].begin(), epl_parts[i].end());
	}
	epl_parts.clear();
}


void DataStore::DataConverter(){

	for(int i = 0 ; i < vertices.size(); i++){
		v_table[vertices[i]->id] = vertices[i];
	}
	vector<Vertex*>().swap(vertices);

	for(int i = 0 ; i < vp_buf.size(); i++){
		hash_map<vid_t, Vertex*>::iterator vIter = v_table.find(vp_buf[i]->vid);
		if(vIter == v_table.end()){
			cout << "ERROR: FAILED TO MATCH ONE ELEMENT in vp_buf" << endl;
			exit(-1);
		}
		Vertex * v = vIter->second;
		v->vp_list.insert(v->vp_list.end(), vp_buf[i]->pkeys.begin(), vp_buf[i]->pkeys.end());
//		cout << v->DebugString(); //TEST
	}
	//clean the vp_buf
	for(int i = 0 ; i < vp_buf.size(); i++) delete vp_buf[i];
	vector<vp_list*>().swap(vp_buf);


	for(int i = 0 ; i < edges.size(); i++){
		e_table[edges[i]->id] = edges[i];
	}
	vector<Edge*>().swap(edges);

	for(int i = 0 ; i < ep_buf.size(); i++){
		hash_map<eid_t, Edge*>::iterator eIter = e_table.find(ep_buf[i]->eid);
		if(eIter == e_table.end()){
			cout << "ERROR: FAILED TO MATCH ONE ELEMENT in ep_buf" << endl;
			exit(-1);
		}
		Edge * e = eIter->second;
		e->ep_list.insert(e->ep_list.end(), ep_buf[i]->pkeys.begin(), ep_buf[i]->pkeys.end());
//		cout << e->DebugString(); //TEST
	}
	//clean the ep_buf
	for(int i = 0 ; i < ep_buf.size(); i++) delete ep_buf[i];
	vector<ep_list*>().swap(ep_buf);

	vpstore_->insert_vertex_properties(vplist);
	//clean the vp_list
	for(int i = 0 ; i < vplist.size(); i++){
//		cout << vplist[i]->DebugString(); //TEST
		delete vplist[i];
	}
	vector<VProperty*>().swap(vplist);

	epstore_->insert_edge_properties(eplist);
	//clean the ep_list
	for(int i = 0 ; i < eplist.size(); i++){
//		cout << eplist[i]->DebugString();  //TEST
		delete eplist[i];
	}
	vector<EProperty*>().swap(eplist);
}

Vertex* DataStore::GetVertex(vid_t v_id){
	CHECK(id_mapper_->IsVertexLocal(v_id));
//	return v_table[v_id];

	hash_map<vid_t, Vertex*>::iterator vt_itr = v_table.find(v_id);
	if(vt_itr != v_table.end()){
		return vt_itr->second;
	}
	return NULL;
}

Edge* DataStore::GetEdge(eid_t e_id){
	CHECK(id_mapper_->IsEdgeLocal(e_id));
//	return e_table[e_id];

	hash_map <eid_t, Edge*> ::iterator et_itr = e_table.find(e_id);
	if(et_itr != e_table.end()){
		return et_itr->second;
	}
	return NULL;
}


bool DataStore::GetPropertyForVertex(int tid, vpid_t vp_id, value_t & val){
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
		return true;
	}
	return false;
}


bool DataStore::GetPropertyForEdge(int tid, epid_t ep_id, value_t & val){
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
		return true;
	}
	return false;
}


void DataStore::get_string_indexes()
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

	string vpk_path = config_->HDFS_INDEX_PATH + "./vtx_property_index";
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

void DataStore::get_vertices()
{
	//check path + arrangement
	const char * indir = config_->HDFS_VTX_SUBFOLDER.c_str();

	if (node_.get_local_rank() == MASTER_RANK)
	{
		if(dir_check(indir) == -1)
			exit(-1);
	}

	if (node_.get_local_rank() == MASTER_RANK)
	{
		vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
		master_scatter(node_, false, arrangement);
		vector<string>& assigned_splits = arrangement[0];
		//reading assigned splits (map)
		for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
			load_vertices(it->c_str());
	}
	else
	{
		vector<string> assigned_splits;
		slave_scatter(node_, false, assigned_splits);
		//reading assigned splits (map)
		for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
			load_vertices(it->c_str());
	}
}

void DataStore::load_vertices(const char* inpath)
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
		}else{
			break;
		}
	}
	hdfsCloseFile(fs, in);
	hdfsDisconnect(fs);
}

//Format
//vid [\t] label [\t] #in_nbs [\t] nb1 [space] nb2 [space] ... #out_nbs nb1 [space] nb2 [space] ...
Vertex* DataStore::to_vertex(char* line)
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

void DataStore::get_edges()
{
	//check path + arrangement
	const char * indir = config_->HDFS_EDGE_SUBFOLDER.c_str();
	if (node_.get_local_rank() == MASTER_RANK)
	{
		if(dir_check(indir) == -1)
			exit(-1);
	}

	if (node_.get_local_rank() == MASTER_RANK)
	{
		vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
		master_scatter(node_, false, arrangement);
		vector<string>& assigned_splits = arrangement[0];
		//reading assigned splits (map)
		for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
			load_edges(it->c_str());
	}
	else
	{
		vector<string> assigned_splits;
		slave_scatter(node_, false, assigned_splits);
		//reading assigned splits (map)
		for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
			load_edges(it->c_str());
	}
}

void DataStore::load_edges(const char* inpath)
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
		}else{
			break;
		}
	}
	hdfsCloseFile(fs, in);
	hdfsDisconnect(fs);
}

//Format
//in_v [\t] out_v [\t] label
Edge* DataStore::to_edge(char* line)
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

void DataStore::get_vplist()
{
	//check path + arrangement
	const char * indir = config_->HDFS_VP_SUBFOLDER.c_str();
	if (node_.get_local_rank() == MASTER_RANK)
	{
		if(dir_check(indir) == -1)
			exit(-1);
	}

	if (node_.get_local_rank() == MASTER_RANK)
	{
		vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
		master_scatter(node_, false, arrangement);
		vector<string>& assigned_splits = arrangement[0];
		//reading assigned splits (map)
		for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
			load_vplist(it->c_str());
	}
	else
	{
		vector<string> assigned_splits;
		slave_scatter(node_, false, assigned_splits);
		//reading assigned splits (map)
		for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
			load_vplist(it->c_str());
	}
}

void DataStore::load_vplist(const char* inpath)
{
	hdfsFS fs = get_hdfs_fs();
	hdfsFile in = get_r_handle(inpath, fs);
	LineReader reader(fs, in);
	while (true)
	{
		reader.read_line();
		if (!reader.eof())
			to_vp(reader.get_line(), vplist, vp_buf);
		else
			break;
	}
	hdfsCloseFile(fs, in);
	hdfsDisconnect(fs);
}

//Format
//vid [\t] [kid:value,kid:value,...]
void DataStore::to_vp(char* line, vector<VProperty*> & vplist, vector<vp_list*> & vp_buf)
{
	VProperty * vp = new VProperty;
	vp_list * vpl = new vp_list;

	char * pch;
	pch = strtok(line, "\t");
	int vid = atoi(pch);
	vp->id = vid;
	vpl->vid = vp->id;
	pch = strtok(NULL, "\t");
	string s(pch);

	vector<string> kvpairs = Tool::split(s, "\[\],");
	for(int i = 0 ; i < kvpairs.size(); i++){
		kv_pair p;
		Tool::get_kvpair(kvpairs[i], p);
		V_KVpair v_pair;
		v_pair.key = vpid_t(vid, p.key);
		v_pair.value = p.value;

		//push to property_list of v
		vp->plist.push_back(v_pair);

		//for property index on v
		vpl->pkeys.push_back((label_t)p.key);

		//get and check property's type
		type_map_itr ptr = vtx_pty_key_to_type.find(p.key);
		if(ptr == vtx_pty_key_to_type.end()){
			vtx_pty_key_to_type[p.key] = p.value.type;
		}else{
			CHECK(ptr->second == p.value.type);
		}
	}

	//sort p_list in vertex
	sort(vpl->pkeys.begin(), vpl->pkeys.end());
	vplist.push_back(vp);
	vp_buf.push_back(vpl);

//	cout << "####### " << vp->DebugString(); //DEBUG
}

void DataStore::get_eplist()
{
	//check path + arrangement
	const char * indir = config_->HDFS_EP_SUBFOLDER.c_str();
	if (node_.get_local_rank() == MASTER_RANK)
	{
		if(dir_check(indir) == -1)
			exit(-1);
	}


	if (node_.get_local_rank() == MASTER_RANK)
	{
		vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
		master_scatter(node_, false, arrangement);
		vector<string>& assigned_splits = arrangement[0];
		//reading assigned splits (map)
		for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
			load_eplist(it->c_str());
	}
	else
	{
		vector<string> assigned_splits;
		slave_scatter(node_, false, assigned_splits);
		//reading assigned splits (map)
		for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
			load_eplist(it->c_str());
	}
}

void DataStore::load_eplist(const char* inpath)
{
	hdfsFS fs = get_hdfs_fs();
	hdfsFile in = get_r_handle(inpath, fs);
	LineReader reader(fs, in);
	while (true)
	{
		reader.read_line();
		if (!reader.eof())
			to_ep(reader.get_line(), eplist, ep_buf);
		else
			break;
	}
	hdfsCloseFile(fs, in);
	hdfsDisconnect(fs);
}

//Format
//eid [\t] [kid:value,kid:value,...]
void DataStore::to_ep(char* line, vector<EProperty*> & eplist, vector<ep_list*> & ep_buf)
{
	EProperty * ep = new EProperty;
	ep_list * epl = new ep_list;
	char * pch;
	pch = strtok(line, "\t");
	int in_v = atoi(pch);
	pch = strtok(NULL, "\t");
	int out_v = atoi(pch);
	ep->id = eid_t(in_v, out_v);
	epl->eid = ep->id;

	pch = strtok(NULL, "\t");
	string s(pch);

	vector<string> kvpairs = Tool::split(s, "\[\],");
	for(int i = 0 ; i < kvpairs.size(); i++){
		kv_pair p;
		Tool::get_kvpair(kvpairs[i], p);

		E_KVpair e_pair;
		e_pair.key = epid_t(in_v, out_v, p.key);
		e_pair.value = p.value;

		ep->plist.push_back(e_pair);
		epl->pkeys.push_back((label_t)p.key);

		//get and check property's type
		type_map_itr ptr = edge_pty_key_to_type.find(p.key);
		if(ptr == edge_pty_key_to_type.end()){
			edge_pty_key_to_type[p.key] = p.value.type;
		}else{
			CHECK(ptr->second == p.value.type);
		}
	}

	//sort p_list in vertex
	sort(epl->pkeys.begin(), epl->pkeys.end());
	eplist.push_back(ep);
	ep_buf.push_back(epl);

//	cout << "####### " << ep->DebugString(); //DEBUG
}

void DataStore::upload_pty_types()
{
	if (node_.get_local_rank() == MASTER_RANK)
	{
		vector<type_map> vtx_key_parts;
		vtx_key_parts.resize(node_.get_local_size());
		master_gather(node_, false, vtx_key_parts);

		for (int i = 0; i < node_.get_local_size(); i++)
		{
			if (i != MASTER_RANK)
			{
				type_map & part = vtx_key_parts[i];
				for (type_map_itr it = part.begin(); it != part.end(); it++)
				{
					uint32_t key = it->first;
					type_map_itr eit = vtx_pty_key_to_type.find(key);
					if (eit == vtx_pty_key_to_type.end())
						vtx_pty_key_to_type[key] = it->second;
					else{
						CHECK(eit->second == it->second);
					}
				}
			}
		}
		worker_barrier(node_); //sync

		vector<type_map> edge_key_parts;
		edge_key_parts.resize(node_.get_local_size());
		master_gather(node_, false, edge_key_parts);

		for (int i = 0; i < node_.get_local_size(); i++)
		{
			if (i != MASTER_RANK)
			{
				type_map & part = edge_key_parts[i];
				for (type_map_itr it = part.begin(); it != part.end(); it++)
				{
					uint32_t key = it->first;
					type_map_itr eit = edge_pty_key_to_type.find(key);
					if (eit == edge_pty_key_to_type.end())
						edge_pty_key_to_type[key] = it->second;
					else{
						CHECK(eit->second == it->second);
					}
				}
			}
		}
		worker_barrier(node_); //sync

		hdfsFS fs = get_hdfs_fs();

		const char * dir = config_->HDFS_PTY_TYPE_PATH.c_str();
		if (hdfsExists(fs, dir) == 0) //exists
		{
			if(hdfs_delete(fs, dir) == -1){
				fprintf(stderr, "%s has already existed, try to delete but failed!\n", dir);
				exit(-1);
			}
		}
		if(hdfsCreateDirectory(fs, dir) == -1){
			fprintf(stderr, "Failed to create folder %s!\n", dir);
			exit(-1);
		}

		string path_to_vtx_key_type = config_->HDFS_PTY_TYPE_PATH + "./vtx_type_table";
		hdfsFile vtx_file = get_w_handle(path_to_vtx_key_type.c_str(), fs);

		for(type_map_itr ptr = vtx_pty_key_to_type.begin(); ptr != vtx_pty_key_to_type.end(); ptr++){
			string str = indexes.vpk2str[ptr->first] + "\t" + to_string(ptr->first) + "\t" + to_string(ptr->second) + "\n";
			char * line = str.c_str();
			if (hdfsWrite(fs, vtx_file, line, str.length()) == -1)
			{
				fprintf(stderr, "Failed to write file %s!\n", path_to_vtx_key_type);
				exit(-1);
			}
		}

		//flush
		if (hdfsFlush(fs, vtx_file))
		{
			fprintf(stderr, "Failed to 'flush' %s!\n", path_to_vtx_key_type);
			exit(-1);
		}
		hdfsCloseFile(fs, vtx_file);



		string path_to_edge_key_type = config_->HDFS_PTY_TYPE_PATH + "./edge_type_table";
		hdfsFile edge_file = get_w_handle(path_to_edge_key_type.c_str(), fs);

		for(type_map_itr ptr = edge_pty_key_to_type.begin(); ptr != edge_pty_key_to_type.end(); ptr++){
			string str = indexes.epk2str[ptr->first] + "\t" + to_string(ptr->first) + "\t" + to_string(ptr->second) + "\n";
			char * line = str.c_str();
			if (hdfsWrite(fs, edge_file, line, str.length()) == -1)
			{
				fprintf(stderr, "Failed to write file %s!\n", path_to_edge_key_type);
				exit(-1);
			}
		}

		//flush
		if (hdfsFlush(fs, edge_file))
		{
			fprintf(stderr, "Failed to 'flush' %s!\n", path_to_edge_key_type);
			exit(-1);
		}
		hdfsCloseFile(fs, edge_file);
		hdfsDisconnect(fs);
	}
	else
	{
		slave_gather(node_, false, vtx_pty_key_to_type);
		worker_barrier(node_);
		slave_gather(node_, false, edge_pty_key_to_type);
		worker_barrier(node_);
	}
}
