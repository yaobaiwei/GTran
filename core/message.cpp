/*
 * message.cpp
 *
 *  Created on: Jun 8, 2018
 *      Author: Hongzhi Chen
 */

#include "core/message.hpp"

ibinstream& operator<<(ibinstream& m, const Branch_Info& info)
{
	m << info.node_id;
	m << info.thread_id;
	m << info.index;
	m << info.key;
	m << info.msg_id;
	m << info.msg_path;
	return m;
}

obinstream& operator>>(obinstream& m, Branch_Info& info)
{
	m >> info.node_id;
	m >> info.thread_id;
	m >> info.index;
	m >> info.key;
	m >> info.msg_id;
	m >> info.msg_path;
	return m;
}

ibinstream& operator<<(ibinstream& m, const Meta& meta)
{
	m << meta.qid;
	m << meta.step;
	m << meta.recver_nid;
	m << meta.recver_tid;
	m << meta.msg_type;
	m << meta.msg_path;
	m << meta.parent_nid;
	m << meta.parent_tid;
	m << meta.branch_infos;
	m << meta.actors;
	return m;
}

obinstream& operator>>(obinstream& m, Meta& meta)
{
	m >> meta.qid;
	m >> meta.step;
	m >> meta.recver_nid;
	m >> meta.recver_tid;
	m >> meta.msg_type;
	m >> meta.msg_path;
	m >> meta.parent_nid;
	m >> meta.parent_tid;
	m >> meta.branch_infos;
	m >> meta.actors;
	return m;
}

std::string Meta::DebugString() const {
	std::stringstream ss;
	ss << "Meta: {";
	ss << "  qid: " << qid;
	ss << ", step: " << step;
	ss << ", recver node: " << recver_nid << ":" << recver_tid;
	ss << ", msg type: " << MsgType[static_cast<int>(msg_type)];
	ss << ", msg path: " << msg_path;
	ss << ", parent node: " << parent_nid;
	ss << ", paraent thread: " << parent_tid;
	if(msg_type == MSG_T::INIT){
		ss << ", actors [";
		for(auto &actor : actors){
			ss  << ActorType[static_cast<int>(actor.actor_type)];
		}
		ss << "]";
	}
	ss << "}";
	return ss.str();
}

ibinstream& operator<<(ibinstream& m, const Message& msg)
{
	m << msg.meta;
	m << msg.data;
	m << msg.max_data_size;
	m << msg.data_size;
	return m;
}

obinstream& operator>>(obinstream& m, Message& msg)
{
	m >> msg.meta;
	m >> msg.data;
	m >> msg.max_data_size;
	m >> msg.data_size;
	return m;
}

void Message::CreateInitMsg(uint64_t qid, int parent_node, int nodes_num, int recv_tid, vector<Actor_Object>& actors, int max_data_size, vector<Message>& vec)
{
	// assign receiver thread id
	Meta m;
	m.qid = qid;
	m.step = 0;
	m.recver_nid = -1;					// assigned in for loop
	m.recver_tid = recv_tid;
	m.parent_nid = parent_node;
	m.parent_tid = recv_tid;
	m.msg_type = MSG_T::INIT;
	m.msg_path = to_string(nodes_num);
	m.actors = actors;

	for(int i = 0; i < nodes_num; i++){
		Message msg;
		msg.meta = m;
		msg.meta.recver_nid = i;
		msg.max_data_size = max_data_size;
		vec.push_back(move(msg));
	}
}


void Message::CreateNextMsg(vector<Actor_Object>& actors, vector<pair<history_t, vector<value_t>>>& data, int num_thread, DataStore* data_store, vector<Message>& vec)
{
	Meta m = this->meta;
	m.step = actors[this->meta.step].next_actor;

	// update recver
	// specify if recver route should not be changed
	bool routeAssigned = update_route(m, actors);

	dispatch_data(m, actors, data, num_thread, data_store, routeAssigned,vec);
	// set disptching path
	string num = to_string(vec.size());
	if(meta.msg_path != ""){
		num = "\t" + num;
	}

	for(auto &msg : vec){
		msg.meta.msg_path += num;
	}
}

void Message::CreateBranchedMsg(vector<Actor_Object>& actors, vector<int>& steps, int num_thread, DataStore* data_store, vector<Message>& vec){
	Meta m = this->meta;

	// append steps num into msg path
	int step_count = steps.size();

	// update branch info
	Branch_Info info;

	// parent msg end path
	string parent_path = "";

	int branch_depth = m.branch_infos.size() - 1;
	if(branch_depth >= 0){
		// use parent branch's route
		info = m.branch_infos[branch_depth];
		parent_path = info.msg_path;

		// update current branch's end path with steps num
		info.msg_path += "\t" + to_string(step_count);
	}else{
		info.node_id = m.parent_nid;
		info.thread_id = m.parent_tid;
		info.key = -1;
		info.msg_path = to_string(step_count);
		info.msg_id = 0;
	}

	string tail = m.msg_path.substr(parent_path.size());
	// append "\t" to tail when first char is not "\t"
	if(tail != "" && tail.find("\t") != 0){
		tail = "\t" + tail;
	}

	//	insert step_count into current msg path
	// 	e.g.:
	// "3\t2\t4" with parent path "3\t2", steps num 5
	//  info.msg_path => "3\t2\t5"
	//  tail = "4"
	//  msg_path = "3\t2\t5\t4"
	m.msg_path = info.msg_path + tail;

	// copy labeled data to each step
	for(int i = 0; i < steps.size(); i ++){
		Meta step_meta = m;

		int step = steps[i];
		bool routeAssigned = false;
		if(actors[step].IsBarrier()){
			step_meta.msg_type = MSG_T::BARRIER;
			routeAssigned = true;
		}
		else{
			step_meta.msg_type = MSG_T::SPAWN;
		}
		info.index = i + 1;
		step_meta.branch_infos.push_back(info);
		step_meta.step = step;

		auto temp = data;
		// feed data to msg
		int count = vec.size();
		dispatch_data(step_meta, actors, temp, num_thread, data_store, routeAssigned, vec);

		// set msg_path for each branch
		for(int j = count; j < vec.size(); j++){
			vec[j].meta.msg_path += "\t" + to_string(vec.size() - count);
		}
	}
}

void Message::CreateBranchedMsgWithHisLabel(vector<Actor_Object>& actors, vector<int>& steps, uint64_t msg_id, int num_thread, DataStore* data_store, vector<Message>& vec){
	Meta m = this->meta;

	// update branch info
	Branch_Info info;
	info.node_id = m.recver_nid;
	info.thread_id = m.recver_tid;
	info.key = m.step;
	info.msg_path = m.msg_path;
	info.msg_id = msg_id;

	// label each data with unique id
	vector<pair<history_t, vector<value_t>>> labeled_data;
	int count = 0;
	for (auto pair : data){
		for(auto& value : pair.second){
			value_t v;
			Tool::str2int(to_string(count ++), v);
			history_t his = pair.first;
			his.push_back(make_pair(m.step, move(v)));
			labeled_data.push_back(make_pair(move(his), vector<value_t>{value}));
		}
	}

	// copy labeled data to each step
	for(int i = 0; i < steps.size(); i ++){
		int step = steps[i];
		Meta step_meta = m;
		bool routeAssigned = false;
		if(actors[step].IsBarrier()){
			step_meta.msg_type = MSG_T::BARRIER;
			routeAssigned = true;
		}
		else{
			step_meta.msg_type = MSG_T::SPAWN;
		}
		info.index = i + 1;
		step_meta.branch_infos.push_back(info);
		step_meta.step = step;

		auto temp = labeled_data;
		// feed data to msg
		int count = vec.size();
		dispatch_data(step_meta, actors, temp, num_thread, data_store, routeAssigned, vec);

		// set msg_path for each branch
		for(int j = count; j < vec.size(); j++){
			vec[j].meta.msg_path += "\t" + to_string(vec.size() - count);
		}
	}
}

void Message::dispatch_data(Meta& m, vector<Actor_Object>& actors, vector<pair<history_t, vector<value_t>>>& data, int num_thread, DataStore* data_store, bool route_assigned, vector<Message>& vec)
{
	// node id to data
	map<int, vector<pair<history_t, vector<value_t>>>> id2data;
	vector<pair<history_t, vector<value_t>>> empty_his;

	int branch_depth = m.branch_infos.size() - 1;
	int his_key = -1;
	if(branch_depth >= 0){
		his_key = m.branch_infos[branch_depth].key;
	}

	// enable route mapping
	if(actors[m.step].actor_type == ACTOR_T::TRAVERSAL){
		for(auto& p: data){
			map<int, vector<value_t>> id2value_t;
			if(p.second.size() == 0){
				auto itr = merge_hisotry(empty_his, p.first, his_key);
				if(itr == empty_his.end()){
					empty_his.push_back(move(p));
				}
			}else{
				// get node id
				for(auto& v : p.second){
					int node = get_node_id(v, data_store);
					id2value_t[node].push_back(move(v));
				}

				// insert his/value pair to corresponding node
				for(auto& item : id2value_t){
					id2data[item.first].push_back(make_pair(p.first, move(item.second)));
				}
			}
		}
	}
	else{
		for(auto& p: data){
			if(p.second.size() == 0){
				auto itr = merge_hisotry(empty_his, p.first, his_key);
				if(itr == empty_his.end()){
					empty_his.push_back(move(p));
				}
			}else{
				id2data[m.recver_nid].push_back(move(p));
			}
		}
	}

	for(auto& item : id2data){
		// feed data to msg
		do{
			Message msg(m);
			msg.max_data_size = this->max_data_size;
			msg.meta.recver_nid = item.first;
			if(! route_assigned){
				msg.meta.recver_tid = (m.recver_tid ++) % num_thread;
			}
			msg.FeedData(item.second);
			vec.push_back(move(msg));
		}
		while((item.second.size() != 0));	// Data no consumed
	}

	// send history with empty data
	if(empty_his.size() != 0){
		// empty data should be send to:
		// 1. barrier actor, msg_type = BARRIER
		// 2. branch actor: which will broadcast empty data to barriers inside each branches for msg collection, msg_type = SPAWN
		// 3. labelled branch parent: which will collect branched msg with label, msg_type = BRANCH
		while(m.step < actors.size()){
			if(actors[m.step].IsBarrier()){
				// to barrier
				break;
			}
			else if(actors[m.step].actor_type == ACTOR_T::BRANCH){
				if(m.step <= this->meta.step){
					// to branch parent, pop back one branch info
					// as barrier actor is not founded in sub branch, continue to search
					m.branch_infos.pop_back();
				}else{
					// to branch actor for the first time, should broadcast empty data to each branches
					break;
				}
			}
			else if(m.step < this->meta.step){
				// to labelled branch parent
				break;
			}

			m.step = actors[m.step].next_actor;
		}

		update_route(m, actors);
		// feed history to msg
		do{
			Message msg(m);
			msg.max_data_size = this->max_data_size;
			msg.FeedData(empty_his);
			vec.push_back(move(msg));
		}
		while((empty_his.size() != 0));	// Data no consumed
	}
}

bool Message::update_route(Meta& m, vector<Actor_Object>& actors){
	int branch_depth = m.branch_infos.size() - 1;
	// update recver route & msg_type
	if(actors[m.step].IsBarrier()){
		if(branch_depth >= 0){
			// barrier actor in branch
			m.recver_nid = m.branch_infos[branch_depth].node_id;
			m.recver_tid = m.branch_infos[branch_depth].thread_id;
		}
		else{
			// barrier actor in main query
			m.recver_nid = m.parent_nid;
			m.recver_tid = m.parent_tid;
		}
		m.msg_type = MSG_T::BARRIER;
		return true;
	}
	else if(m.step <= this->meta.step){
		// to branch parent
		assert(branch_depth >= 0);

		if(actors[m.step].actor_type == ACTOR_T::BRANCH){
			// don't need to send msg back to parent branch step
			// go to next actor of parent
			m.step = actors[m.step].next_actor;
			m.branch_infos.pop_back();

			return update_route(m, actors);
		}else{
			// aggregate labelled branch actors to parent machine
			m.recver_nid = m.branch_infos[branch_depth].node_id;
			m.recver_tid = m.branch_infos[branch_depth].thread_id;
			m.msg_type = MSG_T::BRANCH;
			return true;
		}
	}
	else{
		// normal actor, recver = sender
		m.msg_type = MSG_T::SPAWN;
		return false;
	}
}

int Message::get_node_id(value_t & v, DataStore* data_store)
{
	int type = v.type;
	if (type == 1) {
		vid_t v_id(Tool::value_t2int(v));
		return data_store->GetMachineIdForVertex(v_id);
	} else if (type == 5) {
		eid_t e_id;
		uint2eid_t(Tool::value_t2uint64_t(v), e_id);
		return data_store->GetMachineIdForEdge(e_id);
	} else {
		cout << "Wrong Type when getting node id" << type << endl;
		return -1;
	}
}

bool Message::FeedData(pair<history_t, vector<value_t>>& pair)
{
	size_t in_size = MemSize(pair);
	size_t space = max_data_size - data_size;

	// with sufficient space
	if (in_size <= space){
		data.push_back(pair);
		data_size += in_size;
		pair.second.clear();
		return true;
	}

	// check if able to add history
	size_t his_size = MemSize(pair.first) + sizeof(size_t);
	// no space for history
	if (his_size >= space){
		return false;
	}

	in_size = his_size;
	auto itr = pair.second.begin();
	for (; itr != pair.second.end(); itr ++){
		size_t s = MemSize(*itr);
		if (s + in_size <= space){
			in_size += s;
		}
		else{
			break;
		}
	}

	// feed in data
	if (in_size != his_size){
		data.push_back(make_pair(pair.first, vector<value_t>()));
		int i = data.size() - 1;
		std::move(pair.second.begin(), itr, std::back_inserter(data[i].second));
		itr = pair.second.erase(pair.second.begin(), itr);
		data_size += in_size;
	}
	assert(MemSize(data) == data_size);

	return pair.second.size() == 0;
}

void Message::FeedData(vector<pair<history_t, vector<value_t>>>& vec)
{
	for (auto itr = vec.begin(); itr != vec.end();){
		if(! FeedData(*itr)){
			break;
		}
		itr = vec.erase(itr);
	}
	assert(MemSize(data) == data_size);
}

void Message::CopyData(vector<pair<history_t, vector<value_t>>>& vec)
{
	size_t size = MemSize(vec);
	assert(size < max_data_size);

	data.clear();
	data.insert(data.begin(), vec.begin(), vec.end());

	data_size = size;
}

std::string Message::DebugString() const {
	std::stringstream ss;
	ss << meta.DebugString();
	if (data.size()) {
	  ss << " Body:";
	  for (const auto& d : data)
		  ss << " data_size=" << d.second.size();
	}
	return ss.str();
}

bool operator == (const history_t& l, const history_t& r){
	if(l.size() != r.size()){
		return false;
	}
	// history keys are in ascending order
	// so simply match kv pair one by one
	for(int i = 0; i < l.size(); i++){
		if(l[i] != r[i]){
			return false;
		}
	}
	return true;
}

size_t MemSize(int i)
{
	return sizeof(int);
}

size_t MemSize(char c)
{
	return sizeof(char);
}

size_t MemSize(value_t data)
{
	size_t s = sizeof(uint8_t);
	s += MemSize(data.content);
	return s;
}
