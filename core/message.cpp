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

void Message::CreatInitMsg(uint64_t qid, int parent_node, int nodes_num, int recv_tid, vector<Actor_Object>& actors, int max_data_size, vector<Message>& vec)
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
		vec.push_back(msg);
	}
}


void Message::CreatNextMsg(vector<Actor_Object>& actors, vector<pair<history_t, vector<value_t>>>& data, int num_thread, vector<Message>& vec, int (*mapper)(value_t&))
{
	// get next step
	int step = actors[this->meta.step].next_actor;

	Meta m = this->meta;

	// no data, send msg_path to next barrier or branch
	if(data.size() == 0){
		while(step < actors.size()){
			if(actors[step].IsBarrier()){
				// found next barrier
				break;
			}
			else if(step < this->meta.step){
				// found parent branch
				break;
			}
			step = actors[step].next_actor;
		}
	}
	m.step = step;

	// specify if recver route should not be changed 
	bool routeAssigned = false;

	// update recver route & msg_type
	if(step == actors.size() || actors[step].IsBarrier()){
		int branch_depth = m.branch_infos.size() - 1;
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
		routeAssigned = true;
	}
	else if(m.step < this->meta.step){
		// branch actor
		int branch_depth = m.branch_infos.size() - 1;
		assert(branch_depth >= 0);
		m.recver_nid = m.branch_infos[branch_depth].node_id;
		m.recver_tid = m.branch_infos[branch_depth].thread_id;
		m.msg_type = MSG_T::BRANCH;
		routeAssigned = true;
	}
	else{
		// normal actor, recver = sender
		m.msg_type = MSG_T::SPAWN;
	}

	// node id to data
	map<int, vector<pair<history_t, vector<value_t>>>> id2data;

	// enable route mapping
	if(mapper != NULL && ! routeAssigned){
		for(auto& pair: data){
			map<int, vector<value_t>> id2value_t;
			// get node id
			for(auto& v : pair.second){
				int node = (*mapper)(v);
				id2value_t[node].push_back(v);
			}
			// insert his/value pair to corresponding node
			for(auto& item : id2value_t){
				id2data[item.first].push_back(make_pair(pair.first, item.second));
			}
		}
	}
	else{
		// no mapping, send to recver_nid only
		id2data[m.recver_nid] = std::move(data);
	}

	for(auto& item : id2data){
		// feed data to msg
		do{
			Message msg(m);
			msg.max_data_size = this->max_data_size;
			msg.meta.recver_nid = item.first;
			if(! routeAssigned){
				msg.meta.recver_tid = (m.recver_tid ++) % num_thread;
			}
			msg.FeedData(item.second);
			vec.push_back(msg);
		}
		while((item.second.size() != 0));	// Data no consumed
	}
	// set disptching path
	string num = "\t" + to_string(vec.size());
	for(auto &msg : vec){
		msg.meta.msg_path += num;
	}
}

void Message::CreatBranchedMsg(vector<Actor_Object>& actors, vector<int>& steps, uint64_t msg_id, int num_thread, vector<Message>& vec){
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
	for (auto& pair : data){
		for(auto& value : pair.second){
			value_t v;
			Tool::str2int(to_string(count ++), v);
			history_t his = pair.first;
			his.push_back(make_pair(m.step, v));
			labeled_data.push_back(make_pair(his, vector<value_t>{value}));	
		}
	}

	// copy labeled data to each step
	for(int i = 0; i < steps.size(); i ++){
		int step = steps[i];
		Meta step_meta = m;
		if(actors[step].IsBarrier()){
			step_meta.msg_type = MSG_T::BARRIER;
		}
		else{
			step_meta.msg_type = MSG_T::SPAWN;
		}
		info.index = i;
		step_meta.branch_infos.push_back(info);
		step_meta.step = step;
		
		vector<pair<history_t, vector<value_t>>> temp = labeled_data;
		// feed data to msg
		do{
			Message msg(step_meta);
			msg.max_data_size = this->max_data_size;
			if(step_meta.msg_type != MSG_T::BARRIER){
				msg.meta.recver_tid = (m.recver_tid ++) % num_thread;
			}
			msg.FeedData(temp);
			vec.push_back(msg);
		}
		while((temp.size() != 0));	// Data no consumed
	}
	
	for(auto& msg : vec){
		msg.meta.msg_path += "\t" + to_string(vec.size());
	}
}

void Message::FeedData(pair<history_t, vector<value_t>>& pair)
{
	if (pair.second.size() == 0){
		return;
	}

	size_t in_size = MemSize(pair);
	size_t space = max_data_size - data_size;

	// with sufficient space
	if (in_size <= space){
		data.push_back(pair);
		data_size += in_size;
		pair.second.clear();
		return;
	}

	// check if able to add history
	size_t his_size = MemSize(pair.first) + sizeof(size_t);
	// no space for history
	if (his_size >= space){
		return;
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
		pair.second.erase(pair.second.begin(), itr);
		data_size += in_size;
	}
	if(MemSize(data)  != data_size){
		cout << MemSize(data) << "  " << data_size <<endl;
	}
	assert(MemSize(data) == data_size);
}

void Message::FeedData(vector<pair<history_t, vector<value_t>>>& vec)
{
	for (auto itr = vec.begin(); itr != vec.end();){
		FeedData(*itr);
		// no enough space
		if((*itr).second.size() != 0){
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

template<class T1, class T2>
size_t MemSize(pair<T1, T2> p)
{
	size_t s = MemSize(p.first);
	s += MemSize(p.second);
	return s;
}

template<class T>
size_t MemSize(vector<T> data)
{
	size_t s = sizeof(size_t);
	for (auto t : data){
		s += MemSize(t);
	}
	return s;
}
