/*
 * message.cpp
 *
 *  Created on: Jun 8, 2018
 *      Author: Hongzhi Chen
 */

#include "core/message.hpp"

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
	m << meta.branch_route;
	m << meta.branch_mid;
	m << meta.branch_path;
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
	m >> meta.branch_route;
	m >> meta.branch_mid;
	m >> meta.branch_path;
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
	if(msg_type == MSG_T::FEED){
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
	m.msg_type = MSG_T::FEED;
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

void Message::CreatNextMsg(vector<Actor_Object>& actors, vector<pair<history_t, vector<value_t>>>& data, vector<Message>& vec, int (*mapper)(value_t&))
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

	// disable node mapping when next actor is barrier or branch
	bool disableMapping = false;

	// update recver route & msg_type
	if(step == actors.size() || actors[step].IsBarrier()){
		int branch_index = m.branch_route.size();
		if(branch_index > 0){
			// barrier actor in branch
			m.recver_nid = m.branch_route[branch_index - 1].first;
			m.recver_tid = m.branch_route[branch_index - 1].second;
		}
		else{
			// barrier actor in main query
			m.recver_nid = m.parent_nid;
			m.recver_tid = m.parent_tid;
		}
		m.msg_type = MSG_T::BARRIER;
		disableMapping = true;
	}
	else if(m.step < this->meta.step){
		// branch actor
		int branch_index = m.branch_route.size();
		assert(branch_index > 0);
		m.recver_nid = m.branch_route[branch_index - 1].first;
		m.recver_tid = m.branch_route[branch_index - 1].second;
		m.msg_type = MSG_T::BRANCH;
		disableMapping = true;
	}
	else{
		// normal actor, recver = sender
		m.msg_type = MSG_T::SPAWN;
	}

	// node id to data
	map<int, vector<pair<history_t, vector<value_t>>>> id2data;

	// enable mapping
	if(mapper != NULL && ! disableMapping){
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

void Message::CreatBranchedMsg(vector<Actor_Object>& actors, vector<int>& steps, int msg_id, vector<Message>& vec){
	Meta m = this->meta;

	// update branch info
	m.branch_path.push_back(m.msg_path);
	m.branch_route.push_back(make_pair(m.recver_nid, m.recver_tid));
	m.branch_mid.push_back(msg_id);

	// update msg_path
	m.msg_path += "\t" + to_string(steps.size());

	for(int step : steps){
		Message msg(m);
		if(actors[step].IsBarrier()){
			msg.meta.msg_type = MSG_T::BARRIER;
		}
		else{
			msg.meta.msg_type = MSG_T::SPAWN;
		}
		msg.meta.step = step;
		msg.max_data_size = this->max_data_size;
		msg.CopyData(this->data);
		vec.push_back(msg);
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

void Message::GetActors(vector<Actor_Object>& vec)
{
	if(meta.msg_type == MSG_T::FEED){
		vec = std::move(meta.actors);
	}
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

bool MsgServer::ConsumeMsg(Message& msg)
{
	uint64_t id;
	string end_path;
	GetMsgInfo(msg, id, end_path);

	switch (msg.meta.msg_type) {
		case MSG_T::BRANCH:
			msg.meta.branch_mid.pop_back();
			msg.meta.branch_route.pop_back();
			msg.meta.branch_path.pop_back();
		case MSG_T::BARRIER:
			if(! IsReady(id, end_path, msg.meta.msg_path))
			{
				return false;
			}
			msg.meta.msg_path = end_path;
			break;
	}
	return true;
}

// get msg info for collecting sub msg
void MsgServer::GetMsgInfo(Message& msg, size_t &id, string &end_path)
{

}

bool MsgServer::IsReady(uint64_t id, string end_path, string msg_path)
{
	if(path_counter_.count(id) != 1){
		path_counter_[id] = map<string, int>();
	}
	map<string, int> &counter =  path_counter_[id];
	while (msg_path != end_path){
		int i = msg_path.find_last_of("\t");
		// "\t" should not the the last char
		assert(i + 1 < msg_path.size());
		// get last number
		int num = atoi(msg_path.substr(i + 1).c_str());

		// check key
		if (counter.count(msg_path) != 1){
			counter[msg_path] = 0;
		}

		// current branch is ready
		if ((++counter[msg_path]) == num){
			// reset count to 0
			counter[msg_path] = 0;
			// remove last number
			msg_path = msg_path.substr(0, i == string::npos ? 0 : i);
		}
		else{
			return false;
		}
	}

	// remove map
	auto itr = path_counter_.find(id);
	path_counter_.erase(id);
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
