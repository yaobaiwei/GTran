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
	m << meta.sender_nid;
	m << meta.sender_tid;
	m << meta.recver_nid;
	m << meta.recver_tid;
	m << meta.msg_type;
	m << meta.msg_path;
	m << meta.parent_nid;
	m << meta.parent_tid;
	m << meta.branch_route;
	m << meta.branch_mid;
	m << meta.actors;
	return m;
}

obinstream& operator>>(obinstream& m, Meta& meta)
{
	m >> meta.qid;
	m >> meta.step;
	m >> meta.sender_nid;
	m >> meta.sender_tid;
	m >> meta.recver_nid;
	m >> meta.recver_tid;
	m >> meta.msg_type;
	m >> meta.msg_path;
	m >> meta.parent_nid;
	m >> meta.parent_tid;
	m >> meta.branch_route;
	m >> meta.branch_mid;
	m >> meta.actors;
	return m;
}

std::string Meta::DebugString() const {
	std::stringstream ss;
	ss << "Meta: {";
	ss << "  qid: " << qid;
	ss << ", step: " << step;
	ss << ", sender node: " << sender_nid << ":" << sender_tid;
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
	return m;
}

obinstream& operator>>(obinstream& m, Message& msg)
{
	m >> msg.meta;
	m >> msg.data;
	m >> msg.max_data_size;
	return m;
}

vector<Message> Message::CreatInitMsg(int qid, int sender, int send_tid, int recver_tid, int nodes_num, vector<Actor_Object>& actors, int _max_data_size)
{
	vector<Message> vec;
	Meta m;
	m.qid = qid;
	m.step = 0;
	m.sender_nid = sender;
	m.sender_tid = send_tid;
	m.recver_nid = -1;
	m.recver_tid = recver_tid;
	m.parent_nid = sender;
	m.parent_tid = recver_tid;
	m.msg_type = MSG_T::FEED;
	m.msg_path = to_string(nodes_num);
	m.actors = actors;

	for(int i = 0; i < nodes_num; i++){
		Message msg;
		msg.meta = m;
		msg.meta.recver_nid = i;
		msg.max_data_size = _max_data_size;
		vec.push_back(msg);
	}
	return vec;
}

vector<Message> Message::CreatNextMsg(const vector<Actor_Object>& actors, vector<pair<history_t, vector<value_t>>>& data)
{
	Actor_Object current = actors[meta.step];
	Actor_Object next = actors[current.next_actor];

	Message m(meta);
	// Normal actor will send message to current node & thread;
	m.meta.sender_nid = m.meta.recver_nid;
	m.meta.sender_tid = m.meta.recver_tid;
	m.meta.step = current.next_actor;

	if(next.IsBarrier()){
		int branch_index = m.meta.branch_route.size();
		if(branch_index != 0){
			// barrier in branch
			m.meta.recver_nid = m.meta.branch_route[branch_index - 1].first;
			m.meta.recver_tid = m.meta.branch_route[branch_index - 1].second;
		}
		else{
			// barrier in main query
		}
	}
	else if(current.next_actor < meta.step){
		// send to branch parent
	}

	if(next.actor_type == ACTOR_T::TRAVERSAL){
		// send according to data
	}

}

vector<Message> Message::CreatBranchedMsg(const vector<Actor_Object>& actors){

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
	in_size = MemSize(pair.first) + sizeof(size_t);
	// no space for header
	if (in_size >= space){
		return;
	}

	vector<value_t> vec;
	for (auto itr = pair.second.begin(); itr != pair.second.end(); itr++){
		size_t s = MemSize(*itr);
		if (s + in_size <= max_data_size){
			in_size += s;
			vec.push_back(*itr);
			itr = pair.second.erase(itr);
		}
		else{
			break;
		}
	}

	if (vec.size() != 0){
		data.push_back(make_pair(pair.first, vec));
		data_size += in_size;
	}

	assert(MemSize(data) == data_size);
}

void Message::FeedData(vector<pair<history_t, vector<value_t>>>& vec)
{
	for (auto itr = vec.begin(); itr != vec.end(); itr++){
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

	for(const auto& pair : vec){
		data.push_back(pair);
	}

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

bool MsgServer::ConsumeMsg(Message& msg)
{

	return false;
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
