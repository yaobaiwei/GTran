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
	m << meta.chains;
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
	m >> meta.chains;
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
	ss << ", query chains: [";
	for(auto c : chains){
		ss  << ActorType[static_cast<int>(c)] << ", ";
	}
	ss << "]}";
	return ss.str();
}

ibinstream& operator<<(ibinstream& m, const Message& msg)
{
	m << msg.meta;
	m << msg.data;
	return m;
}

obinstream& operator>>(obinstream& m, Message& msg)
{
	m >> msg.meta;
	m >> msg.data;
	return m;
}

std::string Message::DebugString() const {
	std::stringstream ss;
	ss << meta.DebugString();
	if (data.size()) {
	  ss << " Body:";
	  for (const auto& d : data)
		ss << " data_size=" << d.size();
	}
	return ss.str();
}

Message CreateMessage(MSG_T _type, int _qid, int _step, int _sender_nid, int _sender_tid, int _recver_nid, int _recver_tid,
		vector<ACTOR_T> _chains, SArray<char> data) {
	Message m;
	m.meta.msg_type = _type;
	m.meta.qid = _qid;
	m.meta.step = _step;
	m.meta.sender_nid = _sender_nid;
	m.meta.sender_tid = _sender_tid;
	m.meta.recver_nid = _recver_nid;
	m.meta.recver_tid = _recver_tid;

	if (_chains.size() != 0)
		m.meta.chains.insert(m.meta.chains.end(), _chains.begin(), _chains.end());
	if (data.size() != 0)
		m.AddData(data);
	return m;
}
