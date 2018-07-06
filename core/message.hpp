/*
 * message.hpp
 *
 *  Created on: May 15, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include <vector>
#include <sstream>

#include "base/sarray.hpp"
#include "base/serialization.hpp"
#include "base/type.hpp"

struct Meta {
  // query
  int qid;
  int step;

  // route
  int sender_nid;
  int sender_tid;
  int recver_nid;
  int recver_tid;

  // type
  MSG_T msg_type;

  // chains
  vector<ACTOR_T> chains;

  std::string DebugString() const ;
};

ibinstream& operator<<(ibinstream& m, const Meta& meta);

obinstream& operator>>(obinstream& m, Meta& meta);

struct Message {
	Meta meta;

	std::vector<SArray<char>> data;

	template <typename V>
	void AddData(const SArray<V>& val) {
	    data.push_back(SArray<char>(val));
	}

	std::string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const Message& msg);

obinstream& operator>>(obinstream& m, Message& msg);

Message CreateMessage(MSG_T _type, int _qid, int _step, int _sender_nid, int _sender_tid, int _recver_nid, int _recver_tid,
		vector<ACTOR_T> _chains, SArray<char> data);
