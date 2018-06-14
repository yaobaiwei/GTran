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
  int sender;
  int recver;

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

Message CreateMessage(MSG_T _type, int _qid, int _step, int _sender, int _recver,
		vector<ACTOR_T> _chains, SArray<char> data);
