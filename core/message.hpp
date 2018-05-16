/*
 * message.hpp
 *
 *  Created on: May 15, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include <vector>
#include "base/serialization.hpp"
#include "glog/logging.h"

// Spawn: spawn a new actor
// Feed: "proxy" feed actor a input
// Reply: actor returns the intermidiate result to actor
enum class MSG_T : char { SPAWN, FEED, REPLY, BARRIER, EXIT };
static const char *TypeName[] = {"spawn", "feed", "reply", "barrier", "exit"};

enum class ACTOR_T : char { ADD, PROXY, HW, OUT };
static const char *ActorType[] = {"add", "proxy", "hello world", ""};

struct Meta {
  // query
  int query_id;
  int step;

  // route
  int sender_node;
  int recver_node;

  // type
  MSG_T msg_type;

  // chains
  vector<ACTOR_T> qlink;

  friend ibinstream& operator<<(ibinstream& m, const Meta& meta)
  {
	  m << meta.query_id;
	  m << meta.step;
	  m << meta.sender_node;
	  m << meta.recver_node;
	  m << meta.msg_type;
	  m << meta.qlink;
	  return m;
  }

  friend obinstream& operator>>(obinstream& m, Meta& meta)
  {
	  m >> meta.query_id;
	  m >> meta.step;
	  m >> meta.sender_node;
	  m >> meta.recver_node;
	  m >> meta.msg_type;
	  m >> meta.qlink;
	  return m;
  }

  std::string DebugString() const {
    std::stringstream ss;
    ss << "Meta: {";
    ss << "  query_id: " << query_id;
    ss << ", step: " << step;
    ss << ", sender node: " << sender_node;
    ss << ", recver node: " << recver_node;
    ss << ", msg type: " << TypeName[static_cast<int>(msg_type)];
    ss << ", query chains: [";
    for(auto i : qlink){
    	ss  << ActorType[static_cast<int>(i)] << ", ";
    }
    ss << "]}";
    return ss.str();
  }
};

//Note
//DataElem should implement serialization operators << & >>
template <class DataElem>
class Message {
  Meta meta;
  // vector of intermidiate result
  std::vector<DataElem> data;

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

  void AddData(DataElem & e){
	  data.push_back(e);
  }

  void AddData(vector<DataElem> & e_list) {
    data.insert(std::end(data), std::begin(e_list), std::end(e_list));
  }

  std::string DebugString() const {
    std::stringstream ss;
    ss << meta.DebugString();
    if (data.size()) {
      ss << " data:";
      for (const auto &entry : data)
        ss << " " << entry;
    }
    return ss.str();
  }
};
