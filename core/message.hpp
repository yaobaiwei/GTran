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
#include "actor/actor_object.hpp"

struct Meta {
  // query
  int qid;
  int step;

  // route
  int sender_nid;
  int sender_tid;
  int recver_nid;
  int recver_tid;

  // parent route
  int parent_nid;
  int parent_tid;

  // type
  MSG_T msg_type;

  // Msg disptching path
  string msg_path;

  // branch info
  vector<pair<int, int>> branch_route;
  vector<int> branch_mid;

  // actors chain
  vector<Actor_Object> actors;

  std::string DebugString() const ;
};

ibinstream& operator<<(ibinstream& m, const Meta& meta);

obinstream& operator>>(obinstream& m, Meta& meta);

typedef vector<pair<int, value_t>> history_t;

class Message {
public:
	Meta meta;

	std::vector<pair<history_t, vector<value_t>>> data;

  // size of data
  size_t data_size;
  // maximum size of data
  size_t max_data_size;

  Message() : data_size(0), max_data_size(1048576){}
  Message(const Meta& m) : Message()
  {
    meta = m;
  }

  // data in
  void FeedData(pair<history_t, vector<value_t>>& pair);
  void FeedData(vector<pair<history_t, vector<value_t>>>& vec);

  void CopyData(vector<pair<history_t, vector<value_t>>>& vec);

  // create init msg
  static vector<Message> CreatInitMsg(int qid, int sender, int send_tid, int recver_tid, int nodes_num, vector<Actor_Object>& actors, int max_data_size);

  // clone meta of current msg, update step, receiver, msg_type, msg_path for next
  // actors[step], actors[actors[step].next_actor]?
  vector<Message> CreatNextMsg(const vector<Actor_Object>& actors, vector<pair<history_t, vector<value_t>>>& data);

  // For branch actor, clone msg into multiple pieces
  // num = steps.size()
  // insert current node and thread id into bp_ids
  vector<Message> CreatBranchedMsg(const vector<Actor_Object>& actors);

	std::string DebugString() const;

private:
  int id_;
  friend class MsgServer;
};

ibinstream& operator<<(ibinstream& m, const Message& msg);

obinstream& operator>>(obinstream& m, Message& msg);

class MsgServer{
public:
  MsgServer() : msg_counter(0){}

  // Collect msg and determine msg completed or NOT
  // if msg_type != BARRIER, BRANCH
  //    return true
  // else
  //    run path_counter_ to check and merge data to msg_map_
  bool ConsumeMsg(Message& msg);

private:
  map<uint64_t, map<string, int>> path_counter_;
  map<uint64_t, Message> msg_map_;
  int msg_counter;
};

size_t MemSize(int i);
size_t MemSize(char c);
size_t MemSize(value_t data);

template<class T1, class T2>
size_t MemSize(pair<T1, T2> p);

template<class T>
size_t MemSize(vector<T> data);
