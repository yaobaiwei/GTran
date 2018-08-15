/*
 * message.hpp
 *
 *  Created on: May 15, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include <vector>
#include <sstream>
#include <functional>

#include "base/serialization.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "actor/actor_object.hpp"
#include "storage/data_store.hpp"

#define TEN_MB 1048576

struct Branch_Info{
	// parent route
	int node_id;
	int thread_id;
	// indicate the branch order
	int index;
	// history key of parent
	int key;
	// msg id of parent, unique on each node
	uint64_t msg_id;
	// msg path of parent
	string msg_path;
};

ibinstream& operator<<(ibinstream& m, const Branch_Info& info);

obinstream& operator>>(obinstream& m, Branch_Info& info);

struct Meta {
	// query
	uint64_t qid;
	int step;

	// route
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
	vector<Branch_Info> branch_infos;

	// actors chain
	vector<Actor_Object> actors;

	std::string DebugString() const ;
};

ibinstream& operator<<(ibinstream& m, const Meta& meta);

obinstream& operator>>(obinstream& m, Meta& meta);

typedef vector<pair<int, value_t>> history_t;

bool operator==(const history_t& l, const history_t& r);

// remove histroy after branch_key and find parent history in vector
template<class T>
typename vector<pair<history_t, T>>::iterator merge_hisotry(vector<pair<history_t, T>>& vec, history_t& his, int branch_key);

class Message {
public:
	Meta meta;

	std::vector<pair<history_t, vector<value_t>>> data;

	// size of data
	size_t data_size;
	// maximum size of data
	size_t max_data_size;

	Message() : data_size(sizeof(size_t)), max_data_size(TEN_MB){}
	Message(const Meta& m) : Message()
	{
		meta = m;
	}

	// move data from srouce into msg
	bool InsertData(pair<history_t, vector<value_t>>& pair);
	void InsertData(vector<pair<history_t, vector<value_t>>>& vec);

	// create init msg
	// currently
	// recv_tid = qid % thread_pool.size()
	// parent_node = _my_node.get_local_rank()
	static void CreateInitMsg(uint64_t qid, int parent_node, int nodes_num, int recv_tid, vector<Actor_Object>& actors, vector<Message>& vec);

	// create exit msg, notifying ending of one query
	void CreateExitMsg(int nodes_num, vector<Message>& vec);

	// actors:  actors chain for current message
	// data:    new data processed by actor_type
	// vec:     messages to be send
	// mapper:  function that maps value_t to particular machine, default NULL
	void CreateNextMsg(vector<Actor_Object>& actors, vector<pair<history_t, vector<value_t>>>& data, int num_thread, DataStore* data_store, vector<Message>& vec);

	// actors:  actors chain for current message
	// stpes:   branching steps
	// vec:     messages to be send
	void CreateBranchedMsg(vector<Actor_Object>& actors, vector<int>& steps, int num_thread, DataStore* data_store, vector<Message>& vec);

	// actors:  actors chain for current message
	// stpes:   branching steps
	// msg_id:  assigned by actor to indicate parent msg
	// vec:     messages to be send
	void CreateBranchedMsgWithHisLabel(vector<Actor_Object>& actors, vector<int>& steps, uint64_t msg_id, int num_thread, DataStore* data_store, vector<Message>& vec);

	// create Feed msg
	// Feed data to all node with tid = parent_tid
	void CreateFeedMsg(int key, int nodes_num, vector<value_t>& data, vector<Message>& vec);

	std::string DebugString() const;

private:
	// dispatch input data to different node
	void dispatch_data(Meta& m, vector<Actor_Object>& actors, vector<pair<history_t, vector<value_t>>>& data, int num_thread, DataStore* data_store, vector<Message>& vec);
	// update route to next actor
	bool update_route(Meta& m, vector<Actor_Object>& actors);
	// update route to barrier or labelled branch actors for msg collection
	bool update_collection_route(Meta& m, vector<Actor_Object>& actors);
	// get the node where vertex or edge is stored
	static int get_node_id(value_t & v, DataStore* data_store);
};

ibinstream& operator<<(ibinstream& m, const Message& msg);

obinstream& operator>>(obinstream& m, Message& msg);

size_t MemSize(int i);
size_t MemSize(char c);
size_t MemSize(value_t data);

template<class T1, class T2>
size_t MemSize(pair<T1, T2> p);

template<class T>
size_t MemSize(vector<T> data);

#include "message.tpp"
