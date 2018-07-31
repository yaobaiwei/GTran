/*
 * branch_actor.hpp
 *
 *  Created on: July 16, 2018
 *      Author: Nick Fang
 */
#pragma once
#include <limits.h>

#include "actor/abstract_actor.hpp"
#include "core/result_collector.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"


// Base class for barrier actors
class BarrierActorBase :  public AbstractActor{
public:
	BarrierActorBase(int id, DataStore* data_store) : AbstractActor(id, data_store){}
	void process(int t_id, vector<Actor_Object> & actors, Message & msg){
		// get msg info
		mkey_t key;
		string end_path;
		GetMsgInfo(msg, key, end_path);

		// make sure only one thread executing on same msg key
		{
			unique_lock<mutex> lock(mkey_mutex_);
			mkey_cv_.wait(lock, [&]{
				if(mkey_set_.count(key) == 0){
					mkey_set_.insert(key);
					return true;
				}else{
					return false;
				}
			});
		}
		bool isReady = IsReady(key, end_path, msg.meta.msg_path);
		if(isReady){
			// barrier actor will reset msg path
			msg.meta.msg_path = end_path;
		}
		do_work(t_id, actors, msg, key, isReady);
		// notify blocked threads
		{
			lock_guard<mutex> lock(mkey_mutex_);
			mkey_set_.erase(mkey_set_.find(key));
		}
		mkey_cv_.notify_all();
	}
protected:
	// main logic of barrier actors
	virtual void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady) = 0;

	void get_branch_key(Meta & m, int & key){
		// check if count actor in branch
		// run locally if true
		int branch_depth = m.branch_infos.size();
		key = - 1;
		if(branch_depth != 0){
			key = m.branch_infos[branch_depth - 1].key;
		}
	}

	// projection functions from E/V to label or property
	static bool project_property_edge(int tid, value_t & val, int key, DataStore * datastore){
		eid_t e_id;
		uint2eid_t(Tool::value_t2uint64_t(val),e_id);

		epid_t ep_id(e_id, key);

		if(! datastore->EPKeyIsExist(tid, ep_id)){
			return false;
		}

		val.content.clear();
		datastore->GetPropertyForEdge(tid, ep_id, val);
		return true;
	}

	static bool project_property_vertex(int tid, value_t & val, int key, DataStore * datastore){
		vid_t v_id(Tool::value_t2int(val));
		vpid_t vp_id(v_id, key);

		if(! datastore->VPKeyIsExist(tid, vp_id)){
			return false;
		}

		val.content.clear();
		datastore->GetPropertyForVertex(tid, vp_id, val);
		return true;
	}

	static bool project_label_edge(int tid, value_t & val, int key, DataStore * datastore){
		eid_t e_id;
		uint2eid_t(Tool::value_t2uint64_t(val), e_id);

		val.content.clear();
		label_t label_id;
		string label;
		datastore->GetLabelForEdge(tid, e_id, label_id);
		datastore->GetNameFromIndex(Index_T::E_LABEL, label_id, label);
		Tool::str2str(label, val);
		return true;
	}
	static bool project_label_vertex(int tid, value_t & val, int key, DataStore * datastore){
		vid_t v_id(Tool::value_t2int(val));

		val.content.clear();
		label_t label_id;
		string label;
		datastore->GetLabelForVertex(tid, v_id, label_id);
		datastore->GetNameFromIndex(Index_T::V_LABEL, label_id, label);
		Tool::str2str(label, val);
		return true;
	}

	static bool project_none(int tid, value_t & val, int key, DataStore * datastore){
		return true;
	}

private:
	// lock for mkey_t
	// make sure only one thread executing on same msg key
	mutex mkey_mutex_;
	condition_variable mkey_cv_;
	set<mkey_t> mkey_set_;

	// msg path counter, for checking if collection completed
	map<mkey_t, map<string, int>> path_counters_;
	// lock for collector
	// protect map insert and erase
	mutex counters_mutex_;

	// Check if msg all collected
	bool IsReady(mkey_t key, string end_path, string msg_path)
	{
		// get counter for key
		counters_mutex_.lock();
		auto itr = path_counters_.find(key);
		if(itr == path_counters_.end()){
			itr = path_counters_.insert(itr, make_pair(key, map<string, int>()));
		}
		counters_mutex_.unlock();
		auto& counter = itr->second;

		// check if all msg are collected
		while (msg_path != end_path){
			int i = msg_path.find_last_of("\t");
			// "\t" should not be the the last char
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

		// remove counter from counters map when completed
		{
			lock_guard<mutex> lk(counters_mutex_);
			path_counters_.erase(itr);
		}
		return true;
	}

	// get msg info
	// key : mkey_t, identifier of msg
	// end_path: identifier of msg collection completed
    static void GetMsgInfo(Message& msg, mkey_t &key, string &end_path){
		// init info
		uint64_t msg_id = 0;
		int index = 0;
		end_path = "";

		int branch_depth = msg.meta.branch_infos.size() - 1;
		if(branch_depth >= 0){
			msg_id = msg.meta.branch_infos[branch_depth].msg_id;
			index = msg.meta.branch_infos[branch_depth].index;
			end_path = msg.meta.branch_infos[branch_depth].msg_path;
		}
		key = mkey_t(msg.meta.qid, msg_id, index);
	}
};

class EndActor : public BarrierActorBase
{
public:
	EndActor(int id, DataStore* data_store, Result_Collector * rc) : BarrierActorBase(id, data_store), rc_(rc){}

private:
	Result_Collector * rc_;
	map<mkey_t, vector<value_t>> data_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		thread_mutex_.lock();
		auto itr = data_table_.find(key);
		if(itr == data_table_.end()){
			itr = data_table_.insert(itr, make_pair(key, vector<value_t>()));
		}
		thread_mutex_.unlock();

		auto& data = itr->second;

		// move msg data to data table
		for(auto& pair: msg.data){
			data.insert(data.end(), std::make_move_iterator(pair.second.begin()), std::make_move_iterator(pair.second.end()));
		}

		// all msg are collected
		if(isReady){
			// insert data to result collector
			rc_->InsertResult(msg.meta.qid, data);

			// remove data
			thread_mutex_.lock();
			data_table_.erase(itr);
			thread_mutex_.unlock();
		}
	}
};

class AggregateActor : public BarrierActorBase
{
public:
	AggregateActor(int id, DataStore* data_store, int num_nodes, int num_thread, AbstractMailbox * mailbox) : BarrierActorBase(id, data_store), num_nodes_(num_nodes), num_thread_(num_thread), mailbox_(mailbox){}

private:
	map<mkey_t, vector<value_t>> data_table_;
	map<mkey_t, vector<pair<history_t, vector<value_t>>>> msg_data_table_;
	mutex thread_mutex_;

	int num_nodes_;
	int num_thread_;
	AbstractMailbox * mailbox_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		thread_mutex_.lock();
		auto itr_data = data_table_.find(key);
		auto itr_msg_data = msg_data_table_.find(key);
		if(itr_data == data_table_.end()){
			itr_data = data_table_.insert(itr_data, make_pair(key, vector<value_t>()));
			itr_msg_data = msg_data_table_.insert(itr_msg_data, make_pair(key, vector<pair<history_t, vector<value_t>>>()));
		}
		thread_mutex_.unlock();

		auto& data = itr_data->second;
		auto& msg_data = itr_msg_data->second;

		// move msg data to data table
		for(auto& p: msg.data){
			auto itr = find_if( msg_data.begin(), msg_data.end(),
				[&p](const pair<history_t, vector<value_t>>& element){ return element.first == p.first;});
			if(itr == msg_data.end()){
				itr = msg_data.insert(itr, make_pair(move(p.first), vector<value_t>()));
			}

			itr->second.insert(itr->second.end(), p.second.begin(), p.second.end());
			data.insert(data.end(), std::make_move_iterator(p.second.begin()), std::make_move_iterator(p.second.end()));
		}

		// all msg are collected
		if(isReady){
			Actor_Object& actor = actors[msg.meta.step];
			assert(actor.params.size() == 2);
			int key = Tool::value_t2int(actor.params[1]);

			// insert to current node's storage
			data_store_->InsertAggData(agg_t(msg.meta.qid, key), data);

			vector<Message> v;
			// send aggregated data to other nodes
			msg.CreateFeedMsg(key, num_nodes_, data, v);
			// send input data and history to next actor
			msg.CreateNextMsg(actors, msg_data, num_thread_, data_store_, v);

			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			// remove data
			thread_mutex_.lock();
			data_table_.erase(itr_data);
			msg_data_table_.erase(itr_msg_data);
			thread_mutex_.unlock();
		}
	}
};

class CapActor : public BarrierActorBase
{
public:
	CapActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox) : BarrierActorBase(id, data_store), num_thread_(num_thread), mailbox_(mailbox){}

private:
	mutex thread_mutex_;

	int num_thread_;
	AbstractMailbox * mailbox_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		// all msg are collected
		if(isReady){
			Actor_Object& actor = actors[msg.meta.step];
			vector<pair<history_t, vector<value_t>>> data;
			data.push_back(make_pair(history_t(), vector<value_t>()));

			// calculate max size of one value_t with empty history
			// max msg size - sizeof(data with one empty pair) - sizeof(empty value_t)
			size_t max_size = msg.max_data_size - MemSize(data) - MemSize(value_t());

			assert(actor.params.size() % 2 == 0);

			// side-effect key list
			for (int i = 0; i < actor.params.size(); i+=2) {
				int se_key = Tool::value_t2int(actor.params.at(i));
				string se_string = Tool::value_t2string(actor.params.at(i+1));
				vector<value_t> vec;
				data_store_->GetAggData(agg_t(msg.meta.qid, se_key), vec);

				string temp = se_string + ":[";
				for(auto& val : vec){
					temp += Tool::DebugString(val) + ", ";
				}
				// remove trailing ", "
				if(vec.size() > 0){
					temp.pop_back();
					temp.pop_back();
				}
				temp += "]";

				while(1){
					value_t v;
					// each value_t should have at most max_size
					Tool::str2str(temp.substr(0, max_size), v);
					data[0].second.push_back(v);
					if(temp.size() > max_size){
						temp = temp.substr(max_size);
					}else{
						break;
					}
				}
			}

			vector<Message> v;
			msg.CreateNextMsg(actors, data, num_thread_, data_store_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}
		}
	}
};

class CountActor : public BarrierActorBase
{
public:
	CountActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox) : BarrierActorBase(id, data_store), num_thread_(num_thread), mailbox_(mailbox){}
private:
	int num_thread_;
	AbstractMailbox * mailbox_;

	// data table
	map<mkey_t, vector<pair<history_t, int>>> data_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		thread_mutex_.lock();
		auto itr = data_table_.find(key);
		if(itr == data_table_.end()){
			itr = data_table_.insert(itr, make_pair(key, vector<pair<history_t, int>>()));
		}
		thread_mutex_.unlock();

		auto& data = itr->second;

		int branch_key;
		get_branch_key(msg.meta, branch_key);

		for(auto& p : msg.data){
			int count = p.second.size();
			// find history in data
			auto data_itr = merge_hisotry(data, p.first, branch_key);

			if(data_itr == data.end()){
				data.push_back(make_pair(move(p.first), count));
			}else{
				(*data_itr).second += count;
			}
		}

		// all msg are collected
		if(isReady){
			vector<pair<history_t, vector<value_t>>> msg_data;
			for(auto& p : data){
				value_t v;
				Tool::str2int(to_string(p.second), v);
				msg_data.push_back(make_pair(move(p.first), vector<value_t>{v}));
			}

			vector<Message> v;
			msg.CreateNextMsg(actors, msg_data, num_thread_, data_store_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			// remove data
			thread_mutex_.lock();
			data_table_.erase(itr);
			thread_mutex_.unlock();
		}
	}
};

class DedupActor : public BarrierActorBase
{
public:
	DedupActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox) : BarrierActorBase(id, data_store), num_thread_(num_thread), mailbox_(mailbox){}
private:
	int num_thread_;
	AbstractMailbox * mailbox_;

	// data table
	map<mkey_t, vector<pair<history_t, vector<value_t>>>> data_table_;
	map<mkey_t, vector<pair<history_t, set<history_t>>>> dedup_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		thread_mutex_.lock();
		auto itr_data = data_table_.find(key);
		auto itr_dedup = dedup_table_.find(key);
		if(itr_data == data_table_.end()){
			itr_data = data_table_.insert(itr_data, make_pair(key, vector<pair<history_t, vector<value_t>>>()));
			itr_dedup = dedup_table_.insert(itr_dedup,  make_pair(key, vector<pair<history_t, set<history_t>>>()));
		}
		thread_mutex_.unlock();

		auto& data = itr_data->second;
		auto& dedup_set = itr_dedup->second;

		int branch_key;
		get_branch_key(msg.meta, branch_key);

		// get actor params
		Actor_Object& actor = actors[msg.meta.step];
		set<int> key_set;
		for(auto& param : actor.params){
			key_set.insert(Tool::value_t2int(param));
		}

		for(auto& p : msg.data){
			// find branch history
			history_t temp = p.first;
			auto itr_sp = merge_hisotry(dedup_set, temp, branch_key);

			if(itr_sp == dedup_set.end()){
				itr_sp = dedup_set.insert(itr_sp, make_pair(move(temp), set<history_t>()));
			}

			// skip when data vec is empty
			if(p.second.size() == 0){
				continue;
			}

			// find if current history already added
			auto itr_dp = find_if( data.begin(), data.end(),
				[&p](const pair<history_t, vector<value_t>>& element){ return element.first == p.first;});
			if(itr_dp == data.end()){
				itr_dp = data.insert(itr_dp, make_pair(p.first, vector<value_t>()));
			}

			history_t his;
			if(key_set.size() > 0){
				// dedup history
				// construct history with given key
				for(auto& val : p.first){
					if(key_set.find(val.first) != key_set.end()){
						his.push_back(move(val));
					}
				}
				// find constructed history in his set
				auto itr_his = itr_sp->second.find(his);
				if(itr_his == itr_sp->second.end()){
					itr_dp->second.push_back(move(p.second[0]));
					itr_sp->second.insert(move(his));
				}
			}else{
				// dedup value, should check on all values
				for(auto& val : p.second){
					// construct history with current value
					his.push_back(make_pair(0, val));
					auto itr_his = itr_sp->second.find(his);
					if(itr_his == itr_sp->second.end()){
						itr_dp->second.push_back(move(val));
						itr_sp->second.insert(move(his));
					}
					his.clear();
				}
			}
		}

		// all msg are collected
		if(isReady){
			for(auto& p : dedup_set){
				// branch history not added to data
				if(p.second.size() == 0){
					data.push_back(make_pair(move(p.first), vector<value_t>()));
				}
			}

			vector<Message> v;
			msg.CreateNextMsg(actors, data, num_thread_, data_store_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			// remove data
			thread_mutex_.lock();
			data_table_.erase(itr_data);
			dedup_table_.erase(itr_dedup);
			thread_mutex_.unlock();
		}
	}
};

class GroupActor : public BarrierActorBase
{
public:
	GroupActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox) : BarrierActorBase(id, data_store), num_thread_(num_thread), mailbox_(mailbox){}
private:
	int num_thread_;
	AbstractMailbox * mailbox_;

	// data table
	map<mkey_t, vector<pair<history_t, map<string, vector<value_t>>>>> data_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		thread_mutex_.lock();
		auto itr = data_table_.find(key);
		if(itr == data_table_.end()){
			itr = data_table_.insert(itr, make_pair(key, vector<pair<history_t, map<string, vector<value_t>>>>()));
		}
		thread_mutex_.unlock();

		auto& data = itr->second;

		int branch_key;
		get_branch_key(msg.meta, branch_key);

		// get actor params
		Actor_Object& actor = actors[msg.meta.step];
		assert(actor.params.size() == 4);
		Element_T element_type = (Element_T)Tool::value_t2int(actor.params[1]);
		int keyProjection = Tool::value_t2int(actor.params[2]);
		int valueProjection = Tool::value_t2int(actor.params[3]);

		// get projection function by actor params
		bool(*kp)(int, value_t&, int, DataStore*) = project_none;
		bool(*vp)(int, value_t&, int, DataStore*) = project_none;
		if(keyProjection == 0){
			kp = (element_type == Element_T::VERTEX) ? project_label_vertex : project_label_edge;
		}else if(keyProjection > 0){
			kp = (element_type == Element_T::VERTEX) ? project_property_vertex : project_property_edge;
		}
		if(valueProjection == 0){
			vp = (element_type == Element_T::VERTEX) ? project_label_vertex : project_label_edge;
		}else if(valueProjection > 0){
			vp = (element_type == Element_T::VERTEX) ? project_property_vertex : project_property_edge;
		}

		for(auto& p : msg.data){
			// find history in data
			auto data_itr = merge_hisotry(data, p.first, branch_key);

			if(data_itr == data.end()){
				data_itr = data.insert(data_itr, make_pair(move(p.first), map<string, vector<value_t>>()));
			}

			for(auto& val : p.second){
				value_t k = val;
				value_t v = val;
				if(! kp(t_id, k, keyProjection, data_store_)){
					continue;
				}
				if(! vp(t_id, v, valueProjection, data_store_)){
					continue;
				}
				string key = Tool::DebugString(k);
				data_itr->second[key].push_back(move(v));
			}
		}

		// all msg are collected
		if(isReady){
			bool isCount = Tool::value_t2int(actor.params[0]);
			vector<pair<history_t, vector<value_t>>> msg_data;
			for(auto& p : data){
				map<string, string> kvmap;
				vector<value_t> vec_val;
				for(auto& item : p.second){
					if(isCount){
						kvmap[item.first] = to_string(item.second.size());
					}else{
						string str_vec = "[";
						for(auto& v : item.second){
							str_vec += Tool::DebugString(v) + ", ";
						}
						// remove last ", "
						str_vec = str_vec.substr(0, str_vec.size() - 2);
						str_vec += "]";
						kvmap[item.first] = str_vec;
					}
				}

				Tool::kvmap2value_t(kvmap, vec_val);
				msg_data.push_back(make_pair(move(p.first), move(vec_val)));
			}

			vector<Message> v;
			msg.CreateNextMsg(actors, msg_data, num_thread_, data_store_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			// remove data
			thread_mutex_.lock();
			data_table_.erase(itr);
			thread_mutex_.unlock();
		}
	}
};

class OrderActor : public BarrierActorBase
{
public:
	OrderActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox) : BarrierActorBase(id, data_store), num_thread_(num_thread), mailbox_(mailbox){}
private:
	int num_thread_;
	AbstractMailbox * mailbox_;

	// data table
	map<mkey_t, vector<pair<history_t, map<value_t, multiset<value_t>>>>> data_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		thread_mutex_.lock();
		auto itr = data_table_.find(key);
		if(itr == data_table_.end()){
			itr = data_table_.insert(itr, make_pair(key, vector<pair<history_t, map<value_t, multiset<value_t>>>>()));
		}
		thread_mutex_.unlock();

		auto& data = itr->second;

		// get actor params
		Actor_Object& actor = actors[msg.meta.step];
		assert(actor.params.size() == 3);
		Element_T element_type = (Element_T)Tool::value_t2int(actor.params[0]);
		int keyProjection = Tool::value_t2int(actor.params[1]);

		bool(*kp)(int, value_t&, int, DataStore*) = project_none;
		if(keyProjection == 0){
			kp = (element_type == Element_T::VERTEX) ? project_label_vertex : project_label_edge;
		}else if(keyProjection > 0){
			kp = (element_type == Element_T::VERTEX) ? project_property_vertex : project_property_edge;
		}

		int branch_key;
		get_branch_key(msg.meta, branch_key);

		for(auto& p : msg.data){
			// find history in data
			auto data_itr = merge_hisotry(data, p.first, branch_key);
			if(data_itr == data.end()){
				data_itr = data.insert(data_itr, make_pair(move(p.first), map<value_t, multiset<value_t>>()));
			}

			for(auto& val : p.second){
				value_t key = val;
				if(! kp(t_id, key, keyProjection, data_store_)){
					continue;
				}
				data_itr->second[key].insert(move(val));
			}
		}

		// all msg are collected
		if(isReady){
			Order_T order = (Order_T)Tool::value_t2int(actor.params[2]);
			vector<pair<history_t, vector<value_t>>> msg_data;
			for(auto& p : data){
				vector<value_t> val_vec;
				auto& m = p.second;
				if(order == Order_T::INCR){
					for(auto itr = m.begin(); itr != m.end(); itr++){
						val_vec.insert(val_vec.end(), make_move_iterator(itr->second.begin()), make_move_iterator(itr->second.end()));
					}
				}else{
					for(auto itr = m.rbegin(); itr != m.rend(); itr++){
						val_vec.insert(val_vec.end(), make_move_iterator(itr->second.rbegin()), make_move_iterator(itr->second.rend()));
					}
				}
				msg_data.push_back(make_pair(move(p.first), move(val_vec)));
			}

			vector<Message> v;
			msg.CreateNextMsg(actors, msg_data, num_thread_, data_store_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			// remove data
			thread_mutex_.lock();
			data_table_.erase(itr);
			thread_mutex_.unlock();
		}
	}
};

class RangeActor : public BarrierActorBase
{
public:
	RangeActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox) : BarrierActorBase(id, data_store), num_thread_(num_thread), mailbox_(mailbox){}
private:
	int num_thread_;
	AbstractMailbox * mailbox_;

	// data table
	map<mkey_t, vector<pair<history_t, vector<value_t>>>> data_table_;
	map<mkey_t, vector<pair<history_t, int>>> counter_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){

		thread_mutex_.lock();
		auto itr_data = data_table_.find(key);
		auto itr_counter = counter_table_.find(key);
		if(itr_data == data_table_.end()){
			itr_data = data_table_.insert(itr_data, make_pair(key, vector<pair<history_t, vector<value_t>>>()));
			itr_counter = counter_table_.insert(itr_counter,  make_pair(key, vector<pair<history_t, int>>()));
		}
		thread_mutex_.unlock();

		auto& data = itr_data->second;
		auto& counter = itr_counter->second;

		int branch_key;
		get_branch_key(msg.meta, branch_key);

		// get actor params
		Actor_Object& actor = actors[msg.meta.step];
		assert(actor.params.size() == 2);
		int start = Tool::value_t2int(actor.params[0]);
		int end = Tool::value_t2int(actor.params[1]);
		if(end == -1){
			end = INT_MAX;
		}

		for(auto& p : msg.data){
			// find branch history
			history_t temp = p.first;
			auto itr_cp = merge_hisotry(counter, temp, branch_key);

			// assign num of data in each branch history to 0
			if(itr_cp == counter.end()){
				itr_cp = counter.insert(itr_cp, make_pair(move(temp), 0));
			}

			// skip when exceed limits or no data
			if(itr_cp->second > end || p.second.size() == 0){
				continue;
			}

			// find if current history already added
			auto itr_dp = find_if( data.begin(), data.end(),
				[&p](const pair<history_t, vector<value_t>>& element){ return element.first == p.first;});
			if(itr_dp == data.end()){
				itr_dp = data.insert(itr_dp, make_pair(move(p.first), vector<value_t>()));
			}

			for(auto& val : p.second){
				if(itr_cp->second > end){
					break;
				}
				// insert value when start <= count <= end
				if(itr_cp->second >= start){
					itr_dp->second.push_back(move(val));
				}
				(itr_cp->second) ++;
			}
		}


		// all msg are collected
		if(isReady){
			for(auto& p : counter){
				// branch history not added to data
				if(p.second == 0){
					data.push_back(make_pair(move(p.first), vector<value_t>()));
				}
			}

			vector<Message> v;
			msg.CreateNextMsg(actors, data, num_thread_, data_store_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			// remove data
			thread_mutex_.lock();
			data_table_.erase(itr_data);
			counter_table_.erase(itr_counter);
			thread_mutex_.unlock();
		}
	}
};

class MathActor : public BarrierActorBase
{
public:
	MathActor(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox) : BarrierActorBase(id, data_store), num_thread_(num_thread), mailbox_(mailbox){}
private:
	int num_thread_;
	AbstractMailbox * mailbox_;

	// data table
	map<mkey_t, vector<pair<history_t, pair<value_t, int>>>> data_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		thread_mutex_.lock();
		auto itr = data_table_.find(key);
		if(itr == data_table_.end()){
			itr = data_table_.insert(itr, make_pair(key, vector<pair<history_t, pair<value_t, int>>>()));
		}
		thread_mutex_.unlock();

		auto& data = itr->second;

		int branch_key;
		get_branch_key(msg.meta, branch_key);

		// get actor params
		Actor_Object& actor = actors[msg.meta.step];
		assert(actor.params.size() == 1);
		Math_T math_type = (Math_T)Tool::value_t2int(actor.params[0]);
		void (*op)(pair<value_t, int>&, value_t&);
		switch(math_type){
		case Math_T::SUM:
		case Math_T::MEAN:	op = sum;break;
		case Math_T::MAX:	op = max;break;
		case Math_T::MIN:	op = min;break;
		default: 			cout << "Unexpected math type in MathActor" << endl;
		}

		for(auto& p : msg.data){
			// find history in data
			auto data_itr = merge_hisotry(data, p.first, branch_key);

			if(data_itr == data.end()){
				// insert empty value_t with count = 0
				data_itr = data.insert(data_itr, make_pair(move(p.first), make_pair(value_t(), 0)));
			}

			for(auto& val : p.second){
				op(data_itr->second, val);	// operate on new value
			}
		}

		// all msg are collected
		if(isReady){
			bool isMean = math_type == Math_T::MEAN;
			vector<pair<history_t, vector<value_t>>> msg_data;
			for(auto& p : data){
				pair<value_t, int>& val = p.second;
				vector<value_t> val_vec;
				// count > 0
				if(val.second > 0){
					// convert value to double
					to_double(val, isMean);
					val_vec.push_back(move(val.first));
				}
				msg_data.push_back(make_pair(move(p.first), move(val_vec)));
			}

			vector<Message> v;
			msg.CreateNextMsg(actors, msg_data, num_thread_, data_store_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			// remove data
			thread_mutex_.lock();
			data_table_.erase(itr);
			thread_mutex_.unlock();
		}
	}

	static void sum(pair<value_t, int>& p, value_t& v){
		if(p.second == 0){
			p.first = move(v);
			p.second ++;
			return;
		}
		value_t temp = p.first;
		p.first.content.clear();
		switch(v.type){
		case 1:
			Tool::str2int(to_string(Tool::value_t2int(temp) + Tool::value_t2int(v)), p.first);
			break;
		case 2:
			Tool::str2double(to_string(Tool::value_t2double(temp) + Tool::value_t2double(v)), p.first);
			break;
		}
		p.second ++;
	}
	static void max(pair<value_t, int>& p, value_t& v){
		if(p.second == 0 || p.first < v){
			p.first = move(v);
		}
		p.second ++;
	}

	static void min(pair<value_t, int>& p, value_t& v){
		if(p.second == 0 || p.first > v){
			p.first = move(v);
		}
		p.second ++;
	}

	static void to_double(pair<value_t, int>& p, bool isMean){
		value_t temp = p.first;

		// divide value by count if isMean
		int count = isMean ? p.second : 1;

		p.first.content.clear();
		switch(p.first.type){
		case 1:
			Tool::str2double(to_string((double)Tool::value_t2int(temp) / count), p.first);
			break;
		case 2:
			Tool::str2double(to_string(Tool::value_t2double(temp) / count), p.first);
			break;
		}
	}
};
