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
	BarrierActorBase(int id) : AbstractActor(id){}
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
		auto itr = path_counters_.find(key);
		if(itr == path_counters_.end()){
			counters_mutex_.lock();
			itr = path_counters_.insert(itr, make_pair(key, map<string, int>()));
			counters_mutex_.unlock();
		}
		map<string, int> &counter =  itr->second;

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
			path_counters_.erase(path_counters_.find(key));
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
	EndActor(int id, Result_Collector * rc) : BarrierActorBase(id), rc_(rc){}

private:
	Result_Collector * rc_;
	map<mkey_t, vector<value_t>> data_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		auto itr = data_table_.find(key);
		if(itr == data_table_.end()){
			thread_mutex_.lock();
			itr = data_table_.insert(itr, make_pair(key, vector<value_t>()));
			thread_mutex_.unlock();
		}
		vector<value_t>& data = itr->second;

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
			data_table_.erase(data_table_.find(key));
			thread_mutex_.unlock();
		}
	}
};

class CountActor : public BarrierActorBase
{
public:
	CountActor(int id, int num_thread, AbstractMailbox * mailbox) : BarrierActorBase(id), num_thread_(num_thread), mailbox_(mailbox){}
private:
	int num_thread_;
	AbstractMailbox * mailbox_;

	// data table
	map<mkey_t, vector<pair<history_t, int>>> data_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		auto itr = data_table_.find(key);
		if(itr == data_table_.end()){
			thread_mutex_.lock();
			itr = data_table_.insert(itr, make_pair(key, vector<pair<history_t, int>>()));
			thread_mutex_.unlock();
		}
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
			msg.CreateNextMsg(actors, msg_data, num_thread_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			// remove data
			thread_mutex_.lock();
			data_table_.erase(data_table_.find(key));
			thread_mutex_.unlock();
		}
	}
};

class DedupActor : public BarrierActorBase
{
public:
	DedupActor(int id, int num_thread, AbstractMailbox * mailbox) : BarrierActorBase(id), num_thread_(num_thread), mailbox_(mailbox){}
private:
	int num_thread_;
	AbstractMailbox * mailbox_;

	// data table
	map<mkey_t, vector<pair<history_t, vector<value_t>>>> data_table_;
	map<mkey_t, vector<pair<history_t, set<history_t>>>> dedup_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		auto itr_data = data_table_.find(key);
		auto itr_dedup = dedup_table_.find(key);
		if(itr_data == data_table_.end()){
			thread_mutex_.lock();
			itr_data = data_table_.insert(itr_data, make_pair(key, vector<pair<history_t, vector<value_t>>>()));
			itr_dedup = dedup_table_.insert(itr_dedup,  make_pair(key, vector<pair<history_t, set<history_t>>>()));
			thread_mutex_.unlock();
		}
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
					if(key_set.count(val.first) == 0){
						his.push_back(move(val));
					}
				}
				// if histroy not in set, add one data
				if(itr_sp->second.count(his) == 0 && p.second.size() > 0){
					itr_dp->second.push_back(move(p.second[0]));
					itr_sp->second.insert(move(his));
				}
			}else{
				// dedup value, should check on all values
				for(auto& val : p.second){
					// construct history with current value
					his.push_back(make_pair(0, val));
					if(itr_sp->second.count(his) == 0){
						itr_dp->second.push_back(move(val));
						itr_sp->second.insert(move(his));
					}
					his.clear();
				}
			}
		}

		// all msg are collected
		if(isReady){
			vector<Message> v;
			msg.CreateNextMsg(actors, data, num_thread_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			// remove data
			thread_mutex_.lock();
			data_table_.erase(data_table_.find(key));
			dedup_table_.erase(dedup_table_.find(key));
			thread_mutex_.unlock();
		}
	}
};

class GroupActor : public BarrierActorBase
{
public:
	GroupActor(int id, int num_thread, AbstractMailbox * mailbox, DataStore * datastore) : BarrierActorBase(id), num_thread_(num_thread), mailbox_(mailbox), datastore_(datastore){}
private:
	int num_thread_;
	AbstractMailbox * mailbox_;
	// DataStore
	DataStore * datastore_;

	// data table
	map<mkey_t, vector<pair<history_t, map<string, vector<value_t>>>>> data_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		auto itr = data_table_.find(key);
		if(itr == data_table_.end()){
			thread_mutex_.lock();
			itr = data_table_.insert(itr, make_pair(key, vector<pair<history_t, map<string, vector<value_t>>>>()));
			thread_mutex_.unlock();
		}
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
				if(! kp(t_id, k, keyProjection, datastore_)){
					continue;
				}
				if(! vp(t_id, v, valueProjection, datastore_)){
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
			msg.CreateNextMsg(actors, msg_data, num_thread_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			// remove data
			thread_mutex_.lock();
			data_table_.erase(data_table_.find(key));
			thread_mutex_.unlock();
		}
	}
};

class RangeActor : public BarrierActorBase
{
public:
	RangeActor(int id, int num_thread, AbstractMailbox * mailbox) : BarrierActorBase(id), num_thread_(num_thread), mailbox_(mailbox){}
private:
	int num_thread_;
	AbstractMailbox * mailbox_;

	// data table
	map<mkey_t, vector<pair<history_t, vector<value_t>>>> data_table_;
	map<mkey_t, vector<pair<history_t, int>>> counter_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		auto itr_data = data_table_.find(key);
		auto itr_counter = counter_table_.find(key);
		if(itr_data == data_table_.end()){
			thread_mutex_.lock();
			itr_data = data_table_.insert(itr_data, make_pair(key, vector<pair<history_t, vector<value_t>>>()));
			itr_counter = counter_table_.insert(itr_counter,  make_pair(key, vector<pair<history_t, int>>()));
			thread_mutex_.unlock();
		}
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

			// skip current his when exceed limits
			if(itr_cp->second > end){
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
			vector<Message> v;
			msg.CreateNextMsg(actors, data, num_thread_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			// remove data
			thread_mutex_.lock();
			data_table_.erase(data_table_.find(key));
			counter_table_.erase(counter_table_.find(key));
			thread_mutex_.unlock();
		}
	}
};
