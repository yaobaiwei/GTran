/*
 * branch_actor.hpp
 *
 *  Created on: July 16, 2018
 *      Author: Nick Fang
 */
#pragma once

#include "actor/abstract_actor.hpp"
#include "core/result_collector.hpp"
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

	map<mkey_t, vector<pair<history_t, int>>> data_table_;
	mutex thread_mutex_;

	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady){
		auto itr = data_table_.find(key);
		if(itr == data_table_.end()){
			thread_mutex_.lock();
			itr = data_table_.insert(itr, make_pair(key, vector<pair<history_t, int>>()));
			thread_mutex_.unlock();
		}
		vector<pair<history_t, int>>& data = itr->second;

		// check if count actor in branch
		// run locally if true
		int branch_depth = msg.meta.branch_infos.size();
		int branch_key = - 1;
		if(branch_depth != 0){
			branch_key = msg.meta.branch_infos[branch_depth - 1].key;
		}

		for(auto& p : msg.data){
			int count = p.second.size();

			// remove history after branch_key
			if(branch_key != -1){
				auto pair_itr = find_if( p.first.begin(), p.first.end(),
					[&branch_key](const pair<int, value_t>& element){ return element.first == branch_key;} );
				if(pair_itr != p.first.end()){
					p.first.erase(pair_itr + 1, p.first.end());
				}
			}

			// find history in data
			auto data_itr = find_if( data.begin(), data.end(),
				[&p](const pair<history_t, int>& element){ return element.first == p.first;});

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
