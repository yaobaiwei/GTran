/*
 * message.hpp
 *
 *  Created on: July 13, 2018
 *      Author: Nick Fang
 */

#pragma once

#include "core/message.hpp"

struct mkey_t {
	uint64_t qid;
	uint64_t mid;
	mkey_t() : qid(0), mid(0){}
	mkey_t(uint64_t qid_, uint64_t mid_) : qid(qid_), mid(mid_){}
	mkey_t(const Meta& m) : qid(m.qid){
		mid = 0;

		int branch_depth = m.branch_infos.size() - 1;
		if(branch_depth >= 0){
			mid = m.branch_infos[branch_depth].msg_id;
		}
	}

	bool operator==(const mkey_t& key) const
	{
		if((qid == key.qid) && (mid == key.mid)){
			return true;
		}
		return false;
	}

	bool operator<(const mkey_t& key) const
	{
		if(qid < key.qid){
			return true;
		}else if(qid == key.qid){
			return mid < key.mid;
		}else{
			return false;
		}
	}
};

class Message_Collector{
public:
	Message_Collector(){}

    // check and merge data to msg_map_
    bool IsReady(Message& msg){
		string end_path;
		mkey_t key;
		GetMsgInfo(msg, key, end_path);
		if(CheckPath(key, end_path, msg.meta.msg_path)){
			msg.meta.msg_path = end_path;
			return true;
		}
		return false;
	}

    // get msg info for collecting sub msg
    static void GetMsgInfo(Message& msg, mkey_t &key, string &end_path){
		// init info
		uint64_t msg_id = 0;
		end_path = "";

		int branch_depth = msg.meta.branch_infos.size() - 1;
		if(branch_depth >= 0){
			msg_id = msg.meta.branch_infos[branch_depth].msg_id;
			end_path = msg.meta.branch_infos[branch_depth].msg_path;
		}
		key = mkey_t(msg.meta.qid, msg_id);
	}

private:
    map<mkey_t, map<string, int>> path_counters_;
	mutex counters_mutex_;
	
	// Check if all sub msg are collected
    bool CheckPath(mkey_t key, string end_path, string msg_path)
	{
		// get counter for key
		counters_mutex_.lock();
		map<string, int> &counter =  path_counters_[key];
		counters_mutex_.unlock();
		
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
		
		// remove counter from counters map
		{
			lock_guard<mutex> lk(counters_mutex_);
			path_counters_.erase(path_counters_.find(key));
		}
		return true;
	}
};
