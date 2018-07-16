/*
 * branch_actor.hpp
 *
 *  Created on: July 16, 2018
 *      Author: Nick Fang
 */
#pragma once

#include "actor/abstract_actor.hpp"
#include "utils/tool.hpp"

class EndActor : public ActorWithCollector
{
public:
	EndActor(int id, Result_Collector * rc) : ActorWithCollector(id), rc_(rc){}

private:
	Result_Collector * rc_;
	map<mkey_t, vector<value_t>> data_table_;
	mutex thread_mutex_;

	void DoWork(int t_id, vector<Actor_Object> & actors, Message & msg, bool isReady){
		mkey_t key(msg.meta);
		thread_mutex_.lock();
		vector<value_t>& data = data_table_[key];
		thread_mutex_.unlock();

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
