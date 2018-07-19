/*
 * branch_actor.hpp
 *
 *  Created on: July 13, 2018
 *      Author: Nick Fang
 */

#pragma once

#include "actor/abstract_actor.hpp"
#include "utils/tool.hpp"

struct msg_id_alloc{
	uint64_t id;
	mutex id_mutex;
	msg_id_alloc() : id(1) {}
	void AssignId(uint64_t &msg_id){
		id_mutex.lock();
		msg_id = id ++;
		id_mutex.unlock();
	}
};

// Base class for branch actors
class BranchActorBase :  public AbstractActor{
public:
	BranchActorBase(int id, int num_thread, AbstractMailbox* mailbox, msg_id_alloc* allocator): AbstractActor(id), num_thread_(num_thread), mailbox_(mailbox), id_allocator_(allocator){}
protected:
	// main logic of branch actors
	virtual void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady) = 0;
	virtual void get_steps(Actor_Object & actor, vector<int>& steps) = 0;
	virtual int get_steps_count(Actor_Object & actor) = 0;
	int num_thread_;
	AbstractMailbox* mailbox_;

	uint64_t send_branch_msg(int t_id, vector<Actor_Object> & actors, Message & msg){
		uint64_t msg_id;
		id_allocator_->AssignId(msg_id);

		vector<int> step_vec;
		get_steps(actors[msg.meta.step], step_vec);

		vector<Message> msg_vec;
		msg.CreateBranchedMsg(actors, step_vec, msg_id, num_thread_, msg_vec);
		for (auto& msg : msg_vec){
			mailbox_->Send(t_id, msg);
		}

		return msg_id;
	}
	void process_branch(int t_id, vector<Actor_Object> & actors, Message & msg){
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

		int branch_num = get_steps_count(actors[msg.meta.step]);
		bool isReady = IsReady(key, end_path, msg.meta.msg_path, branch_num);
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

private:
	// lock for mkey_t
	// make sure only one thread executing on same msg key
	mutex mkey_mutex_;
	condition_variable mkey_cv_;
	set<mkey_t> mkey_set_;

	// msg path counter, for checking if collection completed
	map<mkey_t, map<string, int>> path_counters_;
	// branch_num counter, recording num of collected branch
	map<mkey_t, int> branch_num_counters_;
	// lock for collector
	// protect map insert and erase
	mutex counters_mutex_;

	// assign unique msg id
	msg_id_alloc* id_allocator_;

	bool IsReady(mkey_t key, string end_path, string msg_path, int branch_num)
	{
		// get counter for key
		auto itr = path_counters_.find(key);
		if(itr == path_counters_.end()){
			counters_mutex_.lock();
			itr = path_counters_.insert(itr, make_pair(key, map<string, int>()));
			branch_num_counters_[key] = 0;
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

		branch_num_counters_[key] ++;
		if(branch_num_counters_[key] == branch_num)
		{
			// remove counter from counters map when completed
			lock_guard<mutex> lk(counters_mutex_);
			path_counters_.erase(path_counters_.find(key));
			branch_num_counters_.erase(branch_num_counters_.find(key));
			return true;
		}

		return false;
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
			// msg info given by current actor
			msg_id = msg.meta.branch_infos[branch_depth].msg_id;
			end_path = msg.meta.branch_infos[branch_depth].msg_path;
		}
		if(branch_depth >= 1){
			// msg info given by parent actor if any
			index = msg.meta.branch_infos[branch_depth - 1].index;
		}
		key = mkey_t(msg.meta.qid, msg_id, index);
	}
};

class BranchActor : public BranchActorBase{
public:
	BranchActor(int id, int num_thread, AbstractMailbox* mailbox, msg_id_alloc* allocator): BranchActorBase(id, num_thread, mailbox, allocator){}
	void process(int t_id, vector<Actor_Object> & actors,  Message & msg){
		if(msg.meta.msg_type == MSG_T::SPAWN){
			send_branch_msg(t_id, actors, msg);
		}
		else if(msg.meta.msg_type == MSG_T::BRANCH){
			process_branch(t_id, actors, msg);
		}else{
			cout << "Unexpected msg type in branch actor." << endl;
			exit(-1);
		}
	}
private:
	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady)
	{
		Actor_Object& actor = actors[msg.meta.step];

		auto itr = data_table_.find(key);
		if(itr == data_table_.end()){
			data_mutex_.lock();
			itr = data_table_.insert(itr, make_pair(key, vector<pair<history_t, vector<value_t>>>()));
			data_mutex_.unlock();
		}
		vector<pair<history_t, vector<value_t>>> &branch_data = itr->second;

		// get branch type
		assert(actor.params.size() > 1);
		Branch_T type = (Branch_T)Tool::value_t2int(actor.params[0]);

		switch(type){
		case UNION:
			// merge msg data into first branch
			merge_data(msg.meta.step, branch_data, msg.data);
			break;
		case COALESCE:
			cout << "COALESCE not implemented!" << endl;
			exit(-1);
			break;
		case CHOOSE:
			cout << "CHOOSE not implemented!" << endl;
			exit(-1);
			break;
		default:
			cout << "Unexpected branch type in branch actor!" << endl;
			exit(-1);
		}

		if(isReady){
			msg.meta.branch_infos.pop_back();
			vector<Message> v;
			msg.CreateNextMsg(actors, branch_data, num_thread_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			data_mutex_.lock();
			data_table_.erase(data_table_.find(key));
			data_mutex_.unlock();
		}
	}

	void merge_data(int his_key, vector<pair<history_t, vector<value_t>>>& dst, vector<pair<history_t, vector<value_t>>>& src)
	{
		for (auto& src_pair : src){
			// find step key in his
			auto& his = src_pair.first;
			auto his_itr = std::find_if( his.begin(), his.end(),
				[&his_key](const pair<int, value_t>& element){ return element.first == his_key;} );
			if (his_itr != his.end()){
				// remove appended key from history
				his.erase(his_itr);
			}

			// check if his already in dst
			auto data_itr = std::find_if( dst.begin(), dst.end(),
				[&his](const pair<history_t, vector<value_t>>& element){ return element.first == his;} );

			if(data_itr != dst.end()){
				// insert data to existing history
				std::move(src_pair.second.begin(), src_pair.second.end(), std::back_inserter((*data_itr).second));
			}else{
				// create new history in dst
				dst.push_back(move(src_pair));
			}
		}
	}

	void get_steps(Actor_Object & actor, vector<int>& steps)
	{
		vector<value_t> params = actor.params;
		assert(params.size() > 1);
		for(int i = 1; i < params.size(); i++){
			steps.push_back(Tool::value_t2int(params[i]));
		}
	}

	int get_steps_count(Actor_Object & actor)
	{
		return actor.params.size() - 1;
	}
	// store output data of each branched steps
	// mkey_t: key of msg
	map<mkey_t, vector<pair<history_t, vector<value_t>>>> data_table_;
	mutex data_mutex_;
};

class BranchFilterActor : public BranchActorBase{
public:
	BranchFilterActor(int id, int num_thread, AbstractMailbox* mailbox, msg_id_alloc* allocator): BranchActorBase(id, num_thread, mailbox, allocator){}
	void process(int t_id, vector<Actor_Object> & actors,  Message & msg){
		if(msg.meta.msg_type == MSG_T::SPAWN){
			uint64_t msg_id = send_branch_msg(t_id, actors, msg);

			int index = 0;
			if(msg.meta.branch_infos.size() > 0){
				// get index of parent branch if any
				index = msg.meta.branch_infos[msg.meta.branch_infos.size() - 1].index;
			}
			mkey_t key(msg.meta.qid, msg_id, index);

			data_mutex_.lock();
			data_table_[key] = move(msg.data);
			data_mutex_.unlock();
		}
		else if(msg.meta.msg_type == MSG_T::BRANCH){
			process_branch(t_id, actors, msg);
		}else{
			cout << "Unexpected msg type in branch actor." << endl;
			exit(-1);
		}
	}
private:
	void do_work(int t_id, vector<Actor_Object> & actors, Message & msg, mkey_t key, bool isReady)
	{
		auto itr = counter_table_.find(key);
		if(itr == counter_table_.end()){
			data_mutex_.lock();
			itr = counter_table_.insert(itr, make_pair(key, map<int, uint32_t>()));
			data_mutex_.unlock();
		}
		map<int, uint32_t> &counter =  itr->second;

		int info_size = msg.meta.branch_infos.size();
		assert(info_size > 0);
		int branch_index = msg.meta.branch_infos[info_size - 1].index;
		int his_key = msg.meta.branch_infos[info_size - 1].key;
		for(auto& p : msg.data){
			// find history with given key
			auto his_itr = std::find_if( p.first.begin(), p.first.end(),
				[&his_key](const pair<int, value_t>& element){ return element.first == his_key;} );

			// get index of data
			int data_index = Tool::value_t2int((*his_itr).second);

			// update counter
			if(counter.count(data_index) == 0){
				counter[data_index] = 0;
			}
			update_counter(counter[data_index], branch_index);
		}


		if(isReady){
			// get actor info
			Actor_Object& actor = actors[msg.meta.step];
			int num_of_branch = actor.params.size() - 1;
			assert(num_of_branch > 0);
			Filter_T filter_type = (Filter_T)Tool::value_t2int(actor.params[0]);

			// get filter function according to filter type
			bool (*pass)(uint32_t, int);
			switch (filter_type) {
			case Filter_T::AND:	pass = all_success;  break;
			case Filter_T::OR:	pass = any_success;  break;
			case Filter_T::NOT:	pass = none_success; break;
			}

			vector<pair<history_t, vector<value_t>>> &data = data_table_[key];
			int i = 0;

			// filter
			for(auto itr_pair = data.begin(); itr_pair != data.end();){
				auto& value_vec = itr_pair->second;
				for(auto itr_val = value_vec.begin(); itr_val != value_vec.end();){
					// remove data if not passed
					if(! pass(counter[i], num_of_branch)){
						itr_val = value_vec.erase(itr_val);
					}else{
						itr_val ++;
					}
					i ++;
				}

				if(value_vec.size() == 0){
					itr_pair = data.erase(itr_pair);
				}else{
					itr_pair ++;
				}
			}

			// remove last branch info
			msg.meta.branch_infos.pop_back();
			vector<Message> v;
			msg.CreateNextMsg(actors, data, num_thread_, v);
			for(auto& m : v){
				mailbox_->Send(t_id, m);
			}

			data_mutex_.lock();
			data_table_.erase(data_table_.find(key));
			counter_table_.erase(counter_table_.find(key));
			data_mutex_.unlock();
		}
	}

	void get_steps(Actor_Object & actor, vector<int>& steps)
	{
		vector<value_t> params = actor.params;
		assert(params.size() > 1);
		for(int i = 1; i < params.size(); i++){
			steps.push_back(Tool::value_t2int(params[i]));
		}
	}

	int get_steps_count(Actor_Object & actor)
	{
		return actor.params.size() - 1;
	}

	// each bit of counter indicates one branch index
	static inline void update_counter(uint32_t & counter, int branch_index){
		// set corresponding bit to 1
		counter |= ( 1 << branch_index);
	}

	static bool all_success(uint32_t counter, int num_of_branch){
		// check if all required bits are 1
		return counter == ((1 << num_of_branch) - 1);
	}

	static bool none_success(uint32_t counter, int num_of_branch){
		// check if all bits are 0
		return counter == 0;
	}

	static bool any_success(uint32_t counter, int num_of_branch){
		// check if any bit is 1
		return counter >= 1;
	}

	// store input data of current step
	map<mkey_t, vector<pair<history_t, vector<value_t>>>> data_table_;
	// count num of successful branches for each data
	map<mkey_t, map<int, uint32_t>> counter_table_;
	mutex data_mutex_;
};
