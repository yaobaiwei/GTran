/*
 * abstract_actor.hpp
 *
 *  Created on: May 26, 2018
 *      Author: Hongzhi Chen
 */
//
#ifndef ABSTRACT_ACTOR_HPP_
#define ABSTRACT_ACTOR_HPP_

#include <string>
#include <vector>

#include "core/message.hpp"
#include "core/message_collector.hpp"

class AbstractActor {
public:
	AbstractActor(int id):id_(id){}

	virtual ~AbstractActor() {}


	const int GetActorId(){return id_;}


	virtual void process(int t_id, vector<Actor_Object> & actors, Message & msg) = 0;

private:
  // Actor ID
  int id_;
};

// Base class for barrier and branch actors
class ActorWithCollector :  public AbstractActor{
public:
	ActorWithCollector(int id) : AbstractActor(id){}
	virtual void process(int t_id, vector<Actor_Object> & actors, Message & msg){
		CollectAndProcess(t_id, actors, msg);
	}
protected:
	// check if all brother msg are collected and process
	void CollectAndProcess(int t_id, vector<Actor_Object> & actors, Message & msg)
	{
		mkey_t key(msg.meta);
		{
			// make sure only one thread executing on same msg key
			unique_lock<mutex> lock(collection_mutex_);
			collection_lock_.wait(lock, [&]{
				if(key_set_.count(key) == 0){
					key_set_.insert(key);
					return true;
				}else{
					return false;
				}
			});
		}

		bool isReady = collector_.IsReady(msg);
		DoWork(t_id, actors, msg, isReady);

		// notify completed
		{
			lock_guard<mutex> lock(collection_mutex_);
			key_set_.erase(key_set_.find(key));
		}
		collection_lock_.notify_all();
	}

	// All barrier and branch actors should override this function
	virtual void DoWork(int t_id, vector<Actor_Object> & actors, Message & msg, bool isReady) = 0;

private:
	// collect msg
	Message_Collector collector_;
	mutex collection_mutex_;
	condition_variable collection_lock_;
	set<mkey_t> key_set_;
};

#endif /* ABSTRACT_ACTOR_HPP_ */
