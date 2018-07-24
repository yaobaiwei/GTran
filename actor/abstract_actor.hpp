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
#include "storage/data_store.hpp"

class AbstractActor {
public:
	AbstractActor(int id, DataStore* data_store):id_(id), data_store_(data_store){}

	virtual ~AbstractActor() {}


	const int GetActorId(){return id_;}


	virtual void process(int t_id, vector<Actor_Object> & actors, Message & msg) = 0;

protected:
	// Data Store
    DataStore* data_store_;

private:
	// Actor ID
	int id_;
};

#endif /* ABSTRACT_ACTOR_HPP_ */
