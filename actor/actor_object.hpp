/*
* actor_object.hpp
*
*  Created on: Jun 15, 2018
*      Author: Nick Fang
*/
#pragma once

#include <string>
#include <vector>
#include "utils/tool.hpp"
#include "base/type.hpp"
using namespace std;

class Actor_Object
{
public:
	// type
	ACTOR_T actor_type;

	// parameters
	vector<value_t> params;

	// index of next actor
	int next_actor;

	// flag for sending data to remote nodes
	bool send_remote;

	Actor_Object() : next_actor(-1), send_remote(false){}
	Actor_Object(ACTOR_T type) : actor_type(type), next_actor(-1), send_remote(false){}

	void AddParam(int key);
	bool AddParam(string s);
	bool IsBarrier();

	string DebugString();
};

ibinstream& operator<<(ibinstream& m, const Actor_Object& msg);

obinstream& operator>>(obinstream& m, Actor_Object& msg);
