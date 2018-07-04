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

	Actor_Object() : next_actor(-1){}
	Actor_Object(ACTOR_T type) : actor_type(type), next_actor(-1){}

	void AddParam(int key);
	bool AddParam(string s);
	bool IsBarrier();

	string DebugString();

	// parsing value_t for display
	static string value_t2string(value_t & v);
};

ibinstream& operator<<(ibinstream& m, const Actor_Object& msg);

obinstream& operator>>(obinstream& m, Actor_Object& msg);

