/*
* actor_object.hpp
*
*  Created on: Jun 15, 2018
*      Author: Nick Fang
*/
#include "actor/actor_object.hpp"

void Actor_Object::AddParam(int key)
{
	value_t v;
	Tool::str2int(to_string(key), v);
	params.push_back(v);
}

bool Actor_Object::AddParam(string s)
{
	value_t v;
	string s_value = Tool::trim(s, " "); //delete all spaces
	if (! Tool::str2value_t(s_value, v)){
		return false;
	}
	params.push_back(v);
	return true;
}

bool Actor_Object::IsBarrier()
{
	switch (actor_type)
	{
	case ACTOR_T::AGGREGATE:
	case ACTOR_T::COUNT:
	case ACTOR_T::CAP:
	case ACTOR_T::GROUP:
	case ACTOR_T::DEDUP:
	case ACTOR_T::MATH:
	case ACTOR_T::ORDER:
	case ACTOR_T::RANGE:
		return true;
	default:
		return false;
	}
}

string Actor_Object::DebugString()
{
	string s = "Actortype: " + string(ActorType[static_cast<int>(actor_type)]);
	s += ", params: ";
	for (auto v : params)
	{
		s += value_t2string(v) + " ";
	}
	s += ", NextActor: " + to_string(next_actor);
	return s;
}

// parsing value_t for display
string Actor_Object::value_t2string(value_t & v){
	double d;
	int i;
	switch (v.type)
	{
	case 4:
	case 3:return string(v.content.begin(), v.content.end());
	case 2:
		d = Tool::value_t2double(v);
		return to_string(d);
	case 1:
		i = Tool::value_t2int(v);
		return to_string(i);
	case -1:
		return "";
	default:return "(" + string(v.content.begin(), v.content.end()) + ")";
	}
}

ibinstream& operator<<(ibinstream& m, const Actor_Object& obj)
{
	m << obj.actor_type;
	m << obj.next_actor;
	m << obj.params;
	return m;
}

obinstream& operator>>(obinstream& m, Actor_Object& obj)
{
	m >> obj.actor_type;
	m >> obj.next_actor;
	m >> obj.params;
	return m;
}

