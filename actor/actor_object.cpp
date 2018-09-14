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
	params.push_back(move(v));
}

bool Actor_Object::AddParam(string s)
{
	value_t v;
	string s_value = Tool::trim(s, " "); //delete all spaces
	if (! Tool::str2value_t(s_value, v)){
		return false;
	}
	params.push_back(move(v));
	return true;
}

bool Actor_Object::IsBarrier() const
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
	case ACTOR_T::END:
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
		s += Tool::DebugString(v) + " ";
	}
	s += ", NextActor: " + to_string(next_actor);
	s += ", Remote: ";
	s += send_remote ? "Yes" : "No";
	return s;
}

ibinstream& operator<<(ibinstream& m, const Actor_Object& obj)
{
	m << obj.actor_type;
	m << obj.next_actor;
	m << obj.send_remote;
	m << obj.params;
	return m;
}

obinstream& operator>>(obinstream& m, Actor_Object& obj)
{
	m >> obj.actor_type;
	m >> obj.next_actor;
	m >> obj.send_remote;
	m >> obj.params;
	return m;
}
