#include "parser/utils.hpp"

// to type.cpp

ibinstream& operator<<(ibinstream& m, const ACTOR_T& type){
	int t = static_cast<int>(type);
	m << t;
	return m;
}

obinstream& operator>>(obinstream& m, ACTOR_T& type){
	int tmp;
	m >> tmp;
	type = static_cast<ACTOR_T>(tmp);
	return m;
}

Relation operator==(value_t& lhs, value_t& rhs)
{
	if (lhs.type != rhs.type){
		return UNDEFINED;
	}
	string l_value = Tool::value_t2string(lhs);
	string r_value = Tool::value_t2string(rhs);
	return l_value == r_value ? TRUE : FALSE;
}

Relation operator!=(value_t& lhs, value_t& rhs)
{
	if (lhs.type != rhs.type){
		return UNDEFINED;
	}
	string l_value = Tool::value_t2string(lhs);
	string r_value = Tool::value_t2string(rhs);
	return l_value != r_value ? TRUE : FALSE;
}

Relation operator>(value_t& lhs, value_t& rhs)
{
	if (lhs.type > 0x10 || lhs.type != rhs.type){
		return UNDEFINED;
	}
	switch (lhs.type)
	{
	case 1:
		return Tool::value_t2int(lhs) > Tool::value_t2int(rhs) ? TRUE : FALSE;
	case 2:
		return Tool::value_t2double(lhs) > Tool::value_t2double(rhs) ? TRUE : FALSE;
	case 3: case 4:
		return Tool::value_t2string(lhs) > Tool::value_t2string(rhs) ? TRUE : FALSE;
	default:
		return UNDEFINED;
	}
}

Relation operator<(value_t& lhs, value_t& rhs)
{
	if (lhs.type > 0x10 || lhs.type != rhs.type){
		return UNDEFINED;
	}
	switch (lhs.type)
	{
	case 1:
		return Tool::value_t2int(lhs) < Tool::value_t2int(rhs) ? TRUE : FALSE;
	case 2:
		return Tool::value_t2double(lhs) < Tool::value_t2double(rhs) ? TRUE : FALSE;
	case 3: case 4:
		return Tool::value_t2string(lhs) < Tool::value_t2string(rhs) ? TRUE : FALSE;
	default:
		return UNDEFINED;
	}
}

Relation operator>=(value_t& lhs, value_t& rhs)
{
	if (lhs.type > 0x10 || lhs.type != rhs.type){
		return UNDEFINED;
	}
	Relation gt = lhs > rhs;
	Relation eq = lhs == rhs;
	return (gt == TRUE || eq == TRUE) ? TRUE : FALSE;
}

Relation operator<=(value_t& lhs, value_t& rhs)
{
	if (lhs.type > 0x10 || lhs.type != rhs.type){
		return UNDEFINED;
	}
	Relation lt = lhs < rhs;
	Relation eq = lhs == rhs;
	return (lt == TRUE || eq == TRUE) ? TRUE : FALSE;
}