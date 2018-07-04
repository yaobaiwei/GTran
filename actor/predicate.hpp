/*
* actor_object.hpp
*
*  Created on: Jun 15, 2018
*      Author: Nick Fang
*/
#pragma once

#include "utils/tool.hpp"
#include "base/type.hpp"

class Predicate{
public:
	Predicate_T type;
	vector<value_t>	values;

	bool compare(value_t *value = NULL);

	Predicate(Predicate_T p_type, vector<value_t> vec) : type(p_type), values(vec){}

	Predicate(Predicate_T p_type, value_t& value) : type(p_type){
		values = Tool::value_t2vec(value);
	}
};

bool Predicate::compare(value_t *value)
{
	assert(values.size() > 0);

	// no value
	if (value == NULL){
		return type == NONE;
	}

	switch (type)
	{
	case ANY:
		return true;
	case NONE:
		return false;
	case EQ:
		return *value == values[0];
	case NEQ:
		return *value != values[0];
	case LT:
		return *value < values[0];
	case LTE:
		return *value <= values[0];
	case GT:
		return *value > values[0];
	case GTE:
		return *value >= values[0];
	case INSDIE:
		assert(values.size() == 2);
		return *value > values[0] && *value < values[1];
	case OUTSIDE:
		assert(values.size() == 2);
		return *value < values[0] || *value > values[1];
	case BETWEEN:
		assert(values.size() == 2);
		return *value >= values[0] && *value <= values[1];
	case WITHIN:
		for (auto v : values){
			if (v == *value){
				return true;
			}
		}
		return false;
	case WITHOUT:
		for (auto v : values){
			if (v == *value){
				return false;
			}
		}
		return true;
	}
}