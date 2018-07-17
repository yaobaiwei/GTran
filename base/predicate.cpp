/*
 * message.cpp
 *
 *  Created on: July 17, 2018
 *      Author: Nick Fang
 */

#include "base/predicate.hpp"

bool operator ==(const value_t& v1, const value_t& v2)
{
	if(v1.type != v2.type){
		return false;
	}
	return Tool::value_t2string(v1) == Tool::value_t2string(v2);
}

bool operator !=(const value_t& v1, const value_t& v2)
{
	if(v1.type != v2.type){
		return true;
	}
	return Tool::value_t2string(v1) != Tool::value_t2string(v2);
}

bool operator <(const value_t& v1, const value_t& v2)
{
	if(v1.type != v2.type){
		return Tool::value_t2string(v1) < Tool::value_t2string(v2);
	}
	switch (v1.type) {
		case 1:		return Tool::value_t2int(v1) < Tool::value_t2int(v2);
		case 2:	 	return Tool::value_t2double(v1) < Tool::value_t2double(v2);
		default: 	return Tool::value_t2string(v1) < Tool::value_t2string(v2);
	}
}

bool operator >(const value_t& v1, const value_t& v2)
{
	if(v1.type != v2.type){
		return Tool::value_t2string(v1) > Tool::value_t2string(v2);
	}
	switch (v1.type) {
		case 1:		return Tool::value_t2int(v1) > Tool::value_t2int(v2);
		case 2:	 	return Tool::value_t2double(v1) > Tool::value_t2double(v2);
		default: 	return Tool::value_t2string(v1) > Tool::value_t2string(v2);
	}
}

bool operator <=(const value_t& v1, const value_t& v2)
{
	if(v1.type != v2.type){
		return Tool::value_t2string(v1) <= Tool::value_t2string(v2);
	}
	switch (v1.type) {
		case 1:		return Tool::value_t2int(v1) <= Tool::value_t2int(v2);
		case 2:	 	return Tool::value_t2double(v1) <= Tool::value_t2double(v2);
		default: 	return Tool::value_t2string(v1) <= Tool::value_t2string(v2);
	}
}

bool operator >=(const value_t& v1, const value_t& v2)
{
	if(v1.type != v2.type){
		return Tool::value_t2string(v1) >= Tool::value_t2string(v2);
	}
	switch (v1.type) {
		case 1:		return Tool::value_t2int(v1) >= Tool::value_t2int(v2);
		case 2:	 	return Tool::value_t2double(v1) >= Tool::value_t2double(v2);
		default: 	return Tool::value_t2string(v1) >= Tool::value_t2string(v2);
	}
}

bool Predicate::Evaluate(value_t *value)
{
	assert(values.size() > 0);

	// no value
	if (value == NULL){
		return predicate_type == Predicate_T::NONE;
	}

	// has value
	switch (predicate_type)
	{
	case Predicate_T::ANY:
		return true;
	case Predicate_T::NONE:
		return false;
	case Predicate_T::EQ:
		return *value == values[0];
	case Predicate_T::NEQ:
		return *value != values[0];
	case Predicate_T::LT:
		return *value < values[0];
	case Predicate_T::LTE:
		return *value <= values[0];
	case Predicate_T::GT:
		return *value > values[0];
	case Predicate_T::GTE:
		return *value >= values[0];
	case Predicate_T::INSDIE:
		assert(values.size() == 2);
		return *value > values[0] && *value < values[1];
	case Predicate_T::OUTSIDE:
		assert(values.size() == 2);
		return *value < values[0] || *value > values[1];
	case Predicate_T::BETWEEN:
		assert(values.size() == 2);
		return *value >= values[0] && *value <= values[1];
	case Predicate_T::WITHIN:
		for (auto v : values){
			if (v == *value){
				return true;
			}
		}
		return false;
	case Predicate_T::WITHOUT:
		for (auto v : values){
			if (v == *value){
				return false;
			}
		}
		return true;
	}
}
