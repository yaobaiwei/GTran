/*
 * predicate.hpp
 *
 *  Created on: July 17, 2018
 *      Author: Nick Fang
 */

#pragma once

#include "base/type.hpp"
#include "utils/tool.hpp"

bool operator ==(const value_t& v1, const value_t& v2);

bool operator !=(const value_t& v1, const value_t& v2);

bool operator <(const value_t& v1, const value_t& v2);

bool operator >(const value_t& v1, const value_t& v2);

bool operator <=(const value_t& v1, const value_t& v2);

bool operator >=(const value_t& v1, const value_t& v2);

class Predicate{
public:
	Predicate_T predicate_type;
	vector<value_t>	values;

	bool Evaluate(value_t *value = NULL);

	Predicate(Predicate_T p_type, vector<value_t> vec) : predicate_type(p_type), values(vec){}

	Predicate(Predicate_T p_type, value_t& value) : predicate_type(p_type){
		values = Tool::value_t2vec(value);
	}
};
