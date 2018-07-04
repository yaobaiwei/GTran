/*
* parser.hpp
*
*  Created on: Jun 15, 2018
*      Author: Nick Fang
*/
#pragma once

#include <string>
#include <vector>
#include <map>
#include "utils/tool.hpp"
#include "base/type.hpp"
#include "utils/hdfs_core.hpp"
#include "actor/actor_object.hpp"
using namespace std;

class Parser
{
private:
	enum Step_T{
		IN, OUT, BOTH, INE, OUTE, BOTHE, INV, OUTV, BOTHV, AND, AGGREGATE, AS, CAP, CHOOSE, COALESCE, COIN,
		COUNT, DEDUP, EMIT, GROUP, GROUPCOUNT, HAS, HASLABEL, HASKEY, HASVALUE, HASNOT, IS, KEY, LABEL, LIMIT,
		LOOPS, MAX, MEAN, MIN, NOT, OR, ORDER, PROPERTIES, RANGE, REPEAT, SELECT, SKIP, STORE, SUM, TAIL, TIMES,
		UNION, UNTIL, VALUES, WHERE
	};

	enum IO_T { EDGE, VERTEX, INT, DOUBLE, CHAR, STRING, COLLECTION };

	// In/out data type
	IO_T io_type_;

	// tmp actors store
	vector<Actor_Object> actors_;

	// Flags for sub-query
	bool is_in_repeat_;	// is inside repeat branch
	int first_in_sub_;	// first step index in sub query

	// str to enum
	const static map<string, Step_T> str2step;		// step type
	const static map<string, Predicate_T> str2pred;	// predicate type

	// str to id
	map<string, int> str2ls; // label step
	map<string, int> str2se; // side-effect

	// id to enm
	map<int, IO_T> ls2type;	 // label step output type

	// str to id, for property key and label key
	map<string, uint32_t> str2vpk;
	map<string, uint32_t> str2vl;
	map<string, uint32_t> str2epk;
	map<string, uint32_t> str2el;

	// id to value type
	map<uint32_t, uint8_t> vpk2vptype;
	map<uint32_t, uint8_t> epk2eptype;

	// load property and label mapping
	void LoadMapping();

	// IO type checking
	bool IsNumber();
	bool IsValue(uint8_t& type);
	bool IsElement(Element_T& type);
	IO_T Value2IO(uint8_t type);

	// check the type of last actor
	bool CheckLastActor(ACTOR_T type, int* index = NULL);

	// splitting parameters
	vector<string> SplitParam(string& param);
	vector<string> SplitPredicate(string& param, Predicate_T& pred_type);

	void Clear();

	void AppendActor(Actor_Object& actor);

	// Parse query or sub-query
	void DoParse(const string& query);

	// extract steps and corresponding params from query string
	void GetSteps(const string& query, vector<pair<Step_T, string>>& tokens);

	// mapping steps to actors
	void ParseSteps(const vector<pair<Step_T, string>>& tokens);

	// mapping string to label key or property key
	bool ParseKeyId(string key, bool isLabel, int& id, uint8_t* type = NULL);

	// Parse sub-query for branching actors
	void ParseSub(const vector<string>& params, int current_step, bool checkType);

	// Parse predicate
	void ParsePredicate(string& param, uint8_t type, Actor_Object& actor, bool toKey);

	// Parse actors
	void ParseAggregate(const vector<string>& params, Step_T type);
	void ParseAs(const vector<string>& params);
	void ParseBranch(const vector<string>& params, Step_T type);
	void ParseBranchFilter(const vector<string>& params, Step_T type);
	void ParseCap(const vector<string>& params);
	void ParseCoin(const vector<string>& params);
	void ParseCount(const vector<string>& params);
	void ParseDedup(const vector<string>& params);
	void ParseGroup(const vector<string>& params, Step_T type);	// should we support traversal projection?
	void ParseHas(const vector<string>& params, Step_T type);
	void ParseHasLabel(const vector<string>& params);
	void ParseIs(const vector<string>& params);
	void ParseKey(const vector<string>& params);
	void ParseLabel(const vector<string>& params);
	void ParseLoops(const vector<string>& params);
	void ParseMath(const vector<string>& params, Step_T type);
	void ParseOrder(const vector<string>& params);
	void ParseProjection(const string& param, int& key);
	void ParseProperties(const vector<string>& params);
	void ParseRange(const vector<string>& params, Step_T type);
	void ParseRepeat(const vector<string>& params);
	void ParseRepeatModulator(const vector<string>& params, Step_T type);
	void ParseSelect(const vector<string>& params);
	void ParseTraversal(const vector<string>& params, Step_T type);
	void ParseValues(const vector<string>& params);
	void ParseWhere(const vector<string>& params);

public:
	// Parse query string
	bool Parse(const string& query, vector<Actor_Object>& vec, Element_T& startType);

	Parser(){
		LoadMapping();
	}

	//parsing exception
	struct ParserException {
		string message;

		ParserException(const std::string &message) : message(message){}
		ParserException(const char *message) : message(message){}
	};

};

const map<string, Parser::Step_T> Parser::str2step = {
	{ "in", IN },
	{ "out", OUT }, 
	{ "both", BOTH }, 
	{ "inE", INE }, 
	{ "outE", OUTE }, 
	{ "bothE", BOTHE },
	{ "inV", INV }, 
	{ "outV", OUTV }, 
	{ "bothV", BOTHV }, 
	{ "and", AND }, 
	{ "aggregate", AGGREGATE },
	{ "as", AS }, 
	{ "cap", CAP },
	{ "choose", CHOOSE }, 
	{ "coalesce", COALESCE }, 
	{ "coin", COIN }, 
	{ "count", COUNT }, 
	{ "dedup", DEDUP }, 
	{ "emit", EMIT },
	{ "group", GROUP},
	{ "groupCount", GROUPCOUNT},
	{ "has", HAS }, 
	{ "hasLabel", HASLABEL }, 
	{ "hasKey", HASKEY }, 
	{ "hasValue", HASVALUE }, 
	{ "hasNot", HASNOT }, 
	{ "is", IS },
	{ "key", KEY }, 
	{ "label", LABEL }, 
	{ "limit", LIMIT }, 
	{ "loops", LOOPS }, 
	{ "max", MAX }, 
	{ "mean", MEAN },
	{ "min", MIN }, 
	{ "not", NOT }, 
	{ "or", OR }, 
	{ "order", ORDER }, 
	{ "properties", PROPERTIES }, 
	{ "range", RANGE },
	{ "repeat", REPEAT }, 
	{ "select", SELECT }, 
	{ "skip", SKIP }, 
	{ "store", STORE }, 
	{ "sum", SUM }, 
	{ "tail", TAIL },
	{ "times", TIMES }, 
	{ "union", UNION }, 
	{ "until", UNTIL }, 
	{ "values", VALUES }, 
	{ "where", WHERE }
};

const map<string, Predicate_T> Parser::str2pred = {
	{ "eq", Predicate_T::EQ },
	{ "neq", Predicate_T::NEQ },
	{ "lt", Predicate_T::LT },
	{ "lte", Predicate_T::LTE },
	{ "gt", Predicate_T::GT },
	{ "gte", Predicate_T::GTE },
	{ "inside", Predicate_T::INSDIE },
	{ "outside", Predicate_T::OUTSIDE },
	{ "between", Predicate_T::BETWEEN },
	{ "within", Predicate_T::WITHIN },
	{ "without", Predicate_T::WITHOUT }
};