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
#include "utils/config.hpp"
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
	void LoadMapping(Config* config);

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
	void ParseInit(Element_T type);
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
	bool Parse(const string& query, vector<Actor_Object>& vec, string& error_msg);

	Parser(Config *config){
		LoadMapping(config);
	}

	//parsing exception
	struct ParserException {
		string message;

		ParserException(const std::string &message) : message(message){}
		ParserException(const char *message) : message(message){}
	};

};
