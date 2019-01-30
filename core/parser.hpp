/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#pragma once

#include <string>
#include <vector>
#include <map>
#include "actor/actor_object.hpp"
#include "base/type.hpp"
#include "core/index_store.hpp"
#include "utils/tool.hpp"
#include "utils/hdfs_core.hpp"
#include "utils/config.hpp"

using namespace std;

class Parser
{
private:
	//for debug usage
	string TokenToStr(pair<Step_T, string> token);
	string TokensToStr(vector<pair<Step_T, string>> tokens);
	string StepToStr(int step);

	enum IO_T { EDGE, VERTEX, INT, DOUBLE, CHAR, STRING, COLLECTION };

	// In/out data type
	IO_T io_type_;

	// tmp actors store
	vector<Actor_Object> actors_;

	// disable index when (count of given predicate > min_count_ * ratio)
	const static int index_ratio = 3;
	vector<uint64_t> index_count_;
	uint64_t min_count_;

	// first step index in sub query
	int first_in_sub_;

	// record step index of actor which should send data to non-local nodes
	int dispatch_step_;

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

	//after the above 4 key map, a vector of keys will be implemented.

	vector<string> vpks, vlks, epks, elks;

	string vpks_str, vlks_str, epks_str, elks_str;

	// id to value type
	map<uint32_t, uint8_t> vpk2vptype;
	map<uint32_t, uint8_t> epk2eptype;

	Config * config_;
	IndexStore * index_store_;

	// IO type checking
	bool IsNumber();
	bool IsValue(uint8_t& type);
	bool IsElement();
	bool IsElement(Element_T& type);
	IO_T Value2IO(uint8_t type);

	// check the type of last actor
	bool CheckLastActor(ACTOR_T type);

	// check if parameter is query
	bool CheckIfQuery(const string& param);

	// Get priority for re-ordering
	int GetStepPriority(Step_T type);

	// splitting parameters
	void SplitParam(string& param, vector<string>& params);
	void SplitPredicate(string& param, Predicate_T& pred_type, vector<string>& params);

	void Clear();

	void AppendActor(Actor_Object& actor);

	// Parse build index
	void ParseIndex(const string& param);

	// Parse set config
	void ParseSetConfig(const string& param);

	// Parse query or sub-query
	void DoParse(const string& query);

	// extract steps and corresponding params from query string
	void GetSteps(const string& query, vector<pair<Step_T, string>>& tokens);

	// Re-ordering Optimization
	void ReOrderSteps(vector<pair<Step_T, string>>& tokens);

	// mapping steps to actors
	void ParseSteps(const vector<pair<Step_T, string>>& tokens);

	// mapping string to label key or property key
	bool ParseKeyId(string key, bool isLabel, int& id, uint8_t* type = NULL);

	//error message for unexpected key
	string ExpectedKey(bool isLabel);

	// Parse sub-query for branching actors
	void ParseSub(const vector<string>& params, int current_step, bool checkType);

	// Parse predicate
	void ParsePredicate(string& param, uint8_t type, Actor_Object& actor, bool toKey);

	// Parse actors
	void ParseInit(Element_T type);
	void ParseAggregate(const vector<string>& params);
	void ParseAs(const vector<string>& params);
	void ParseBranch(const vector<string>& params);
	void ParseBranchFilter(const vector<string>& params, Step_T type);
	void ParseCap(const vector<string>& params);
	void ParseCount(const vector<string>& params);
	void ParseDedup(const vector<string>& params);
	void ParseGroup(const vector<string>& params, Step_T type);	// should we support traversal projection?
	void ParseHas(const vector<string>& params, Step_T type);
	void ParseHasLabel(const vector<string>& params);
	void ParseIs(const vector<string>& params);
	void ParseKey(const vector<string>& params);
	void ParseLabel(const vector<string>& params);
	void ParseMath(const vector<string>& params, Step_T type);
	void ParseOrder(const vector<string>& params);
	void ParseProperties(const vector<string>& params);
	void ParseRange(const vector<string>& params, Step_T type);
	void ParseCoin(const vector<string>& params);
	void ParseRepeat(const vector<string>& params);
	void ParseSelect(const vector<string>& params);
	void ParseTraversal(const vector<string>& params, Step_T type);
	void ParseValues(const vector<string>& params);
	void ParseWhere(const vector<string>& params);

public:
	// Parse query string
	bool Parse(const string& query, vector<Actor_Object>& vec, string& error_msg);

	Parser(IndexStore* index_store): index_store_(index_store)
	{
		config_ = Config::GetInstance();
	}

	int GetPid(Element_T type, string& property);

	void ReadSnapshot();
	void WriteSnapshot();

	// load property and label mapping
	void LoadMapping();

	//parsing exception
	struct ParserException{
		string message;

		ParserException(const std::string &message) : message(message){}
		ParserException(const char *message) : message(message){}
	};

};
