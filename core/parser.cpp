#include "core/parser.hpp"
#include <iostream>

void Parser::LoadMapping(){
	hdfsFS fs = get_hdfs_fs();

	// load vertex label
	string vl_path = config_->HDFS_INDEX_PATH + "./vtx_label";
	hdfsFile vl_file = get_r_handle(vl_path.c_str(), fs);
	LineReader vl_reader(fs, vl_file);
	while (true)
	{
		vl_reader.read_line();
		if (!vl_reader.eof())
		{
			char * line = vl_reader.get_line();
			char * pch;
			pch = strtok(line, "\t");
			string key(pch);
			pch = strtok(NULL, "\t");
			label_t id = atoi(pch);

			str2vl[key] = id;
		}
		else
			break;
	}
	hdfsCloseFile(fs, vl_file);

	// load vertex property key and type
	string vp_path = config_->HDFS_INDEX_PATH + "./vtx_property_index";
	hdfsFile vp_file = get_r_handle(vp_path.c_str(), fs);
	LineReader vp_reader(fs, vp_file);
	while (true)
	{
		vp_reader.read_line();
		if (!vp_reader.eof())
		{
			char * line = vp_reader.get_line();
			char * pch;
			pch = strtok(line, "\t");
			string key(pch);
			pch = strtok(NULL, "\t");
			uint32_t id = atoi(pch);
			pch = strtok(NULL, "\t");
			uint8_t type = atoi(pch);

			str2vpk[key] = id;
			vpk2vptype[id] = type;
		}
		else
			break;
	}
	hdfsCloseFile(fs, vp_file);

	// load edge label
	string el_path = config_->HDFS_INDEX_PATH + "./edge_label";
	hdfsFile el_file = get_r_handle(el_path.c_str(), fs);
	LineReader el_reader(fs, el_file);
	while (true)
	{
		el_reader.read_line();
		if (!el_reader.eof())
		{
			char * line = el_reader.get_line();
			char * pch;
			pch = strtok(line, "\t");
			string key(pch);
			pch = strtok(NULL, "\t");
			label_t id = atoi(pch);

			str2el[key] = id;
		}
		else
			break;
	}
	hdfsCloseFile(fs, el_file);

	// load edge property key and type
	string ep_path = config_->HDFS_INDEX_PATH + "./edge_property_index";
	hdfsFile ep_file = get_r_handle(ep_path.c_str(), fs);
	LineReader ep_reader(fs, ep_file);
	while (true)
	{
		ep_reader.read_line();
		if (!ep_reader.eof())
		{
			char * line = ep_reader.get_line();
			char * pch;
			pch = strtok(line, "\t");
			string key(pch);
			pch = strtok(NULL, "\t");
			uint32_t id = atoi(pch);
			pch = strtok(NULL, "\t");
			uint8_t type = atoi(pch);

			str2epk[key] = id;
			epk2eptype[id] = type;
		}
		else
			break;
	}
	hdfsCloseFile(fs, ep_file);
}

bool Parser::Parse(const string& query, vector<Actor_Object>& vec, string& error_msg)
{
	Clear();
	// check prefix
	if (query.find("g.V().") == 0){
		ParseInit(Element_T::VERTEX);
		io_type_ = IO_T::VERTEX;
	}
	else if (query.find("g.E().") == 0){
		ParseInit(Element_T::EDGE);
		io_type_ = IO_T::EDGE;
	}
	else{
		error_msg = "'g.V().' or 'g.E().' expected";
		return false;
	}

	// trim blanks and remove prefix
	string q = query.substr(6);
	q = Tool::trim(q, " ");

	try{
		DoParse(q);
	}
	catch (ParserException e){
		error_msg = "Parsing Error: " + e.message;
		return false;
	}

	for (auto actor : actors_){
		vec.push_back(actor);
	}

	vec.push_back(Actor_Object(ACTOR_T::END));

	return true;
}

vector<string> Parser::SplitParam(string& param)
{
	param = Tool::trim(param, " ");
	int len = param.size();
	if (len > 0 && param[len - 1] == ','){
		throw ParserException("unexpected ',' at: " + param);
	}
	vector<string> tmp;
	Tool::splitWithEscape(param, ",", tmp);
	vector<string> params;
	string p = "";
	int balance = 0;
	for (auto& itr : tmp){
		// only split ',' which is not encased by '()'
		for (char i : itr){
			switch (i){
			case '(': balance++; break;
			case ')': balance--; break;
			}
		}
		p = p + "," + itr;
		if (balance == 0){
			params.push_back(Tool::trim(p, " ,"));
			p = "";
		}
	}
	return params;
}

vector<string> Parser::SplitPredicate(string& param, Predicate_T& pred_type)
{
	vector<string> pred_params;
	param = Tool::trim(param, " ");
	vector<string> pred;
	Tool::splitWithEscape(param, "()", pred);
	int len = pred.size();
	if (len == 0){
		pred_type = Predicate_T::ANY;
		pred_params.push_back("-1");
	}
	else if (len == 1)
	{
		pred_type = Predicate_T::EQ;
		pred_params.push_back(pred[0]);
	}
	else if (len == 2 && str2pred.count(pred[0]) != 0){
		pred_type = str2pred.at(pred[0]);
		pred_params = SplitParam(pred[1]);
	}
	else{
		throw ParserException("unexpected predicate: " + param);
	}
	return pred_params;
}

bool Parser::IsNumber(){
	return (io_type_ == INT || io_type_ == DOUBLE);
}
bool Parser::IsValue(uint8_t& type){
	switch (io_type_)
	{
	case IO_T::INT:
		type = 1;
		break;
	case IO_T::DOUBLE:
		type = 2;
		break;
	case IO_T::CHAR:
		type = 3;
		break;
	case IO_T::STRING:
		type = 4;
		break;
	default:
		return false;
	}
	return true;
}

bool Parser::IsElement(Element_T& type){
	switch (io_type_)
	{
	case IO_T::VERTEX:
		type = Element_T::VERTEX;
		return true;
	case IO_T::EDGE:
		type = Element_T::EDGE;
		return true;
	default:
		return false;
	}
}
Parser::IO_T Parser::Value2IO(uint8_t type){
	switch (type)
	{
	case 1:
		return IO_T::INT;
	case 2:
		return IO_T::DOUBLE;
	case 3:
		return IO_T::CHAR;
	case 4:
		return IO_T::STRING;
	default:
		throw ParserException("unexpected error");
	}
}

void Parser::DoParse(const string& query)
{
	vector<pair<Step_T, string>> tokens;
	// extract steps from query
	GetSteps(query, tokens);

	// Parse steps to actors_
	ParseSteps(tokens);
}

void Parser::Clear()
{
	actors_.clear();
	str2ls.clear();
	ls2type.clear();
	str2se.clear();
	is_in_repeat_ = false;
	first_in_sub_ = 0;
}

void Parser::AppendActor(Actor_Object& actor){
	actor.next_actor = actors_.size() + 1;
	actors_.push_back(actor);
}

bool Parser::CheckLastActor(ACTOR_T type, int* index){
	int current = actors_.size();
	int itr = actors_.size() - 1;

	// not actor in sub query
	if (itr < first_in_sub_){
		return false;
	}

	// find last actor
	while (actors_[itr].next_actor != current){
		itr = actors_[itr].next_actor;
	}

	if (index != NULL){
		*index = itr;
	}

	return actors_[itr].actor_type == type;
}

bool Parser::ParseKeyId(string key, bool isLabel, int& id, uint8_t *type)
{
	map<string, uint32_t> *kmap;
	map<uint32_t, uint8_t> *vmap;

	key = Tool::trim(key, "\"\'");

	if (io_type_ == VERTEX){
		kmap = isLabel ? &str2vl : &str2vpk;
		vmap = &vpk2vptype;
	}
	else if (io_type_ == EDGE){
		kmap = isLabel ? &str2el : &str2epk;
		vmap = &epk2eptype;
	}
	else{
		return false;
	}

	if (kmap->count(key) != 1){
		return false;
	}

	id = kmap->at(key);
	if (!isLabel && type != NULL){
		*type = vmap->at(id);
	}
	return true;
}

void Parser::GetSteps(const string& query, vector<pair<Step_T, string>>& tokens)
{
	int lbpos = 0;	// pos of left bracket
	int pos = 0;
	int parentheses = 0;
	int length = query.length();
	if (length == 0){
		throw ParserException("empty query");
	}

	string step, params;

	while ((lbpos = query.find('(', pos)) != string::npos){
		// get step name
		step = query.substr(pos, lbpos - pos);
		if (str2step.count(step) != 1){
			throw ParserException("unexpected step: " + step);
		}
		pos = lbpos;
		parentheses = 1;

		// match brackets
		while (pos < length){
			pos++;
			if (query[pos] == '('){
				parentheses++;
			}
			else if (query[pos] == ')'){
				parentheses--;
				// get params string
				if (parentheses == 0){
					params = query.substr(lbpos + 1, pos - lbpos - 1);
					tokens.push_back(make_pair(str2step.at(step), params));
					pos++;
					if (pos != length && query[pos ++] != '.'){
						throw ParserException("expect '.' after ')'");
					}
					break;
				}
			}
		}
	}

	// check parentheses balance
	if (parentheses != 0){
		throw ParserException("parentheses not balanced");
	}

	// checking ending with ')'
	if (pos != length){
		throw ParserException("unexpected words at the end: '" + query.substr(pos - 1) + "'");
	}
}

void Parser::ParseSteps(const vector<pair<Step_T, string>>& tokens) {
	for (auto stepToken : tokens){
		Step_T type = stepToken.first;
		vector<string> params = SplitParam(stepToken.second);
		switch (type){
		//AggregateActor
		case AGGREGATE:case STORE:
			ParseAggregate(params, type); break;
		//As Actor
		case AS:
			ParseAs(params); break;
		//Branch Actors
		case CHOOSE:case COALESCE:case UNION:
			ParseBranch(params, type); break;
		//BranchFilter Actors
		case AND:case NOT:case OR:
			ParseBranchFilter(params, type); break;
		//Cap Actor
		case CAP:
			ParseCap(params); break;
		//Coin Actor
		case COIN:
			ParseCoin(params); break;
		//Count Actor
		case COUNT:
			ParseCount(params); break;
		//Dedup Actor
		case DEDUP:
			ParseDedup(params); break;
		//Group Actor
		case GROUP:case GROUPCOUNT:
			ParseGroup(params, type); break;
		//Has Actors
		case HAS:case HASKEY:case HASVALUE:case HASNOT:
			ParseHas(params, type); break;
		//HasLabel Actors
		case HASLABEL:
			ParseHasLabel(params); break;
		//Is Actor
		case IS:
			ParseIs(params); break;
		//Key Actor
		case KEY:
			ParseKey(params); break;
		//Label Actor
		case LABEL:
			ParseLabel(params); break;
		//Math Actor
		case MAX:case MEAN:case MIN:case SUM:
			ParseMath(params, type); break;
		//Order Actor
		case ORDER:
			ParseOrder(params); break;
		//Property Actor
		case PROPERTIES:
			ParseProperties(params); break;
		//Range Actor
		case LIMIT:case RANGE:case SKIP:case TAIL:
			ParseRange(params, type); break;
		//Repeat Actor
		case REPEAT:
			ParseRepeat(params); break;
		case LOOPS:
			ParseLoops(params); break;
		case EMIT:case UNTIL:case TIMES:
			ParseRepeatModulator(params, type); break;
		//Select Actor
		case SELECT:
			ParseSelect(params); break;
		//Traversal Actors
		case IN:case OUT:case BOTH:case INE:case OUTE:case BOTHE:case INV:case OUTV:case BOTHV:
			ParseTraversal(params, type); break;
		//Values Actor
		case VALUES:
			ParseValues(params); break;
		//Where	Actor
		case WHERE:
			ParseWhere(params); break;
		default:throw ParserException("Unexpected step");
		}
	}
}

void Parser::ParseSub(const vector<string>& params, int current, bool filterBranch)
{
	int sub_step = actors_.size();
	IO_T current_type = io_type_;
	IO_T sub_type;
	bool first = true;

	int m_first_in_sub = first_in_sub_;
	for (const string &sub : params){
		// restore input type before parsing next sub query
		io_type_ = current_type;
		first_in_sub_ = actors_.size();

		// Parse sub-query and add to actors_ list
		DoParse(sub);

		// check sub query type
		if (first){
			sub_type = io_type_;
			first = false;
		}
		else if (!filterBranch && sub_type != io_type_){
			throw ParserException("expect same output type in sub queries");
		}

		// update sub_step of branch actor
		actors_[current].AddParam(sub_step);

		sub_step = actors_.size() - 1;

		// update the last actor of sub query
		int last_of_branch = sub_step;
		sub_step++;
		while (actors_[last_of_branch].next_actor != sub_step)
		{
			last_of_branch = actors_[last_of_branch].next_actor;
		}
		actors_[last_of_branch].next_actor = current;

	}
	// update next step of branch actor
	actors_[current].next_actor = sub_step;
	if (filterBranch){
		io_type_ = current_type;		// restore type for filtering actor
	}
	first_in_sub_ = m_first_in_sub;
}

void Parser::ParsePredicate(string& param, uint8_t type, Actor_Object& actor, bool toKey)
{
	Predicate_T pred_type;
	value_t pred_param;
	vector<string> pred_params = SplitPredicate(param, pred_type);

	if (toKey){
		map<string, int> *map;
		if (pred_type == Predicate_T::WITHIN || pred_type == Predicate_T::WITHOUT){
			map = &str2se;
		}
		else{
			map = &str2ls;
		}
		// Parse string to key
		for (int i = 0; i < pred_params.size(); i++){
			if (map->count(pred_params[i]) != 1){
				throw ParserException("unexpected key: " + pred_params[i]);
			}
			pred_params[i] = to_string(map->at(pred_params[i]));
		}
	}

	switch (pred_type){
		// scalar predicate
	case Predicate_T::GT:		case Predicate_T::GTE:		case Predicate_T::LT:
	case Predicate_T::LTE:		case Predicate_T::EQ:		case Predicate_T::NEQ:
	case Predicate_T::ANY:
		if (pred_params.size() != 1){
			throw ParserException("expect only one param: " + param);
		}
		Tool::str2value_t(pred_params[0], pred_param);
		break;

		// collection predicate
		// where (inside, outside, between) only accept 2 numbers
	case Predicate_T::INSDIE:	case Predicate_T::OUTSIDE:	case Predicate_T::BETWEEN:
		if (pred_params.size() != 2){
			throw ParserException("expect two params: " + param);
		}
	case Predicate_T::WITHIN:	case Predicate_T::WITHOUT:
		if (!Tool::vec2value_t(pred_params, pred_param, type)){
			throw ParserException("predicate type not match: " + param);
		}
		break;
	}

	actor.AddParam(pred_type);
	actor.params.push_back(pred_param);
}

void Parser::ParseInit(Element_T type)
{
	//@ InitActor params: (Element_T type)
	//  o_type = E/V
	Actor_Object actor(ACTOR_T::INIT);
	actor.AddParam(type);
	AppendActor(actor);
}

void Parser::ParseAggregate(const vector<string>& params, Step_T type)
{
	//@ AggregateActor params: (bool islazy, int side_effect_key)
	//  i_type = o_type = any
	Actor_Object actor(ACTOR_T::AGGREGATE);
	if (params.size() != 1){
		throw ParserException("expect one parameter for aggregate");
	}

	bool islazy;
	switch (type){
	case Step_T::AGGREGATE:	islazy = false; break;
	case Step_T::STORE:		islazy = true; break;
	default:		throw ParserException("unexpected error");
	}
	actor.AddParam(islazy);

	// get side-effect key id by string
	string key = params[0];
	if (str2se.count(key) == 0){
		str2se[key] = str2se.size();
	}
	actor.AddParam(str2se[key]);

	AppendActor(actor);
}

void Parser::ParseAs(const vector<string>& params)
{
	//@ AsActor params: (int label_step_key)
	//  i_type = o_type = any
	Actor_Object actor(ACTOR_T::AS);
	if (params.size() != 1){
		throw ParserException("expect one parameter for as");
	}

	// get label step key id by string
	string key = params[0];
	if (str2ls.count(key) != 0){
		throw ParserException("duplicated key: " + key);
	}
	int ls_id = actors_.size();
	str2ls[key] = ls_id;
	actor.AddParam(ls_id);

	// store output type of label step
	ls2type[ls_id] = io_type_;

	AppendActor(actor);
}

void Parser::ParseBranch(const vector<string>& params, Step_T type)
{
	//@ BranchActor params: (Branch_T branchType, int sub_steps, ...)
	//  i_type = any, o_type = subquery->o_type
	Actor_Object actor(ACTOR_T::BRANCH);
	if (params.size() < 1){
		throw ParserException("expect at least one parameter for branch");
	}

	int branchType;
	switch(type){
	case Step_T::UNION:		branchType = Branch_T::UNION; break;
	case Step_T::COALESCE:	branchType = Branch_T::COALESCE; break;
	case Step_T::CHOOSE:
		branchType = Branch_T::CHOOSE;
		if (params.size() < 2 || params.size() > 3){
			throw ParserException("expect two or three parameters for choose");
		}
		break;
	default:		throw ParserException("unexpected error");
	}
	actor.AddParam(branchType);

	int current = actors_.size();
	AppendActor(actor);

	// Parse sub query
	ParseSub(params, current, false);
}

void Parser::ParseBranchFilter(const vector<string>& params, Step_T type)
{
	//@ BranchFilterActor params: (Filter_T filterType, int sub_steps, ...)
	//  i_type = o_type
	Actor_Object actor(ACTOR_T::BRANCHFILTER);
	if (params.size() < 1){
		throw ParserException("expect at least one parameter for branch filter");
	}

	int filterType;
	switch (type){
	case Step_T::AND:	filterType = Filter_T::AND; break;
	case Step_T::OR:	filterType = Filter_T::OR; break;
	case Step_T::NOT:	filterType = Filter_T::NOT; break;
	default:	throw ParserException("unexpected error");
	}
	actor.AddParam(filterType);

	int current = actors_.size();
	AppendActor(actor);

	// Parse sub query
	ParseSub(params, current, true);
}

void Parser::ParseCap(const vector<string>& params)
{
	//@ CapsActor params: ([int side_effect_key, string side_effect_string]...)
	//  i_type = any, o_type = collection
	Actor_Object actor(ACTOR_T::CAP);
	if (params.size() < 1){
		throw ParserException("expect at least one parameter for cap");
	}

	// get side_effect_key id by string
	for (string key : params)
	{
		if (str2se.count(key) == 0){
			throw ParserException("unexpected key in cap: " + key);
		}
		actor.AddParam(str2se[key]);
		actor.AddParam(key);
	}

	AppendActor(actor);
	io_type_ = COLLECTION;
}

void Parser::ParseCoin(const vector<string>& params)
{
	//@ CoinActor params: (double probability)
	//  i_type = o_type = any
	Actor_Object actor(ACTOR_T::CAP);
	if (params.size() != 1){
		throw ParserException("expect one parameter for coin");
	}

	int type = Tool::checktype(params[0]);
	if (type != 1 && type != 2)
	{
		throw ParserException("expect number for coin");
	}

	string prob = params[0];
	if (prob.find('.') == string::npos){
		prob += ".0";
	}
	actor.AddParam(prob);

	AppendActor(actor);
}

void Parser::ParseCount(const vector<string>& params)
{
	//@ CountActor params: ()
	//  i_type = any, o_type = int
	Actor_Object actor(ACTOR_T::COUNT);
	if (params.size() != 0){
		throw ParserException("expect no parameter for count");
	}

	AppendActor(actor);
	io_type_ = IO_T::INT;
}

void Parser::ParseDedup(const vector<string>& params)
{
	//@ DedupActor params: (int label_step_key...)
	//  i_type = o_type = any
	Actor_Object actor(ACTOR_T::DEDUP);
	for (string key : params)
	{
		// get label step key id by string
		if (str2ls.count(key) == 0){
			throw ParserException("unexpected key in dedup: " + key);
		}
		actor.AddParam(str2ls[key]);
	}

	AppendActor(actor);
}

void Parser::ParseGroup(const vector<string>& params, Step_T type)
{
	//@ GroupActor params: (bool isCount, Element_T type, int keyProjection, int valueProjection) where -1 indicating no projection
	//  i_type = any, o_type = collection
	Actor_Object actor(ACTOR_T::GROUP);
	if (params.size() > 2){
		throw ParserException("expect at most two params in group");
	}

	int isCount = type == GROUPCOUNT;
	actor.AddParam(isCount);

	Element_T element_type;
	if(params.size() > 0){
		if (!IsElement(element_type)){
			throw ParserException("expect vertex/edge input for group by key");
		}
	}
	actor.AddParam(element_type);

	// add projection actor
	for (string param : params)
	{
		int key = 0;
		if (param != "label")
		{
			if (!ParseKeyId(param, false, key))
			{
				throw ParserException("no such property key:" + param);
			}
		}
		actor.AddParam(key);
	}

	// add default
	while (actor.params.size() != 4){
		actor.AddParam(-1);
	}

	AppendActor(actor);
	io_type_ = COLLECTION;
}

void Parser::ParseHas(const vector<string>& params, Step_T type)
{
	//@ HasActor params: (Element_T type, [int pid, Predicate_T  p_type, vector values]...)
	//  i_type = o_type = VERTX/EDGE
	if (params.size() < 1){
		throw ParserException("expect at least one param for has");
	}

	Element_T element_type;
	if (!IsElement(element_type)){
		throw ParserException("expect vertex/edge input for has");
	}

	if (!CheckLastActor(ACTOR_T::HAS)){
		Actor_Object tmp(ACTOR_T::HAS);
		tmp.AddParam(element_type);
		AppendActor(tmp);
	}
	Actor_Object &actor = actors_[actors_.size() - 1];

	string pred_param = "";
	int key = 0;
	uint8_t vtype = 0;
	switch (type){
	case HAS:
		if (params.size() > 2){
			throw ParserException("expect at most two params for has");
		}

		if (!ParseKeyId(params[0], false, key, &vtype))
		{
			throw ParserException("Unexpected key: " + params[0]);
		}
		if (params.size() == 2){
			pred_param = params[1];
		}
		actor.AddParam(key);
		ParsePredicate(pred_param, vtype, actor, false);
		break;
	case HASVALUE:
		for (string param : params){
			actor.AddParam(-1);
			actor.AddParam(Predicate_T::EQ);
			if (!actor.AddParam(param)){
				throw ParserException("unexpected value: " + param);
			}
		}
		break;
	case HASNOT:
		for (string param : params){
			if (!ParseKeyId(param, false, key)){
				throw ParserException("unexpected key in hasNot : " + param);
			}
			actor.AddParam(key);
			actor.AddParam(Predicate_T::NONE);
			actor.AddParam(-1);
		}
		break;
	case HASKEY:
		for (string param : params){
			if (!ParseKeyId(param, false, key)){
				throw ParserException("unexpected key in hasKey : " + param);
			}
			actor.AddParam(key);
			actor.AddParam(Predicate_T::ANY);
			actor.AddParam(-1);
		}
		break;
	default: throw ParserException("unexpected error");
	}
}

void Parser::ParseHasLabel(const vector<string>& params)
{
	//@ HasLabelActor params: (Element_T type, int lid...)
	//  i_type = o_type = VERTX/EDGE
	if (params.size() < 1){
		throw ParserException("expect at least one param for hasLabel");
	}

	Element_T element_type;
	if (!IsElement(element_type)){
		throw ParserException("expect vertex/edge input for hasLabel");
	}

	if (!CheckLastActor(ACTOR_T::HASLABEL)){
		Actor_Object tmp(ACTOR_T::HASLABEL);
		tmp.AddParam(element_type);
		AppendActor(tmp);
	}
	Actor_Object &actor = actors_[actors_.size() - 1];

	int lid;
	for(auto& param : params){
		if (!ParseKeyId(param, true, lid)){
			throw ParserException("unexpected label in hasLabel : " + param);
		}
		actor.AddParam(lid);
	}
}

void Parser::ParseIs(const vector<string>& params)
{
	//@ IsActor params: ((Predicate_T  p_type, vector values)...)
	//  i_type = o_type = int/double/char/string
	if (params.size() != 1){
		throw ParserException("expect one param for is");
	}

	uint8_t type;
	if (!IsValue(type)){
		throw ParserException("unexpected input type for is");
	}

	if (!CheckLastActor(ACTOR_T::IS)){
		Actor_Object tmp(ACTOR_T::IS);
		AppendActor(tmp);
	}

	Actor_Object &actor = actors_[actors_.size() - 1];
	string param = params[0];
	ParsePredicate(param, type, actor, false);
}

void Parser::ParseKey(const vector<string>& params)
{
	//@ KeyActor params: (Element_T type)
	//  i_type = VERTX/EDGE, o_type = string
	Actor_Object actor(ACTOR_T::KEY);
	if (params.size() != 0){
		throw ParserException("expect no parameter for key");
	}

	Element_T element_type;
	if (!IsElement(element_type)){
		throw ParserException("expect vertex/edge input for hasLabel");
	}
	actor.AddParam(element_type);

	AppendActor(actor);
	io_type_ = IO_T::STRING;
}

void Parser::ParseLabel(const vector<string>& params)
{
	//@ LabelActor params: (Element_T type)
	//  i_type = VERTX/EDGE, o_type = string
	Actor_Object actor(ACTOR_T::LABEL);
	if (params.size() != 0){
		throw ParserException("expect no parameter for label");
	}

	Element_T element_type;
	if (!IsElement(element_type)){
		throw ParserException("expect vertex/edge input for hasLabel");
	}
	actor.AddParam(element_type);

	AppendActor(actor);
	io_type_ = IO_T::STRING;
}

void Parser::ParseLoops(const vector<string>& params)
{
	//@ LoopsActor params: ()
	//  i_type = any, o_type = int
	Actor_Object actor(ACTOR_T::LOOPS);
	if (params.size() != 0){
		throw ParserException("expect no parameter for loops");
	}

	if (!is_in_repeat_){
		throw ParserException("expect loops in repeat or repeat modulators");
	}

	AppendActor(actor);
	io_type_ = IO_T::INT;
}

void Parser::ParseMath(const vector<string>& params, Step_T type)
{
	//@ LabelActor params: (Math_T mathType)
	//  i_type = NUMBER, o_type = DOUBLE
	Actor_Object actor(ACTOR_T::MATH);
	if (params.size() != 0){
		throw ParserException("expect no parameter for math");
	}

	if (!IsNumber()){
		throw ParserException("expect number input for math related step");
	}

	int mathType;
	switch (type)
	{
	case Step_T::MAX:	mathType = Math_T::MAX; break;
	case Step_T::MEAN:	mathType = Math_T::MEAN; break;
	case Step_T::MIN:	mathType = Math_T::MIN; break;
	case Step_T::SUM:	mathType = Math_T::SUM; break;
	default: throw ParserException("unexpected error");
	}
	actor.AddParam(mathType);

	AppendActor(actor);
	io_type_ = IO_T::DOUBLE;
}

void Parser::ParseOrder(const vector<string>& params)
{
	//@ OrderActor params: (Element_T element_type, int projectionKey, Order_T order) where -1 indicating no projection
	//  i_type = o_type = any

	Actor_Object actor(ACTOR_T::ORDER);
	if (params.size() > 2){
		throw ParserException("expect at most two params in order");
	}

	Element_T element_type;
	int key = -1;
	Order_T order = Order_T::INCR;

	for (string param : params)
	{
		if(param == "incr" || param == "decr"){
			// input param is order type
			order = param == "incr" ? Order_T::INCR : Order_T::DECR;
		}else{
			// input param is projection key
			if (!IsElement(element_type)){
				throw ParserException("expect vertex/edge input for order by key");
			}
			if (param != "label")
			{
				if (!ParseKeyId(param, false, key))
				{
					throw ParserException("no such property key:" + param);
				}
			}else{
				key = 0;
			}
		}
	}

	actor.AddParam(element_type);
	actor.AddParam(key);
	actor.AddParam(order);
	AppendActor(actor);
}

void Parser::ParseProperties(const vector<string>& params)
{
	//@ PropertiesActor params: (Element_T type, int pid...)
	//  i_type = VERTX/EDGE, o_type = COLLECTION
	Actor_Object actor(ACTOR_T::PROPERTY);

	Element_T element_type;
	if (!IsElement(element_type)){
		throw ParserException("expect vertex/edge input for hasLabel");
	}
	actor.AddParam(element_type);

	int key;
	for (string param : params){
		if (!ParseKeyId(param, false, key)){
			throw ParserException("unexpected key: " + param);
		}
		actor.AddParam(key);
	}

	AppendActor(actor);
	io_type_ = IO_T::COLLECTION;
}

void Parser::ParseRange(const vector<string>& params, Step_T type)
{
	//@ RangeActor params: (int start, int end)
	//  i_type = o_type = any
	Actor_Object actor(ACTOR_T::RANGE);

	vector<int> vec;
	for (string param : params){
		if (Tool::checktype(param) != 1){
			throw ParserException("expect number but get: " + param);
		}
		vec.push_back(atoi(param.c_str()));
	}

	int start = 0;
	int end = -1;
	switch (type)
	{
	case Step_T::RANGE:
		if (params.size() != 2){
			throw ParserException("expect two parameters for range");
		}
		start = vec[0];
		end = vec[1];
		break;
	case Step_T::LIMIT:
		if (params.size() != 1){
			throw ParserException("expect one parameter for limit");
		}
		end = vec[0] - 1;
		break;
	case Step_T::SKIP:
		if (params.size() != 1){
			throw ParserException("expect one parameter for skip");
		}
		start = vec[0];
		break;
	case Step_T::TAIL:
		throw ParserException("tail is not supported");
		if (params.size() != 1){
			throw ParserException("expect one parameter for skip");
		}
		start = -vec[0];
		break;
	default: throw ParserException("unexpected error");
	}
	actor.AddParam(start);
	actor.AddParam(end);

	AppendActor(actor);
}

void Parser::ParseRepeat(const vector<string>& params)
{
	//@ RepeatActor params: (int times, int emit_step, int until_step, int repeat_step)
	//  i_type = o_type = any
	Actor_Object actor(ACTOR_T::REPEAT);
	if (params.size() != 1){
		throw ParserException("expect only one parameter for repeat");
	}

	int current = actors_.size();
	actor.AddParam(0);					//times
	actor.AddParam(-1);				//defualt no emit step
	actor.AddParam(-1);				//default no until step
	AppendActor(actor);

	IO_T current_type = io_type_;
	bool m_is_in_repeat_ = is_in_repeat_; //save status
	is_in_repeat_ = true;
	ParseSub(params, current, false);	//repeat step and next step
	is_in_repeat_ = m_is_in_repeat_;		//restore status

	if (io_type_ != current_type){
		throw ParserException("expect same in/out type in repeat");
	}
}

void Parser::ParseRepeatModulator(const vector<string>& params, Step_T type)
{
	//@ RepeatActor params: (int times, int emit_step, int until_step, int repeat_step)
	//  i_type = o_type = any
	if (params.size() != 1){
		throw ParserException("expect only one param for modulators");
	}

	// find repeat actor
	int repeat = 0;
	if (!CheckLastActor(ACTOR_T::REPEAT, &repeat)){
		throw ParserException("expect repeat before modulators");
	}

	int pos = 0;
	bool m_is_in_repeat_ = is_in_repeat_;
	is_in_repeat_ = true;
	switch (type)
	{
	case Step_T::EMIT:
		ParseSub(params, repeat, true);
		pos = 1;
		break;
	case Step_T::UNTIL:
		ParseSub(params, repeat, true);
		pos = 2;
		break;
	case Step_T::TIMES:
		if (Tool::checktype(params[0]) != 1){
			throw ParserException("expect number for times");
		}
		actors_[repeat].AddParam(params[0]);
		pos = 0;
		break;
	default: throw ParserException("unexpected step");
	}
	is_in_repeat_ = m_is_in_repeat_;

	Actor_Object &actor = actors_[repeat];
	swap(actor.params[pos], actor.params[4]);	// update param in specified pos
	actor.params.pop_back();
}

void Parser::ParseSelect(const vector<string>& params)
{
	//@ SelectActor params: ([int label_step_key, string label_step_string]..)
	//  i_type = any, o_type = COLLECTION / according step
	Actor_Object actor(ACTOR_T::SELECT);

	if (params.size() < 1){
		throw ParserException("expect at least one params for select");
	}

	int key;
	IO_T type;
	for (string param : params){
		if (str2ls.count(param) == 0){
			throw ParserException("unexpected label step: " + param);
		}
		key = str2ls[param];
		type = ls2type[key];
		actor.AddParam(key);
		actor.AddParam(param);
	}

	AppendActor(actor);
	if (params.size() == 1){
		io_type_ = type;
	}
	else{
		io_type_ = IO_T::COLLECTION;
	}
}

void Parser::ParseTraversal(const vector<string>& params, Step_T type)
{
	//@ TraversalActor params: (Element_T inType, Element_T outType, Direction_T direction, int label_id)
	//  i_type = E/V, o_type = E/V
	Actor_Object actor(ACTOR_T::TRAVERSAL);
	int traversal_type = type;
	Element_T inType;
	Element_T outType;
	Direction_T dir;

	if (traversal_type <= 2){
		// in/out/both
		if (params.size() > 1){
			throw ParserException("expect at most one param for in/out/both");
		}
		if (io_type_ != IO_T::VERTEX){
			throw ParserException("expect vertex input for in/out/both");
		}
		inType = Element_T::VERTEX;
		outType = Element_T::VERTEX;
	}
	else if (traversal_type > 2 && traversal_type <= 5){
		// in/out/bothE
		if (params.size() > 1){
			throw ParserException("expect at most one param for in/out/bothE");
		}
		if (io_type_ != IO_T::VERTEX){
			throw ParserException("expect vertex input for in/out/bothE");
		}
		inType = Element_T::VERTEX;
		outType = Element_T::EDGE;
	}
	else if (traversal_type > 5){
		// in/out/bothV
		if (params.size() != 0){
			throw ParserException("expect no param for in/out/bothV");
		}
		if (io_type_ != IO_T::EDGE){
			throw ParserException("expect vertex input for in/out/bothV");
		}
		inType = Element_T::EDGE;
		outType = Element_T::VERTEX;
	}

	if (traversal_type % 3 == 0){
		dir = Direction_T::IN;
	}
	else if (traversal_type % 3 == 1){
		dir = Direction_T::OUT;
	}
	else{
		dir = Direction_T::BOTH;
	}

	int lid = -1;
	// get label id
	if (params.size() == 1){
		io_type_ = IO_T::EDGE;
		if (! ParseKeyId(params[0], true, lid)){
			throw ParserException("unexpected label: " + params[0]);
		}
	}

	actor.AddParam(inType);
	actor.AddParam(outType);
	actor.AddParam(dir);
	actor.AddParam(lid);
	AppendActor(actor);

	io_type_ = (outType == Element_T::EDGE) ? IO_T::EDGE : IO_T::VERTEX;
}

void Parser::ParseValues(const vector<string>& params)
{
	//@ ValuesActor params: (Element_t type, int pid...)
	//  i_type = VERTX/EDGE, o_type = according to pid
	Actor_Object actor(ACTOR_T::VALUES);

	Element_T element_type;
	if (!IsElement(element_type)){
		throw ParserException("expect vertex/edge input for hasLabel");
	}
	actor.AddParam(element_type);

	int key;
	uint8_t vtype;
	uint8_t outType = 4;
	bool first = true;
	for (string param : params){
		if (!ParseKeyId(param, false, key, &vtype)){
			throw ParserException("unexpected key: " + param);
		}
		if (first){
			outType = vtype;
			first = false;
		}
		else if(outType != vtype){
			throw ParserException("expect same type of key in values");
		}
		actor.AddParam(key);
	}

	AppendActor(actor);
	io_type_ = Value2IO(outType);
}

void Parser::ParseWhere(const vector<string>& params)
{
	//@ WhereActor params: ((int label_step_key, predicate Type, vector label/side-effect_id)...)
	//  first label_step_key == -1 indicating
	//  i_type = o_type = any
	if (params.size() > 2 || params.size() == 0){
		throw ParserException("expect one or two params for where");
	}

	bool is_query = false;

	// check param type -> subquery/predicate
	if (params.size() == 1){
		int pos = params[0].find("(");
		string step = params[0].substr(0, pos);
		// matching step name
		if (str2step.count(step) == 1){
			is_query = true;
		}
	}

	if (is_query){
		// parse where step as branch filter actor
		try{
			ParseBranchFilter(params, Step_T::AND);
		}
		catch (ParserException ex){
			throw ParserException("error when parsing where: " + ex.message);
		}
	}
	else{
		string param = params[0];
		int label_step_key = -1;
		if (params.size() == 2){
			if (str2ls.count(param) != 1){
				throw ParserException("Unexpected label step: " + param);
			}
			label_step_key = str2ls[param];
			param = params[1];
		}

		if (!CheckLastActor(ACTOR_T::WHERE)){
			Actor_Object tmp(ACTOR_T::WHERE);
			AppendActor(tmp);
		}
		Actor_Object &actor = actors_[actors_.size() - 1];
		actor.AddParam(label_step_key);
		ParsePredicate(param, 1, actor, true);
	}
}


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
