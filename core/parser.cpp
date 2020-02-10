/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Nick Fang (jcfang6@cse.cuhk.edu.hk)
         Modified by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/
#include <iostream>

#include "core/parser.hpp"

void Parser::LoadMapping(DataStorage* data_storage) {
    indexes = data_storage->indexes_;

    // these *_str will be used when given error key in a query (return to the client as error message)
    for (auto vpk_pair : indexes->str2vpk) {
        vpks.push_back(vpk_pair.first);
        vpks_str += vpk_pair.first + " ";
    }
    for (auto epk_pair : indexes->str2epk) {
        epks.push_back(epk_pair.first);
        epks_str += epk_pair.first + " ";
    }

    for (auto vlk_pair : indexes->str2vl) {
        vlks.push_back(vlk_pair.first);
        vlks_str += vlk_pair.first + " ";
    }
    for (auto elk_pair : indexes->str2el) {
        elks.push_back(elk_pair.first);
        elks_str += elk_pair.first + " ";
    }
}

int Parser::GetPid(Element_T type, string& property) {
    if (property == "label") {
        return 0;
    } else {
        unordered_map<string, label_t>::iterator itr;
        if (type == Element_T::VERTEX) {
            itr = indexes->str2vpk.find(property);
            if (itr == indexes->str2vpk.end()) {
                cout << "wrong property : " << property << endl;
                return -1;
            }
        } else {
            itr = indexes->str2epk.find(property);
            if (itr == indexes->str2epk.end()) {
                cout << "wrong property : " << property << endl;
                return -1;
            }
        }

        if (!index_store->IsIndexEnabled(type, itr->second)) {
            cout << "Property is not enabled: " << property << endl;
            return -1;
        }
        return itr->second;
    }
}

bool Parser::Parse(const string& trx_input, TrxPlan& plan, string& error_msg) {
    /* One ParserObject for one transaction, which guarantees thread safety.
     * The pointer of the Parser ("this") is passed to ParserObject, allowing ParserObject to
     *      access global members for all transactions.
     */
    ParserObject parser_object(this);

    return parser_object.Parse(trx_input, plan, error_msg);
}

bool ParserObject::Parse(const string& trx_input, TrxPlan& plan, string& error_msg) {
    ClearTrx();
    vector<string> lines;
    Tool::split(trx_input, ";\n", lines);
    trx_plan = &plan;
    plan.query_plans_.resize(lines.size() + 1);

    for (string& line : lines) {
        Tool::trim(line, " ");
        plan.deps_count_[line_index] = 0;
        if (!ParseLine(line, plan.query_plans_[line_index].experts, error_msg)) {
            return false;
        }

        if (!is_read_only_) {
            // update query depends on all previous steps, starting from last update query
            uint8_t begin = (last_update > 0) ? last_update : 0;
            for (uint8_t i = begin; i < line_index; i++) {
                plan.RegDependency(i, line_index);
            }
            last_update = line_index;
        } else {
            // read only query depends on last update query
            // other deps will also be inserted in RegPlaceHolder
            if (last_update >= 0) {
                plan.RegDependency(last_update, line_index);
            }
        }
        line_index++;
    }

    // Add validation expert and finish expert (commit or abort)
    AddCommitStatement(plan);

    return true;
}

bool ParserObject::ParseLine(const string& line, vector<Expert_Object>& vec, string& error_msg) {
    ClearQuery();
    bool build_index = false;
    bool set_config = false;
    bool display_status = false;
    string error_prefix = "Parser error at line ";
    // check prefix
    if (line.find("BuildIndex") == 0) {
        build_index = true;
        error_prefix = "Build Index error: ";
    } else if (line.find("SetConfig") == 0) {
        set_config = true;
        error_prefix = "Set Config error: ";
    } else if (line.find("DisplayStatus") == 0) {
        display_status = true;
        error_prefix = "Display Status error";
    }
    /*
    else {
        error_msg = "1. Execute query with 'g.V()' or 'g.E()'\n";
        error_msg += "2. Set up index by BuildIndex(V/E, propertyname)\n";
        error_msg += "3. Change config by SetConfig(config_name, t/f)\n";
        error_msg += "4. Run emulator mode with 'emu <file>'";
        return false;
    }*/

    try {
        if (build_index) {
            ParseIndex(line);
        } else if (set_config) {
            ParseSetConfig(line);
        } else if (display_status) {
            ParseDisplayStatus(line);
        } else {
            string query, return_name;
            ParseInit(line, return_name, query);
            ParseQuery(query);
            if (return_name != "") {
                place_holder[return_name] = make_pair(line_index, io_type_);
            }
        }
    }
    catch (ParserException e) {
        error_msg = error_prefix + to_string(line_index + 1) + ":\n"
                    + line + "\n"+ e.message;
        return false;
    }

    int i = 0;
    for (auto& expert : experts_) {
        if (expert.expert_type == EXPERT_T::ADDE) {
            // Need to check addE expert params
            AddEdgeMethodType fromType = Tool::value_t2int(expert.params[1]);
            AddEdgeMethodType toType = Tool::value_t2int(expert.params[3]);
            int count = 0;
            // No from/to = 0, both with placeholer = 4
            if (fromType != AddEdgeMethodType::NotApplicable) {
                count += (fromType == AddEdgeMethodType::PlaceHolder) ? 2 : 1;
            }
            if (toType != AddEdgeMethodType::NotApplicable) {
                count += (toType == AddEdgeMethodType::PlaceHolder) ? 2 : 1;
            }

            if ((i == 0 && count != 4) ||
                (i != 0 && (count == 0 || count == 4))) {
                // 1: g.addE, need both from and to steps with PlaceHolder type
                // 2: not the first step, need at least one from/to, and do not allow both steps with placeholder
                error_msg = error_prefix + to_string(line_index + 1) + ":\n"
                            + line + "\n"+ "addE params not match";
                return false;
            }
        }
        vec.push_back(move(expert));
        i++;
    }

    vec.emplace_back(EXPERT_T::END);

    return true;
}

void ParserObject::SplitParam(string& param, vector<string>& params) {
    param = Tool::trim(param, " ");
    int len = param.size();
    if (len > 0 && param[len - 1] == ',') {
        throw ParserException("unexpected ',' at: " + param);
    }
    vector<string> tmp;
    Tool::splitWithEscape(param, ",", tmp);
    string p = "";
    int balance = 0;
    for (auto& itr : tmp) {
        // only split ',' which is not encased by '()'
        for (char i : itr) {
            switch (i) {
              case '(': balance++; break;
              case ')': balance--; break;
            }
        }
        p = p + "," + itr;
        if (balance == 0) {
            params.push_back(Tool::trim(p, " ,"));
            p = "";
        }
    }
    return params;
}

void ParserObject::SplitPredicate(string& param, Predicate_T& pred_type, vector<string>& pred_params) {
    param = Tool::trim(param, " ");
    vector<string> pred;
    Tool::splitWithEscape(param, "()", pred);
    int len = pred.size();
    if (len == 0) {
        pred_type = Predicate_T::ANY;
        pred_params.push_back("-1");
    } else if (len == 1) {
        pred_type = Predicate_T::EQ;
        pred_params.push_back(pred[0]);
    } else if (len == 2 && str2pred.count(pred[0]) != 0) {
        pred_type = str2pred.at(pred[0]);
        SplitParam(pred[1], pred_params);
    } else {
        throw ParserException("unexpected predicate: " + param);
    }
    return pred_params;
}

bool ParserObject::IsNumber() {
    return (io_type_ == INT || io_type_ == DOUBLE);
}
bool ParserObject::IsValue(uint8_t& type) {
    switch (io_type_) {
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

bool ParserObject::IsElement() {
    switch (io_type_) {
      case IO_T::VERTEX:
      case IO_T::EDGE:
      case IO_T::VP:
      case IO_T::EP:
        return true;
      default:
        return false;
    }
}

bool ParserObject::IsElement(Element_T& type) {
    switch (io_type_) {
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
ParserObject::IO_T ParserObject::Value2IO(uint8_t type) {
    switch (type) {
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

void ParserObject::RegPlaceHolder(const string& var, int step, int param_index, IO_T type) {
    auto itr = place_holder.find(var);

    if (itr == place_holder.end()) {
        throw ParserException("Unexpected variable '" + var + "'");
    }

    auto& p = itr->second;
    if (p.second != type) {
        throw ParserException("Expect " + string(IOType[type]) + " but get '" + var
                               + "' with type " + string(IOType[p.second]));
    }
    trx_plan->RegPlaceHolder(p.first, line_index, step, param_index);
}

void ParserObject::ParseIndex(const string& param) {
    vector<string> params;
    Tool::splitWithEscape(param, ",() ", params);
    if (params.size() != 3) {
        throw ParserException("expect 2 parameters");
    }

    Expert_Object expert(EXPERT_T::INDEX);

    Element_T type;
    if (params[1] == "V") {
        type = Element_T::VERTEX;
        io_type_ = IO_T::VERTEX;
    } else if (params[1] == "E") {
        type = Element_T::EDGE;
        io_type_ = IO_T::EDGE;
    } else {
        throw ParserException("expect V/E but get: " + params[1]);
    }

    int property_key = 0;
    Tool::trim(params[2], "\"");
    if (params[2] != "label" && !ParseKeyId(params[2], false, property_key)) {
        throw ParserException("unexpected property key: " + params[2] + ", expected is " + ExpectedKey(false));
    }

    expert.AddParam(type);
    expert.AddParam(property_key);
    AppendExpert(expert);
    is_read_only_ = false;
}

void ParserObject::ParseSetConfig(const string& param) {
    vector<string> params;
    Tool::splitWithEscape(param, ",() ", params);
    if (params.size() != 3) {
        throw ParserException("expect 2 parameters");
    }

    Expert_Object expert(EXPERT_T::CONFIG);

    Tool::trim(params[1], "\"");
    Tool::trim(params[2], "\"");

    value_t v;
    Tool::str2str(params[1], v);
    expert.params.push_back(v);

    bool enable;
    if (params[2] == "enable"
        || params[2][0] == 'y'
        || params[2][0] == 't') {
            enable = true;
            expert.AddParam(enable);
    } else if (params[2] == "disable"
        || params[2][0] == 'n'
        || params[2][0] == 'f') {
            enable = false;
            expert.AddParam(enable);
    } else if (Tool::checktype(params[2]) == 1 || Tool::checktype(params[2]) == 4) {
        v.content.clear();
        Tool::str2int(params[2], v);
        expert.params.push_back(v);
    } else {
        throw ParserException("expect 'enable' or 'y' or 't'");
    }

    AppendExpert(expert);
    is_read_only_ = false;
}

void ParserObject::ParseDisplayStatus(const string& param) {
    vector<string> params;
    Tool::splitWithEscape(param, ",() ", params);
    if (params.size() != 2) {
        throw ParserException("expect 1 parameter");
    }

    Expert_Object expert(EXPERT_T::STATUS);

    Tool::trim(params[1], "\"");

    value_t v;
    Tool::str2str(params[1], v);
    expert.params.push_back(v);

    AppendExpert(expert);
    is_read_only_ = false;
}

void ParserObject::ParseQuery(const string& query) {
    vector<pair<Step_T, string>> tokens;
    // extract steps from query
    GetSteps(query, tokens);

    // Optimization
    ReOrderSteps(tokens);

    // Parse steps to experts_
    ParseSteps(tokens);
}

void ParserObject::ClearTrx() {
    expert_index = 0;
    line_index = 0;
    side_effect_key = 0;
    last_update = -1;
    place_holder.clear();
}

void ParserObject::ClearQuery() {
    experts_.clear();
    index_count_.clear();
    str2ls_.clear();
    ls2type_.clear();
    str2se_.clear();
    min_count_ = -1;  // max of uint64_t
    first_in_sub_ = 0;
    is_read_only_ = true;
}

void ParserObject::AppendExpert(Expert_Object& expert) {
    expert.next_expert = experts_.size() + 1;
    expert.index = expert_index++;
    experts_.push_back(move(expert));
}

void ParserObject::RemoveLastExpert() {
    experts_.erase(experts_.end() - 1);
    expert_index--;
}

bool ParserObject::CheckLastExpert(EXPERT_T type) {
    int current = experts_.size();
    int itr = experts_.size() - 1;

    // not expert in sub query
    if (itr < first_in_sub_) {
        return false;
    }

    // find last expert
    while (experts_[itr].next_expert != current) {
        itr = experts_[itr].next_expert;
    }

    return experts_[itr].expert_type == type;
}

bool ParserObject::CheckIfQuery(const string& param) {
    int pos = param.find("(");
    string step = param.substr(0, pos);

    return str2step.count(step) == 1;
}

int ParserObject::GetStepPriority(Step_T type) {
    switch (type) {
      case Step_T::IS:
      case Step_T::WHERE:
        return 0;
      case Step_T::HAS:
      case Step_T::HASNOT:
      case Step_T::HASKEY:
      case Step_T::HASVALUE:
        return 1;
      case Step_T::HASLABEL:
        return 2;
      case Step_T::AND:
      case Step_T::OR:
      case Step_T::NOT:
        return 3;
      case Step_T::DEDUP:
        return 4;
      case Step_T::AS:
        return 5;
      case Step_T::ORDER:
        return 6;
      default:
        return -1;
    }
}

bool ParserObject::ParseKeyId(string key, bool isLabel, int& id, uint8_t *type) {
    unordered_map<string, label_t> *kmap;
    unordered_map<string, uint8_t> *vmap;

    key = Tool::trim(key, "\"\'");

    if (io_type_ == VERTEX) {
        kmap = isLabel ? &(parser_->indexes->str2vl) : &(parser_->indexes->str2vpk);
        vmap = &(parser_->indexes->str2vptype);
    } else if (io_type_ == EDGE) {
        kmap = isLabel ? &(parser_->indexes->str2el) : &(parser_->indexes->str2epk);
        vmap = &(parser_->indexes->str2eptype);
    } else {
        return false;
    }

    if (kmap->count(key) != 1) {
        return false;
    }

    id = kmap->at(key);
    if (!isLabel && type != NULL) {
        *type = vmap->at(to_string(id));
    }
    return true;
}

string ParserObject::ExpectedKey(bool isLabel) {
    string ret;

    if (io_type_ == VERTEX) {
        if (isLabel)
            ret = parser_->vlks_str;
        else
            ret = parser_->vpks_str;
    } else if (io_type_ == EDGE) {
        if (isLabel)
            ret = parser_->elks_str;
        else
            ret = parser_->epks_str;
    } else {
        ret = "ParserObject::ExpectedKey() no io_type";
    }

    return ret;
}

void ParserObject::GetSteps(const string& query, vector<pair<Step_T, string>>& tokens) {
    int lbpos = 0;    // pos of left bracket
    int pos = 0;
    int parentheses = 0;
    int length = query.length();
    if (length == 0) {
        throw ParserException("empty query");
    }

    string step, params;

    while ((lbpos = query.find('(', pos)) != string::npos) {
        // get step name
        step = query.substr(pos, lbpos - pos);
        if (str2step.count(step) != 1) {
            throw ParserException("unexpected step: " + step);
        }
        pos = lbpos;
        parentheses = 1;

        // match brackets
        while (pos < length) {
            pos++;
            if (query[pos] == '(') {
                parentheses++;
            } else if (query[pos] == ')') {
                parentheses--;
                // get params string
                if (parentheses == 0) {
                    params = query.substr(lbpos + 1, pos - lbpos - 1);
                    Tool::trim(params, " ");
                    tokens.push_back(make_pair(str2step.at(step), params));
                    pos++;
                    if (pos != length && query[pos ++] != '.') {
                        throw ParserException("expect '.' after ')'");
                    }
                    break;
                }
            }
        }
    }

    // check parentheses balance
    if (parentheses != 0) {
        throw ParserException("parentheses not balanced");
    }

    // checking ending with ')'
    if (pos != length) {
        throw ParserException("unexpected words at the end: '" + query.substr(pos - 1) + "'");
    }
}

void ParserObject::ReOrderSteps(vector<pair<Step_T, string>>& tokens) {
    if (parser_->config->global_enable_step_reorder) {
        for (int i = 1; i < tokens.size(); i ++) {
            int priority = GetStepPriority(tokens[i].first);

            if (priority != -1) {
                int current = i;
                bool checkAs = false;

                // Should not move where and dedup step before as step
                // when they will access label key
                if (tokens[i].first == Step_T::WHERE) {
                    if (CheckIfQuery(tokens[i].second)) {
                        // Where step => And step
                        priority = GetStepPriority(Step_T::AND);
                    } else {
                        checkAs = true;
                    }
                } else if (tokens[i].first == Step_T::DEDUP) {
                    checkAs = tokens[i].second.size() != 0;
                }

                // Go through previous steps
                for (int j = i - 1; j >= 0; j --) {
                    if (checkAs && tokens[j].first == Step_T::AS) {
                        break;
                    } else if (GetStepPriority(tokens[j].first) > priority) {
                        // move current expert forward
                        swap(tokens[current], tokens[j]);
                        current = j;
                    } else {
                        break;
                    }
                }
            }
        }
    }
}

void ParserObject::ParseSteps(const vector<pair<Step_T, string>>& tokens) {
    for (auto stepToken : tokens) {
        Step_T type = stepToken.first;
        vector<string> params;
        SplitParam(stepToken.second, params);

        switch (type) {
          // AddE Expert
          case Step_T::ADDE:
            ParseAddE(params); break;
          case Step_T::FROM: case Step_T::TO:
            ParseFromTo(params, type); break;
          // AddV Expert:
          case Step_T::ADDV:
            ParseAddV(params); break;
          // AggregateExpert
          case Step_T::AGGREGATE:
            ParseAggregate(params); break;
          // As Expert
          case Step_T::AS:
            ParseAs(params); break;
          // Branch ExpertsW
          case Step_T::UNION:
            ParseBranch(params); break;
          // BranchFilter Experts
          case Step_T::AND:case Step_T::NOT:case Step_T::OR:
            ParseBranchFilter(params, type); break;
          // Cap Expert
          case Step_T::CAP:
            ParseCap(params); break;
          // Count Expert
          case Step_T::COUNT:
            ParseCount(params); break;
          // Dedup Expert
          case Step_T::DEDUP:
            ParseDedup(params); break;
          // Drop Expert
          case Step_T::DROP:
            ParseDrop(params); break;
          // Group Expert
          case Step_T::GROUP:case Step_T::GROUPCOUNT:
            ParseGroup(params, type); break;
          // Has Experts
          case Step_T::HAS:case Step_T::HASKEY:case Step_T::HASVALUE:case Step_T::HASNOT:
            ParseHas(params, type); break;
          // HasLabel Experts
          case Step_T::HASLABEL:
            ParseHasLabel(params); break;
          // Is Expert
          case Step_T::IS:
            ParseIs(params); break;
          // Key Expert
          case Step_T::KEY:
            ParseKey(params); break;
          // Label Expert
          case Step_T::LABEL:
            ParseLabel(params); break;
          // Math Expert
          case Step_T::MAX:case Step_T::MEAN:case Step_T::MIN:case Step_T::SUM:
            ParseMath(params, type); break;
          // Order Expert
          case Step_T::ORDER:
            ParseOrder(params); break;
          // Properties Expert
          case Step_T::PROPERTIES:
            ParseProperties(params); break;
          // Property Expert
          case Step_T::PROPERTY:
            ParseProperty(params); break;
          // Range Expert
          case Step_T::LIMIT:case Step_T::RANGE:case Step_T::SKIP:
            ParseRange(params, type); break;
          // Coin Expert
          case Step_T::COIN:
            ParseCoin(params); break;
          // Repeat Expert
          case Step_T::REPEAT:
            ParseRepeat(params); break;
          // Select Expert
          case Step_T::SELECT:
            ParseSelect(params); break;
          // Traversal Experts
          case Step_T::IN:case Step_T::OUT:case Step_T::BOTH:case Step_T::INE:case Step_T::OUTE:
          case Step_T::BOTHE:case Step_T::INV:case Step_T::OUTV:case Step_T::BOTHV:
            ParseTraversal(params, type); break;
          // Values Expert
          case Step_T::VALUES:
            ParseValues(params); break;
          // Where Expert
          case Step_T::WHERE:
            ParseWhere(params); break;
          default:throw ParserException("Unexpected step");
        }
    }
}

void ParserObject::ParseSub(const vector<string>& params, int current, bool filterBranch) {
    int sub_step = experts_.size();
    IO_T current_type = io_type_;
    IO_T sub_type;
    bool first = true;

    int m_first_in_sub = first_in_sub_;
    for (const string &sub : params) {
        // restore input type before parsing next sub query
        io_type_ = current_type;
        first_in_sub_ = experts_.size();

        // Parse sub-query and add to experts_ list
        ParseQuery(sub);

        // check sub query type
        if (first) {
            sub_type = io_type_;
            first = false;
        } else if (!filterBranch && sub_type != io_type_) {
            throw ParserException("expect same output type in sub queries");
        }

        // update sub_step of branch expert
        experts_[current].AddParam(sub_step);

        sub_step = experts_.size() - 1;

        // update the last expert of sub query
        int last_of_branch = sub_step;
        sub_step++;
        while (experts_[last_of_branch].next_expert != sub_step) {
            last_of_branch = experts_[last_of_branch].next_expert;
        }
        experts_[last_of_branch].next_expert = current;
    }
    // update next step of branch expert
    experts_[current].next_expert = sub_step;
    if (filterBranch) {
        io_type_ = current_type;        // restore type for filtering expert
    }
    first_in_sub_ = m_first_in_sub;
}

void ParserObject::ParsePredicate(string& param, uint8_t type, Expert_Object& expert, bool toKey) {
    Predicate_T pred_type;
    value_t pred_param;
    vector<string> pred_params;
    SplitPredicate(param, pred_type, pred_params);

    if (toKey) {
        map<string, int> *key_map;
        if (pred_type == Predicate_T::WITHIN || pred_type == Predicate_T::WITHOUT) {
            key_map = &str2se_;
        } else {
            key_map = &str2ls_;
        }

        // Parse string to key
        for (int i = 0; i < pred_params.size(); i++) {
            if (key_map->count(pred_params[i]) != 1) {
                // aggregate avail key
                string keys_str;
                for (auto map_kv : *key_map) {
                    keys_str += map_kv.first + " ";
                }
                throw ParserException("unexpected key: " + pred_params[i] + ", avail is " + keys_str);
            }
            pred_params[i] = to_string(key_map->at(pred_params[i]));
        }
    }

    switch (pred_type) {
        // scalar predicate
      case Predicate_T::GT: case Predicate_T::GTE: case Predicate_T::LT:
      case Predicate_T::LTE: case Predicate_T::EQ: case Predicate_T::NEQ:
      case Predicate_T::ANY:
        if (pred_params.size() != 1) {
            throw ParserException("expect only one param: " + param);
        }
        if (!Tool::str2value_t(pred_params[0], pred_param)) {
            throw ParserException("unexpected value: " + param);
        }
        break;

        // collection predicate
        // where (inside, outside, between) only accept 2 numbers
      case Predicate_T::INSIDE: case Predicate_T::OUTSIDE: case Predicate_T::BETWEEN:
        if (pred_params.size() != 2) {
            throw ParserException("expect two params: " + param);
        }
      case Predicate_T::WITHIN: case Predicate_T::WITHOUT:
        if (!Tool::vec2value_t(pred_params, pred_param, type)) {
            throw ParserException("predicate type not match: " + param);
        }
        break;
    }

    expert.AddParam(pred_type);
    expert.params.push_back(pred_param);
}

void ParserObject::ParseInit(const string& line, string& var_name, string& query) {
    // @InitExpert params: (Element_T type, bool with_input, uint64_t [vids/eids] )
    // o_type = E/V
    Expert_Object expert(EXPERT_T::INIT);

    // Check "=" sign , extract return variable name and query string
    std::size_t idx = line.find("=");
    if (idx != string::npos) {
        var_name = line.substr(0, idx);
        Tool::trim(var_name, " ");
        if (var_name == "") {
            throw ParserException("expect variable name at the left of '='");
        }
        query = line.substr(idx + 1);
        Tool::trim(query, " ");
    } else {
        var_name = "";
        query = line;
    }

    Element_T element_type;
    // Check begining of query
    if (query.find("g.V") == 0) {
        io_type_ = IO_T::VERTEX;
        element_type = Element_T::VERTEX;
    } else if (query.find("g.E") == 0) {
        io_type_ = IO_T::EDGE;
        element_type = Element_T::EDGE;
    } else if (query.find("g.addV")) {
        io_type_ = IO_T::VERTEX;
        query = query.substr(2);
        return;
    } else if (query.find("g.addE")) {
        io_type_ = IO_T::EDGE;
        query = query.substr(2);
        return;
    } else {
        throw ParserException("Execute query with g.V or g.E");
    }


    idx = query.find(").");
    bool with_input = false;
    if (idx == string::npos) {
        throw ParserException("Execute query with g.V() or g.E()");
    } else if (idx < 4) {
        throw ParserException("Execute query with g.V() or g.E()");
    } else if (idx > 4) {
        string var = query.substr(4, idx - 4);
        RegPlaceHolder(var, 0, 2, io_type_);
        with_input =  true;
    }

    // skip g.V().
    query = query.substr(idx + 2);
    if (query.length() < 3) {
        throw ParserException("Unexpected query ending with '" + query + "''");
    }

    expert.AddParam(element_type);
    expert.AddParam(with_input);
    AppendExpert(expert);
}

void ParserObject::ParseAddE(const vector<string>& params) {
    //@ AddEExpert params: (int label, AddEdgeMethodType type, value_t labelKey/src_vid, AddEdgeMethodType type, value_t labelKey/dst_vid)
    //  i_type = Vertex, o_type = Edge
    Expert_Object expert(EXPERT_T::ADDE);
    if (params.size() != 1) {
        throw ParserException("expect one parameter for addE");
    }

    if (io_type_ != IO_T::VERTEX) {
        throw ParserException("expect vertex before addE");
    }
    io_type_ = IO_T::EDGE;

    int lid;
    if (!ParseKeyId(params[0], true, lid)) {
        throw ParserException("unexpected label in addE : " + params[0] + ", expected is " + ExpectedKey(true));
    }

    expert.AddParam(lid);
    // Add default value for rest params
    expert.AddParam(AddEdgeMethodType::NotApplicable);
    expert.params.emplace_back();
    expert.AddParam(AddEdgeMethodType::NotApplicable);
    expert.params.emplace_back();

    AppendExpert(expert);
    trx_plan->trx_type_ |= TRX_ADD;
    is_read_only_ = false;
}

void ParserObject::ParseFromTo(const vector<string>& params, Step_T type) {
    // Append params to AddE Expert
    if (!CheckLastExpert(EXPERT_T::ADDE)) {
        throw ParserException("expect 'addE()' before from/to");
    }
    Expert_Object &expert = experts_[experts_.size() - 1];

    int param_index;
    switch (type) {
      case Step_T::FROM: param_index = 1; break;  // 1 is the position of from_method_type
      case Step_T::TO: param_index = 3; break;  // 3 is the position of to_method_type
      default:
        throw ParserException("unexpected error");
    }

    bool isLabelStep = false;
    int labelKey = -1;
    if (str2ls_.count(params[0]) != 0) {
        isLabelStep = true;
        labelKey = str2ls_[params[0]];
    } else if (place_holder.count(params[0]) != 0) {
        RegPlaceHolder(params[0], experts_.size() - 1, param_index + 1, IO_T::VERTEX);
    } else {
        throw ParserException("unexpected varaiable " + params[0]);
    }

    expert.ModifyParam(isLabelStep ? AddEdgeMethodType::StepLabel : AddEdgeMethodType::PlaceHolder, param_index);
    expert.ModifyParam(labelKey, param_index + 1);
}

void ParserObject::ParseAddV(const vector<string>& params) {
    //@ AddVExpert params: (int label)
    //  i_type = any, o_type = Vertex
    Expert_Object expert(EXPERT_T::ADDV);
    if (params.size() != 1) {
        throw ParserException("expect one parameter for addV");
    }

    io_type_ = IO_T::VERTEX;
    int lid;
    if (!ParseKeyId(params[0], true, lid)) {
        throw ParserException("unexpected label in addV : " + params[0] + ", expected is " + ExpectedKey(true));
    }

    expert.AddParam(lid);
    AppendExpert(expert);
    trx_plan->trx_type_ |= TRX_ADD;
    is_read_only_ = false;
}

void ParserObject::ParseAggregate(const vector<string>& params) {
    //@ AggregateExpert params: (int side_effect_key)
    //  i_type = o_type = any
    Expert_Object expert(EXPERT_T::AGGREGATE);
    if (params.size() != 1) {
        throw ParserException("expect one parameter for aggregate");
    }

    // get side-effect key id by string
    string key = params[0];
    if (str2se_.count(key) == 0) {
        str2se_[key] = side_effect_key++;
    }
    expert.AddParam(str2se_[key]);
    expert.send_remote = IsElement();

    AppendExpert(expert);
}

void ParserObject::ParseAs(const vector<string>& params) {
    //@ AsExpert params: (int label_step_key)
    //  i_type = o_type = any
    Expert_Object expert(EXPERT_T::AS);
    if (params.size() != 1) {
        throw ParserException("expect one parameter for as");
    }

    // get label step key id by string
    string key = params[0];
    if (str2ls_.count(key) != 0) {
        throw ParserException("duplicated key: " + key);
    }
    int ls_id = experts_.size();
    str2ls_[key] = ls_id;
    expert.AddParam(ls_id);

    // store output type of label step
    ls2type_[ls_id] = io_type_;

    AppendExpert(expert);
}

void ParserObject::ParseBranch(const vector<string>& params) {
    // @BranchExpert params: (int sub_steps, ...)
    //  i_type = any, o_type = subquery->o_type
    Expert_Object expert(EXPERT_T::BRANCH);
    if (params.size() < 1) {
        throw ParserException("expect at least one parameter for branch");
    }

    int current = experts_.size();
    AppendExpert(expert);

    // Parse sub query
    ParseSub(params, current, false);
}

void ParserObject::ParseBranchFilter(const vector<string>& params, Step_T type) {
    // @BranchFilterExpert params: (Filter_T filterType, int sub_steps, ...)
    //  i_type = o_type
    Expert_Object expert(EXPERT_T::BRANCHFILTER);
    if (params.size() < 1) {
        throw ParserException("expect at least one parameter for branch filter");
    }

    int filterType;
    switch (type) {
      case Step_T::AND:    filterType = Filter_T::AND; break;
      case Step_T::OR:    filterType = Filter_T::OR; break;
      case Step_T::NOT:    filterType = Filter_T::NOT; break;
      default:    throw ParserException("unexpected error");
    }
    expert.AddParam(filterType);

    int current = experts_.size();
    AppendExpert(expert);

    // Parse sub query
    ParseSub(params, current, true);
}

void ParserObject::ParseCap(const vector<string>& params) {
    //@ CapsExpert params: ([int side_effect_key, string side_effect_string]...)
    //  i_type = any, o_type = collection
    Expert_Object expert(EXPERT_T::CAP);
    if (params.size() < 1) {
        throw ParserException("expect at least one parameter for cap");
    }

    // get side_effect_key id by string
    for (string key : params) {
        if (str2se_.count(key) == 0) {
            throw ParserException("unexpected key in cap: " + key);
        }
        expert.AddParam(str2se_[key]);
        expert.AddParam(key);
    }

    AppendExpert(expert);
    io_type_ = COLLECTION;
}

void ParserObject::ParseCount(const vector<string>& params) {
    //@ CountExpert params: ()
    //  i_type = any, o_type = int
    Expert_Object expert(EXPERT_T::COUNT);
    if (params.size() != 0) {
        throw ParserException("expect no parameter for count");
    }

    AppendExpert(expert);
    io_type_ = IO_T::INT;
}

void ParserObject::ParseDedup(const vector<string>& params) {
    //@ DedupExpert params: (int label_step_key...)
    //  i_type = o_type = any
    Expert_Object expert(EXPERT_T::DEDUP);
    for (string key : params) {
        // get label step key id by string
        if (str2ls_.count(key) == 0) {
            throw ParserException("unexpected key in dedup: " + key);
        }
        expert.AddParam(str2ls_[key]);
    }

    expert.send_remote = IsElement();
    AppendExpert(expert);
}

void ParserObject::ParseDrop(const vector<string>& params) {
    //@ DropExpert params: (Element_T element_type, bool isProperty)
    if (params.size() != 0) {
        throw ParserException("expect no param in drop");
    }
    Expert_Object expert(EXPERT_T::DROP);
    Element_T element_type;
    bool isProperty = false;
    switch (io_type_) {
      case IO_T::VP:      isProperty = true;
      case IO_T::VERTEX:  element_type = Element_T::VERTEX; break;
      case IO_T::EP:      isProperty = true;
      case IO_T::EDGE:    element_type = Element_T::EDGE; break;
      default:
        throw ParserException("Unexpected input type before drop");
    }
    expert.AddParam(element_type);
    expert.AddParam(isProperty);
    AppendExpert(expert);

    // For Vertex, ConnectedEdge need one more expert to handle
    if (io_type_ == IO_T::VERTEX) {
        Expert_Object next_expert(EXPERT_T::DROP);
        next_expert.AddParam(Element_T::EDGE);
        next_expert.AddParam(false);
        AppendExpert(next_expert);
    }

    trx_plan->trx_type_ |= TRX_DELETE;
    is_read_only_ = false;
}

void ParserObject::ParseGroup(const vector<string>& params, Step_T type) {
    //@ GroupExpert params: (bool isCount, Element_T type, int label_step_key)
    //  i_type = any, o_type = collection
    Expert_Object expert(EXPERT_T::GROUP);
    if (params.size() > 2) {
        throw ParserException("expect at most two params in group");
    }

    int isCount = type == Step_T::GROUPCOUNT;
    expert.AddParam(isCount);

    Element_T element_type;
    int ls_key = -1;
    if (params.size() > 0) {
        if (!IsElement(element_type)) {
            throw ParserException("expect vertex/edge input for group by key");
        }

        int proj_key[2] = {-1, -1};
        for (int i = 0; i < params.size(); i++) {
            if (params[i] != "label") {
                if (!ParseKeyId(params[i], false, proj_key[i])) {
                    throw ParserException("no such property key: " + params[i] + ", expected is " + ExpectedKey(false));
                }
            } else {
                proj_key[i] = 0;
            }
        }
        ls_key = experts_.size();
        ParseProject(element_type, proj_key[0], proj_key[1]);
    }

    expert.AddParam(ls_key);

    AppendExpert(expert);
    io_type_ = COLLECTION;
}

void ParserObject::ParseHas(const vector<string>& params, Step_T type) {
    //@ HasExpert params: (Element_T type, [int pid, Predicate_T  p_type, vector values]...)
    //  i_type = o_type = VERTX/EDGE
    if (params.size() < 1) {
        throw ParserException("expect at least one param for has");
    }

    Element_T element_type;
    if (!IsElement(element_type)) {
        throw ParserException("expect vertex/edge input for has");
    }

    if (!CheckLastExpert(EXPERT_T::HAS)) {
        Expert_Object tmp(EXPERT_T::HAS);
        tmp.AddParam(element_type);
        AppendExpert(tmp);
    }
    Expert_Object &expert = experts_[experts_.size() - 1];

    string pred_param = "";
    int key = 0;
    uint8_t vtype = 0;

    switch (type) {
      case Step_T::HAS:
        /*
            key               = params[0]
            pred_type         = parse(params[1])
            pred_value        = parse(params[1])
        */
        if (params.size() > 2) {
            throw ParserException("expect at most two params for has");
        }

        if (!ParseKeyId(params[0], false, key, &vtype)) {
            throw ParserException("Unexpected key: " + params[0] + ", expected is " + ExpectedKey(false));
        }
        if (params.size() == 2) {
            pred_param = params[1];
        }
        expert.AddParam(key);
        ParsePredicate(pred_param, vtype, expert, false);
        break;
      case Step_T::HASVALUE:
        /*
            key               = -1
            pred_type         = EQ
            pred_value        = parse(param)
        */
        key = -1;
        for (string param : params) {
            expert.AddParam(key);
            expert.AddParam(Predicate_T::EQ);
            if (!expert.AddParam(param)) {
                throw ParserException("unexpected value: " + param);
            }
        }
        break;
      case Step_T::HASNOT:
        /*
            key               = params[0]
            pred_type         = NONE
            pred_value        = -1
        */
        if (params.size() != 1) {
            throw ParserException("expect at most two params for hasNot");
        }

        if (!ParseKeyId(params[0], false, key)) {
            throw ParserException("unexpected key in hasNot : " + params[0] + ", expected is " + ExpectedKey(false));
        }
        expert.AddParam(key);
        expert.AddParam(Predicate_T::NONE);
        expert.AddParam(-1);
        break;
      case Step_T::HASKEY:
        /*
            key               = params[0]
            pred_type         = ANY
            pred_value        = -1
        */
        if (params.size() != 1) {
            throw ParserException("expect at most two params for hasKey");
        }
        if (!ParseKeyId(params[0], false, key)) {
            throw ParserException("unexpected key in hasKey : " + params[0] + ", expected is " + ExpectedKey(false));
        }

        expert.AddParam(key);
        expert.AddParam(Predicate_T::ANY);
        expert.AddParam(-1);
        break;
      default: throw ParserException("unexpected error");
    }

    // When has expert is after init expert
    if (experts_.size() == 2 && key != -1) {
        int size = expert.params.size();
        Predicate_T pred_type = (Predicate_T) Tool::value_t2int(expert.params[size - 2]);
        PredicateValue pred(pred_type, expert.params[size - 1]);

        uint64_t count = 0;
        bool enabled = parser_->index_store->IsIndexEnabled(element_type, key, &pred, &count);

        if (enabled && count / index_ratio < min_count_) {
            Expert_Object &init_expert = experts_[0];
            init_expert.params.insert(init_expert.params.end(),
                                    make_move_iterator(expert.params.end() - 3),
                                    make_move_iterator(expert.params.end()));
            expert.params.resize(expert.params.size() - 3);

            // update min_count
            if (count < min_count_) {
                min_count_ = count;

                // remove all predicate with large count from init expert
                int i = 0;
                for (auto itr = index_count_.begin(); itr != index_count_.end();) {
                    if (*itr / index_ratio >= min_count_) {
                        itr = index_count_.erase(itr);
                        int first = 1 + 3 * i;
                        move(init_expert.params.begin() + first,
                            init_expert.params.begin() + first + 3,
                            back_inserter(expert.params));
                        init_expert.params.erase(init_expert.params.begin() + first,
                                                init_expert.params.begin() + first + 3);
                    } else {
                        itr++;
                        i++;
                    }
                }
            }

            index_count_.push_back(count);
            // no predicate in has expert params
            if (expert.params.size() == 1) {
                RemoveLastExpert();
            }
        }
    }
}

void ParserObject::ParseHasLabel(const vector<string>& params) {
    //@ HasLabelExpert params: (Element_T type, int lid...)
    //  i_type = o_type = VERTX/EDGE
    if (params.size() < 1) {
        throw ParserException("expect at least one param for hasLabel");
    }

    Element_T element_type;
    if (!IsElement(element_type)) {
        throw ParserException("expect vertex/edge input for hasLabel");
    }

    if (!CheckLastExpert(EXPERT_T::HASLABEL)) {
        Expert_Object tmp(EXPERT_T::HASLABEL);
        tmp.AddParam(element_type);
        AppendExpert(tmp);
    }
    Expert_Object &expert = experts_[experts_.size() - 1];

    int lid;
    for (auto& param : params) {
        if (!ParseKeyId(param, true, lid)) {
            throw ParserException("unexpected label in hasLabel : " + param + ", expected is " + ExpectedKey(true));
        }
        expert.AddParam(lid);
    }

    // When hasLabel expert is after init expert
    if (experts_.size() == 2) {
        // if index_enabled
        Predicate_T pred_type = Predicate_T::WITHIN;
        vector<value_t> pred_params = expert.params;
        pred_params.erase(pred_params.begin());
        PredicateValue pred(pred_type, pred_params);

        uint64_t count = 0;
        if (parser_->index_store->IsIndexEnabled(element_type, 0, &pred, &count)) {
            RemoveLastExpert();

            value_t v;
            Tool::vec2value_t(pred_params, v);
            // add params to init expert
            Expert_Object &init_expert = experts_[0];
            init_expert.AddParam(0);
            init_expert.AddParam(pred_type);
            init_expert.params.push_back(v);
        }
    }
}

void ParserObject::ParseIs(const vector<string>& params) {
    //@ IsExpert params: ((Predicate_T  p_type, vector values)...)
    //  i_type = o_type = int/double/char/string
    if (params.size() != 1) {
        throw ParserException("expect one param for is");
    }

    uint8_t type;
    if (!IsValue(type)) {
        throw ParserException("unexpected input type for is");
    }

    if (!CheckLastExpert(EXPERT_T::IS)) {
        Expert_Object tmp(EXPERT_T::IS);
        AppendExpert(tmp);
    }

    Expert_Object &expert = experts_[experts_.size() - 1];
    string param = params[0];
    ParsePredicate(param, type, expert, false);
}

void ParserObject::ParseKey(const vector<string>& params) {
    //@ KeyExpert params: (Element_T type)
    //  i_type = VERTX/EDGE, o_type = string
    Expert_Object expert(EXPERT_T::KEY);
    if (params.size() != 0) {
        throw ParserException("expect no parameter for key");
    }

    Element_T element_type;
    if (!IsElement(element_type)) {
        throw ParserException("expect vertex/edge input for key");
    }
    expert.AddParam(element_type);

    AppendExpert(expert);
    io_type_ = IO_T::STRING;
}

void ParserObject::ParseLabel(const vector<string>& params) {
    //@ LabelExpert params: (Element_T type)
    //  i_type = VERTX/EDGE, o_type = string
    Expert_Object expert(EXPERT_T::LABEL);
    if (params.size() != 0) {
        throw ParserException("expect no parameter for label");
    }

    Element_T element_type;
    if (!IsElement(element_type)) {
        throw ParserException("expect vertex/edge input for label");
    }
    expert.AddParam(element_type);

    AppendExpert(expert);
    io_type_ = IO_T::STRING;
}

void ParserObject::ParseMath(const vector<string>& params, Step_T type) {
    //@ LabelExpert params: (Math_T mathType)
    //  i_type = NUMBER, o_type = DOUBLE
    Expert_Object expert(EXPERT_T::MATH);
    if (params.size() != 0) {
        throw ParserException("expect no parameter for math");
    }

    if (!IsNumber()) {
        throw ParserException("expect number input for math related step");
    }

    int mathType;
    switch (type) {
      case Step_T::MAX:    mathType = Math_T::MAX; break;
      case Step_T::MEAN:    mathType = Math_T::MEAN; break;
      case Step_T::MIN:    mathType = Math_T::MIN; break;
      case Step_T::SUM:    mathType = Math_T::SUM; break;
      default: throw ParserException("unexpected error");
    }
    expert.AddParam(mathType);

    AppendExpert(expert);
    io_type_ = IO_T::DOUBLE;
}

void ParserObject::ParseOrder(const vector<string>& params) {
    //@ OrderExpert params: (Element_T element_type, int label_step_key, Order_T order)
    //  i_type = o_type = any

    Expert_Object expert(EXPERT_T::ORDER);
    if (params.size() > 2) {
        throw ParserException("expect at most two params in order");
    }

    Element_T element_type;
    int ls_key = -1;  // label step key of project expert, -1 indicating no projection
    Order_T order = Order_T::INCR;

    for (string param : params) {
        if (param == "incr" || param == "decr") {
            // input param is order type
            order = param == "incr" ? Order_T::INCR : Order_T::DECR;
        } else {
            // input param is projection key
            if (!IsElement(element_type)) {
                throw ParserException("expect vertex/edge input for order by key");
            }
            int key = 0;
            if (param != "label") {
                if (!ParseKeyId(param, false, key)) {
                    throw ParserException("no such property key:" + param + ", expected is " + ExpectedKey(false));
                }
            }
            ls_key = experts_.size();
            ParseProject(element_type, key, -1);
        }
    }

    expert.AddParam(ls_key);  // Label step key of project expert
    expert.AddParam(order);
    expert.send_remote = IsElement();
    AppendExpert(expert);
}

void ParserObject::ParseProject(Element_T element_type, int key_id, int value_id) {
    //@ ProjectExpert params: (Element_T type, int key_id, int value_id)
    // Project V/E to key value pair according to property id
    Expert_Object expert(EXPERT_T::PROJECT);
    expert.AddParam(element_type);
    expert.AddParam(key_id);
    expert.AddParam(value_id);
    AppendExpert(expert);
}

void ParserObject::ParseProperties(const vector<string>& params) {
    //@ PropertiesExpert params: (Element_T type, int pid...)
    //  i_type = VERTX/EDGE, o_type = COLLECTION
    Expert_Object expert(EXPERT_T::PROPERTIES);

    Element_T element_type;
    if (!IsElement(element_type)) {
        throw ParserException("expect vertex/edge input for properties");
    }
    expert.AddParam(element_type);

    int key;
    for (string param : params) {
        if (!ParseKeyId(param, false, key)) {
            throw ParserException("unexpected key in ParseProperties: " + param +
                                ", expected is " + ExpectedKey(false));
        }
        expert.AddParam(key);
    }

    AppendExpert(expert);
    io_type_ = (element_type == Element_T::VERTEX) ? IO_T::VP : IO_T::EP;
}

void ParserObject::ParseProperty(const vector<string>& params) {
    //@ RangeExpert params: (Element_T element_type, int pid, value_t value)
    //  i_type = o_type = Vertex/Edge
    Expert_Object expert(EXPERT_T::PROPERTY);

    if (params.size() != 2) {
        throw ParserException("expect two params for property");
    }

    Element_T element_type;
    if (!IsElement(element_type)) {
        throw ParserException("expect vertex/edge input for property");
    }
    expert.AddParam(element_type);

    int key;
    uint8_t key_type;
    if (!ParseKeyId(params[0], false, key, &key_type)) {
        throw ParserException("unexpected key in property: " + params[0] + ", expected is " + ExpectedKey(false));
    }
    int value_type = Tool::checktype(params[1]);
    if (value_type != key_type) {
        throw ParserException("property key type no match with value type in property()");
    }

    expert.AddParam(key);
    expert.AddParam(params[1]);
    AppendExpert(expert);
    trx_plan->trx_type_ |= TRX_UPDATE;
    is_read_only_ = false;
}

void ParserObject::ParseRange(const vector<string>& params, Step_T type) {
    //@ RangeExpert params: (int start, int end)
    //  i_type = o_type = any
    Expert_Object expert(EXPERT_T::RANGE);

    vector<int> vec;
    for (string param : params) {
        if (Tool::checktype(param) != 1) {
            throw ParserException("expect number but get: " + param);
        }
        vec.push_back(atoi(param.c_str()));
    }

    int start = 0;
    int end = -1;
    switch (type) {
      case Step_T::RANGE:
        if (params.size() != 2) {
            throw ParserException("expect two parameters for range");
        }
        start = vec[0];
        end = vec[1];
        break;
      case Step_T::LIMIT:
        if (params.size() != 1) {
            throw ParserException("expect one parameter for limit");
        }
        end = vec[0] - 1;
        break;
      case Step_T::SKIP:
        if (params.size() != 1) {
            throw ParserException("expect one parameter for skip");
        }
        start = vec[0];
        break;
      default: throw ParserException("unexpected error");
    }
    expert.AddParam(start);
    expert.AddParam(end);
    expert.send_remote = IsElement();
    AppendExpert(expert);
}


void ParserObject::ParseCoin(const vector<string>& params) {
    //@ CoinExpert params: (double pass_rate)
    //  i_type = o_type = any
    Expert_Object expert(EXPERT_T::COIN);

    vector<int> vec;

    if (params.size() != 1) {
        throw ParserException("one parameter in range of [0, 1] of coin step is needed");
    }

    // check if [0, 1]
    string param = params[0];

    double val = atof(param.c_str());

    if (!(val >= 0.0 && val <= 1.0)) {
        throw ParserException("expected a value in range [0.0, 1.0]");
    }

    // find floating point
    if (param.find(".") == string::npos) {
        // a integer, 0 or 1
        param += ".0";
    }

    expert.AddParam(param);

    expert.send_remote = IsElement();
    AppendExpert(expert);
}

void ParserObject::ParseRepeat(const vector<string>& params) {
    // @ Act just as union
    Expert_Object expert(EXPERT_T::REPEAT);
    // Expert_Object expert(EXPERT_T::BRANCH);
    if (params.size() < 1) {
        throw ParserException("expect at least one parameter for branch");
    }

    int current = experts_.size();
    AppendExpert(expert);

    // Parse sub query
    ParseSub(params, current, false);
}

void ParserObject::ParseSelect(const vector<string>& params) {
    //@ SelectExpert params: ([int label_step_key, string label_step_string]..)
    //  i_type = any, o_type = COLLECTION / according step
    Expert_Object expert(EXPERT_T::SELECT);

    if (params.size() < 1) {
        throw ParserException("expect at least one params for select");
    }

    int key;
    IO_T type;
    for (string param : params) {
        if (str2ls_.count(param) == 0) {
            throw ParserException("unexpected label step: " + param);
        }
        key = str2ls_[param];
        type = ls2type_[key];
        expert.AddParam(key);
        expert.AddParam(param);
    }


    if (params.size() == 1) {
        io_type_ = type;
        expert.send_remote = IsElement();
    } else {
        io_type_ = IO_T::COLLECTION;
    }

    AppendExpert(expert);
}

void ParserObject::ParseTraversal(const vector<string>& params, Step_T type) {
    //@ TraversalExpert params: (Element_T inType, Element_T outType, Direction_T direction, int label_id)
    //  i_type = E/V, o_type = E/V
    Expert_Object expert(EXPERT_T::TRAVERSAL);
    int traversal_type = static_cast<int>(type);
    Element_T inType;
    Element_T outType;
    Direction_T dir;

    if (traversal_type <= 2) {
        // in/out/both
        if (params.size() > 1) {
            throw ParserException("expect at most one param for in/out/both");
        }
        if (io_type_ != IO_T::VERTEX) {
            throw ParserException("expect vertex input for in/out/both");
        }
        inType = Element_T::VERTEX;
        outType = Element_T::VERTEX;
    } else if (traversal_type > 2 && traversal_type <= 5) {
        // in/out/bothE
        if (params.size() > 1) {
            throw ParserException("expect at most one param for in/out/bothE");
        }
        if (io_type_ != IO_T::VERTEX) {
            throw ParserException("expect vertex input for in/out/bothE");
        }
        inType = Element_T::VERTEX;
        outType = Element_T::EDGE;
    } else if (traversal_type > 5) {
        // in/out/bothV
        if (params.size() != 0) {
            throw ParserException("expect no param for in/out/bothV");
        }
        if (io_type_ != IO_T::EDGE) {
            throw ParserException("expect vertex input for in/out/bothV");
        }
        inType = Element_T::EDGE;
        outType = Element_T::VERTEX;
    }

    if (traversal_type % 3 == 0) {
        dir = Direction_T::IN;
    } else if (traversal_type % 3 == 1) {
        dir = Direction_T::OUT;
    } else {
        dir = Direction_T::BOTH;
    }

    int lid = -1;
    // get label id
    if (params.size() == 1) {
        io_type_ = IO_T::EDGE;
        if (!ParseKeyId(params[0], true, lid)) {
            throw ParserException("unexpected label: " + params[0] + ", expected is " + ExpectedKey(true));
        }
    }

    expert.AddParam(inType);
    expert.AddParam(outType);
    expert.AddParam(dir);
    expert.AddParam(lid);
    expert.send_remote = true;
    AppendExpert(expert);

    io_type_ = (outType == Element_T::EDGE) ? IO_T::EDGE : IO_T::VERTEX;
}

void ParserObject::ParseValues(const vector<string>& params) {
    // @ ValuesExpert params: (Element_t type, int pid...)
    //  i_type = VERTX/EDGE, o_type = according to pid
    Expert_Object expert(EXPERT_T::VALUES);

    Element_T element_type;
    if (!IsElement(element_type)) {
        throw ParserException("expect vertex/edge input for values");
    }
    expert.AddParam(element_type);

    int key;
    uint8_t vtype;
    uint8_t outType = 4;
    bool first = true;
    for (string param : params) {
        if (!ParseKeyId(param, false, key, &vtype)) {
            throw ParserException("unexpected key in ParseValues: " + param + ", expected is " + ExpectedKey(false));
        }
        if (first) {
            outType = vtype;
            first = false;
        } else if (outType != vtype) {
            throw ParserException("expect same type of key in values");
        }
        expert.AddParam(key);
    }

    AppendExpert(expert);
    io_type_ = Value2IO(outType);
}

void ParserObject::ParseWhere(const vector<string>& params) {
    //@ WhereExpert params: ((int label_step_key, predicate Type, vector label/side-effect_id)...)
    //  first label_step_key == -1 indicating
    //  i_type = o_type = any
    if (params.size() > 2 || params.size() == 0) {
        throw ParserException("expect one or two params for where");
    }

    bool is_query = false;

    // check param type -> subquery/predicate
    if (params.size() == 1) {
        is_query = CheckIfQuery(params[0]);
    }

    if (is_query) {
        // parse where step as branch filter expert
        try {
            ParseBranchFilter(params, Step_T::AND);
        }
        catch (ParserException ex) {
            throw ParserException("error when parsing where: " + ex.message);
        }
    } else {
        string param = params[0];
        int label_step_key = -1;
        if (params.size() == 2) {
            if (str2ls_.count(param) != 1) {
                throw ParserException("Unexpected label step: " + param);
            }
            label_step_key = str2ls_[param];
            param = params[1];
        }

        if (!CheckLastExpert(EXPERT_T::WHERE)) {
            Expert_Object tmp(EXPERT_T::WHERE);
            AppendExpert(tmp);
        }
        Expert_Object &expert = experts_[experts_.size() - 1];
        expert.AddParam(label_step_key);
        ParsePredicate(param, 1, expert, true);
    }
}

void ParserObject::AddCommitStatement(TrxPlan& plan) {
    // Add Validation Query
    vector<Expert_Object> valid_vec;
    valid_vec.emplace_back(EXPERT_T::VALIDATION);
    valid_vec[0].next_expert = 1;

    // Add post validation query
    valid_vec.emplace_back(EXPERT_T::POSTVALIDATION);
    valid_vec[1].next_expert = 2;

    // Add commit query
    valid_vec.emplace_back(EXPERT_T::TERMINATE);
    valid_vec[2].next_expert = 3;

    plan.query_plans_[line_index].experts = move(valid_vec);
    plan.query_plans_[line_index].is_process = false;
    plan.deps_count_[line_index] = 0;
    uint8_t begin = (last_update > 0) ? last_update : 0;
    for (uint8_t i = begin; i < line_index; i++) {
        plan.RegDependency(i, line_index);
    }
}

const map<string, Step_T> ParserObject::str2step = {
    { "in", Step_T::IN },
    { "out", Step_T::OUT },
    { "both", Step_T::BOTH },
    { "inE", Step_T::INE },
    { "outE", Step_T::OUTE },
    { "bothE", Step_T::BOTHE },
    { "inV", Step_T::INV },
    { "outV", Step_T::OUTV },
    { "bothV", Step_T::BOTHV },
    { "addE", Step_T::ADDE },
    { "addV", Step_T::ADDV },
    { "and", Step_T::AND },
    { "aggregate", Step_T::AGGREGATE },
    { "as", Step_T::AS },
    { "cap", Step_T::CAP },
    { "count", Step_T::COUNT },
    { "dedup", Step_T::DEDUP },
    { "drop", Step_T::DROP },
    { "from", Step_T::FROM},
    { "group", Step_T::GROUP},
    { "groupCount", Step_T::GROUPCOUNT},
    { "has", Step_T::HAS },
    { "hasLabel", Step_T::HASLABEL },
    { "hasKey", Step_T::HASKEY },
    { "hasValue", Step_T::HASVALUE },
    { "hasNot", Step_T::HASNOT },
    { "is", Step_T::IS },
    { "key", Step_T::KEY },
    { "label", Step_T::LABEL },
    { "limit", Step_T::LIMIT },
    { "max", Step_T::MAX },
    { "mean", Step_T::MEAN },
    { "min", Step_T::MIN },
    { "not", Step_T::NOT },
    { "or", Step_T::OR },
    { "order", Step_T::ORDER },
    { "property", Step_T::PROPERTY },
    { "properties", Step_T::PROPERTIES },
    { "range", Step_T::RANGE },
    { "select", Step_T::SELECT },
    { "skip", Step_T::SKIP },
    { "sum", Step_T::SUM },
    { "to", Step_T::TO},
    { "union", Step_T::UNION },
    { "values", Step_T::VALUES },
    { "where", Step_T::WHERE },
    { "coin", Step_T::COIN },
    { "repeat", Step_T::REPEAT }
};

const map<string, Predicate_T> ParserObject::str2pred = {
    { "eq", Predicate_T::EQ },
    { "neq", Predicate_T::NEQ },
    { "lt", Predicate_T::LT },
    { "lte", Predicate_T::LTE },
    { "gt", Predicate_T::GT },
    { "gte", Predicate_T::GTE },
    { "inside", Predicate_T::INSIDE },
    { "outside", Predicate_T::OUTSIDE },
    { "between", Predicate_T::BETWEEN },
    { "within", Predicate_T::WITHIN },
    { "without", Predicate_T::WITHOUT }
};

const char *ParserObject::IOType[] = { "edge", "vertex", "int", "double", "char", "string", "collection" };
