/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/
#include <map>

#include "core/message.hpp"

ibinstream& operator<<(ibinstream& m, const Branch_Info& info) {
    m << info.node_id;
    m << info.thread_id;
    m << info.index;
    m << info.key;
    m << info.msg_id;
    m << info.msg_path;
    return m;
}

obinstream& operator>>(obinstream& m, Branch_Info& info) {
    m >> info.node_id;
    m >> info.thread_id;
    m >> info.index;
    m >> info.key;
    m >> info.msg_id;
    m >> info.msg_path;
    return m;
}

ibinstream& operator<<(ibinstream& m, const Meta& meta) {
    m << meta.qid;
    m << meta.step;
    m << meta.msg_type;
    m << meta.recver_nid;
    m << meta.recver_tid;
    m << meta.parent_nid;
    m << meta.parent_tid;
    m << meta.msg_path;
    m << meta.branch_infos;
    if (meta.msg_type == MSG_T::INIT) {
        m << meta.qplan;
    }
    return m;
}

obinstream& operator>>(obinstream& m, Meta& meta) {
    m >> meta.qid;
    m >> meta.step;
    m >> meta.msg_type;
    m >> meta.recver_nid;
    m >> meta.recver_tid;
    m >> meta.parent_nid;
    m >> meta.parent_tid;
    m >> meta.msg_path;
    m >> meta.branch_infos;
    if (meta.msg_type == MSG_T::INIT) {
        m >> meta.qplan;
    }
    return m;
}

std::string Meta::DebugString() const {
    std::stringstream ss;
    ss << "Meta: {";
    ss << "  qid: " << qid;
    ss << ", step: " << step;
    ss << ", recver node: " << recver_nid << ":" << recver_tid;
    ss << ", msg type: " << MsgType[static_cast<int>(msg_type)];
    ss << ", msg path: " << msg_path;
    ss << ", parent node: " << parent_nid;
    ss << ", paraent thread: " << parent_tid;
    if (msg_type == MSG_T::INIT) {
        ss << ", actors [";
        for (auto &actor : qplan.actors) {
            ss  << ActorType[static_cast<int>(actor.actor_type)];
        }
        ss << "]";
    }
    ss << "}";
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const Message& msg) {
    m << msg.meta;
    m << msg.data;
    m << msg.max_data_size;
    m << msg.data_size;
    return m;
}

obinstream& operator>>(obinstream& m, Message& msg) {
    m >> msg.meta;
    m >> msg.data;
    m >> msg.max_data_size;
    m >> msg.data_size;
    return m;
}

void Message::AssignParamsByLocality(vector<QueryPlan>& qplans) {
    // Currently only init actor need to consider data locality
    // [TODO](Nick) : Add more type if necessary. e.g: AddE
    //                If only init actor, remove for loop
    //                Else i < qplan.actors.size()
    QueryPlan& qplan = qplans[0];
    for (int i = 0; i < 1; i ++) {
        Actor_Object& actor = qplans[0].actors[i];
        vector<value_t>& params = actor.params;
        // start position of V/E data which need to be redistributed
        int data_index = -1;

        switch (actor.actor_type) {
          case ACTOR_T::INIT:
            // If init actor has input from place holder
            if (!Tool::value_t2int(params[1])) {
                continue;
            }
            data_index = 2;
            break;
          default:
            continue;
        }

        SimpleIdMapper * id_mapper = SimpleIdMapper::GetInstance();
        map<int, vector<value_t>> nid2data;
        // Redistribute V/E to corresponding machine
        for (int j = data_index; j < params.size(); j ++) {
            int node = get_node_id(params[j], id_mapper);
            nid2data[node].push_back(move(params[j]));
        }

        // Assign params to corresponding query plan
        for (int j = 0; j < qplans.size(); j++) {
            vector<value_t>& actor_params = qplans[j].actors[i].params;
            // Remove params after data_index
            actor_params.resize(data_index);
            // Insert corresponding data
            actor_params.insert(actor_params.end(),
                                make_move_iterator(nid2data[j].begin()),
                                make_move_iterator(nid2data[j].end()));
        }
    }
}

void Message::CreateInitMsg(uint64_t qid, int parent_node, int nodes_num, int recv_tid,
                            QueryPlan& qplan, vector<Message>& vec) {
    // assign receiver thread id
    Meta m;
    m.qid = qid;
    m.step = 0;
    m.recver_nid = -1;                    // assigned in for loop
    m.recver_tid = recv_tid;
    m.parent_nid = parent_node;
    m.parent_tid = recv_tid;
    m.msg_type = MSG_T::INIT;
    m.msg_path = to_string(nodes_num);

    // Redistribute actor params
    vector<QueryPlan> qplans(nodes_num - 1, qplan);
    qplans.push_back(move(qplan));
    AssignParamsByLocality(qplans);

    for (int i = 0; i < nodes_num; i++) {
        Message msg;
        msg.meta = m;
        msg.meta.recver_nid = i;
        msg.meta.qplan = move(qplans[i]);
        vec.push_back(move(msg));
    }
}

void Message::CreateBroadcastMsg(MSG_T msg_type, int nodes_num, vector<Message>& vec) {
    Meta m = this->meta;
    m.step++;
    m.msg_type = msg_type;
    m.recver_tid = this->meta.parent_tid;
    m.msg_path = to_string(nodes_num);

    for (int i = 0; i < nodes_num; i ++) {
        m.recver_nid = i;
        Message msg(m);
        vec.push_back(move(msg));
    }
}

void Message::CreateAbortMsg(const vector<Actor_Object>& actors) {
    // To EndActor directly
    meta.step = actors.size() - 1;
    meta.msg_type = MSG_T::ABORT;
    meta.recver_nid = meta.parent_nid;
    meta.recver_tid = meta.parent_tid;
}

void Message::CreateNextMsg(const vector<Actor_Object>& actors, vector<pair<history_t, vector<value_t>>>& data,
                        int num_thread, CoreAffinity* core_affinity, vector<Message>& vec) {
    Meta m = this->meta;
    m.step = actors[this->meta.step].next_actor;

    int count = vec.size();
    dispatch_data(m, actors, data, num_thread, core_affinity, vec);

    // set disptching path
    string num = to_string(vec.size() - count);
    if (meta.msg_path != "") {
        num = "\t" + num;
    }


    for (int i = count; i < vec.size(); i++) {
        vec[i].meta.msg_path += num;
    }
}

void Message::CreateBranchedMsg(const vector<Actor_Object>& actors, vector<int>& steps, int num_thread,
                            CoreAffinity * core_affinity, vector<Message>& vec) {
    Meta m = this->meta;

    // append steps num into msg path
    int step_count = steps.size();

    // update branch info
    Branch_Info info;

    // parent msg end path
    string parent_path = "";

    int branch_depth = m.branch_infos.size() - 1;
    if (branch_depth >= 0) {
        // use parent branch's route
        info = m.branch_infos[branch_depth];
        parent_path = info.msg_path;

        // update current branch's end path with steps num
        info.msg_path += "\t" + to_string(step_count);
    } else {
        info.node_id = m.parent_nid;
        info.thread_id = m.parent_tid;
        info.key = -1;
        info.msg_path = to_string(step_count);
        info.msg_id = 0;
    }

    string tail = m.msg_path.substr(parent_path.size());
    // append "\t" to tail when first char is not "\t"
    if (tail != "" && tail.find("\t") != 0) {
        tail = "\t" + tail;
    }

    //    insert step_count into current msg path
    //     e.g.:
    // "3\t2\t4" with parent path "3\t2", steps num 5
    //  info.msg_path => "3\t2\t5"
    //  tail = "4"
    //  msg_path = "3\t2\t5\t4"
    m.msg_path = info.msg_path + tail;



    // copy labeled data to each step
    for (int i = 0; i < steps.size(); i ++) {
        Meta step_meta = m;

        int step = steps[i];
        info.index = i + 1;
        step_meta.branch_infos.push_back(info);
        step_meta.step = step;

        auto temp = data;
        // dispatch data to msg vec
        int count = vec.size();
        dispatch_data(step_meta, actors, temp, num_thread, core_affinity, vec);

        // set msg_path for each branch
        for (int j = count; j < vec.size(); j++) {
            vec[j].meta.msg_path += "\t" + to_string(vec.size() - count);
        }
    }
}

void Message::CreateBranchedMsgWithHisLabel(const vector<Actor_Object>& actors, vector<int>& steps, uint64_t msg_id,
                            int num_thread, CoreAffinity * core_affinity, vector<Message>& vec) {
    Meta m = this->meta;

    // update branch info
    Branch_Info info;
    info.node_id = m.recver_nid;
    info.thread_id = m.recver_tid;
    info.key = m.step;
    info.msg_path = m.msg_path;
    info.msg_id = msg_id;

    // label each data with unique id
    vector<pair<history_t, vector<value_t>>> labeled_data;
    int count = 0;
    for (auto pair : data) {
        if (pair.second.size() == 0) {
            labeled_data.push_back(move(pair));
        }
        for (auto& value : pair.second) {
            value_t v;
            Tool::str2int(to_string(count ++), v);
            history_t his = pair.first;
            his.emplace_back(m.step, move(v));
            vector<value_t> val_vec;
            val_vec.push_back(move(value));
            labeled_data.emplace_back(move(his), move(val_vec));
        }
    }
    // copy labeled data to each step
    for (int i = 0; i < steps.size(); i ++) {
        int step = steps[i];
        Meta step_meta = m;
        info.index = i + 1;
        step_meta.branch_infos.push_back(info);
        step_meta.step = step;

        vector<pair<history_t, vector<value_t>>> temp;
        if (i == steps.size() - 1) {
            temp = move(labeled_data);
        } else {
            temp = labeled_data;
        }

        // dispatch data to msg vec
        int count = vec.size();
        dispatch_data(step_meta, actors, temp, num_thread, core_affinity, vec);

        // set msg_path for each branch
        for (int j = count; j < vec.size(); j++) {
            vec[j].meta.msg_path += "\t" + to_string(vec.size() - count);
        }
    }
}

void Message::CreateFeedMsg(int key, int nodes_num, vector<value_t>& data, vector<Message>& vec) {
    Meta m;
    m.qid = this->meta.qid;
    m.recver_tid = this->meta.parent_tid;
    m.step = key;
    m.msg_type = MSG_T::FEED;

    auto temp_data = make_pair(history_t(), data);


    for (int i = 0; i < nodes_num; i++) {
        // skip parent node
        if (i == meta.parent_nid) {
            continue;
        }
        Message msg(m);
        msg.meta.recver_nid = i;
        msg.data.push_back(temp_data);
        vec.push_back(move(msg));
    }
}

void Message::dispatch_data(Meta& m, const vector<Actor_Object>& actors, vector<pair<history_t, vector<value_t>>>& data,
                        int num_thread, CoreAffinity * core_affinity, vector<Message>& vec) {
    Meta cm = m;
    bool route_assigned = update_route(m, actors);
    bool empty_to_barrier = update_collection_route(cm, actors);
    // <node id, data>
    map<int, vector<pair<history_t, vector<value_t>>>> id2data;
    // store history with empty data
    vector<pair<history_t, vector<value_t>>> empty_his;

    bool is_count = actors[m.step].actor_type == ACTOR_T::COUNT;

    // enable route mapping
    if (!route_assigned && actors[this->meta.step].send_remote) {
        SimpleIdMapper * id_mapper = SimpleIdMapper::GetInstance();
        for (auto& p : data) {
            map<int, vector<value_t>> id2value_t;
            if (p.second.size() == 0) {
                if (empty_to_barrier)
                    empty_his.push_back(move(p));
                continue;
            }

            // get node id
            for (auto& v : p.second) {
                int node = get_node_id(v, id_mapper);
                id2value_t[node].push_back(move(v));
            }

            // insert his/value pair to corresponding node
            for (auto& item : id2value_t) {
                id2data[item.first].emplace_back(p.first, move(item.second));
            }
        }

        // no data is added to next actor
        if (id2data.size() == 0 && empty_his.size() == 0) {
            empty_his.emplace_back(history_t(), vector<value_t>());
        }
    } else {
        for (auto& p : data) {
            if (p.second.size() == 0) {
                if (empty_to_barrier)
                    empty_his.push_back(move(p));
                continue;
            }
            if (is_count) {
                value_t v;
                Tool::str2int(to_string(p.second.size()), v);
                id2data[m.recver_nid].emplace_back(move(p.first), vector<value_t>{move(v)});
            } else {
                id2data[m.recver_nid].push_back(move(p));
            }
        }

        // no data is added to next actor
        if (id2data.find(m.recver_nid) == id2data.end() && empty_his.size() == 0) {
            empty_his.emplace_back(history_t(), vector<value_t>());
        }
    }

    for (auto& item : id2data) {
        // insert data to msg
        do {
            Message msg(m);
            msg.max_data_size = this->max_data_size;
            msg.meta.recver_nid = item.first;
            // if (! route_assigned) {
            msg.meta.recver_tid = core_affinity->GetThreadIdForActor(actors[m.step].actor_type);
            // }
            msg.InsertData(item.second);
            vec.push_back(move(msg));
        } while ((item.second.size() != 0));    // Data no consumed
    }

    // send history with empty data
    if (empty_his.size() != 0) {
        // insert history to msg
        do {
            Message msg(cm);
            msg.max_data_size = this->max_data_size;
            msg.meta.recver_tid = core_affinity->GetThreadIdForActor(actors[cm.step].actor_type);
            msg.InsertData(empty_his);
            vec.push_back(move(msg));
        } while ((empty_his.size() != 0));    // Data no consumed
    }
}

bool Message::update_route(Meta& m, const vector<Actor_Object>& actors) {
    int branch_depth = m.branch_infos.size() - 1;
    // update recver route & msg_type
    if (actors[m.step].IsBarrier()) {
        if (branch_depth >= 0) {
            // barrier actor in branch
            m.recver_nid = m.branch_infos[branch_depth].node_id;
            // m.recver_tid = m.branch_infos[branch_depth].thread_id;
        } else {
            // barrier actor in main query
            m.recver_nid = m.parent_nid;
            // m.recver_tid = m.parent_tid;
        }
        m.msg_type = MSG_T::BARRIER;
        return true;
    } else if (m.step <= this->meta.step) {
        // to branch parent
        assert(branch_depth >= 0);

        if (actors[m.step].actor_type == ACTOR_T::BRANCH || actors[m.step].actor_type == ACTOR_T::REPEAT) {
            // don't need to send msg back to parent branch step
            // go to next actor of parent
            m.step = actors[m.step].next_actor;
            m.branch_infos.pop_back();

            return update_route(m, actors);
        } else {
            // aggregate labelled branch actors to parent machine
            m.recver_nid = m.branch_infos[branch_depth].node_id;
            m.recver_tid = m.branch_infos[branch_depth].thread_id;
            m.msg_type = MSG_T::BRANCH;
            return true;
        }
    } else {
        // normal actor, recver = sender
        m.msg_type = MSG_T::SPAWN;
        return false;
    }
}

bool Message::update_collection_route(Meta& m, const vector<Actor_Object>& actors) {
    bool to_barrier = false;
    // empty data should be send to:
    // 1. barrier actor, msg_type = BARRIER
    // 2. branch actor: broadcast empty data to barriers inside each branches for msg collection, msg_type = SPAWN
    // 3. labelled branch parent: which will collect branched msg with label, msg_type = BRANCH
    while (m.step < actors.size()) {
        if (actors[m.step].IsBarrier()) {
            // to barrier
            to_barrier = true;
            break;
        } else if (actors[m.step].actor_type == ACTOR_T::BRANCH || actors[m.step].actor_type == ACTOR_T::REPEAT) {
            if (m.step <= this->meta.step) {
                // to branch parent, pop back one branch info
                // as barrier actor is not founded in sub branch, continue to search
                m.branch_infos.pop_back();
            } else {
                // to branch actor for the first time, should broadcast empty data to each branches
                break;
            }
        } else if (m.step <= this->meta.step) {
            // to labelled branch parent
            break;
        }

        m.step = actors[m.step].next_actor;
    }

    update_route(m, actors);

    return to_barrier;
}

int Message::get_node_id(const value_t & v, SimpleIdMapper * id_mapper) {
    int type = v.type;
    if (type == 1) {
        vid_t v_id(Tool::value_t2int(v));
        return id_mapper->GetMachineIdForVertex(v_id);
    } else if (type == 5) {
        eid_t e_id;
        uint2eid_t(Tool::value_t2uint64_t(v), e_id);
        return id_mapper->GetMachineIdForEdge(e_id);
    } else {
        cout << "Wrong Type when getting node id" << type << endl;
        return -1;
    }
}

bool Message::InsertData(pair<history_t, vector<value_t>>& pair) {
    size_t space = max_data_size - data_size;
    size_t his_size = MemSize(pair.first) + sizeof(size_t);

    if (pair.second.size() == 0) {
        data.push_back(pair);
        data_size += his_size;
        return true;
    }

    // check if able to add history
    // no space for history
    if (his_size >= space) {
        return false;
    }

    size_t in_size = his_size;
    auto itr = pair.second.begin();
    for (; itr != pair.second.end(); itr ++) {
        size_t s = MemSize(*itr);
        if (s + in_size <= space) {
            in_size += s;
        } else {
            break;
        }
    }

    // move data
    if (in_size != his_size) {
        vector<value_t> temp;
        std::move(pair.second.begin(), itr, std::back_inserter(temp));
        pair.second.erase(pair.second.begin(), itr);
        data.emplace_back(pair.first, move(temp));
        data_size += in_size;
    }

    return pair.second.size() == 0;
}

void Message::InsertData(vector<pair<history_t, vector<value_t>>>& vec) {
    auto itr = vec.begin();
    for (; itr != vec.end(); itr++) {
        if (!InsertData(*itr)) {
            break;
        }
    }
    vec.erase(vec.begin(), itr);
}

std::string Message::DebugString() const {
    std::stringstream ss;
    ss << meta.DebugString();
    if (data.size()) {
      ss << " Body:";
      for (const auto& d : data)
          ss << " data_size=" << d.second.size();
    }
    return ss.str();
}

bool operator == (const history_t& l, const history_t& r) {
    if (l.size() != r.size()) {
        return false;
    }
    // history keys are in ascending order
    // so simply match kv pair one by one
    for (int i = 0; i < l.size(); i++) {
        if (l[i] != r[i]) {
            return false;
        }
    }
    return true;
}

size_t MemSize(const int& i) {
    return sizeof(int);
}

size_t MemSize(const char& c) {
    return sizeof(char);
}

size_t MemSize(const value_t& data) {
    size_t s = sizeof(uint8_t);
    s += MemSize(data.content);
    return s;
}
