template<class T>
void BarrierActorBase<T>::process(const QueryPlan & qplan, Message & msg) {
    int tid = TidMapper::GetInstance()->GetTid();

    // get msg info
    mkey_t key;
    string end_path;
    GetMsgInfo(msg, key, end_path);

    typename BarrierDataTable::accessor ac;
    data_table_.insert(ac, key);

    bool isReady = IsReady(ac, msg.meta, end_path);

    do_work(tid, qplan, msg, ac, isReady);

    if (isReady) {
        data_table_.erase(ac);
        // don't need to send out msg when next actor is still barrier actor
        if (is_next_barrier(qplan.actors, msg.meta.step)) {
            // move to next actor
            msg.meta.step = qplan.actors[msg.meta.step].next_actor;
            if (qplan.actors[msg.meta.step].actor_type == ACTOR_T::COUNT) {
                for (auto& p : msg.data) {
                    value_t v;
                    Tool::str2int(to_string(p.second.size()), v);
                    p.second.clear();
                    p.second.push_back(move(v));
                }
            }
        }
    }
}
template<class T>
int BarrierActorBase<T>::get_branch_key(Meta & m) {
    // check if barrier actor in branch
    // run locally if true
    int branch_depth = m.branch_infos.size();
    int key = - 1;
    if (branch_depth != 0) {
        key = m.branch_infos[branch_depth - 1].key;
    }
    return key;
}

template<class T>
bool BarrierActorBase<T>::get_history_value(history_t& his, int history_key, value_t& val, bool erase_his = false) {
    if (history_key >= 0) {
        // find key from history
        auto his_itr = std::find_if(his.begin(), his.end(),
            [&history_key](const pair<int, value_t>& element)
                { return element.first == history_key; });

        if (his_itr != his.end()) {
            val = move(his_itr->second);
            // some barrier actors will remove hisotry after branch key
            if (erase_his) {
                his.erase(his_itr + 1, his.end());
            }
            return true;
        }
    }
    return false;
}
template<class T>
int BarrierActorBase<T>::get_branch_value(history_t& his, int branch_key, bool erase_his = true) {
    value_t val;
    if (!get_history_value(his, branch_key, val, erase_his)) {
        return -1;
    }
    return Tool::value_t2int(val);
}
template<class T>
bool BarrierActorBase<T>::IsReady(typename BarrierDataTable::accessor& ac, Meta& m, string end_path) {
    map<string, int>& counter = ac->second.path_counter;
    string msg_path = m.msg_path;
    // check if all msg are collected
    while (msg_path != end_path) {
        int i = msg_path.find_last_of("\t");
        // "\t" should not be the the last char
        assert(i + 1 < msg_path.size());
        // get last number
        int num = atoi(msg_path.substr(i + 1).c_str());

        // check key
        if (counter.count(msg_path) != 1) {
            counter[msg_path] = 0;
        }

        // current branch is ready
        if ((++counter[msg_path]) == num) {
            // reset count to 0
            counter[msg_path] = 0;
            // remove last number
            msg_path = msg_path.substr(0, i == string::npos ? 0 : i);
        } else {
            return false;
        }
    }
    m.msg_path = end_path;
    return true;
}

template<class T>
void BarrierActorBase<T>::GetMsgInfo(Message& msg, mkey_t &key, string &end_path) {
    // init info
    uint64_t msg_id = 0;
    int index = 0;
    end_path = "";

    int branch_depth = msg.meta.branch_infos.size() - 1;
    if (branch_depth >= 0) {
        msg_id = msg.meta.branch_infos[branch_depth].msg_id;
        index = msg.meta.branch_infos[branch_depth].index;
        end_path = msg.meta.branch_infos[branch_depth].msg_path;
    }
    key = mkey_t(msg.meta.qid, msg_id, index);
}
