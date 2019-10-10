/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
         Modified by Chenghuan Huang (chhuang@cse.cuhk.edu.hk), Jian Zhang (jzhang@cse.cuhk.edu.hk)
*/

#ifndef WORKER_HPP_
#define WORKER_HPP_

#include <map>
#include <string>
#include <utility>
#include <vector>
#include <bitset>

#include "utils/zmq.hpp"
#include "base/core_affinity.hpp"
#include "base/node.hpp"
#include "base/type.hpp"
#include "base/thread_safe_queue.hpp"
#include "base/throughput_monitor.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"

#include "core/coordinator.hpp"
#include "core/exec_plan.hpp"
#include "core/message.hpp"
#include "core/id_mapper.hpp"
#include "core/buffer.hpp"
#include "core/RCT.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/tcp_mailbox.hpp"
#include "core/transactions_table.hpp"
#include "core/actors_adapter.hpp"
#include "core/progress_monitor.hpp"
#include "core/parser.hpp"
#include "core/result_collector.hpp"

#include "layout/pmt_rct_table.hpp"
#include "layout/index_store.hpp"
#include "layout/data_storage.hpp"
#include "layout/garbage_collector.hpp"
#include "core/trx_table_stub_zmq.hpp"
#include "core/trx_table_stub_rdma.hpp"


struct Pack {
    qid_t id;
    QueryPlan qplan;
};

struct ValidationQueryPack {
    vector<uint64_t> trx_id_list;
    // Since we need to query RCT from multible workers,
    // a counter is needed to ensure that all trx_id_list from those workers.
    int collected_count = 0;
    Pack pack;
};

struct EmuTrxString {
    int num_rand_values = 0;
    int trx_type = -1;

    string query;
    vector<Element_T> types;
    vector<int> pkeys;

    void DebugPrint() {
        cout << "TrxString: " << endl;
        cout << "\tNumOfRandValues: " << num_rand_values << endl;
        cout << "\tQuery: " << query << endl;
        for (int i = 0; i < pkeys.size(); i++) {
            cout << "\tPkey: " << pkeys.at(i) << " with type " << (types.at(i) == Element_T::VERTEX ? "Vtx" : "Edge") << endl;
        }
    }
};

struct ParseTrxReq {
    string trx_str;
    string client_host;
    int trx_type;
    bool is_emu_mode;

    ParseTrxReq() {}
    ParseTrxReq(string _trx_str, string _client_host, int _trx_type, bool _is_emu_mode) :
        trx_str(_trx_str), client_host(_client_host), trx_type(_trx_type), is_emu_mode(_is_emu_mode) {}
};

class Worker {
 public:
    Worker(Node & my_node, vector<Node> & workers) :
            my_node_(my_node), workers_(workers) {
        config_ = Config::GetInstance();
        is_emu_mode_ = false;
    }

    ~Worker() {
        for (int i = 0; i < senders_.size(); i++) {
            delete senders_[i];
        }
        delete rc_;
        delete thpt_monitor_;
        delete receiver_;
        delete parser_;
        delete index_store_;
    }

    void Init() {
        receiver_ = new zmq::socket_t(context_, ZMQ_PULL);
        thpt_monitor_ = new ThroughputMonitor();
        rc_ = new ResultCollector;

        char addr[64];
        snprintf(addr, sizeof(addr), "tcp://*:%d", my_node_.tcp_port);
        receiver_->bind(addr);

        for (int i = 0; i < my_node_.get_local_size(); i++) {
            if (i != my_node_.get_local_rank()) {
                zmq::socket_t * sender = new zmq::socket_t(context_, ZMQ_PUSH);
                snprintf(addr, sizeof(addr), "tcp://%s:%d", workers_[i].hostname.c_str(), workers_[i].tcp_port);
                sender->connect(addr);
                senders_.push_back(sender);
            }
        }
    }

    void RunEMU(string& cmd, string& client_host) {
        string emu_host = "EMUWORKER";
        uint64_t trx_id;
        bool is_main_worker = false;

        // Only for EMUHost, for result reply
        // to client, this trx_id will not register
        // anywhere else
        TrxPlan emu_command_plan;

        if (client_host != emu_host) {
            is_main_worker = true;
            ibinstream in;
            in << emu_host;
            in << cmd;

            for (int i = 0; i < senders_.size(); i++) {
                zmq::message_t msg(in.size());
                memcpy((void *)msg.data(), in.get_buf(), in.size());
                senders_[i]->send(msg);
            }

            coordinator_->RegisterTrx(trx_id);
            emu_command_plan = TrxPlan(trx_id, client_host);
        }

        // Read thpt config and query set fn from cmd
        cmd = cmd.substr(3);
        cmd = Tool::trim(cmd, " ");
        istringstream iss(cmd);
        string config_fn, query_set_fn;
        iss >> config_fn >> query_set_fn;

        // Check files
        ifstream config_ifs(config_fn);
        ifstream query_ifs(query_set_fn);
        if (!config_ifs.good()) {
            cout << "file not found: " << config_fn << endl;
            return;
        }

        if (!query_ifs.good()) {
            cout << "file not found: " << query_set_fn << endl;
            return;
        }

        // Read Config file
        string line_in_file;
        uint64_t test_time, paralellfactor;
        int r_ratio, w_ratio;  // Read v.s. Write (e.g. 20 : 80)
        config_ifs >> test_time >> paralellfactor >> r_ratio >> w_ratio;

        test_time *= 1000000;

        // Read query file
        string query_line;
        //  Key: INSERT, READ, UPDATE, MIX
        unordered_map<string, vector<EmuTrxString>> trx_map;
        unordered_map<string, int> trx_count_map;
        vector<string> ldbc_person_ori_id_set;
        vector<string> ldbc_first_name_set;
        vector<string> amazon_brand_set;
        string cur_trx_type = "NULL";
        int trx_counter = 0;
        bool read_ldbc_person_id = false;
        bitset<3> read_file_flag(string("000"));  // 0: person_id, 1: first_name, 2: brand
        while (getline(query_ifs, query_line)) {
            if (query_line.at(0) == '#') {
                continue;
            } else if (query_line.at(0) == '[') {
                cur_trx_type = query_line.substr(1, query_line.length() -2);
                trx_map.emplace(cur_trx_type, vector<EmuTrxString>());
                trx_count_map.emplace(cur_trx_type, 0);
            } else {
               if (cur_trx_type == "NULL") { cout << "Wrong Query Type" << endl; return; }

               EmuTrxString cur_trx_string;
               if (!CheckEmulationQuery(is_main_worker, query_line, emu_command_plan, cur_trx_string, read_file_flag)) { return; }

               cur_trx_string.trx_type = trx_counter;
               trx_counter++;

               trx_map.at(cur_trx_type).emplace_back(cur_trx_string);
               trx_count_map.at(cur_trx_type)++;
            }
        }

        if (read_file_flag.test(0)) {
            string fn = "/data/aaron/oltp/person_ori_id/all_person_ori_id_" + to_string(my_node_.get_local_rank()) + "_of_" + to_string(my_node_.get_local_size()) + ".txt";

            ifstream ifs(fn);
            if (!ifs.good()) { cout << "person_ori_id file not good" << endl; return; }
            string person_ori_id;
            while (getline(ifs, person_ori_id)) {
                ldbc_person_ori_id_set.emplace_back(person_ori_id);
            }
        }

        if (read_file_flag.test(1)) {
            string fn = "/data/aaron/oltp/first_name/first_name_" + to_string(my_node_.get_local_rank()) + "_of_" + to_string(my_node_.get_local_size()) + ".txt";

            ifstream ifs(fn);
            if (!ifs.good()) { cout << "first_name file not good" << endl; return; }
            string first_name;
            while (getline(ifs, first_name)) {
                ldbc_first_name_set.emplace_back(first_name);
            }
        }

        if (read_file_flag.test(2)) {
            string fn = "/data/aaron/oltp/amazon_brand/amazon_brand_" + to_string(my_node_.get_local_rank()) + "_of_" + to_string(my_node_.get_local_size()) + ".txt";

            ifstream ifs(fn);
            if (!ifs.good()) { cout << "brand file not good" << endl; return; }
            string brand;
            while (getline(ifs, brand)) {
                amazon_brand_set.emplace_back(brand);
            }
        }

        int ldbc_person_ori_id_size = ldbc_person_ori_id_set.size();
        int ldbc_first_name_size = ldbc_first_name_set.size();
        int amazon_brand_size = amazon_brand_set.size();
        unordered_set<int> person_id_count;

        // wait until all previous query done
        while (thpt_monitor_->WorksRemaining() != 0) {cout << "waiting for remaining transactions." << endl; }

        is_emu_mode_ = true;
        srand(time(NULL));
        regex match("\\$RAND");

        vector<string> pushed_trxs;

        // suppose one query will be generated within 10 us
        pushed_trxs.reserve(test_time / 10);
        // wait for all nodes
        worker_barrier(my_node_);

        thpt_monitor_->StartEmu();
        if (is_main_worker) { cout << "RunEmu Starts" << endl; }
        uint64_t start = timer::get_usec();
        int first_name_idx_count = 0;
        int brand_idx_count = 0;
        while (timer::get_usec() - start < test_time) {
            if (thpt_monitor_->WorksRemaining() > paralellfactor) {
                continue;
            }

            // pick read or write transaction
            EmuTrxString emu_trx_string;
            int r = rand() % 100;
            bool is_update = false;
            if (r < r_ratio) {
                // Read-Only Transaction
                // READ, MIX
                int total_trx_number = trx_count_map.at("READ") + trx_count_map.at("MIX");
                CHECK(total_trx_number > 0) << "No Read-Only Transaction Provided";
                int inner_r = rand() % total_trx_number;
                if (inner_r < trx_count_map.at("READ")) {
                    // READ
                    emu_trx_string = trx_map.at("READ").at(inner_r);
                } else {
                    // MIX
                    emu_trx_string = trx_map.at("MIX").at(inner_r - trx_count_map.at("READ"));
                }
            } else {
                is_update = true;
                // Update Transaction
                // INSERT, UPDATE, DROP
                int total_trx_number = trx_count_map.at("INSERT") + trx_count_map.at("UPDATE") + trx_count_map.at("DROP");
                CHECK(total_trx_number > 0) << "No Update Transaction Provided";
                int inner_r = rand() % total_trx_number;
                if (inner_r < trx_count_map.at("INSERT")) {
                    // INSERT
                    emu_trx_string = trx_map.at("INSERT").at(inner_r);
                } else if (inner_r >= trx_count_map.at("INSERT") && inner_r < (trx_count_map.at("UPDATE") + trx_count_map.at("INSERT"))) {
                    // UPDATE
                    emu_trx_string = trx_map.at("UPDATE").at(inner_r - trx_count_map.at("INSERT"));
                } else {
                    // DROP
                    emu_trx_string = trx_map.at("DROP").at(inner_r - trx_count_map.at("INSERT") - trx_count_map.at("UPDATE"));
                }
            }

            string trx_tmp = emu_trx_string.query;
            if (trx_tmp.find("$READ") != string::npos) {
                vector<string> rand_val;
                if (trx_tmp.find("ori_id") != string::npos) {
                    int person_idx;
                    uint64_t start_time = timer::get_usec();
                    while (true) {
                        person_idx = rand() % ldbc_person_ori_id_size;
                        if (person_id_count.find(person_idx) == person_id_count.end()) {
                            person_id_count.emplace(person_idx);
                            break;
                        } else {
                            uint64_t end_time = timer::get_usec(); 
                            if (start_time - end_time > 10000) {  // 10ms
                                person_id_count.clear();
                            }
                        }
                    }

                    rand_val.emplace_back(ldbc_person_ori_id_set.at(person_idx));
                } else if (trx_tmp.find("firstName") != string::npos) {
                    rand_val.emplace_back(ldbc_first_name_set.at(first_name_idx_count));
                    first_name_idx_count++;

                    if (first_name_idx_count >= ldbc_first_name_size) {
                        first_name_idx_count = 0;
                    }
                } else if (trx_tmp.find("brand") != string::npos) {
                    while (!CheckCandidateString(amazon_brand_set.at(brand_idx_count))) {
                        brand_idx_count++;

                        if (brand_idx_count >= amazon_brand_size) {
                            brand_idx_count = 0;
                        }
                    }

                    rand_val.emplace_back(amazon_brand_set.at(brand_idx_count));
                    brand_idx_count++;

                    if (brand_idx_count >= amazon_brand_size) {
                        brand_idx_count = 0;
                    }
                } else {
                    cout << "Wrong $READ for trx "  << trx_tmp << endl;
                    return;
                }
                trx_tmp = Tool::my_regex_replace(trx_tmp, regex("\\$READ"), rand_val); 
            } else {
                if (emu_trx_string.num_rand_values != 0) {
                    vector<string> rand_values;
                    for (int i = 0; i < emu_trx_string.pkeys.size(); i++) {
                        string r_val;
                        if (!index_store_->GetRandomValue(emu_trx_string.types.at(i), emu_trx_string.pkeys.at(i), r_val, is_update)) {
                            cout << "not values for property " << emu_trx_string.pkeys.at(i) << " stored in node " << my_node_.get_local_rank() << endl;
                            break;
                        }
                        rand_values.emplace_back(r_val);
                    }

                    trx_tmp = Tool::my_regex_replace(trx_tmp, match, rand_values);
                }
            }

            RequestParsingTrx(trx_tmp, client_host, emu_trx_string.trx_type, true);
            pushed_trxs.emplace_back(trx_tmp);

            thpt_monitor_->PrintThroughput(my_node_.get_local_rank());
        }
        thpt_monitor_->StopEmu();

        while (thpt_monitor_->WorksRemaining() != 0) {
            cout << "Node " << my_node_.get_local_rank() << " still has " << thpt_monitor_->WorksRemaining() << "queries" << endl;

            usleep(500000);
        }

        double thpt = thpt_monitor_->GetThroughput();
        int num_completed_trx = thpt_monitor_->GetCompletedTrx();
        int num_aborted_trx = thpt_monitor_->GetAbortedTrx();
        map<int, vector<uint64_t>> latency_map;
        thpt_monitor_->GetLatencyMap(latency_map);

        if (my_node_.get_local_rank() == 0) {
            vector<double> thpt_list;
            vector<map<int, vector<uint64_t>>> map_list;
            vector<int> num_completed_trx_list;
            vector<int> num_aborted_trx_list;

            thpt_list.resize(my_node_.get_local_size());
            map_list.resize(my_node_.get_local_size());
            num_completed_trx_list.resize(my_node_.get_local_size());
            num_aborted_trx_list.resize(my_node_.get_local_size());

            master_gather(my_node_, false, thpt_list);
            master_gather(my_node_, false, map_list);
            master_gather(my_node_, false, num_completed_trx_list);
            master_gather(my_node_, false, num_aborted_trx_list);


            cout << "#################################" << endl;
            cout << "Emulator result with workload r:w is " << r_ratio << ":" << w_ratio << " and parrell factor: " << paralellfactor << endl;
            cout << "Throughput of node 0: " << thpt << " K queries/sec" << endl;
            for (int i = 1; i < my_node_.get_local_size(); i++) {
                thpt += thpt_list[i];
                num_completed_trx += num_completed_trx_list[i];
                num_aborted_trx += num_aborted_trx_list[i];
                cout << "Throughput of node " << i << ": " << thpt_list[i] << " K queries/sec" << endl;
            }
            double abort_rate = (double) num_aborted_trx * 100 / num_completed_trx;
            cout << "Total Throughput : " << thpt << " K queries/sec" << endl;
            cout << "Abort Rate : " << abort_rate << "%" << endl;
            cout << "#################################" << endl;

            map_list[0] = move(latency_map);
            thpt_monitor_->PrintCDF(map_list);
        } else {
            slave_gather(my_node_, false, thpt);
            slave_gather(my_node_, false, latency_map);
            slave_gather(my_node_, false, num_completed_trx);
            slave_gather(my_node_, false, num_aborted_trx);
        }

        thpt_monitor_->PrintThptToFile(my_node_.get_local_rank());

        // Write RW set to file
        string rw_fn = "Read_Write_Set_" + to_string(my_node_.get_local_rank()) + ".txt"; 
        ofstream rw_ofs(rw_fn, ofstream::out);

        ReadWriteRecord rw_rec;
        while (RW_SET_RECORD_QUEUE.try_pop(rw_rec)) {
            rw_ofs << rw_rec.DebugString();
        }

        // output all commited_queries to file
        string ofname = "Thpt_Queries_" + to_string(my_node_.get_local_rank()) + ".txt";
        ofstream ofs(ofname, ofstream::out);
        ofs << pushed_trxs.size() << endl;
        for (auto & trx : pushed_trxs) {
            ofs << trx << endl;
        }

        index_store_->CleanRandomCount();

        is_emu_mode_ = false;

        // send reply to client
        if (is_main_worker) {
            value_t v;
            Tool::str2str("Run Emu Mode Done", v);
            vector<value_t> result = {v};
            // thpt_monitor_->SetEmuStartTime(qid.value());
            // rc_->InsertResult(qid.value(), result);
            emu_command_plan.FillResult(-1, result);
            ReplyClient(emu_command_plan);
        }
    }

    bool CheckEmulationQuery(bool is_main_worker, string& emu_query_string, TrxPlan & plan, EmuTrxString & trx_string, bitset<3> & read_flag) {
        istringstream iss(emu_query_string);
        iss >> trx_string.query;

        if (trx_string.query.find("$READ") != string::npos) {
            string read_key;
            iss >> read_key;
            if (read_key == "ori_id") {
                read_flag.set(0, 1);
            } else if (read_key == "firstName") {
                read_flag.set(1, 1);
            } else if (read_key == "brand") {
                read_flag.set(2, 1);
            } else {
                cout << "Unexpected read key :" << read_key << endl;
                return false;
            }
            return true;
        }

        if (trx_string.query.find("$RAND") == string::npos) {
            return true;
        }

        // Get Number of property key
        while (!iss.eof()) {
            string cur_type, cur_pkey;
            iss >> cur_type >> cur_pkey;
            Element_T type;
            // get Type
            if (cur_type == "V") {
                type = Element_T::VERTEX;
                trx_string.types.emplace_back(type);
            } else if (cur_type == "E") {
                type = Element_T::EDGE;
                trx_string.types.emplace_back(type);
            } else {
                if (is_main_worker) {
                    string return_msg = "EMU Invalid Object Type";
                    value_t v;
                    Tool::str2str(return_msg, v);
                    vector<value_t> vec = {v};
                    thpt_monitor_->RecordStart(plan.trxid);
                    plan.FillResult(-1, vec);
                    ReplyClient(plan);
                }
                return false;
            }

            int pid = parser_->GetPid(type, cur_pkey);
            if (pid == -1) {
                if (is_main_worker) {
                    string return_msg = "EMU Invalid Random PKey";
                    value_t v;
                    Tool::str2str(return_msg, v);
                    vector<value_t> vec = {v};
                    plan.FillResult(-1, vec);
                    ReplyClient(plan);
                }
                return false;
            }

            trx_string.num_rand_values++;
            trx_string.pkeys.emplace_back(pid);
        }

        return true;
    }

    bool CheckCandidateString(string& str) {
        if (str.find(";") != string::npos ||
            str.find("=") != string::npos ||
            str.find("(") != string::npos ||
            str.find(")") != string::npos) {
            return false;
        }

        return true;
    }

    /**
     *  Submit a request of parsing transaction string to a thread safe queue.
     */
    void RequestParsingTrx(string trx_str, string client_host, int trx_type = -1, bool is_emu_mode = false) {
        ParseTrxReq req(trx_str, client_host, trx_type, is_emu_mode);
        pending_parse_trx_req_.Push(req);
    }

    void ProcessingParseTrxReq() {
        while (true) {
            ParseTrxReq req;
            pending_parse_trx_req_.WaitAndPop(req);
            // Parse the transaction, and push the transaction to be executed
            ParseTransaction(req.trx_str, req.client_host, req.trx_type, req.is_emu_mode);
        }
    }

    /**
     *  Parse the transaction string into TrxPlan
     */
    void ParseTransaction(string trx_str, string client_host, int trx_type, bool is_emu_mode) {
        uint64_t trxid;
        coordinator_->RegisterTrx(trxid);

        TrxPlan plan(trxid, client_host);
        if (is_emu_mode_) { thpt_monitor_->RecordStart(trxid, trx_type); }

        string error_msg;
        bool success = parser_->Parse(trx_str, plan, error_msg);

        if (success) {
            TrxPlanAccessor accessor;
            plans_.insert(accessor, trxid);
            accessor->second = move(plan);

            // Request begin time
            TimestampRequest req(trxid, TIMESTAMP_TYPE::BEGIN_TIME);
            pending_timestamp_request_.Push(req);
        } else {
  ERROR:
            if (is_emu_mode) {
                cout << "[" << client_host <<  "] Parser Failed: " << trx_str << " with error " << error_msg << endl;
            } else {
                value_t v;
                Tool::str2str(error_msg, v);
                vector<value_t> vec = {v};
                plan.FillResult(-1, vec);
                ReplyClient(plan);
            }
        }
    }

    /**
     *  regular recv thread for transaction processing request
     */
    void RecvRequest() {
        // Fake id and start time
        while (1) {
            zmq::message_t request;
            receiver_->recv(&request);

            char* buf = new char[request.size()];
            memcpy(buf, reinterpret_cast<char*>(request.data()), request.size());
            obinstream um(buf, request.size());

            string client_host;
            string query;

            um >> client_host;
            um >> query;
            cout << "worker_node" << my_node_.get_local_rank()
                    << " gets one QUERY: \"" << query << "\" from host "
                    << client_host << endl;

            if (query.find("emu") == 0) {
                RunEMU(query, client_host);
            } else {
                // parse and insert into plans_
                RequestParsingTrx(query, client_host);
            }
        }
    }

    /* Get all queries without dependency from the given TrxPlan.
     * For each query, pack it into a Pack with its query_index.
     *
     * For non-validation query, just send initMsg of it.
     * For validation query:
     *      Trx is readonly:
     *          Update status in trx_table, send initMsg of it.
     *      Trx is not readonly:
     *          Store the Pack in a map, and push request for CT.
     */
    bool RegisterQuery(TrxPlan& plan, int mailbox_tid) {
        vector<QueryPlan> qplans;
        // Get query plans of next level if any
        if (plan.NextQueries(qplans)) {
            for (QueryPlan& qplan : qplans) {
                // Register qid in result collector
                qid_t qid(plan.trxid, qplan.query_index);
                rc_->Register(qid.value());

                Pack pkg;
                pkg.id = qid;
                pkg.qplan = move(qplan);

                if (pkg.qplan.actors[0].actor_type == ACTOR_T::VALIDATION) {
                    if (pkg.qplan.trx_type != TRX_READONLY) {
                        VPackAccessor accessor;
                        validaton_query_pkgs_.insert(accessor, pkg.qplan.trxid);

                        ValidationQueryPack v_pkg;
                        v_pkg.pack = move(pkg);

                        // Before obtaining CT and quering RCT, the qplan cannot be executed.
                        accessor->second = move(v_pkg);

                        // Request commit time
                        TimestampRequest req(pkg.qplan.trxid, TIMESTAMP_TYPE::COMMIT_TIME);
                        pending_timestamp_request_.Push(req);
                    } else {
                        // For readonly trx, do not need to allocate CT and query RCT.
                        trx_table_->modify_status(plan.trxid, TRX_STAT::VALIDATING, plan.GetStartTime());

                        SendInitMsgForQuery(pkg, mailbox_tid);
                    }
                } else {
                    SendInitMsgForQuery(pkg, mailbox_tid);
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Send the results of Transaction back to the Client
     */
    void ReplyClient(TrxPlan& plan) {
        ibinstream m;
        vector<value_t> results;
        plan.GetResult(results);
        m << plan.client_host;  // client hostname
        m << results;  // query results
        m << (timer::get_usec() - plan.start_time);   // execution time

        zmq::message_t msg(m.size());
        memcpy(reinterpret_cast<void*>(msg.data()), m.get_buf(), m.size());

        zmq::socket_t sender(context_, ZMQ_PUSH);
        char addr[64];
        // port calculation is based on our self-defined protocol
        snprintf(addr, sizeof(addr), "tcp://%s:%d", plan.client_host.c_str(),
                workers_[my_node_.get_local_rank()].tcp_port + my_node_.get_world_rank());
        sender.connect(addr);
        // cout << "worker_node" << my_node_.get_local_rank()
        //         << " sends the results to Client " << plan.client_host << endl;
        sender.send(msg);
        monitor_->IncreaseCounter(1);
    }

    void NotifyTrxFinished(uint64_t trx_id, uint64_t bt) {
        // printf("[Worker%d] EraseTrx(%lu)\n", my_node_.get_local_rank(), bt);
        running_trx_list_->EraseTrx(bt);
    }

    // Do not put any compute intensive tasks in this function.
    // Use queues to dispatch them to other threads.
    void RecvNotification() {
        while (1) {
            obinstream out;
            mailbox_->RecvNotification(out);

            int notification_type;
            out >> notification_type;

            if (notification_type == (int)(NOTIFICATION_TYPE::RCT_TIDS)) {
                // RCT query result from remote workers
                vector<uint64_t> trx_id_list;
                uint64_t trx_id;
                out >> trx_id >> trx_id_list;

                InsertQueryRCTResult(trx_id, trx_id_list, config_->global_num_threads + Config::recv_notification_tid);
            } else if (notification_type == (int)(NOTIFICATION_TYPE::UPDATE_STATUS)) {
                int n_id;
                uint64_t trx_id;
                int status_i;
                bool is_read_only;
                out >> n_id >> trx_id >> status_i >> is_read_only;

                // P->V request will not go here. (Directly append to pending_trx_updates_)
                CHECK(status_i != (int)(TRX_STAT::VALIDATING));

                UpdateTrxStatusReq req{n_id, trx_id, TRX_STAT(status_i), is_read_only};
                pending_trx_updates_.Push(req);
            } else if (notification_type == (int)(NOTIFICATION_TYPE::QUERY_RCT)) {
                // RCT query request from remote workers
                int n_id;
                uint64_t bt, ct, trx_id;
                out >> n_id >> trx_id >> bt >> ct;

                QueryRCTRequest request(n_id, trx_id, bt, ct);
                pending_rct_query_request_.Push(request);
            } else {
                CHECK(false);
            }
        }
    }

    // Create the initMsg of a qplan in pkg, and send it out.
    // Need to specify the tid, since RDMAMailbox needs to find the corresponding send_buf via tid.
    void SendInitMsgForQuery(Pack pkg, int mailbox_tid) {
        vector<Message> msgs;
        Message::CreateInitMsg(
            pkg.id.value(),
            my_node_.get_local_rank(),
            my_node_.get_local_size(),
            core_affinity_->GetThreadIdForActor(ACTOR_T::INIT),
            pkg.qplan,
            msgs);
        for (int i = 0 ; i < my_node_.get_local_size(); i++) {
            mailbox_->Send(mailbox_tid, msgs[i]);
        }
        mailbox_->Sweep(mailbox_tid);
    }

    void ProcessAllocatedTimestamp() {
        while (true) {
            // The timestamp is allocated in Coordinator::ProcessTimestampRequest
            AllocatedTimestamp allocated_ts;
            pending_allocated_timestamp_.WaitAndPop(allocated_ts);
            uint64_t trx_id = allocated_ts.trx_id;

            if (allocated_ts.ts_type == TIMESTAMP_TYPE::COMMIT_TIME) {
                // Non-readonly transactions, CT allocated
                uint64_t ct = allocated_ts.timestamp;
                // printf("[Worker%d] Allocated CT(%lu)\n", my_node_.get_local_rank(), ct);

                if (config_->isolation_level == ISOLATION_LEVEL::SERIALIZABLE) {
                    rct_->insert_trx(ct, trx_id);
                }
                trx_table_->modify_status(trx_id, TRX_STAT::VALIDATING, ct);

                TrxPlanAccessor accessor;
                plans_.find(accessor, trx_id);

                TrxPlan& plan = accessor->second;
                uint64_t bt = plan.GetStartTime();

                // Firstly, query the local RCT
                std::vector<uint64_t> trx_id_list;
                rct_->query_trx(bt, ct - 1, trx_id_list);
                InsertQueryRCTResult(trx_id, trx_id_list, config_->global_num_threads + Config::process_allocated_ts_tid);

                int notification_type = (int)(NOTIFICATION_TYPE::QUERY_RCT);
                ibinstream in;
                in << notification_type << my_node_.get_local_rank() << trx_id << bt << ct;

                // Secondly, query the RCT on other workers (send the query RCT request).
                for (int i = 0; i < config_->global_num_workers; i++)
                    if (i != my_node_.get_local_rank())
                        mailbox_ ->SendNotification(i, in);
            } else if (allocated_ts.ts_type == TIMESTAMP_TYPE::BEGIN_TIME) {
                // BT allocated.
                uint64_t bt = allocated_ts.timestamp;
                // printf("[Worker%d] Allocated BT(%lu)\n", my_node_.get_local_rank(), bt);
                running_trx_list_->InsertTrx(bt);

                TrxPlanAccessor accessor;
                plans_.find(accessor, trx_id);

                TrxPlan& plan = accessor->second;

                trx_table_->insert_single_trx(trx_id, bt, plan.GetTrxType() == TRX_READONLY);

                // Set bt for TrxPlan
                plan.SetST(bt);

                if (!RegisterQuery(plan, config_->global_num_threads + Config::process_allocated_ts_tid)) {
                    string error_msg = "Error: Empty transaction";
                    value_t v;
                    Tool::str2str(error_msg, v);
                    vector<value_t> vec = {v};
                    plan.FillResult(-1, vec);
                    ReplyClient(plan);
                    NotifyTrxFinished(trx_id, plan.GetStartTime());
                    plans_.erase(accessor);
                }
            } else if (allocated_ts.ts_type == TIMESTAMP_TYPE::FINISH_TIME) {
                // The finish time for a non-readonly transaction is allocated.
                uint64_t ft = allocated_ts.timestamp;
                // Record it in the TrxTable, to help the GC thread decide when to erase it in the TrxTable.
                trx_table_->record_nro_trx_with_ft(trx_id, ft);
            } else {
                CHECK(false);
            }
        }
    }

    // For non-readonly transaction, need to query trx_ids from RCT from all workers,
    // before the validation query can be sent out.
    void InsertQueryRCTResult(uint64_t trx_id, const vector<uint64_t>& trx_id_list, int mailbox_tid) {
        VPackAccessor accessor;
        validaton_query_pkgs_.find(accessor, trx_id);

        ValidationQueryPack& v_pkg = accessor->second;

        v_pkg.trx_id_list.insert(v_pkg.trx_id_list.end(), trx_id_list.begin(), trx_id_list.end());
        v_pkg.collected_count++;

        if (v_pkg.collected_count == config_->global_num_workers) {
            // RCT trx_id_list on all workers are collected.

            for (auto & trxID : v_pkg.trx_id_list) {
                value_t v;
                Tool::uint64_t2value_t(trxID, v);
                v_pkg.pack.qplan.actors[0].params.emplace_back(v);
            }

            // Release the validation query.
            SendInitMsgForQuery(v_pkg.pack, mailbox_tid);

            validaton_query_pkgs_.erase(accessor);
        }
    }

    void Start() {
        // =================IdMapper========================
        SimpleIdMapper * id_mapper = SimpleIdMapper::GetInstance(&my_node_);

        // =================CoreAffinity====================
        core_affinity_ = new CoreAffinity();
        core_affinity_->Init();
        cout << "[Worker" << my_node_.get_local_rank() << "]: DONE -> Init Core Affinity" << endl;

        // =================PrimitiveRCTTable===============
        PrimitiveRCTTable * pmt_rct_table_ = PrimitiveRCTTable::GetInstance();
        pmt_rct_table_->Init();
        cout << "[Worker" << my_node_.get_local_rank() << "]: DONE -> Init PrimitiveRCTTable" << endl;

        // =================RDMABuffer======================
        Buffer* buf = Buffer::GetInstance(&my_node_);
        cout << "[Worker" << my_node_.get_local_rank()
                << "]: DONE -> Register RDMA MEM, SIZE = "
                << buf->GetBufSize() << endl;

        // =================TrxTable=========================
        trx_table_ = TransactionTable::GetInstance();

        // =================Coordinator=========================
        coordinator_ = Coordinator::GetInstance();
        coordinator_->Init(&my_node_);
        coordinator_->GetQueuesFromWorker(&pending_timestamp_request_, &pending_allocated_timestamp_,
                                          &pending_trx_updates_, &pending_trx_reads_, &pending_rct_query_request_);

        cout << "[Worker" << my_node_.get_local_rank() << "]: DONE -> coordinator_->Init()" << endl;

        // =================MailBox=========================
        if (config_->global_use_rdma) {
            mailbox_ = new RdmaMailbox(my_node_, buf);
        } else {
            mailbox_ = new TCPMailbox(my_node_);
        }
        mailbox_->Init(workers_);
        cout << "[Worker" << my_node_.get_local_rank()
             << "]: DONE -> Mailbox->Init()" << endl;

        // =================TransactionTableStub============
        if (config_->global_use_rdma) {
            trx_table_stub_ = RDMATrxTableStub::GetInstance(mailbox_, &pending_trx_updates_);
        } else {
            trx_table_stub_ = TcpTrxTableStub::GetInstance(mailbox_, workers_, &pending_trx_updates_);
        }
        trx_table_stub_->Init();
        cout << "[Worker" << my_node_.get_local_rank()
             << "]: DONE -> TrxTableStub->Init()" << endl;

        // =================DataStorage=====================
        data_storage_ = DataStorage::GetInstance();
        data_storage_->Init();

        // =================IndexStorage=====================
        index_store_ = IndexStore::GetInstance();
        index_store_->Init();

        // =================ParserLoadMapping===============
        parser_ = new Parser(index_store_);
        parser_->LoadMapping(data_storage_);
        cout << "[Worker" << my_node_.get_local_rank() << "]: DONE -> Parser_->LoadMapping()" << endl;

        // =================Monitor=========================
        monitor_ = new Monitor(my_node_);
        monitor_->Start();
        cout << "[Worker" << my_node_.get_local_rank() << "]: DONE -> monitor_->Start()" << endl;

        // =================RCT=========================
        rct_ = RCTable::GetInstance();

        // =================RunningTrxList=========================
        running_trx_list_ = RunningTrxList::GetInstance();
        running_trx_list_->Init(my_node_);

        coordinator_->GetInstancesFromWorker(trx_table_, mailbox_, rct_, workers_);
        coordinator_->PrepareSockets();

        // =================Timestamp generator thread=================
        thread timestamp_generator(&Coordinator::ProcessTimestampRequest, coordinator_);
        cout << "[Worker" << my_node_.get_local_rank() << "]: " << "Waiting for init of timestamp generator" << endl;
        coordinator_->WaitForDistributedClockInit();

        // =================Other threads=================
        // Recv&Send Thread
        thread recvreq(&Worker::RecvRequest, this);
        // Process notification msgs among workers
        thread recvnotification(&Worker::RecvNotification, this);
        // Deal with allocated timestamps
        thread timestamp_consumer(&Worker::ProcessAllocatedTimestamp, this);
        // Send RCT query request to remote workers
        thread process_rct_query_request(&Coordinator::ProcessQueryRCTRequest, coordinator_);
        // Execute TrxTable modification request from update_status
        thread trx_table_write_executor(&Coordinator::ProcessTrxTableWriteReqs, coordinator_);
        // Perform clock calibration
        thread timestamp_calibration(&Coordinator::PerformCalibration, coordinator_);
        // Parse transaction
        vector<thread> parser_threads;
        for (int i = 0; i < config_->num_parser_threads; i++)
            parser_threads.emplace_back(&Worker::ProcessingParseTrxReq, this);

        // For non-rdma configuration
        thread *trx_table_tcp_read_listener, *trx_table_tcp_read_executor, *running_trx_min_bt_listener;
        if (!config_->global_use_rdma) {
            // Receive remote TrxTable read request
            trx_table_tcp_read_listener = new thread(&Coordinator::ListenTCPTrxReads, coordinator_);
            // Reply remote TrxTable read request
            trx_table_tcp_read_executor = new thread(&Coordinator::ProcessTCPTrxReads, coordinator_);
            // Receive and reply remote MIN_BT query request
            running_trx_min_bt_listener = new thread(&RunningTrxList::ProcessReadMinBTRequest, running_trx_list_);
        }

        worker_barrier(my_node_);
        cout << "[Worker" << my_node_.get_local_rank() << "]: " << my_node_.DebugString();
        worker_barrier(my_node_);

        // =================ActorAdapter====================
        ActorAdapter * actor_adapter = new ActorAdapter(my_node_, rc_, mailbox_, core_affinity_);
        actor_adapter->Start();
        cout << "[Worker" << my_node_.get_local_rank() << "]: DONE -> actor_adapter->Start()" << endl;

        // =================GarbageCollector================
        // GarbageCollector must init after ActorAdapter since it require GlobalMinBt
        garbage_collector_ = GarbageCollector::GetInstance();
        garbage_collector_->Init();
        cout << "[Worker" << my_node_.get_local_rank() << "]: DONE -> garbage_collector->Start()" << endl;


        worker_barrier(my_node_);
        fflush(stdout);
        worker_barrier(my_node_);

        // pop out the query result from collector, automatically block when it's empty and wait
        // fake, should find a way to stop
        while (1) {
            reply re;
            rc_->Pop(re);

            // Get trxid from replied qid
            qid_t qid;
            uint2qid_t(re.qid, qid);

            TrxPlanAccessor accessor;
            plans_.find(accessor, qid.trxid);
            TrxPlan& plan = accessor->second;

            if (re.reply_type == ReplyType::NOTIFY_ABORT) {
                plan.Abort();
                continue;
            } else if (re.reply_type == ReplyType::RESULT_ABORT) {
                // For Run Emu Dubeg
                // cout << Tool::DebugString(re.results[0]) << endl;
                plan.FillResult(qid.id, re.results);
            } else if (re.reply_type == ReplyType::RESULT_NORMAL) {
                if (!plan.FillResult(qid.id, re.results)) {
                    trx_table_stub_->update_status(plan.trxid, TRX_STAT::ABORT);
                }
            } else {
                CHECK(false);
            }

            if (!RegisterQuery(plan, config_->global_num_threads + Config::main_thread_tid)) {
                // Reply to client when transaction is finished
                if (!is_emu_mode_) { // If Running EMU, do NOT send result back
                    ReplyClient(plan);
                }

                if (is_emu_mode_) { 
                    TRX_STAT trx_stat;
                    trx_table_stub_->read_status(plan.trxid, trx_stat);

                    thpt_monitor_->RecordEnd(qid.trxid, trx_stat == TRX_STAT::ABORT);
                }
                NotifyTrxFinished(qid.trxid, plan.GetStartTime());
                // if not readonly, abtain its finished time
                if (plan.GetTrxType() != TRX_READONLY) {
                    TimestampRequest req(qid.trxid, TIMESTAMP_TYPE::FINISH_TIME);
                    pending_timestamp_request_.Push(req);
                }

                plans_.erase(accessor);
            }
        }

        actor_adapter->Stop();
        monitor_->Stop();
        garbage_collector_->Stop();

        recvreq.join();
        recvnotification.join();
        trx_table_write_executor.join();
        timestamp_generator.join();
        timestamp_consumer.join();
        process_rct_query_request.join();
        if (!config_->global_use_rdma) {
            trx_table_tcp_read_listener->join();
            trx_table_tcp_read_executor->join();
            running_trx_min_bt_listener->join();
        }
        timestamp_calibration.join();
        for (auto& parser_thread : parser_threads)
            parser_thread.join();
    }

 private:
    Node & my_node_;

    vector<Node> & workers_;
    Config * config_;
    CoreAffinity* core_affinity_;
    Parser* parser_;
    IndexStore* index_store_;
    ResultCollector * rc_;
    Monitor * monitor_;
    AbstractMailbox* mailbox_;
    GarbageCollector * garbage_collector_;

    bool is_emu_mode_;
    ThroughputMonitor * thpt_monitor_;

    zmq::context_t context_;
    zmq::socket_t * receiver_;

    tbb::concurrent_hash_map<uint64_t, TrxPlan> plans_;
    typedef tbb::concurrent_hash_map<uint64_t, TrxPlan>::accessor TrxPlanAccessor;
    tbb::concurrent_hash_map<uint64_t, ValidationQueryPack> validaton_query_pkgs_;
    typedef tbb::concurrent_hash_map<uint64_t, ValidationQueryPack>::accessor VPackAccessor;

    vector<zmq::socket_t *> senders_;

    DataStorage* data_storage_ = nullptr;
    TrxTableStub * trx_table_stub_;

    RCTable* rct_;
    TransactionTable* trx_table_;

    ThreadSafeQueue<UpdateTrxStatusReq> pending_trx_updates_;
    ThreadSafeQueue<ReadTrxStatusReq> pending_trx_reads_;
    ThreadSafeQueue<TimestampRequest> pending_timestamp_request_;
    ThreadSafeQueue<AllocatedTimestamp> pending_allocated_timestamp_;
    ThreadSafeQueue<QueryRCTRequest> pending_rct_query_request_;
    ThreadSafeQueue<ParseTrxReq> pending_parse_trx_req_;

    Coordinator* coordinator_;
    RunningTrxList* running_trx_list_;
};
#endif /* WORKER_HPP_ */
