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
#include "core/trx_table_stub_zmq.hpp"
#include "core/trx_table_stub_rdma.hpp"


struct Pack {
    qid_t id;
    QueryPlan qplan;
};

struct ValidationPack {
    vector<uint64_t> trx_id_list;
    int collected_count = 0;
    Pack pack;
};

struct QueryRCTRequest {
    QueryRCTRequest() : trx_id(0) {};
    QueryRCTRequest(int _n_id, uint64_t _trx_id, uint64_t _bt, uint64_t _ct) :
                    n_id(_n_id), trx_id(_trx_id), bt(_bt), ct(_ct) {}
    int n_id;
    uint64_t trx_id, bt, ct;
};

struct QueryRCTResult {
    uint64_t trx_id;
    vector<uint64_t> trx_id_list;
};

class Worker {
 public:
    Worker(Node & my_node, vector<Node> & workers, Node & master) :
            my_node_(my_node), workers_(workers), master_(master) {
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

        if (!config_->global_use_rdma) {
            trx_read_recv_socket = new zmq::socket_t(context_, ZMQ_PULL);
            snprintf(addr, sizeof(addr), "tcp://*:%d", my_node_.tcp_port + 3 + 2 * config_->global_num_threads);
            trx_read_recv_socket->bind(addr);
            DLOG(INFO) << "[Master] bind " << string(addr);
            trx_read_rep_sockets.resize(config_->global_num_threads *
                                        config_->global_num_workers);

            // connect to p+3+global_num_threads ~ p+2+2*global_num_threads
            for (int i = 0; i < config_->global_num_workers; ++i) {
                Node& r_node = GetNodeById(workers_, i + 1);

                for (int j = 0; j < config_->global_num_threads; ++j) {
                    trx_read_rep_sockets[socket_code(i, j)] =
                        new zmq::socket_t(context_, ZMQ_PUSH);
                    snprintf(
                        addr, sizeof(addr), "tcp://%s:%d",
                        workers_[i].ibname.c_str(),
                        r_node.tcp_port + j + 3 + config_->global_num_threads);
                    trx_read_rep_sockets[socket_code(i, j)]->connect(addr);
                    DLOG(INFO) << "[Master] connects to " << string(addr);
                }
            }
        }
    }

    /* Not suitable for transaction base emulation, should be modified later
    void RunEMU(string& cmd, string& client_host){
        string emu_host = "EMUWORKER";
        qid_t qid;
        bool is_main_worker = false;

        // Check if the first worker that get request from client
        if(client_host != emu_host){
            is_main_worker = true;
            ibinstream in;
            in << emu_host;
            in << cmd;

            for(int i = 0; i < senders_.size(); i++){
                zmq::message_t msg(in.size());
                memcpy((void *)msg.data(), in.get_buf(), in.size());
                senders_[i]->send(msg);
            }

            qid = qid_t(my_node_.get_local_rank(), ++num_query);
            rc_->Register(qid.value(), client_host);
        }

        cmd = cmd.substr(3);
        string file_name = Tool::trim(cmd, " ");
        ifstream ifs(file_name);
        if (!ifs.good()) {
            cout << "file not found: " << file_name << endl;
            return;
        }
        uint64_t test_time, parrellfactor, ratio;
        ifs >> test_time >> parrellfactor;

        test_time *= 1000000;
        int n_type = 0;
        ifs >> n_type;
        assert(n_type > 0);

        vector<string> queries;
        vector<pair<Element_T, int>> query_infos;
        vector<int> ratios;
        for(int i = 0; i < n_type; i++){
            string query;
            string property_key;
            int ratio;
            ifs >> query >> property_key >> ratio;
            ratios.push_back(ratio);
            Element_T e_type;
            if(query.find("g.V()") == 0){
                e_type = Element_T::VERTEX;
            }else{
                e_type = Element_T::EDGE;
            }

            int pid = parser_->GetPid(e_type, property_key);
            if(pid == -1){
                if(is_main_worker){
                    value_t v;
                    Tool::str2str("Emu Mode Error", v);
                    vector<value_t> result = {v};
                    thpt_monitor_->RecordStart(qid.value());
                    rc_->InsertResult(qid.value(), result);
                }
                return ;
            }

            queries.push_back(query);
            query_infos.emplace_back(e_type, pid);
        }

        // wait until all previous query done
        while(thpt_monitor_->WorksRemaining() != 0);

        is_emu_mode_ = true;
        srand(time(NULL));
        regex match("\\$RAND");

        vector<string> commited_queries;

        // suppose one query will be generated within 10 us
        commited_queries.reserve(test_time / 10);
        // wait for all nodes
        worker_barrier(my_node_);

        thpt_monitor_->StartEmu();
        uint64_t start = timer::get_usec();
        while(timer::get_usec() - start < test_time){
            if(thpt_monitor_->WorksRemaining() > parrellfactor){
                continue;
            }
            // pick random query type
            int query_type = mymath::get_distribution(rand(), ratios);

            // get query info
            string query_temp = queries[query_type];
            Element_T element_type = query_infos[query_type].first;
            int pid = query_infos[query_type].second;

            // generate random value
            string rand_value;
            if(!index_store_->GetRandomValue(element_type, pid, rand(), rand_value)){
                cout << "Not values for property " << pid << " stored in node " << my_node_.get_local_rank() << endl;
                break;
            }

            query_temp = regex_replace(query_temp, match, rand_value);
            // run query
            ParseAndSendQuery(query_temp, emu_host, query_type);
            commited_queries.push_back(move(query_temp));
            if(is_main_worker){
                thpt_monitor_->PrintThroughput();
            }
        }
        thpt_monitor_->StopEmu();

        while(thpt_monitor_->WorksRemaining() != 0){
            cout << "Node " << my_node_.get_local_rank() << " still has " << thpt_monitor_->WorksRemaining() << "queries" << endl;
            usleep(500000);
        }

        double thpt = thpt_monitor_->GetThroughput();
        map<int,vector<uint64_t>> latency_map;
        thpt_monitor_->GetLatencyMap(latency_map);

        if(my_node_.get_local_rank() == 0){
            vector<double> thpt_list;
            vector<map<int,vector<uint64_t>>> map_list;
            thpt_list.resize(my_node_.get_local_size());
            map_list.resize(my_node_.get_local_size());
            master_gather(my_node_, false, thpt_list);
            master_gather(my_node_, false, map_list);


            cout << "#################################" << endl;
            cout << "Emulator result with " << n_type << " classes and parrell factor: " << parrellfactor << endl;
            cout << "Throughput of node 0: " << thpt << " K queries/sec" << endl;
            for(int i = 1; i < my_node_.get_local_size(); i++){
                thpt += thpt_list[i];
                cout << "Throughput of node " << i << ": " << thpt_list[i] << " K queries/sec" << endl;
            }
            cout << "Total Throughput : " << thpt << " K queries/sec" << endl;
            cout << "#################################" << endl;

            map_list[0] = move(latency_map);
            thpt_monitor_->PrintCDF(map_list);
        }else{
            slave_gather(my_node_, false, thpt);
            slave_gather(my_node_, false, latency_map);
        }

        //output all commited_queries to file
        string ofname = "Thpt_Queries_" + to_string(my_node_.get_local_rank()) + ".txt";
        ofstream ofs(ofname, ofstream::out);
        ofs << commited_queries.size() << endl;
        for(auto& query : commited_queries){
            ofs << query << endl;
        }

        is_emu_mode_ = false;

        // send reply to client
        if(is_main_worker){
            value_t v;
            Tool::str2str("Run Emu Mode Done", v);
            vector<value_t> result = {v};
            thpt_monitor_->SetEmuStartTime(qid.value());
            rc_->InsertResult(qid.value(), result);
        }
    }
    */

    /**
     * Parse the query string into TrxPlan
     */
    void ParseTransaction(string query, string client_host) {
        uint64_t trxid;
        coordinator_->RegisterTrx(trxid);

        TrxPlan plan(trxid, 0, client_host);
        string error_msg;
        bool success = parser_->Parse(query, plan, error_msg);

        if (success) {
            TrxPlanAccessor accessor;
            plans_.insert(accessor, trxid);
            accessor->second = move(plan);

            TimestampRequest req(trxid, false);
            pending_timestamp_request_.Push(req);
        } else {
  ERROR:
            value_t v;
            Tool::str2str(error_msg, v);
            vector<value_t> vec = {v};
            plan.FillResult(-1, vec);
            ReplyClient(plan);
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

            /*
            if(query.find("emu") == 0){
                RunEMU(query, client_host);
            }else{
                ParseAndSendQuery(query, client_host);
            }*/
            // parse and insert into plans_
            ParseTransaction(query, client_host);
        }
    }

    /**
     * To split one query (one line) from the current TrxPlan,
     * to form a package after assigning the qid
     */
    bool RegisterQuery(TrxPlan& plan) {
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
                        validaton_pkgs_.insert(accessor, pkg.qplan.trxid);

                        ValidationPack v_pkg;
                        v_pkg.pack = move(pkg);

                        accessor->second = move(v_pkg);

                        TimestampRequest req(pkg.qplan.trxid, true);
                        pending_timestamp_request_.Push(req);
                    } else {
                        // do not need to query rct
                        UpdateTrxStatusReq req{-1, plan.trxid, TRX_STAT::VALIDATING, true, plan.GetStartTime()};
                        pending_trx_updates_.Push(req);
                        queue_.Push(pkg);
                    }
                } else {
                    // Push query plan to SendQueryMsg queue
                    queue_.Push(pkg);
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Send the results of Tran back to the Client
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
        cout << "worker_node" << my_node_.get_local_rank()
                << " sends the results to Client " << plan.client_host << endl;
        sender.send(msg);
        monitor_->IncreaseCounter(1);
    }

    void NotifyTrxFinished(uint64_t trx_id, uint64_t bt) {
        // printf("[Worker%d] EraseTrx(%lu)\n", my_node_.get_local_rank(), bt);
        // fflush(stdout);
        running_trx_list_->EraseTrx(bt);
    }

    void RecvNotification() {
        while (1) {
            obinstream out;
            mailbox_->RecvNotification(out);

            int notification_type;
            out >> notification_type;

            if (notification_type == (int)(NOTIFICATION_TYPE::RCT_TIDS)) {
                vector<uint64_t> trxIDList;
                uint64_t trxid;
                out >> trxid >> trxIDList;

                QueryRCTResult query_result;
                query_result.trx_id = trxid;
                query_result.trx_id_list.swap(trxIDList);

                pending_rct_query_result_.Push(query_result);
            } else if (notification_type == (int)(NOTIFICATION_TYPE::UPDATE_STATUS)) {
                // P->V will not go here.
                int n_id;
                uint64_t trx_id;
                int status_i;
                bool is_read_only;
                out >> n_id >> trx_id >> status_i >> is_read_only;

                if (status_i != (int)(TRX_STAT::VALIDATING)) {
                    UpdateTrxStatusReq req{n_id, trx_id, TRX_STAT(status_i), is_read_only};
                    pending_trx_updates_.Push(req);
                }
            } else if (notification_type == (int)(NOTIFICATION_TYPE::QUERY_RCT)) {
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

    void Debug() {
        MPITimestamper::BindToLogicalCore(CPUInfoUtil::GetInstance()->GetTotalThreadCount() - 1);
        coordinator_->WaitForMPITimestamperInit();
        while (1) {
            sleep(5);
            uint64_t min_bt = running_trx_list_->GetGlobalMinBT();
        }
    }

    void SendQueryMsg(AbstractMailbox * mailbox, CoreAffinity * core_affinity) {
        while (1) {
            Pack pkg;
            queue_.WaitAndPop(pkg);

            vector<Message> msgs;
            Message::CreateInitMsg(
                pkg.id.value(),
                my_node_.get_local_rank(),
                my_node_.get_local_size(),
                core_affinity->GetThreadIdForActor(ACTOR_T::INIT),
                pkg.qplan,
                msgs);
            for (int i = 0 ; i < my_node_.get_local_size(); i++) {
                mailbox->Send(config_->global_num_threads, msgs[i]);
            }
            mailbox->Sweep(config_->global_num_threads);
        }
    }

    void ProcessTrxTableWriteReqs() {
        while (true) {
            // pop a req
            UpdateTrxStatusReq req;
            pending_trx_updates_.WaitAndPop(req);

            // check if P->V
            if (req.new_status == TRX_STAT::VALIDATING) {
                trx_table_ -> modify_status(req.trx_id, req.new_status, req.ct);
            } else {
                trx_table_ -> modify_status(req.trx_id, req.new_status);
            }
        }
    }

    // cover only TCP read
    void ListenTCPTrxReads(){
        while(true){
            ReadTrxStatusReq req;
            obinstream out;
            zmq::message_t zmq_req_msg;
            if (trx_read_recv_socket -> recv(&zmq_req_msg, 0) < 0) {
                CHECK(false) << "[Master::ListenTCPTrxReads] Master failed to recv TCP read";
            }
            // DLOG(INFO) << "[Master::ListenTCPTrxReads] recvs a read status req";
            char* buf = new char[zmq_req_msg.size()];
            memcpy(buf, zmq_req_msg.data(), zmq_req_msg.size());
            out.assign(buf, zmq_req_msg.size(), 0);

            out >> req.n_id >> req.t_id >> req.trx_id >> req.read_ct;
            pending_trx_reads_.Push(req);
        }
    }

    void ProcessTCPTrxReads() {
        while (true) {
            // pop a req
            ReadTrxStatusReq req;
            pending_trx_reads_.WaitAndPop(req);

            // printf("[Worker%d ProcessTCPTrxReads] %s\n", my_node_.get_local_rank(), req.DebugString().c_str());

            ibinstream in;
            if (req.read_ct) {
                uint64_t ct_;
                TRX_STAT status;
                trx_table_ -> query_ct(req.trx_id, ct_);
                trx_table_ -> query_status(req.trx_id, status);
                int status_i = (int) status;
                in << ct_;
                in << status_i;
                // DLOG(INFO) << "[Master::query_status] ct of " << req.trx_id << " is " << ct_;
            } else {
                TRX_STAT status;
                trx_table_ -> query_status(req.trx_id, status);
                int status_i = (int) status;
                in << status_i;
                // DLOG(INFO) << "[Master::query_status] status of " << req.trx_id << " is " << status_i;
            }

            zmq::message_t zmq_send_msg(in.size());
            memcpy(reinterpret_cast<void*>(zmq_send_msg.data()), in.get_buf(),
                   in.size());
            trx_read_rep_sockets[socket_code(req.n_id, req.t_id)] -> send(zmq_send_msg);
        }
    }

    void ProcessAllocatedTimestamp() {
        while (true) {
            AllocatedTimestamp allocated_ts;
            pending_allocated_timestamp_.WaitAndPop(allocated_ts);
            uint64_t trx_id = allocated_ts.trx_id;

            if (allocated_ts.is_ct) {
                // for not readonly
                uint64_t ct = allocated_ts.timestamp;
                // printf("[Worker%d] Allocated CT(%lu)\n", my_node_.get_local_rank(), ct);

                rct_->insert_trx(ct, trx_id);

                UpdateTrxStatusReq req{-1, trx_id, TRX_STAT::VALIDATING, true, ct};
                pending_trx_updates_.Push(req);

                TrxPlanAccessor accessor;
                plans_.find(accessor, trx_id);

                TrxPlan& plan = accessor->second;
                uint64_t bt = plan.GetStartTime();

                // first, query the local RCT
                std::set<uint64_t> trx_ids;
                rct_->query_trx(bt, ct - 1, trx_ids);
                std::vector<uint64_t> trx_ids_vec(trx_ids.begin(), trx_ids.end());
                QueryRCTResult query_result;
                query_result.trx_id = trx_id;
                query_result.trx_id_list.swap(trx_ids_vec);
                pending_rct_query_result_.Push(query_result);

                int notification_type = (int)(NOTIFICATION_TYPE::QUERY_RCT);
                ibinstream in;
                in << notification_type << my_node_.get_local_rank() << trx_id << bt << ct;

                for (int i = 0; i < config_->global_num_workers; i++)
                    if (i != my_node_.get_local_rank())
                        mailbox_ ->SendNotification(i, in);
            } else {
                uint64_t bt = allocated_ts.timestamp;
                // printf("[Worker%d] Allocated BT(%lu)\n", my_node_.get_local_rank(), bt);
                fflush(stdout);
                running_trx_list_->InsertTrx(bt);

                trx_table_->insert_single_trx(trx_id, bt);

                TrxPlanAccessor accessor;
                plans_.find(accessor, trx_id);

                TrxPlan& plan = accessor->second;
                plan.SetST(bt);

                if (!RegisterQuery(plan)) {
                    string error_msg = "Error: Empty transaction";
                    value_t v;
                    Tool::str2str(error_msg, v);
                    vector<value_t> vec = {v};
                    plan.FillResult(-1, vec);
                    ReplyClient(plan);
                    NotifyTrxFinished(trx_id, plan.GetStartTime());
                    plans_.erase(accessor);
                }
            }
        }
    }

    void ProcessQueryRCTResult() {
        while (true) {
            QueryRCTResult rct_query_result;
            pending_rct_query_result_.WaitAndPop(rct_query_result);

            VPackAccessor accessor;
            validaton_pkgs_.find(accessor, rct_query_result.trx_id);

            ValidationPack& v_pkg = accessor->second;

            v_pkg.trx_id_list.insert(v_pkg.trx_id_list.end(), rct_query_result.trx_id_list.begin(), rct_query_result.trx_id_list.end());
            v_pkg.collected_count++;

            if (v_pkg.collected_count == config_->global_num_workers) {
                for (auto & trxID : v_pkg.trx_id_list) {
                    value_t v;
                    Tool::uint64_t2value_t(trxID, v);
                    v_pkg.pack.qplan.actors[0].params.emplace_back(v);
                }

                queue_.Push(v_pkg.pack);

                validaton_pkgs_.erase(accessor);
            }
        }
    }

    void ProcessQueryRCTRequest() {
        while (true) {
            QueryRCTRequest request;
            pending_rct_query_request_.WaitAndPop(request);

            std::set<uint64_t> trx_ids;
            rct_ -> query_trx(request.bt, request.ct - 1, trx_ids);

            std::vector<uint64_t> trx_ids_vec(trx_ids.begin(), trx_ids.end());

            ibinstream in;
            int notification_type = (int)(NOTIFICATION_TYPE::RCT_TIDS);
            in << notification_type << request.trx_id;
            in << trx_ids_vec;
            mailbox_->SendNotification(request.n_id, in);
        }
    }

    void Start() {
        // =================IdMapper========================
        SimpleIdMapper * id_mapper = SimpleIdMapper::GetInstance(&my_node_);

        // =================CoreAffinity====================
        CoreAffinity * core_affinity = new CoreAffinity();
        core_affinity->Init();
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

        // =================Coordinator=========================
        coordinator_ = Coordinator::GetInstance();
        coordinator_->Init(&my_node_, &pending_timestamp_request_, &pending_allocated_timestamp_);
        cout << "[Worker" << my_node_.get_local_rank() << "]: DONE -> coordinator_->Init()" << endl;

        // =================MPITimestamper========================
        timestamper_ = MPITimestamper::GetInstance();
        // Use this thread to initialize MPITimestamper
        mpi_timestamper_initialized_ = false;  // set true in ProcessObtainingTimestamp
        thread timestamp_obtainer(&Coordinator::ProcessObtainingTimestamp, coordinator_);

        // =================MailBox=========================
        if (config_->global_use_rdma) {
            mailbox_ = new RdmaMailbox(my_node_, master_, buf);
        } else {
            mailbox_ = new TCPMailbox(my_node_, master_);
        }
        mailbox_->Init(workers_);
        cout << "[Worker" << my_node_.get_local_rank()
             << "]: DONE -> Mailbox->Init()" << endl;

        // =================TransactionTableStub============
        if (config_->global_use_rdma) {
            trx_table_stub_ = RDMATrxTableStub::GetInstance(mailbox_);
        } else {
            trx_table_stub_ = TcpTrxTableStub::GetInstance(master_, mailbox_, workers_);
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

        // =================TrxTable=========================
        trx_table_ = TransactionTable::GetInstance();

        // =================RunningTrxList=========================
        running_trx_list_ = RunningTrxList::GetInstance();
        running_trx_list_->Init(my_node_);

        // =================Recv&SendThread=================
        thread recvreq(&Worker::RecvRequest, this);
        thread sendmsg(&Worker::SendQueryMsg, this, mailbox_, core_affinity);
        thread recvnotification(&Worker::RecvNotification, this);
        thread trx_table_write_executor(&Worker::ProcessTrxTableWriteReqs, this);
        thread timestamp_consumer(&Worker::ProcessAllocatedTimestamp, this);
        thread process_rct_query_request(&Worker::ProcessQueryRCTRequest, this);
        thread process_rct_query_result(&Worker::ProcessQueryRCTResult, this);

        thread *trx_table_tcp_read_listener, *trx_table_tcp_read_executor, *running_trx_min_bt_listener;
        if (!config_->global_use_rdma) {
            trx_table_tcp_read_listener = new thread(&Worker::ListenTCPTrxReads, this);
            trx_table_tcp_read_executor = new thread(&Worker::ProcessTCPTrxReads, this);
            running_trx_min_bt_listener = new thread(&RunningTrxList::ProcessReadMinBTRequest, running_trx_list_);
        }
        thread debug(&Worker::Debug, this);

        cout << "[Worker" << my_node_.get_local_rank() << "]: " << "Waiting for init of timestamp generator" << endl;
        coordinator_->WaitForMPITimestamperInit();

        thread timestamp_calibration(&Coordinator::PerformCalibration, coordinator_);

        worker_barrier(my_node_);
        cout << "[Worker" << my_node_.get_local_rank() << "]: " << my_node_.DebugString();
        worker_barrier(my_node_);

        // =================ActorAdapter====================
        ActorAdapter * actor_adapter = new ActorAdapter(my_node_, rc_, mailbox_, core_affinity);
        actor_adapter->Start();
        cout << "[Worker" << my_node_.get_local_rank() << "]: DONE -> actor_adapter->Start()" << endl;

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
                plan.FillResult(qid.id, re.results);
            } else if (re.reply_type == ReplyType::RESULT_NORMAL) {
                if (!plan.FillResult(qid.id, re.results)) {
                    trx_table_stub_->update_status(plan.trxid, TRX_STAT::ABORT);
                }
            } else {
                CHECK(false);
            }

            if (!RegisterQuery(plan) && !is_emu_mode_) {
                // Reply to client when transaction is finished
                ReplyClient(plan);
                NotifyTrxFinished(qid.trxid, plan.GetStartTime());
                plans_.erase(accessor);
            }
        }

        actor_adapter->Stop();
        monitor_->Stop();

        recvreq.join();
        sendmsg.join();
        recvnotification.join();
        trx_table_write_executor.join();
        timestamp_obtainer.join();
        timestamp_consumer.join();
        process_rct_query_request.join();
        process_rct_query_result.join();
        if (!config_->global_use_rdma) {
            trx_table_tcp_read_listener->join();
            trx_table_tcp_read_executor->join();
            running_trx_min_bt_listener->join();
        }
        debug.join();
        timestamp_calibration.join();
    }

 private:
    Node & my_node_;
    Node & master_;

    vector<Node> & workers_;
    Config * config_;
    Parser* parser_;
    IndexStore* index_store_;
    ThreadSafeQueue<Pack> queue_;
    ResultCollector * rc_;
    Monitor * monitor_;
    AbstractMailbox* mailbox_;
    MPITimestamper* timestamper_;
    atomic<bool> mpi_timestamper_initialized_;

    bool is_emu_mode_;
    ThroughputMonitor * thpt_monitor_;

    zmq::context_t context_;
    zmq::socket_t * receiver_;

    tbb::concurrent_hash_map<uint64_t, TrxPlan> plans_;
    typedef tbb::concurrent_hash_map<uint64_t, TrxPlan>::accessor TrxPlanAccessor;
    tbb::concurrent_hash_map<uint64_t, ValidationPack> validaton_pkgs_;
    typedef tbb::concurrent_hash_map<uint64_t, ValidationPack>::accessor VPackAccessor;

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
    ThreadSafeQueue<QueryRCTResult> pending_rct_query_result_;

    zmq::socket_t * trx_read_recv_socket;
    vector<zmq::socket_t *> trx_read_rep_sockets;

    Coordinator* coordinator_;
    RunningTrxList* running_trx_list_;

    inline int socket_code(int n_id, int t_id) {
        return config_ -> global_num_threads * n_id + t_id;
    }
};
#endif /* WORKER_HPP_ */
