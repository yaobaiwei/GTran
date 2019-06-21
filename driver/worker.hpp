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

#include "core/exec_plan.hpp"
#include "core/message.hpp"
#include "core/id_mapper.hpp"
#include "core/buffer.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/tcp_mailbox.hpp"
#include "core/actors_adapter.hpp"
#include "core/index_store.hpp"
#include "core/progress_monitor.hpp"
#include "core/parser.hpp"
#include "core/result_collector.hpp"

#include "layout/pmt_rct_table.hpp"
#include "layout/data_storage.hpp"
#include "core/trx_table_stub_zmq.hpp"
#include "core/trx_table_stub_rdma.hpp"


struct Pack {
    qid_t id;
    QueryPlan qplan;
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
        index_store_ = new IndexStore();
        parser_ = new Parser(index_store_);
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
    void ParseTransaction(string query, uint64_t trxid, uint64_t st, string client_host) {
        TrxPlan plan(trxid, st, client_host);
        string error_msg;
        bool success = parser_->Parse(query, plan, error_msg);

        if (success) {
            if (RegisterQuery(plan)) {
                // Only take lower 56 bits of trxid
                // Since qid = (trxid : 56, query_index : 8)
                plans_[trxid] = move(plan);
            } else {
                error_msg = "Error: Empty transaction";
                goto ERROR;
            }
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
            uint64_t trxid;
            uint64_t st;

            um >> client_host;
            um >> query;
            um >> trxid;
            um >> st;
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
            ParseTransaction(query, trxid, st, client_host);
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

                if (qplan.actors[0].actor_type == ACTOR_T::VALIDATION) {
                    vector<uint64_t> trxIDList;
                    trx_table_stub_->update_status(plan.trxid, TRX_STAT::VALIDATING, qplan.trx_type == TRX_READONLY, &trxIDList);
                    for (auto & trxID : trxIDList) {
                        value_t v;
                        Tool::uint64_t2value_t(trxID, v);
                        qplan.actors[0].params.emplace_back(v);
                    }
                }

                // Push query plan to SendQueryMsg queue
                Pack pkg;
                pkg.id = qid;
                pkg.qplan = move(qplan);
                queue_.Push(pkg);
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

        // =================MailBox=========================
        AbstractMailbox * mailbox;
        if (config_->global_use_rdma) {
            mailbox = new RdmaMailbox(my_node_, master_, buf);
        } else {
            mailbox = new TCPMailbox(my_node_, master_);
        }
        mailbox->Init(workers_);
        cout << "[Worker" << my_node_.get_local_rank()
             << "]: DONE -> Mailbox->Init()" << endl;

        // =================TransactionTableStub============
        if (config_->global_use_rdma) {
            trx_table_stub_ = RDMATrxTableStub::GetInstance(mailbox);
        } else {
            trx_table_stub_ = TcpTrxTableStub::GetInstance(master_, mailbox);
        }
        trx_table_stub_->Init();
        cout << "[Worker" << my_node_.get_local_rank()
             << "]: DONE -> TrxTableStub->Init()" << endl;

        // =================DataStorage=====================
        data_storage_ = DataStorage::GetInstance();
        data_storage_->Init();

        // =================ParserLoadMapping===============
        parser_->LoadMapping(data_storage_);
        cout << "[Worker" << my_node_.get_local_rank() << "]: DONE -> Parser_->LoadMapping()" << endl;

        // =================Monitor=========================
        monitor_ = new Monitor(my_node_);
        monitor_->Start();
        cout << "[Worker" << my_node_.get_local_rank() << "]: DONE -> monitor_->Start()" << endl;

        // =================Recv&SendThread=================
        thread recvreq(&Worker::RecvRequest, this);
        thread sendmsg(&Worker::SendQueryMsg, this, mailbox, core_affinity);

        worker_barrier(my_node_);
        cout << "[Worker" << my_node_.get_local_rank() << "]: " << my_node_.DebugString();
        worker_barrier(my_node_);

        // =================ActorAdapter====================
        ActorAdapter * actor_adapter = new ActorAdapter(my_node_, rc_, mailbox, core_affinity, index_store_);
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

            TrxPlan& plan = plans_[qid.trxid];

            if (re.isAbort) {
                plan.Abort();
            } else {
                if (!plan.FillResult(qid.id, re.results)) {
                    trx_table_stub_->update_status(plan.trxid, TRX_STAT::ABORT);
                }
            }

            if (!RegisterQuery(plan) && !is_emu_mode_) {
                // Reply to client when transaction is finished
                ReplyClient(plan);
            }
        }

        actor_adapter->Stop();
        monitor_->Stop();

        recvreq.join();
        sendmsg.join();
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

    bool is_emu_mode_;
    ThroughputMonitor * thpt_monitor_;

    zmq::context_t context_;
    zmq::socket_t * receiver_;

    map<uint64_t, TrxPlan> plans_;
    vector<zmq::socket_t *> senders_;

    DataStorage* data_storage_ = nullptr;
    TrxTableStub * trx_table_stub_;
};
#endif /* WORKER_HPP_ */
