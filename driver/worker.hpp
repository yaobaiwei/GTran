/*
 * worker.hpp
 *
 *  Created on: Jun 21, 2018
 *      Author: Hongzhi Chen
 */

#ifndef WORKER_HPP_
#define WORKER_HPP_

#include "utils/zmq.hpp"

#include "base/core_affinity.hpp"
#include "base/node.hpp"
#include "base/type.hpp"
#include "base/thread_safe_queue.hpp"
#include "base/throughput_monitor.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"

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
#include "storage/data_store.hpp"

#include "utils/mpi_profiler.hpp"
#include "storage/mpi_snapshot.hpp"

struct Pack{
	qid_t id;
	vector<Actor_Object> actors;
};

class Worker{
public:
	Worker(Node & my_node, vector<Node> & workers): my_node_(my_node), workers_(workers)
	{
		config_ = &Config::GetInstance();
		num_query = 0;
		is_emu_mode_ = false;
	}

	~Worker(){
		for(int i = 0; i < senders_.size(); i++){
			delete senders_[i];
		}

		delete receiver_;
		delete w_listener_;
		delete parser_;
		delete index_store_;
		delete rc_;
	}

	void Init(){
		index_store_ = new IndexStore();
		parser_ = new Parser(index_store_);
		receiver_ = new zmq::socket_t(context_, ZMQ_PULL);
		w_listener_ = new zmq::socket_t(context_, ZMQ_REP);
		thpt_monitor_ = new Throughput_Monitor();
		rc_ = new Result_Collector;
		char addr[64];
		char w_addr[64];
		sprintf(addr, "tcp://*:%d", my_node_.tcp_port);
		sprintf(w_addr, "tcp://*:%d", my_node_.tcp_port + config_->global_num_threads + 1);
		receiver_->bind(addr);
		w_listener_->bind(w_addr);

		for(int i = 0; i < my_node_.get_local_size(); i++){
			if(i != my_node_.get_local_rank()){
				zmq::socket_t * sender = new zmq::socket_t(context_, ZMQ_PUSH);
				sprintf(addr, "tcp://%s:%d", workers_[i].hostname.c_str(), workers_[i].tcp_port);
				sender->connect(addr);
				senders_.push_back(sender);
			}
		}
	}

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
		// config file for emulator
		// file format:
		// first line: #test_time(sec) [/t] #parrell_factor
		// first line: #num of query type
		// following with n queries:
		//		query_string [/t] property_key_string
		// each query_string contains one unkown:
		// g.V().has("name", "%RAND")[/t]price
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

	void WorkerListener(DataStore * datastore){
		while(1) {
			zmq::message_t request;
			w_listener_->recv(&request);

			char* buf = new char[request.size()];
			memcpy(buf, (char *)request.data(), request.size());
			obinstream um(buf, request.size());

			uint64_t id;
			int elem_type;

			um >> id;
			um >> elem_type;

			value_t val;
			ibinstream m;

			switch(elem_type) {
				case Element_T::VERTEX:
					datastore->tcp_helper->GetPropertyForVertex(id, val);
					break;
				case Element_T::EDGE:
					datastore->tcp_helper->GetPropertyForEdge(id, val);
					break;
				default:
					cout << "Wrong element type" << endl;
			}

			m << val;
			zmq::message_t msg(m.size());
			memcpy((void *)msg.data(), m.get_buf(), m.size());
			w_listener_->send(msg);
		}
	}

	void ParseAndSendQuery(string query, string client_host, int query_type = -1){
		qid_t qid(my_node_.get_local_rank(), ++num_query);
		thpt_monitor_->RecordStart(qid.value(), query_type);

		rc_->Register(qid.value(), client_host);

		vector<Actor_Object> actors;
		string error_msg;
		bool success = parser_->Parse(query, actors, error_msg);

		if(success){
			Pack pkg;
			pkg.id = qid;

			//a debug function is needed
			// printf("parse success! from id %d, actor cnt = %d\n"
			// 	, my_node_.get_local_rank(), actors.size());

			// for(int i = 0; i < actors.size(); i++)
			// {
			// 	printf("i = %d,  %s\n", i, actors[i].DebugString().c_str());
			// }

			pkg.actors = move(actors);

			queue_.Push(pkg);
		}else{
			value_t v;
			Tool::str2str(error_msg, v);
			vector<value_t> vec = {v};
			rc_->InsertResult(qid.value(), vec);
		}
	}

	void RecvRequest(){
		while(1)
		{
			zmq::message_t request;
			receiver_->recv(&request);

			char* buf = new char[request.size()];
			memcpy(buf, (char *)request.data(), request.size());
			obinstream um(buf, request.size());

			string client_host;
			string query;

			um >> client_host; //get the client hostname for returning results.
			um >> query;
			cout << "worker_node" << my_node_.get_local_rank() << " gets one QUERY: \"" << query <<"\" from host " << client_host << endl;

			if(query.find("emu") == 0){
				RunEMU(query, client_host);
			}else{
				ParseAndSendQuery(query, client_host);
			}
		}
	}

	void SendQueryMsg(AbstractMailbox * mailbox, CoreAffinity * core_affinity){
		while(1){
			Pack pkg;
			queue_.WaitAndPop(pkg);

			vector<Message> msgs;
			Message::CreateInitMsg(pkg.id.value(), my_node_.get_local_rank(), my_node_.get_local_size(),core_affinity->GetThreadIdForActor(ACTOR_T::INIT), pkg.actors, msgs);
			for(int i = 0 ; i < my_node_.get_local_size(); i++){
				mailbox->Send(config_->global_num_threads, msgs[i]);
			}
			mailbox->Sweep(config_->global_num_threads);
		}
	}

	void Start(){

		MPIProfiler* pf = MPIProfiler::GetInstance("gq_worker_initial", my_node_.local_comm);
		pf->InsertLabel("rdma_mem");
		pf->InsertLabel("mailbox");
		pf->InsertLabel("datastore");
		pf->InsertLabel("get_string_indexes");
		pf->InsertLabel("get_vertices");
		pf->InsertLabel("get_vplist");
		pf->InsertLabel("get_eplist");
		pf->InsertLabel("shuffle");

		pf->InsertLabel("v_local1");
		pf->InsertLabel("v_alltoall");
		pf->InsertLabel("v_local2");
		pf->InsertLabel("e_local1");
		pf->InsertLabel("e_alltoall");
		pf->InsertLabel("e_local2");
		pf->InsertLabel("vp_local1");
		pf->InsertLabel("vp_alltoall");
		pf->InsertLabel("vp_local2");
		pf->InsertLabel("ep_local1");
		pf->InsertLabel("ep_alltoall");
		pf->InsertLabel("ep_local2");
		pf->InsertLabel("vp_lists_local1");
		pf->InsertLabel("vp_lists_alltoall");
		pf->InsertLabel("vp_lists_local2");

		pf->InsertLabel("data_converter");
		pf->InsertLabel("load_mapping");
		pf->InsertLabel("post_others");

		//initial MPIConfigNamer
		MPIConfigNamer* p = MPIConfigNamer::GetInstanceP(my_node_.local_comm);
		p->AppendHash(config_->HDFS_INDEX_PATH + 
					  config_->HDFS_VTX_SUBFOLDER + 
					  config_->HDFS_VP_SUBFOLDER + 
					  config_->HDFS_EP_SUBFOLDER + 
					  p->ultos(config_->key_value_ratio_in_rdma) + 
					  p->ultos(config_->global_vertex_property_kv_sz_gb) + 
					  p->ultos(config_->global_edge_property_kv_sz_gb));

		//initial MPISnapshot
		MPISnapshot* snapshot = MPISnapshot::GetInstanceP(config_->SNAPSHOT_PATH);

		// snapshot->DisableRead();


		//===================prepare stage=================
		NaiveIdMapper * id_mapper = new NaiveIdMapper(my_node_);

		//init core affinity
		CoreAffinity * core_affinity = new CoreAffinity();
		core_affinity->Init();
		cout << "DONE -> Init Core Affinity" << endl;

		//set the in-memory layout for RDMA buf
		pf->STPF("rdma_mem");
		Buffer * buf = new Buffer(my_node_);
		cout << "DONE -> Register RDMA MEM, SIZE = " << buf->GetBufSize() << endl;
		pf->EDPF("rdma_mem");

		pf->STPF("mailbox");
		AbstractMailbox * mailbox;
		if (config_->global_use_rdma)
			mailbox = new RdmaMailbox(my_node_, buf);
		else
			mailbox = new TCPMailbox(my_node_);
		mailbox->Init(workers_);
		pf->EDPF("mailbox");

		cout << "DONE -> Mailbox->Init()" << endl;

		pf->STPF("datastore");
		DataStore * datastore = new DataStore(my_node_, id_mapper, buf);
		DataStore::StaticInstanceP(datastore);
		datastore->Init(workers_);
		pf->EDPF("datastore");

		cout << "DONE -> DataStore->Init()" << endl;

		//read snapshot area
		{
			parser_->ReadSnapshot();
			datastore->ReadSnapshot();
			//to do:
			//ep, vp store -> read snapshot
		}

		datastore->LoadDataFromHDFS();
		worker_barrier(my_node_);

		//=======data shuffle==========
		pf->STPF("shuffle");
		datastore->Shuffle();
		pf->EDPF("shuffle");
		cout << "DONE -> DataStore->Shuffle()" << endl;
		//=======data shuffle==========

		pf->STPF("data_converter");
		datastore->DataConverter();
		pf->EDPF("data_converter");
		worker_barrier(my_node_);

		cout << "DONE -> Datastore->DataConverter()" << endl;

		pf->STPF("load_mapping");
		parser_->LoadMapping();
		pf->EDPF("load_mapping");
		cout << "DONE -> Parser_->LoadMapping()" << endl;

		//write snapshot area
		{
			parser_->WriteSnapshot();
			datastore->WriteSnapshot();
		}

		pf->STPF("post_others");
		thread recvreq(&Worker::RecvRequest, this);
		thread sendmsg(&Worker::SendQueryMsg, this, mailbox, core_affinity);

		// for TCP use
		thread w_listener;
		if (!config_->global_use_rdma)
			w_listener = thread(&Worker::WorkerListener, this, datastore);

		Monitor * monitor = new Monitor(my_node_);
		monitor->Start();
		
		worker_barrier(my_node_);

		//actor driver starts
		ActorAdapter * actor_adapter = new ActorAdapter(my_node_, rc_, mailbox, datastore, core_affinity, index_store_);
		actor_adapter->Start();
		cout << "DONE -> actor_adapter->Start()" << endl;
		pf->EDPF("post_others");

		pf->PrintSummary();


		//pop out the query result from collector, automatically block when it's empty and wait
		//fake, should find a way to stop
		while(1){
			reply re;
			rc_->Pop(re);
			// Node::SingleTrap("rc_->Pop(re);");

			uint64_t time_ = thpt_monitor_->RecordEnd(re.qid);

			if(!is_emu_mode_){
				ibinstream m;
				m << re.hostname; //client hostname
				m << re.results; //query results
				m << time_;   //execution time

				zmq::message_t msg(m.size());
				memcpy((void *)msg.data(), m.get_buf(), m.size());

				zmq::socket_t sender(context_, ZMQ_PUSH);
				char addr[64];
				//port calculation is based on our self-defined protocol
				sprintf(addr, "tcp://%s:%d", re.hostname.c_str(), workers_[my_node_.get_local_rank()].tcp_port + my_node_.get_world_rank());
				sender.connect(addr);
				cout << "worker_node" << my_node_.get_local_rank() << " sends the results to Client " << re.hostname << endl;
				sender.send(msg);

				monitor->IncreaseCounter(1);
			}
		}

		actor_adapter->Stop();
		monitor->Stop();

		recvreq.join();
		sendmsg.join();
		if (!config_->global_use_rdma)
			w_listener.join();
	}

private:
	Node & my_node_;
	vector<Node> & workers_;
	Config * config_;
	Parser* parser_;
	IndexStore* index_store_;
	ThreadSafeQueue<Pack> queue_;
	Result_Collector * rc_;
	uint32_t num_query;

	bool is_emu_mode_;
	Throughput_Monitor * thpt_monitor_;

	zmq::context_t context_;
	zmq::socket_t * receiver_;
	zmq::socket_t * w_listener_;

	vector<zmq::socket_t *> senders_;
};



#endif /* WORKER_HPP_ */
