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

struct Pack{
	qid_t id;
	vector<Actor_Object> actors;
};

class Worker{
public:
	Worker(Node & my_node, Config * config, vector<Node> & workers): my_node_(my_node), config_(config), workers_(workers){
		num_query = 0;
	}

	~Worker(){
		delete receiver_;
		delete w_listener_;
		delete parser_;
		delete index_store_;
		delete rc_;
	}

	void Init(){
		index_store_ = new IndexStore(config_);
		parser_ = new Parser(config_, index_store_);
		receiver_ = new zmq::socket_t(context_, ZMQ_PULL);
		w_listener_ = new zmq::socket_t(context_, ZMQ_REP);
		rc_ = new Result_Collector;
		char addr[64];
		char w_addr[64];
		sprintf(addr, "tcp://*:%d", my_node_.tcp_port);
		sprintf(w_addr, "tcp://*:%d", my_node_.tcp_port + config_->global_num_threads + 1);
		receiver_->bind(addr);
		w_listener_->bind(w_addr);
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

	void RecvRequest(){
		while(1)
		{
			zmq::message_t request;
			receiver_->recv(&request);
			qid_t qid(my_node_.get_local_rank(), ++num_query);
			timer_map[qid.value()] = timer::get_usec();

			char* buf = new char[request.size()];
			memcpy(buf, (char *)request.data(), request.size());
			obinstream um(buf, request.size());

			string client_host;
			string query;

			um >> client_host; //get the client hostname for returning results.
			um >> query;
			cout << "Worker" << my_node_.get_world_rank() << " gets one QUERY: " << query << endl;

			rc_->Register(qid.value(), client_host);

			vector<Actor_Object> actors;
			string error_msg;
			bool success = parser_->Parse(query, actors, error_msg);

			if(success){
				Pack pkg;
				pkg.id = qid;
				pkg.actors = move(actors);

				queue_.Push(pkg);
			}else{
				value_t v;
				Tool::str2str(error_msg, v);
				vector<value_t> vec = {v};
				rc_->InsertResult(qid.value(), vec);
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

		//===================prepare stage=================
		NaiveIdMapper * id_mapper = new NaiveIdMapper(my_node_, config_);

		//init core affinity
		CoreAffinity * core_affinity = new CoreAffinity(config_);
		core_affinity->Init();
		cout << "DONE -> Init Core Affinity" << endl;

		//set the in-memory layout for RDMA buf
		Buffer * buf = new Buffer(my_node_, config_);
		cout << "DONE -> Register RDMA MEM, SIZE = " << buf->GetBufSize() << endl;

		AbstractMailbox * mailbox;
		if (config_->global_use_rdma)
			mailbox = new RdmaMailbox(my_node_, config_, buf);
		else 
			mailbox = new TCPMailbox(my_node_, config_);
		mailbox->Init(workers_);

		cout << "DONE -> Mailbox->Init()" << endl;

		DataStore * datastore = new DataStore(my_node_, config_, id_mapper, buf);
		datastore->Init(workers_);

		cout << "DONE -> DataStore->Init()" << endl;

		datastore->LoadDataFromHDFS();
		worker_barrier(my_node_);

		//=======data shuffle==========
		datastore->Shuffle();
		cout << "DONE -> DataStore->Shuffle()" << endl;
		//=======data shuffle==========

		datastore->DataConverter();
		worker_barrier(my_node_);

		cout << "DONE -> Datastore->DataConverter()" << endl;

		parser_->LoadMapping();
		cout << "DONE -> Parser_->LoadMapping()" << endl;

		thread recvreq(&Worker::RecvRequest, this);
		thread sendmsg(&Worker::SendQueryMsg, this, mailbox, core_affinity);

		// for TCP use
		thread w_listener;
		if (!config_->global_use_rdma)
			w_listener = thread(&Worker::WorkerListener, this, datastore);

		Monitor * monitor = new Monitor(my_node_);
		monitor->Start();

		//actor driver starts
		ActorAdapter * actor_adapter = new ActorAdapter(my_node_, config_, rc_, mailbox, datastore, core_affinity, index_store_);
		actor_adapter->Start();
		cout << "DONE -> actor_adapter->Start()" << endl;

		//pop out the query result from collector, automatically block when it's empty and wait
		//fake, should find a way to stop
		while(1){
			reply re;
			rc_->Pop(re);

			uint64_t time_ = timer::get_usec() - timer_map[re.qid];

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
			cout << "Worker" << my_node_.get_world_rank() << " sends the results to Client " << re.hostname << endl;
			sender.send(msg);

			monitor->IncreaseCounter(1);
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

	map<uint64_t, uint64_t> timer_map;

	zmq::context_t context_;
	zmq::socket_t * receiver_;
	zmq::socket_t * w_listener_;
};



#endif /* WORKER_HPP_ */
