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
#include "core/actors_adapter.hpp"
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
		delete parser_;
		delete rc_;
	}

	void Init(){
		parser_ = new Parser(config_);
		receiver_ = new zmq::socket_t(context_, ZMQ_PULL);
		rc_ = new Result_Collector;
		char addr[64];
		sprintf(addr, "tcp://*:%d", my_node_.tcp_port);
		receiver_->bind(addr);
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
			cout << "Worker" << my_node_.get_world_rank() << " gets one QUERY: " << query << endl;

			qid_t qid(my_node_.get_local_rank(), ++num_query);
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

	void SendQueryMsg(RdmaMailbox * mailbox){
		while(1){
			Pack pkg;
			queue_.WaitAndPop(pkg);

			vector<Message> msgs;
			Message::CreateInitMsg(pkg.id.value(), my_node_.get_local_rank(), my_node_.get_local_size(), pkg.id.value() % config_->global_num_threads, pkg.actors, msgs);
			for(int i = 0 ; i < my_node_.get_local_size(); i++){
				mailbox->Send(i, msgs[i]);
			}
		}
	}

	void Start(){

		//===================prepare stage=================
		NaiveIdMapper * id_mapper = new NaiveIdMapper(my_node_, config_);

		//init core affinity
		CoreAffinity * core_affinity = new CoreAffinity(my_node_.get_world_rank());
		core_affinity->Init(config_->global_num_threads);
		cout << "DONE -> Init Core Affinity" << endl;

		//set the in-memory layout for RDMA buf
		Buffer * buf = new Buffer(my_node_, config_);
		cout << "DONE -> Register RDMA MEM, SIZE = " << buf->GetBufSize() << endl;

		//init the rdma mailbox
		RdmaMailbox * mailbox = new RdmaMailbox(my_node_, config_, buf);
		mailbox->Init(workers_);

		cout << "DONE -> RdmaMailbox->Init()" << endl;

		DataStore * datastore = new DataStore(my_node_, config_, id_mapper, buf);
		datastore->Init();

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
		thread sendmsg(&Worker::SendQueryMsg, this, mailbox);
		Monitor * monitor = new Monitor(my_node_);
		monitor->Start();

		//actor driver starts
		ActorAdapter * actor_adapter = new ActorAdapter(my_node_, config_, rc_, mailbox, datastore, core_affinity);
		actor_adapter->Start();
		cout << "DONE -> actor_adapter->Start()" << endl;

		//pop out the query result from collector, automatically block when it's empty and wait
		//fake, should find a way to stop
		while(1){
			reply re;
			rc_->Pop(re);

			ibinstream m;
			m << re.first; //client hostname
			m << re.second; //query results

			zmq::message_t msg(m.size());
			memcpy((void *)msg.data(), m.get_buf(), m.size());

			zmq::socket_t sender(context_, ZMQ_PUSH);
			char addr[64];
			//port calculation is based on our self-defined protocol
			sprintf(addr, "tcp://%s:%d", re.first.c_str(), workers_[my_node_.get_local_rank()].tcp_port + my_node_.get_world_rank());
			sender.connect(addr);
			cout << "Worker" << my_node_.get_world_rank() << " sends the results to Client " << re.first << endl;
			sender.send(msg);

			monitor->IncreaseCounter(1);
		}

		actor_adapter->Stop();
		monitor->Stop();

		recvreq.join();
		sendmsg.join();
	}

private:
	Node & my_node_;
	vector<Node> & workers_;
	Config * config_;
	Parser* parser_;
	ThreadSafeQueue<Pack> queue_;
	Result_Collector * rc_;
	uint32_t num_query;

	zmq::context_t context_;
	zmq::socket_t * receiver_;
};



#endif /* WORKER_HPP_ */
