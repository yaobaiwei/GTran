/*
 * worker.hpp
 *
 *  Created on: Jun 21, 2018
 *      Author: Hongzhi Chen
 */

#ifndef WORKER_HPP_
#define WORKER_HPP_

#include "utils/zmq.hpp"

#include "base/node.hpp"
#include "base/type.hpp"
#include "base/sarray.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"

#include "core/message.hpp"
#include "core/id_mapper.hpp"
#include "core/buffer.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/actors_adapter.hpp"
#include "core/progress_monitor.hpp"
#include "core/parser.hpp"
#include "storage/data_store.hpp"

class Worker{
public:
	Worker(Node & my_node, Config * config, vector<Node> & workers): my_node_(my_node), config_(config), workers_(workers){}

	~Worker(){
		delete receiver_;
		delete parser_;
	}

	void Init(){
		parser_ = new Parser(config_);
		receiver_ = new zmq::socket_t(context_, ZMQ_PULL);
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


			//Once received a query, we should parse it first and process
			//TODO
			//The result will be inserted into a Queue
			//one thread fetches the queue and return it to the corresponding client based on its hostname

			string client_host;
			string query;

			um >> client_host; //get the client hostname for returning results.
			um >> query;
			cout << "Worker" << my_node_.get_world_rank() << " gets one QUERY: " << query << endl;

			vector<Actor_Object> vec;
			Element_T elem;
			parser_->Parse(query, vec, elem);
			
			sleep(1); //simulate processing time
			string result;
			for(auto obj : vec){
				result.append(obj.DebugString());
			}

			ibinstream m;
			m << result;
			zmq::message_t msg(m.size());
			memcpy((void *)msg.data(), m.get_buf(), m.size());

			zmq::socket_t sender(context_, ZMQ_PUSH);
			char addr[64];
			//port calculation is based on our self-defined protocol
			sprintf(addr, "tcp://%s:%d", client_host.c_str(), workers_[my_node_.get_local_rank()].tcp_port + my_node_.get_world_rank());
			sender.connect(addr);
			cout << "Worker" << my_node_.get_world_rank() << " sends the results to Client " << client_host << endl;
			sender.send(msg);
		}
	}

	void Start(){
		NaiveIdMapper * id_mapper = new NaiveIdMapper(my_node_, config_);
		id_mapper->Init();

		cout <<  "DONE -> NaiveIdMapper->Init()" << endl;

		//set the in-memory layout for RDMA buf
		Buffer * buf = new Buffer(my_node_, config_);
		buf->Init();
		cout <<  "DONE -> Buff->Init()" << endl;

		//init the rdma mailbox
		RdmaMailbox * mailbox = new RdmaMailbox(my_node_, config_, id_mapper, buf);
		mailbox->Init(workers_);

		cout << "DONE -> RdmaMailbox->Init()" << endl;

		DataStore * datastore = new DataStore(my_node_, config_, id_mapper, buf);
		datastore->Init();

		cout << "DONE -> DataStore->Init()" << endl;

		datastore->LoadDataFromHDFS();
		worker_barrier(my_node_);

		//cout << "DONE -> DataStore->LoadDataFromHDFS()" << endl;
		//=======data shuffle==========
		datastore->Shuffle();
		//cout << "DONE -> DataStore->Shuffle()" << endl;
		//=======data shuffle==========

		datastore->DataConverter();
		worker_barrier(my_node_);
		cout << "DONE -> Datastore->DataConverter()" << endl;

//		//TEST
//		vid_t vid(3);
//		if(id_mapper->IsVertexLocal(vid)){
//			Vertex* v = datastore->GetVertex(vid);
//			cout << "RANK: " << my_node_.get_local_rank() << " GET VTX.IN_NBS => ";
//			for(auto & nb : v->in_nbs){
//				cout << nb.vid << ", ";
//			}
//			cout << endl;
//		}
//
//		eid_t eid(6,3);
//		if(id_mapper->IsEdgeLocal(eid)){
//			Edge* e = datastore->GetEdge(eid);
//			cout << "RANK: " << my_node_.get_local_rank() << " GET EDGE.PPT_LIST => ";
//			for(auto & label : e->ep_list){
//				cout <<  label << ", ";
//			}
//			cout << endl;
//		}
//
//		vpid_t vpid(6,2);
//		value_t val;
//		datastore->GetPropertyForVertex(0, vpid, val);
//		cout << "RANK " << my_node_.get_local_rank() << " GET VP.VALUE => ";
//		switch(val.type){
//			case 1:
//				cout << Tool::value_t2int(val) << endl;
//				break;
//			case 2:
//				cout << Tool::value_t2double(val) << endl;
//				break;
//			case 3:
//				cout << Tool::value_t2char(val) << endl;
//				break;
//			case 4:
//				cout << Tool::value_t2string(val) << endl;
//				break;
//		}
//
//		epid_t epid(1,3,1);
//		value_t val2;
//		datastore->GetPropertyForEdge(0, epid, val2);
//		cout << "RANK " << my_node_.get_local_rank() << " GET EP.VALUE => ";
//		switch(val2.type){
//			case 1:
//				cout << to_string(Tool::value_t2int(val2)) << endl;
//				break;
//			case 2:
//				cout << to_string(Tool::value_t2double(val2)) << endl;
//				break;
//			case 3:
//				cout << to_string(Tool::value_t2char(val2)) << endl;
//				break;
//			case 4:
//				cout << Tool::value_t2string(val2) << endl;
//				break;
//		}
//
//		label_t vl;
//		datastore->GetLabelForVertex(0, vid, vl);
//		cout << "RANK " << my_node_.get_local_rank() << " GET VTX LABEL => " << (char)vl << endl;
//
//		label_t el;
//		datastore->GetLabelForEdge(0, eid, el);
//		cout << "RANK " << my_node_.get_local_rank() << " GET EDGE LABEL => " << (char)el << endl;


//TEST the commun function within each worker by rdma

//		for(int i = 0 ; i < my_node_.get_local_size(); i++){
//			MSG_T type = MSG_T::FEED;
//			int qid = i;
//			int step = 0;
//			int sender = my_node_.get_local_rank();
//			int recver = i;
//			vector<ACTOR_T> chains;
//			chains.push_back(ACTOR_T::HW);
//			SArray<char> data;
//			data.push_back(48+my_node_.get_local_rank());
//			data.push_back(48+i);
//			Message msg = CreateMessage(type, qid, step, sender, recver,chains, data);
//			mailbox->Send(0,msg);
//		}


		thread recv(&Worker::RecvRequest, this);
		Monitor * monitor = new Monitor(my_node_);
		monitor->Start();

		//actor driver starts
		ActorAdapter * actor_adapter = new ActorAdapter(config_, my_node_, mailbox);
		actor_adapter->Start();

		actor_adapter->Stop();
		monitor->Stop();
		recv.join();
	}

private:
	Node & my_node_;
	Config * config_;
	vector<Node> & workers_;
	Parser* parser_;

	zmq::context_t context_;
	zmq::socket_t * receiver_;
};



#endif /* WORKER_HPP_ */
