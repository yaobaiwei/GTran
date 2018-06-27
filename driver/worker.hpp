/*
 * worker.hpp
 *
 *  Created on: Jun 21, 2018
 *      Author: Hongzhi Chen
 */

#ifndef WORKER_HPP_
#define WORKER_HPP_

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
#include "storage/data_store.hpp"

class Worker{
public:
	Worker(Node & node, Config * config, string host_fname): node_(node), config_(config), host_fname_(host_fname){}

	void Start(){
		NaiveIdMapper * id_mapper = new NaiveIdMapper(node_, config_);
		id_mapper->Init();

		cout <<  "DONE -> NaiveIdMapper->Init()" << endl;

		//set the in-memory layout for RDMA buf
		Buffer * buf = new Buffer(node_, config_);
		buf->Init();

		//init the rdma mailbox
		RdmaMailbox * mailbox = new RdmaMailbox(node_, config_, id_mapper, buf);
		mailbox->Init(host_fname_);

		cout << "DONE -> RdmaMailbox->Init()" << endl;

		DataStore * datastore = new DataStore(node_, config_, id_mapper, buf);
		datastore->Init();

		cout << "DONE -> DataStore->Init()" << endl;

		datastore->LoadDataFromHDFS();
		worker_barrier(node_);

		//cout << "DONE -> DataStore->LoadDataFromHDFS()" << endl;
		//=======data shuffle==========
		datastore->Shuffle();
		//cout << "DONE -> DataStore->Shuffle()" << endl;
		//=======data shuffle==========

		datastore->DataConverter();
		worker_barrier(node_);
		cout << "DONE -> Datastore->DataConverter()" << endl;

//		//TEST
//		vid_t vid(3);
//		if(id_mapper->IsVertexLocal(vid)){
//			Vertex* v = datastore->GetVertex(vid);
//			cout << "RANK: " << node_.get_local_rank() << " GET VTX.IN_NBS => ";
//			for(auto & nb : v->in_nbs){
//				cout << nb.vid << ", ";
//			}
//			cout << endl;
//		}
//
//		eid_t eid(6,3);
//		if(id_mapper->IsEdgeLocal(eid)){
//			Edge* e = datastore->GetEdge(eid);
//			cout << "RANK: " << node_.get_local_rank() << " GET EDGE.PPT_LIST => ";
//			for(auto & label : e->ep_list){
//				cout <<  label << ", ";
//			}
//			cout << endl;
//		}
//
//		vpid_t vpid(6,2);
//		value_t val;
//		datastore->GetPropertyForVertex(0, vpid, val);
//		cout << "RANK " << node_.get_local_rank() << " GET VP.VALUE => ";
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
//		cout << "RANK " << node_.get_local_rank() << " GET EP.VALUE => ";
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
//		cout << "RANK " << node_.get_local_rank() << " GET VTX LABEL => " << (char)vl << endl;
//
//		label_t el;
//		datastore->GetLabelForEdge(0, eid, el);
//		cout << "RANK " << node_.get_local_rank() << " GET EDGE LABEL => " << (char)el << endl;


		for(int i = 0 ; i < node_.get_local_size(); i++){
			MSG_T type = MSG_T::FEED;
			int qid = i;
			int step = 0;
			int sender = node_.get_local_rank();
			int recver = i;
			vector<ACTOR_T> chains;
			chains.push_back(ACTOR_T::HW);
			SArray<char> data;
			data.push_back(48+node_.get_local_rank());
			data.push_back(48+i);
			Message msg = CreateMessage(type, qid, step, sender, recver,chains, data);
			mailbox->Send(0,msg);
		}

		Monitor * monitor = new Monitor(node_);
		thread th = monitor->Start();

		//actor driver starts
		ActorAdapter * actor_adapter = new ActorAdapter(config_, node_, mailbox);
		actor_adapter->Start();

		actor_adapter->Stop();
		monitor->Stop(th);
	}

private:
	Node & node_;
	Config * config_;
	string host_fname_;
};



#endif /* WORKER_HPP_ */
