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
#include "storage/data_store.hpp"

class Worker{
public:
	Worker(Node & node, Config * config, string host_fname): node_(node), config_(config), host_fname_(host_fname){}

	void Start(){
		NaiveIdMapper * id_mapper = new NaiveIdMapper(config_, node_);
		id_mapper->Init();

		LOG(INFO) <<  "DONE -> NaiveIdMapper->Init()" << endl;

		//set the in-memory layout for RDMA buf
		Buffer * buf = new Buffer(config_);
		buf->Init();

		LOG(INFO) << "DONE -> Buffer->Init()" << endl;

		//init the rdma mailbox
		RdmaMailbox * mailbox = new RdmaMailbox(config_, id_mapper, buf);
		mailbox->Init(host_fname_);

		LOG(INFO) << "DONE -> RdmaMailbox->Init()" << endl;

		DataStore * datastore = new DataStore(config_, id_mapper, buf);
		datastore->Init();

		LOG(INFO) << "DONE -> DataStore->Init()" << endl;

		datastore->LoadDataFromHDFS();
		worker_barrier();

		//=======data shuffle==========
		datastore->Shuffle();
		//=======data shuffle==========

		datastore->DataConverter();
		worker_barrier();
		LOG(INFO) << "DONE -> Datastore->DataConverter()" << endl;

		//TEST
		for(int i = 0 ; i < get_num_nodes(); i++){
			MSG_T type = MSG_T::FEED;
			int qid = i;
			int step = 0;
			int sender = get_node_id();
			int recver = i;
			vector<ACTOR_T> chains;
			chains.push_back(ACTOR_T::HW);
			SArray<char> data;
			data.push_back(48+get_node_id());
			data.push_back(48+i);
			Message msg = CreateMessage(type, qid, step, sender, recver,chains, data);
			mailbox->Send(0,msg);
		}

		//actor driver starts
		ActorAdapter * actor_adapter = new ActorAdapter(config_, node_, mailbox);
		actor_adapter->Start();

		actor_adapter->Stop();
	}

private:
	Node & node_;
	Config * config_;
	string host_fname_;
};



#endif /* WORKER_HPP_ */
