/*
 * master.hpp
 *
 *  Created on: Jun 21, 2018
 *      Author: Hongzhi Chen
 */

#ifndef MASTER_HPP_
#define MASTER_HPP_

#include <map>
#include <vector>
#include <thread>
#include <stdlib.h>

#include "base/node.hpp"
#include "base/communication.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"

using namespace std;

struct Progress
{
	uint32_t num_tasks;
};

class Master{
public:
	Master(Node & node, Config * config): node_(node), config_(config){
		is_end_ = false;
	}

	void ProgListener(){
		while(1){
			vector<uint32_t> prog = recv_data<vector<uint32_t>>(node_, MPI_ANY_SOURCE, true, MONITOR_CHANNEL);
			//DEBUG
			cout << "RANK " << node_.get_world_rank() << "=> RECV PROG " << prog[0] << " / " << prog[1] << endl;

			int src = prog[0];  //the slave ID
			Progress & p = progress_map_[src];
			if(prog[1] != -1){
				p.num_tasks = prog[1];
			}else{
				progress_map_.erase(src);
			}
			if(progress_map_.size() == 0)
				break;
		}
	}

	int ProgScheduler()
	{
		//the result should not be the same worker
		int max = -1;
		int max_index = -1;
		map<int, Progress>::iterator m_iter;
		for(m_iter = progress_map_.begin(); m_iter != progress_map_.end(); m_iter++)
		{
			if(m_iter->second.num_tasks > max)
			{
				max = m_iter->second.num_tasks;
				max_index = m_iter->first;
			}
		}
		if(max_index != -1)
			return max_index;
		return rand() % node_.get_local_size() + 1;
	}

	void GetRequest(){
		while(1)
		{
			int client_id = 0;
			//zmq_recv to fill the client_id
			int target_engine_id = ProgScheduler();
			//zmq_send send #target_engine_id# to client_id
		}
	}

	void Start(){
		thread listen(&Master::ProgListener,this);
		thread response(&Master::GetRequest, this);

		int end_tag = 0;
		while(end_tag < node_.get_local_size())
		{
			int tag = recv_data<int>(node_, MPI_ANY_SOURCE, true, MSCOMMUN_CHANNEL);
			if(tag == DONE)
			{
				end_tag++;
			}
		}

		listen.join();
		response.join();
	}

private:
	Node & node_;
	Config * config_;
	map<int, Progress> progress_map_;

	bool is_end_;
};

#endif /* MASTER_HPP_ */
