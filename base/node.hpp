/*
 * node.hpp
 *
 *  Created on: May 10, 2018
 *      Author: Hongzhi Chen
 *  Modified on Nov 2018, by Chenghuan Huang
 */
//
#ifndef NODE_HPP_
#define NODE_HPP_

#include <string>
#include <sstream>

#include <mpi.h>
#include "base/serialization.hpp"
#include <iostream>

using namespace std;

class Node {
public:
	MPI_Comm local_comm;
	string hostname;
	string ibname;
	int tcp_port;
	int rdma_port;

	Node():world_rank_(0), world_size_(0), local_rank_(0), local_size_(0), color_(0){}

	int get_world_rank()
	{
		return world_rank_;
	}

	void set_world_rank(int world_rank)
	{
		world_rank_ = world_rank;
	}

	int get_world_size()
	{
		return world_size_;
	}

	void set_world_size(int world_size)
	{
		world_size_ = world_size;
	}

	int get_local_rank()
	{
		return local_rank_;
	}

	void set_local_rank(int local_rank)
	{
		local_rank_ = local_rank;
	}

	int get_local_size()
	{
		return local_size_;
	}

	void set_local_size(int local_size)
	{
		local_size_ = local_size;
	}

	int get_color()
	{
		return color_;
	}

	void set_color(int color)
	{
		color_ = color;
	}

	std::string DebugString() const {
		std::stringstream ss;
		ss << "Node: { world_rank = " << world_rank_ << " world_size = " << world_size_ << " local_rank = "
				<< local_rank_ << " local_size = " << local_size_ << " color = " << color_
				<< " hostname = " << hostname << " ibname = " << ibname << "}" << endl;
		return ss.str();
	}

	////<Nov 2018, Chenghuan Huang>
	//if not parameter are given, then it do not modify the static instance
	//yet another deep dark fantasy of magical C++
	static Node StaticInstance(Node* p = NULL)
	{
		static Node* static_instance_p_ = NULL;
		if(p)
		{
			if(static_instance_p_)
				delete static_instance_p_;
			static_instance_p_ = new Node;
			*static_instance_p_ = *p;
		}
			
		assert(static_instance_p_ != NULL);
		return *static_instance_p_;
	}

	//sequential debug print among local_comm
	void LocalSequentialDebugPrint(string s)
	{
		for(int i = 0; i < local_size_; i++)
		{
			if(i == local_rank_)
			{
				cout<<"local_rank_ == "<<local_rank_<<"  "<<s<<endl;
				fflush(stdout);
			}
			MPI_Barrier(local_comm);
		}
	}
	////</Nov 2018, Chenghuan Huang>

private:
	int world_rank_;
	int world_size_;
	int local_rank_;
	int local_size_;
	int color_;
	
};

#endif /* NODE_HPP_ */
