/*
 * node.hpp
 *
 *  Created on: May 10, 2018
 *      Author: Hongzhi Chen
 */
//
#ifndef NODE_HPP_
#define NODE_HPP_

#include <string>
#include <sstream>

#include <mpi.h>
#include "base/serialization.hpp"

using namespace std;

struct Node {
public:
	MPI_Comm local_comm;
	char hostname[MPI_MAX_PROCESSOR_NAME];

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
				<< local_rank_ << " local_size = " << local_size_ << " color = " << color_ << " hostname = " << hostname << " }" << endl;
		return ss.str();
	}

private:
	int world_rank_;
	int world_size_;
	int local_rank_;
	int local_size_;
	int color_;
};

#endif /* NODE_HPP_ */
