/*
 * kvstore.hpp
 *
 *  Created on: May 30, 2018
 *      Author: Hongzhi Chen
 */

#ifndef KVSTORE_HPP_
#define KVSTORE_HPP_

#include "core/buffer.hpp"

//TODO implement it
class KVStore{
public:
	KVStore(){};
	KVStore(char * buf, uint64_t size):  buffer_(buf), size_(size){}

	void Init(){
		memset(buffer_, 0, size_);
	}

private:
	static const int NUM_LOCKS = 1024;
	static const int ASSOCIATIVITY = 8;

	char * buffer_;
	uint64_t size_;
};

#endif /* KVSTORE_HPP_ */
