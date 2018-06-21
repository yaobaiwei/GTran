/*
 * hoster.hpp
 *
 *  Created on: Jun 21, 2018
 *      Author: Hongzhi Chen
 */

#ifndef HOSTER_HPP_
#define HOSTER_HPP_

#include "base/node.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"

class Hoster{
public:
	Hoster(Node & node, Config * config): node_(node), config_(config){}

	void Start(){

	}

private:
	Node & node_;
	Config * config_;
};

#endif /* HOSTER_HPP_ */
