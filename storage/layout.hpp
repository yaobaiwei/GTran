/*
 * layout.hpp
 *
 *  Created on: May 10, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include <cstdint>
#include <vector>

#include "utils/type.hpp"

using namespace std;

struct Vertex {
	vid_t id;
	label_t label;
	vector<vid_t> in_nbs;
	vector<vid_t> out_nbs;
	vector<label_t> vp_list;
};

struct Edge {
//	vid_t v_1;
//	vid_t v_2;
	eid_t id;
	label_t label;
	vector<label_t> ep_list;
};

struct V_KVpair {
	vpid_t key;
	value_t value;
};

struct VProperty{
	vid_t id;
	vector<V_KVpair> plist;
};

struct E_KVpair {
	epid_t key;
	value_t value;
};

struct EProperty {
//	vid_t v_1;
//	vid_t v_2;
	eid_t id;
	vector<E_KVpair> plist;
};




