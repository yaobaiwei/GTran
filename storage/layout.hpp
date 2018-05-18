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

//const int CACHE_LINE = 64;
//const int PROPERTY_BLOCK_SIZE = 1024;

//typedef struct __attribute__((aligned (CACHE_LINE))) {
//    bool used_flag;
//    uint32_t v_1;
//    uint32_t v_2;
//    uint32_t v1_next_out;
//    uint32_t v1_next_in;
//    uint32_t v2_next_out;
//    uint32_t v2_next_in;
//    uint32_t property_id;
//} Edge;
//
//// Property block id starts from 1
//// The size should not exceed PROPERTY_BLOCK
//typedef struct __attribute__((aligned (PROPERTY_BLOCK_SIZE))) {
//    bool used_flag;
//    char payoff[1000];
//    uint32_t next_block;
//} PropertyBlock;

struct Vertex {
	vid_t id;
	vector<vid_t> in_nbs;
	vector<vid_t> out_nbs;
	label_t label;
    ptr_t p_addr;
};

struct Edge {
	vid_t v_1;
	vid_t v_2;
	label_t label;
	ptr_t p_addr;
} Edge;

struct KV{
	//uint32_t key;
	value_t value;
}Property_KV;




