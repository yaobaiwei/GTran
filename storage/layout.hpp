/*
 * layout.hpp
 *
 *  Created on: May 10, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include <cstdint>

// FOR VERTEX AND EDGE 
// __attributed__((packed)) might be worse 
// __attribute__((aligned)) could be used for better cache performance 

const int CACHE_LINE = 64;
const int PROPERTY_BLOCK_SIZE = 1024;

typedef struct Vertex {
    uint32_t next_in_rel;
    uint32_t next_out_rel;
    uint32_t next_property;
    uint32_t label;
}Vertex;

typedef struct Edge {
	uint32_t v_1;
	uint32_t v_2;
	uint32_t rel_type;
	uint32_t v_1_next_in_rel;
	uint32_t v_1_next_out_rel;
	uint32_t next_property;
} Edge;

typedef struct Property_KV{
	//uint32_t key;
	uint32_t value;
}Property_KV;

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


