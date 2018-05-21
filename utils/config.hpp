/*
 * config.hpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */

#ifndef CONFIG_HPP_
#define CONFIG_HPP_

#include <cstdint>
#include <string>
#include "utils/unit.hpp"
#include "storage/layout.hpp"

using namespace std;

struct Config{
	//immutable_config
	//============================HDFS Parameters==========================
	string HDFS_HOST_ADDRESS;
	int HDFS_PORT;
	string HDFS_INPUT_PATH;

	string HDFS_INDEX_PATH;

	string HDFS_VTX_SUBFOLDER;
	string HDFS_EDGE_SUBFOLDER;
	string HDFS_VP_SUBFOLDER;
	string HDFS_EP_SUBFOLDER;

	string HDFS_OUTPUT_PATH;
	//==========================System Parameters==========================
	int global_num_machines;
	int global_num_threads;

	// vertex_nodes_sz = num_vertex_nodes * sz_per_vertex_node (fixed value. bytes)
	int global_vertex_nodes_sz_gb;

	// edge_nodes_sz = num_edge_nodes * sz_per_vertex_node (fixed value. bytes)
	int global_edge_nodes_sz_gb;

	// TODO: the structure of property is not decided yet
	int global_vertex_property_kv_sz_gb;
	int global_edge_property_kv_sz_gb;

	// TODO: send_buffer_sz should be equal or less than recv_buffer_sz
	// per send buffer should be exactly ONE msg size
	int global_per_send_buffer_sz_mb;

	// per recv buffer should be able to contain up to N msg
	int global_per_recv_buffer_sz_mb;

	bool global_use_rdma;
	bool global_enable_caching;
	bool global_enable_workstealing;


	//================================================================
	//mutable_config
	//maps to vertex_nodes_sz_gb
	uint32_t max_num_vertex_node;

	//maps to edge_nodes_sz_gb
	uint32_t max_num_edge_node;

	//maps to global_vertex_property_kv_sz_gb
	uint32_t max_num_vertex_property;

	//maps to global_edge_property_kv_sz_gb
	uint32_t max_num_edge_property;

	// data_store = vertex_nodes_sz + edge_nodes_sz + vertex_property_kv_sz + edge_property_kv_sz
	uint64_t datastore_sz;
	uint64_t datastore_offset;

	// send_buffer_sz = num_threads * global_per_send_buffer_sz_mb
	int send_buffer_sz;
	// send_buffer_offset = data_store_sz
	uint64_t send_buffer_offset;

	// recv_buffer_sz = num_machines * global_per_recv_buffer_sz_mb
	int recv_buffer_sz;
	// recv_buffer_offset = global_datastore_sz_gb + send_buffer_sz_mb
	uint64_t recv_buffer_offset;

	// buffer_sz = data_store_sz + send_buffer_sz + recv_buffer_sz
	uint64_t buffer_sz;


	//================================================================
	//settle down after data loading
	uint64_t datastore;
	uint64_t send_buf;
	uint64_t recv_buf;

	uint32_t num_vertex_node;
	uint32_t num_edge_node;
	uint32_t num_vertex_property;
	uint32_t num_edge_property;

    // how to partition INT(UINT32_T) into v_id / e_id / p_id
	uint32_t VID_EID;
	uint32_t EID_PID;

    // info of machines
    Node my_node;
    vector<Node> nodes;

    //TODO
    void set_nodes_config(){

    }

    void set_more(){
    	max_num_vertex_node = GiB2B(global_vertex_nodes_sz_gb) / sizeof(Vertex);
    	max_num_vertex_node = GiB2B(global_edge_nodes_sz_gb) / sizeof(Edge);
    	max_num_vertex_property = GiB2B(global_vertex_property_kv_sz_gb) / sizeof(Property_KV);
    	max_num_edge_property = GiB2B(global_edge_property_kv_sz_gb) / sizeof(Property_KV);

    	datastore_sz = GiB2B(global_vertex_nodes_sz_gb) +  GiB2B(global_edge_nodes_sz_gb) +
    					GiB2B(global_vertex_property_kv_sz_gb) + GiB2B(global_edge_property_kv_sz_gb);
    	datastore_offset = 0;

    	send_buffer_sz = global_num_threads * MiB2B(global_per_send_buffer_sz_mb);
    	send_buffer_offset = datastore_offset + datastore_sz;

    	recv_buffer_sz = global_num_machines * MiB2B(global_per_recv_buffer_sz_mb);
    	recv_buffer_offset = send_buffer_offset + send_buffer_sz;

    	buffer_sz = datastore_sz + send_buffer_sz + recv_buffer_sz;
    }

};

#endif /* CONFIG_HPP_ */
