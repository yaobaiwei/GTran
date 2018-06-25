/*
 * config.hpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */
//
#ifndef CONFIG_HPP_
#define CONFIG_HPP_

#include <cstdint>
#include <string>
#include "utils/unit.hpp"
#include "utils/hdfs_core.hpp"
#include "glog/logging.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "utils/iniparser/iniparser.h"

#ifdef __cplusplus
}
#endif

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

	string HDFS_PTY_TYPE_PATH;

	string HDFS_OUTPUT_PATH;
	//==========================System Parameters==========================
	int global_num_machines;
	int global_num_threads;


	int global_vertex_property_kv_sz_gb;
	int global_edge_property_kv_sz_gb;


	// send_buffer_sz should be equal or less than recv_buffer_sz
	// per send buffer should be exactly ONE msg size
	int global_per_send_buffer_sz_mb;


	// per recv buffer should be able to contain up to N msg
	int global_per_recv_buffer_sz_mb;


	bool global_use_rdma;
	bool global_enable_caching;
	bool global_enable_workstealing;


	//================================================================
	//mutable_config

	// kvstore = vertex_property_kv_sz + edge_property_kv_sz
	uint64_t kvstore_sz;
	uint64_t kvstore_offset;

	// send_buffer_sz = num_threads * global_per_send_buffer_sz_mb
	uint64_t send_buffer_sz;
	// send_buffer_offset = data_store_sz
	uint64_t send_buffer_offset;

	// recv_buffer_sz = num_machines * global_per_recv_buffer_sz_mb
	uint64_t recv_buffer_sz;
	// recv_buffer_offset = global_datastore_sz_gb + send_buffer_sz_mb
	uint64_t recv_buffer_offset;

	// buffer_sz = kvstore_sz + send_buffer_sz + recv_buffer_sz
	uint64_t buffer_sz;


	//================================================================
	//settle down after data loading
	char * kvstore;
	char * send_buf;
	char * recv_buf;

	uint32_t num_vertex_node;
	uint32_t num_edge_node;
	uint32_t num_vertex_property;
	uint32_t num_edge_property;

    void Init(){
    	dictionary *ini;
		int val, val_not_found = -1;
		char *str, *str_not_found = "null";

		const char* GQUERY_HOME = getenv("GQUERY_HOME");
		if(GQUERY_HOME == NULL)
		{
			fprintf(stderr, "must conf the ENV: GQUERY_HOME. exits.\n");
			exit(-1);
		}
		string conf_path(GQUERY_HOME);
		conf_path.append("/gquery-conf.ini");
		ini = iniparser_load(conf_path.c_str());
		if(ini == NULL)
		{
			fprintf(stderr, "can not open %s. exits.\n", "gquery-conf.ini");
			exit(-1);
		}

		// [HDFS]
		str = iniparser_getstring(ini,"HDFS:HDFS_HOST_ADDRESS", str_not_found);
		if(strcmp(str, str_not_found)!=0) HDFS_HOST_ADDRESS = str;
		else
		{
			fprintf(stderr, "must enter the HDFS_HOST_ADDRESS. exits.\n");
			exit(-1);
		}

		val = iniparser_getint(ini, "HDFS:HDFS_PORT", val_not_found);
		if(val!=val_not_found) HDFS_PORT=val;
		else
		{
			fprintf(stderr, "must enter the HDFS_PORT. exits.\n");
			exit(-1);
		}

		str = iniparser_getstring(ini, "HDFS:HDFS_INPUT_PATH", str_not_found);
		if(strcmp(str, str_not_found)!=0) HDFS_INPUT_PATH=str;
		else
		{
			fprintf(stderr, "must enter the HDFS_INPUT_PATH. exits.\n");
			exit(-1);
		}

		str = iniparser_getstring(ini, "HDFS:HDFS_INDEX_PATH", str_not_found);
		if(strcmp(str, str_not_found)!=0) HDFS_INDEX_PATH=str;
		else
		{
			fprintf(stderr, "must enter the HDFS_INDEX_PATH. exits.\n");
			exit(-1);
		}

		str = iniparser_getstring(ini, "HDFS:HDFS_VTX_SUBFOLDER", str_not_found);
		if(strcmp(str, str_not_found)!=0) HDFS_VTX_SUBFOLDER=str;
		else
		{
			fprintf(stderr, "must enter the HDFS_VTX_SUBFOLDER. exits.\n");
			exit(-1);
		}

		str = iniparser_getstring(ini, "HDFS:HDFS_EDGE_SUBFOLDER", str_not_found);
		if(strcmp(str, str_not_found)!=0) HDFS_EDGE_SUBFOLDER=str;
		else
		{
			fprintf(stderr, "must enter the HDFS_EDGE_SUBFOLDER. exits.\n");
			exit(-1);
		}

		str = iniparser_getstring(ini, "HDFS:HDFS_VP_SUBFOLDER", str_not_found);
		if(strcmp(str, str_not_found)!=0) HDFS_VP_SUBFOLDER=str;
		else
		{
			fprintf(stderr, "must enter the HDFS_VP_SUBFOLDER. exits.\n");
			exit(-1);
		}

		str = iniparser_getstring(ini, "HDFS:HDFS_EP_SUBFOLDER", str_not_found);
		if(strcmp(str, str_not_found)!=0) HDFS_EP_SUBFOLDER=str;
		else
		{
			fprintf(stderr, "must enter the HDFS_EP_SUBFOLDER. exits.\n");
			exit(-1);
		}

		str = iniparser_getstring(ini, "HDFS:HDFS_PTY_TYPE_PATH", str_not_found);
		if(strcmp(str, str_not_found)!=0) HDFS_PTY_TYPE_PATH=str;
		else
		{
			fprintf(stderr, "must enter the HDFS_PTY_TYPE_PATH. exits.\n");
			exit(-1);
		}

		str = iniparser_getstring(ini, "HDFS:HDFS_OUTPUT_PATH", str_not_found);
		if(strcmp(str, str_not_found)!=0) HDFS_OUTPUT_PATH=str;
		else
		{
			fprintf(stderr, "must enter the HDFS_OUTPUT_PATH. exits.\n");
			exit(-1);
		}

		//[SYSTEM]
		val = iniparser_getint(ini, "SYSTEM:NUM_WORKER_NODES", val_not_found);
		if(val!=val_not_found) global_num_machines=val;
		else
		{
			fprintf(stderr, "must enter the NUM_MACHINES. exits.\n");
			exit(-1);
		}

		val = iniparser_getint(ini, "SYSTEM:NUM_THREADS", val_not_found);
		if(val!=val_not_found) global_num_threads=val;
		else
		{
			fprintf(stderr, "must enter the NUM_THREADS. exits.\n");
			exit(-1);
		}


		val = iniparser_getint(ini, "SYSTEM:VTX_P_KV_SZ_GB", val_not_found);
		if(val!=val_not_found) global_vertex_property_kv_sz_gb=val;
		else
		{
			fprintf(stderr, "must enter the VTX_P_KV_SZ_GB. exits.\n");
			exit(-1);
		}

		val = iniparser_getint(ini, "SYSTEM:EDGE_P_KV_SZ_GB", val_not_found);
		if(val!=val_not_found) global_edge_property_kv_sz_gb=val;
		else
		{
			fprintf(stderr, "must enter the EDGE_P_KV_SZ_GB. exits.\n");
			exit(-1);
		}

		val = iniparser_getint(ini, "SYSTEM:PER_SEND_BUF_SZ_MB", val_not_found);
		if(val!=val_not_found) global_per_send_buffer_sz_mb=val;
		else
		{
			fprintf(stderr, "must enter the PER_SEND_BUF_SZ_MB. exits.\n");
			exit(-1);
		}

		val = iniparser_getint(ini, "SYSTEM:PER_RECV_BUF_SZ_MB", val_not_found);
		if(val!=val_not_found) global_per_recv_buffer_sz_mb=val;
		else
		{
			fprintf(stderr, "must enter the PER_RECV_BUF_SZ_MB. exits.\n");
			exit(-1);
		}

		val = iniparser_getboolean(ini, "SYSTEM:USE_RDMA", val_not_found);
		if(val!=val_not_found) global_use_rdma=val;
		else
		{
			fprintf(stderr, "must enter the USE_RDMA. exits.\n");
			exit(-1);
		}

		val = iniparser_getboolean(ini, "SYSTEM:ENABLE_CACHE", val_not_found);
		if(val!=val_not_found) global_enable_caching=val;
		else
		{
			fprintf(stderr, "must enter the ENABLE_CACHE. exits.\n");
			exit(-1);
		}

		val = iniparser_getboolean(ini, "SYSTEM:ENABLE_STEALING", val_not_found);
		if(val!=val_not_found) global_enable_workstealing=val;
		else
		{
			fprintf(stderr, "must enter the ENABLE_STEALING. exits.\n");
			exit(-1);
		}

		iniparser_freedict(ini);


    	kvstore_sz = GiB2B(global_vertex_property_kv_sz_gb) + GiB2B(global_edge_property_kv_sz_gb);
    	kvstore_offset = 0;

    	send_buffer_sz = global_num_threads * MiB2B(global_per_send_buffer_sz_mb);
    	send_buffer_offset = kvstore_offset + kvstore_sz;

    	recv_buffer_sz = global_num_machines * MiB2B(global_per_recv_buffer_sz_mb);
    	recv_buffer_offset = send_buffer_offset + send_buffer_sz;

    	buffer_sz = kvstore_sz + send_buffer_sz + recv_buffer_sz;

    	//init hdfs
    	hdfs_init(HDFS_HOST_ADDRESS, HDFS_PORT);

    	LOG(INFO) << DebugString();
    }

    string DebugString() const {
    	std::stringstream ss;
    	ss << "HDFS_HOST_ADDRESS : " << HDFS_HOST_ADDRESS << endl;
    	ss << "HDFS_PORT : " << HDFS_PORT << endl;
    	ss << "HDFS_INPUT_PATH : " << HDFS_INPUT_PATH << endl;
    	ss << "HDFS_INDEX_PATH : " << HDFS_INDEX_PATH << endl;
    	ss << "HDFS_VTX_SUBFOLDER : " << HDFS_VTX_SUBFOLDER << endl;
    	ss << "HDFS_EDGE_SUBFOLDER : " << HDFS_EDGE_SUBFOLDER << endl;
    	ss << "HDFS_VP_SUBFOLDER : " << HDFS_VP_SUBFOLDER << endl;
    	ss << "HDFS_EP_SUBFOLDER : " << HDFS_EP_SUBFOLDER << endl;
    	ss << "HDFS_OUTPUT_PATH : " << HDFS_OUTPUT_PATH << endl;

    	ss << "global_num_machines : " << global_num_machines << endl;
    	ss << "global_num_threads : " << global_num_threads << endl;

    	ss << "global_vertex_property_kv_sz_gb : " << global_vertex_property_kv_sz_gb << endl;
    	ss << "global_edge_property_kv_sz_gb : " << global_edge_property_kv_sz_gb << endl;
    	ss << "global_per_send_buffer_sz_mb : " << global_per_send_buffer_sz_mb << endl;
    	ss << "global_per_recv_buffer_sz_mb : " << global_per_recv_buffer_sz_mb << endl;

    	ss << "global_use_rdma : " << global_use_rdma << endl;
    	ss << "global_enable_caching : " << global_enable_caching << endl;
    	ss << "global_enable_workstealing : " << global_enable_workstealing << endl;

    	return ss.str();
    }
};

#endif /* CONFIG_HPP_ */
