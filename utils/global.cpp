//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen

#include "utils/global.hpp"

int _my_rank;
int _num_nodes;

void init_worker(int * argc, char*** argv)
{
	int provided;
	MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
	if(provided != MPI_THREAD_MULTIPLE)
	{
		printf("MPI do not Support Multiple thread\n");
		exit(0);
	}
	MPI_Comm_size(MPI_COMM_WORLD, &_num_nodes);
	MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
}

void worker_finalize()
{
	MPI_Finalize();
}

void worker_barrier()
{
	MPI_Barrier(MPI_COMM_WORLD);
}

//============================

void mk_dir(const char *dir)
{
	char tmp[256];
	char *p = NULL;
	size_t len;

	snprintf(tmp, sizeof(tmp), "%s", dir);
	len = strlen(tmp);
	if(tmp[len - 1] == '/') tmp[len - 1] = '\0';
	for (p = tmp + 1; *p; p++)
	{
		if (*p == '/')
		{
			*p = 0;
			mkdir(tmp, S_IRWXU);
			*p = '/';
		}
	}
	mkdir(tmp, S_IRWXU);
}

void rm_dir(string path)
{
	DIR* dir = opendir(path.c_str());
	struct dirent * file;
	while ((file = readdir(dir)) != NULL)
	{
		if(strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0)
			continue;
		string filename = path + "/" + file->d_name;
		remove(filename.c_str());
	}
	if (rmdir(path.c_str()) == -1)
	{
		perror ("The following error occurred");
		exit(-1);
	}
}

void check_dir(string path, bool force_write)
{
	if(access(path.c_str(), F_OK) == 0 )
	{
		if (force_write)
		{
			rm_dir(path);
			mk_dir(path.c_str());
		}
		else
		{
			cout << path <<  " already exists!" << endl;
			exit(-1);
		}
	}
	else
	{
		mk_dir(path.c_str());
	}
}

//=========================================================
void load_config(Config & config)
{
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
	if(strcmp(str, str_not_found)!=0) config.HDFS_HOST_ADDRESS = str;
	else
	{
		fprintf(stderr, "must enter the HDFS_HOST_ADDRESS. exits.\n");
		exit(-1);
	}

	val = iniparser_getint(ini, "HDFS:HDFS_PORT", val_not_found);
	if(val!=val_not_found) config.HDFS_PORT=val;
	else
	{
		fprintf(stderr, "must enter the HDFS_PORT. exits.\n");
		exit(-1);
	}

	str = iniparser_getstring(ini, "HDFS:HDFS_INPUT_PATH", str_not_found);
	if(strcmp(str, str_not_found)!=0) config.HDFS_INPUT_PATH=val;
	else
	{
		fprintf(stderr, "must enter the HDFS_INPUT_PATH. exits.\n");
		exit(-1);
	}

	str = iniparser_getstring(ini, "HDFS:HDFS_OUTPUT_PATH", str_not_found);
	if(strcmp(str, str_not_found)!=0) config.HDFS_OUTPUT_PATH=val;
	else
	{
		fprintf(stderr, "must enter the HDFS_INPUT_PATH. exits.\n");
		exit(-1);
	}

	//[SYSTEM]
	val = iniparser_getint(ini, "SYSTEM:NUM_MACHINES", val_not_found);
	if(val!=val_not_found) config.global_num_machines=val;
	else
	{
		fprintf(stderr, "must enter the NUM_MACHINES. exits.\n");
		exit(-1);
	}

	val = iniparser_getint(ini, "SYSTEM:NUM_THREADS", val_not_found);
	if(val!=val_not_found) config.global_num_threads=val;
	else
	{
		fprintf(stderr, "must enter the NUM_THREADS. exits.\n");
		exit(-1);
	}

	val = iniparser_getint(ini, "SYSTEM:VTX_SZ_GB", val_not_found);
	if(val!=val_not_found) config.global_vertex_nodes_sz_gb=val;
	else
	{
		fprintf(stderr, "must enter the VTX_SZ_GB. exits.\n");
		exit(-1);
	}

	val = iniparser_getint(ini, "SYSTEM:EDGE_SZ_GB", val_not_found);
	if(val!=val_not_found) config.global_edge_nodes_sz_gb=val;
	else
	{
		fprintf(stderr, "must enter the EDGE_SZ_GB. exits.\n");
		exit(-1);
	}

	val = iniparser_getint(ini, "SYSTEM:VTX_P_KV_SZ_GB", val_not_found);
	if(val!=val_not_found) config.global_vertex_property_kv_sz_gb=val;
	else
	{
		fprintf(stderr, "must enter the VTX_P_KV_SZ_GB. exits.\n");
		exit(-1);
	}

	val = iniparser_getint(ini, "SYSTEM:EDGE_P_KV_SZ_GB", val_not_found);
	if(val!=val_not_found) config.global_edge_property_kv_sz_gb=val;
	else
	{
		fprintf(stderr, "must enter the EDGE_P_KV_SZ_GB. exits.\n");
		exit(-1);
	}

	val = iniparser_getint(ini, "SYSTEM:PER_SEND_BUF_SZ_MB", val_not_found);
	if(val!=val_not_found) config.global_per_send_buffer_sz_mb=val;
	else
	{
		fprintf(stderr, "must enter the PER_SEND_BUF_SZ_MB. exits.\n");
		exit(-1);
	}

	val = iniparser_getint(ini, "SYSTEM:PER_RECV_BUF_SZ_MB", val_not_found);
	if(val!=val_not_found) config.global_per_recv_buffer_sz_mb=val;
	else
	{
		fprintf(stderr, "must enter the PER_RECV_BUF_SZ_MB. exits.\n");
		exit(-1);
	}

	val = iniparser_getboolean(ini, "SYSTEM:USE_RDMA", val_not_found);
	if(val!=val_not_found) config.global_use_rdma=val;
	else
	{
		fprintf(stderr, "must enter the USE_RDMA. exits.\n");
		exit(-1);
	}

	val = iniparser_getboolean(ini, "SYSTEM:ENABLE_CACHE", val_not_found);
	if(val!=val_not_found) config.global_enable_caching=val;
	else
	{
		fprintf(stderr, "must enter the ENABLE_CACHE. exits.\n");
		exit(-1);
	}

	val = iniparser_getboolean(ini, "SYSTEM:ENABLE_STEALING", val_not_found);
	if(val!=val_not_found) config.global_enable_workstealing=val;
	else
	{
		fprintf(stderr, "must enter the ENABLE_STEALING. exits.\n");
		exit(-1);
	}

	iniparser_freedict(ini);
}

//=====================================================================
