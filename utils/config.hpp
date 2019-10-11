/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#ifndef CONFIG_HPP_
#define CONFIG_HPP_

#include <cstdint>
#include <string>
#include "utils/unit.hpp"
#include "utils/hdfs_core.hpp"
#include "glog/logging.h"
#include <core/common.hpp>

#ifdef __cplusplus
extern "C" {
#endif

#include "utils/iniparser/iniparser.h"

#ifdef __cplusplus
}
#endif

class Config{
    // immutable_config
    // ============================HDFS Parameters==========================

 private:
    Config(const Config&);  // not to def
    Config& operator=(const Config&);  // not to def

    Config() {}

 public:
    static Config* GetInstance() {
        static Config config_single_instance;
        return &config_single_instance;
    }

    string HDFS_HOST_ADDRESS;
    int HDFS_PORT;
    string HDFS_INPUT_PATH;

    string HDFS_INDEX_PATH;

    string HDFS_VTX_SUBFOLDER;
    string HDFS_VP_SUBFOLDER;
    string HDFS_EP_SUBFOLDER;

    string HDFS_OUTPUT_PATH;

    string SNAPSHOT_PATH;  // if can be left to blank

    // ==========================System Parameters==========================
    ISOLATION_LEVEL isolation_level;
    int global_num_workers;
    int global_num_threads;
    int num_gc_consumer;
    int num_parser_threads;

    // Thread id for using one-sided RDMA outside the thread pool of ActorAdapter
    static const int main_thread_tid = 0;
    static const int process_allocated_ts_tid = 1;
    static const int recv_notification_tid = 2;
    static const int update_min_bt_tid = 3;
    static const int perform_calibration_tid = 4;

    static const int extra_rdma_rc_thread_count = 5;

    // Count of extra RDMA send-buf in RDMAMainbox, for:
    // 1. Worker::Start
    // 2. Worker::ProcessAllocatedTimestamp
    // 3. Worker::RecvNotification
    static const int extra_send_buf_count = 3;

    int global_vertex_property_kv_sz_gb;
    int global_edge_property_kv_sz_gb;

    // memory pool elememt count
    int global_ve_row_pool_size;
    int global_vp_row_pool_size;
    int global_ep_row_pool_size;
    int global_v_mvcc_pool_size;
    int global_e_mvcc_pool_size;
    int global_vp_mvcc_pool_size;
    int global_ep_mvcc_pool_size;

    // enable recording the utilization of memory pool
    bool global_enable_mem_pool_utilization_record;

    // Predict the usage of MemPool and ValueStore before filling container
    bool predict_container_usage;

    // send_buffer_sz should be equal or less than recv_buffer_sz
    // per send buffer should be exactly ONE msg size
    int global_per_send_buffer_sz_mb;


    // per recv buffer should be able to contain up to N msg
    int global_per_recv_buffer_sz_mb;

    // transaction table
    int trx_table_sz_mb;

    // read the configuration from gquery-ini
    bool global_use_rdma;
    bool global_enable_caching;
    bool global_enable_core_binding;
    bool global_enable_actor_division;
    bool global_enable_step_reorder;
    bool global_enable_indexing;
    bool global_enable_workstealing;
    bool global_enable_garbage_collect;
    bool global_enable_opt_preread;
    bool global_enable_opt_validation;


    int max_data_size;

    int Erase_V_Task_THRESHOLD;
    int Erase_OUTE_Task_THRESHOLD;
    int Erase_INE_Task_THRESHOLD;
    int VMVCC_GC_Task_THRESHOLD;
    int VPMVCC_GC_Task_THRESHOLD;
    int EPMVCC_GC_Task_THRESHOLD;
    int EMVCC_GC_Task_THRESHOLD;
    int Topo_Row_GC_Task_THRESHOLD;
    int Topo_Row_Defrag_Task_THRESHOLD;
    int VP_Row_GC_Task_THRESHOLD;
    int VP_Row_Defrag_Task_THRESHOLD;
    int EP_Row_GC_Task_THRESHOLD;
    int EP_Row_Defrag_Task_THRESHOLD;
    int Topo_Index_GC_RATIO;
    // TopoIndexTask Threshold is always 1 to make sure
    // the task will be pushed out immediately once created
    const int Topo_Index_GC_Task_THRESHOLD = 1;
    int Prop_Index_GC_RATIO;
    int Prop_Index_GC_Task_THRESHOLD;
    int RCT_GC_Task_THRESHOLD;

    // ================================================================
    // mutable_config

    // kvstore = vertex_property_kv_sz + edge_property_kv_sz
    uint64_t kvstore_sz;
    uint64_t kvstore_offset;

    // send_buffer_sz = (num_threads + 1) * global_per_send_buffer_sz_mb
    // one more thread for worker SendQueryMsg thread
    uint64_t send_buffer_sz;
    // send_buffer_offset = kvstore_sz + kvstore_offset
    uint64_t send_buffer_offset;

    // recv_buffer_sz = (num_machines - 1) * num_threads *global_per_recv_buffer_sz_mb
    uint64_t recv_buffer_sz;
    // recv_buffer_offset = send_buffer_sz + send_buffer_offset
    uint64_t recv_buffer_offset;

    // local_head_buffer_sz = (num_machines - 1) * num_threads *sizeof(uint64_t)
    uint64_t local_head_buffer_sz;
    // local_head_buffer_offset = recv_buffer_sz + recv_buffer_offset
    uint64_t local_head_buffer_offset;

    // remote_head_buffer_sz = (num_machines - 1) * num_threads *sizeof(uint64_t)
    uint64_t remote_head_buffer_sz;
    // remote_head_buffer_offset = local_head_buffer_sz + local_head_buffer_offset
    uint64_t remote_head_buffer_offset;

    // Always be 64 * num_workers.
    uint64_t min_bt_buffer_sz;
    // min_bt_buffer_offset = remote_head_buffer_sz + remote_head_buffer_offset
    uint64_t min_bt_buffer_offset;

    // For timestamp calibration
    uint64_t ts_sync_buffer_sz;
    uint64_t ts_sync_buffer_offset;

    // transaction status table on master
    uint64_t trx_table_sz;
    uint64_t trx_table_offset;
    uint64_t ASSOCIATIVITY = 8;
    uint64_t MI_RATIO = 80;
    uint64_t trx_num_total_buckets;
    uint64_t trx_num_main_buckets;
    uint64_t trx_num_indirect_buckets;
    uint64_t trx_num_slots;

    // two sided send buffer sz = global_per_send_buffer_sz_mb
    uint64_t dgram_send_buffer_sz;
    // two sided send buffer offset = remote_head_buffer_sz + remote_head_buffer_offset;
    uint64_t dgram_send_buffer_offset;

    // two sided recv buffer sz = global_per_recv_buffer_sz_mb
    uint64_t dgram_recv_buffer_sz;
    // two sided recv buffer offset = dgram_send_buffer_sz + dgram_send_buffer_offset;
    uint64_t dgram_recv_buffer_offset;

    // RDMA mem for RC QP(read/write)
    // conn_buf_sz = kvstore_sz + send_buffer_sz + recv_buffer_sz + local_head_buffer_sz +remote_head_buffer_sz
    uint64_t conn_buf_sz;
    // RDMA mem for UD QP(read/write)
    // dgram_buf_sz = dgram_send_buffer_sz + dgram_recv_buffer_sz
    uint64_t dgram_buf_sz;
    // buffer_sz =  conn_buf_sz + dgram_buf_sz
    uint64_t buffer_sz;


    // ================================================================
    // settle down after data loading
    char * kvstore;
    char * send_buf;
    char * recv_buf;
    char * local_head_buf;
    char * remote_head_buf;
    char * trx_table;
    char * dgram_send_buf;
    char * dgram_recv_buf;

    uint32_t num_vertex_node;
    uint32_t num_edge_node;
    uint32_t num_vertex_property;
    uint32_t num_edge_property;

    void Init() {
        dictionary *ini;
        int val, val_not_found = -1;
        char *str, *str_not_found = "null";

        Node node = Node::StaticInstance();

        const char* GQUERY_HOME = getenv("GQUERY_HOME");
        if (GQUERY_HOME == NULL) {
            fprintf(stderr, "must conf the ENV: GQUERY_HOME. exits.\n");
            exit(-1);
        }
        string conf_path(GQUERY_HOME);
        conf_path.append("/gquery-conf.ini");
        ini = iniparser_load(conf_path.c_str());
        if (ini == NULL) {
            fprintf(stderr, "can not open %s. exits.\n", "gquery-conf.ini");
            exit(-1);
        }

        // [HDFS]
        str = iniparser_getstring(ini, "HDFS:HDFS_HOST_ADDRESS", str_not_found);
        if (strcmp(str, str_not_found) != 0) {
            HDFS_HOST_ADDRESS = str;
        } else {
            fprintf(stderr, "must enter the HDFS_HOST_ADDRESS. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "HDFS:HDFS_PORT", val_not_found);
        if (val != val_not_found) {
            HDFS_PORT = val;
        } else {
            fprintf(stderr, "must enter the HDFS_PORT. exits.\n");
            exit(-1);
        }

        str = iniparser_getstring(ini, "HDFS:HDFS_INPUT_PATH", str_not_found);
        if (strcmp(str, str_not_found) != 0) {
            HDFS_INPUT_PATH = str;
        } else {
            fprintf(stderr, "must enter the HDFS_INPUT_PATH. exits.\n");
            exit(-1);
        }

        str = iniparser_getstring(ini, "HDFS:HDFS_INDEX_PATH", str_not_found);
        if (strcmp(str, str_not_found) != 0) {
            HDFS_INDEX_PATH = str;
        } else {
            fprintf(stderr, "must enter the HDFS_INDEX_PATH. exits.\n");
            exit(-1);
        }

        str = iniparser_getstring(ini, "HDFS:HDFS_VTX_SUBFOLDER", str_not_found);
        if (strcmp(str, str_not_found) != 0) {
            HDFS_VTX_SUBFOLDER = str;
        } else {
            fprintf(stderr, "must enter the HDFS_VTX_SUBFOLDER. exits.\n");
            exit(-1);
        }

        str = iniparser_getstring(ini, "HDFS:HDFS_VP_SUBFOLDER", str_not_found);
        if (strcmp(str, str_not_found) != 0) {
            HDFS_VP_SUBFOLDER = str;
        } else {
            fprintf(stderr, "must enter the HDFS_VP_SUBFOLDER. exits.\n");
            exit(-1);
        }

        str = iniparser_getstring(ini, "HDFS:HDFS_EP_SUBFOLDER", str_not_found);
        if (strcmp(str, str_not_found) != 0) {
            HDFS_EP_SUBFOLDER = str;
        } else {
            fprintf(stderr, "must enter the HDFS_EP_SUBFOLDER. exits.\n");
            exit(-1);
        }

        str = iniparser_getstring(ini, "HDFS:HDFS_OUTPUT_PATH", str_not_found);
        if (strcmp(str, str_not_found) != 0) {
            HDFS_OUTPUT_PATH = str;
        } else {
            fprintf(stderr, "must enter the HDFS_OUTPUT_PATH. exits.\n");
            exit(-1);
        }

        // // [SYSTEM]
        // val = iniparser_getint(ini, "SYSTEM:NUM_WORKER_NODES", val_not_found);
        // if (val != val_not_found) global_num_workers=val;
        // else
        // {
        //     fprintf(stderr, "must enter the NUM_MACHINES. exits.\n");
        //     exit(-1);
        // }

        global_num_workers = node.get_world_size() - 1;


        str = iniparser_getstring(ini, "SYSTEM:ISOLATION_LEVEL", str_not_found);

        if (strcmp(str, str_not_found) != 0) {
            string level = str;
            if (str == string("SNAPSHOT")) {
                isolation_level = ISOLATION_LEVEL::SNAPSHOT;
            } else if (str == string("SERIALIZABLE")) {
                isolation_level = ISOLATION_LEVEL::SERIALIZABLE;
            } else {
                fprintf(stderr, "ISOLATION_LEVEL of %s is not available, must be in {SNAPSHOT, SERIALIZABLE}. exits.\n",
                        str);
                exit(-1);
            }
            if (node.get_world_rank() == MASTER_RANK)
                printf("ISOLATION_LEVEL = %s\n", str);
        } else {
            fprintf(stderr, "must enter the ISOLATION_LEVEL in {SNAPSHOT, SERIALIZABLE}. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:NUM_THREADS", val_not_found);
        if (val != val_not_found) {
            global_num_threads = val;
        } else {
            fprintf(stderr, "must enter the NUM_THREADS. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:NUM_GC_CONSUMER", val_not_found);
        if (val != val_not_found) {
            num_gc_consumer = val;
        } else {
            fprintf(stderr, "must enter the NUM_GC_CONSUMER. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:NUM_PARSER_THREADS", val_not_found);
        if (val != val_not_found) {
            num_parser_threads = val;
        } else {
            fprintf(stderr, "must enter the NUM_PARSER_THREADS. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:VTX_P_KV_SZ_GB", val_not_found);
        if (val != val_not_found) {
            global_vertex_property_kv_sz_gb = val;
        } else {
            fprintf(stderr, "must enter the VTX_P_KV_SZ_GB. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:EDGE_P_KV_SZ_GB", val_not_found);
        if (val != val_not_found) {
            global_edge_property_kv_sz_gb = val;
        } else {
            fprintf(stderr, "must enter the EDGE_P_KV_SZ_GB. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:PER_SEND_BUF_SZ_MB", val_not_found);
        if (val != val_not_found) {
            global_per_send_buffer_sz_mb = val;
        } else {
            fprintf(stderr, "must enter the PER_SEND_BUF_SZ_MB. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:PER_RECV_BUF_SZ_MB", val_not_found);
        if (val != val_not_found) {
            global_per_recv_buffer_sz_mb = val;
        } else {
            fprintf(stderr, "must enter the PER_RECV_BUF_SZ_MB. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:VE_ROW_POOL_SIZE", val_not_found);
        if (val != val_not_found) {
            global_ve_row_pool_size = val;
        } else {
            fprintf(stderr, "must enter the VE_ROW_POOL_SIZE. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:VP_ROW_POOL_SIZE", val_not_found);
        if (val != val_not_found) {
            global_vp_row_pool_size = val;
        } else {
            fprintf(stderr, "must enter the VP_ROW_POOL_SIZE. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:EP_ROW_POOL_SIZE", val_not_found);
        if (val != val_not_found) {
            global_ep_row_pool_size = val;
        } else {
            fprintf(stderr, "must enter the EP_ROW_POOL_SIZE. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:V_MVCC_POOL_SIZE", val_not_found);
        if (val != val_not_found) {
            global_v_mvcc_pool_size = val;
        } else {
            fprintf(stderr, "must enter the V_MVCC_POOL_SIZE. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:E_MVCC_POOL_SIZE", val_not_found);
        if (val != val_not_found) {
            global_e_mvcc_pool_size = val;
        } else {
            fprintf(stderr, "must enter the E_MVCC_POOL_SIZE. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:VP_MVCC_POOL_SIZE", val_not_found);
        if (val != val_not_found) {
            global_vp_mvcc_pool_size = val;
        } else {
            fprintf(stderr, "must enter the VP_MVCC_POOL_SIZE. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:EP_MVCC_POOL_SIZE", val_not_found);
        if (val != val_not_found) {
            global_ep_mvcc_pool_size = val;
        } else {
            fprintf(stderr, "must enter the EP_MVCC_POOL_SIZE. exits.\n");
            exit(-1);
        }

        val = iniparser_getboolean(ini, "SYSTEM:PREDICT_CONTAINER_USAGE", val_not_found);
        if (val != val_not_found) {
            predict_container_usage = val;
        } else {
            fprintf(stderr, "must enter the PREDICT_CONTAINER_USAGE. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:TRX_TABLE_SZ_MB", val_not_found);
        if (val != val_not_found) {
            trx_table_sz_mb = val;
        } else {
            fprintf(stderr, "must enter the TRX_TABLE_SZ_MB. exits.\n");
            exit(-1);
        }

        val = iniparser_getboolean(ini, "SYSTEM:USE_RDMA", val_not_found);
        if (val != val_not_found) {
            global_use_rdma = val;
        } else {
            fprintf(stderr, "must enter the USE_RDMA. exits.\n");
            exit(-1);
        }

        val = iniparser_getboolean(ini, "SYSTEM:ENABLE_MEM_POOL_UTILIZATION_RECORD", val_not_found);
        if (val != val_not_found) {
            global_enable_mem_pool_utilization_record = val;
        } else {
            fprintf(stderr, "must enter the ENABLE_MEM_POOL_UTILIZATION_RECORD. exits.\n");
            exit(-1);
        }

        val = iniparser_getboolean(ini, "SYSTEM:ENABLE_CACHE", val_not_found);
        if (val != val_not_found) {
            global_enable_caching = val;
        } else {
            fprintf(stderr, "must enter the ENABLE_CACHE. exits.\n");
            exit(-1);
        }

        val = iniparser_getboolean(ini, "SYSTEM:ENABLE_CORE_BIND", val_not_found);
        if (val != val_not_found) {
            global_enable_core_binding = val;
        } else {
            fprintf(stderr, "must enter the ENABLE_CORE_BIND. exits.\n");
            exit(-1);
        }

        val = iniparser_getboolean(ini, "SYSTEM:ENABLE_ACTOR_DIVISION", val_not_found);
        if (val != val_not_found) {
            global_enable_actor_division = val;
        } else {
            fprintf(stderr, "must enter the ENABLE_ACTOR_DIVISION. exits.\n");
            exit(-1);
        }

        val = iniparser_getboolean(ini, "SYSTEM:ENABLE_STEP_REORDER", val_not_found);
        if (val != val_not_found) {
            global_enable_step_reorder = val;
        } else {
            fprintf(stderr, "must enter the ENABLE_ACTOR_REORDER. exits.\n");
            exit(-1);
        }

        val = iniparser_getboolean(ini, "SYSTEM:ENABLE_INDEXING", val_not_found);
        if (val != val_not_found) {
            global_enable_indexing = val;
        } else {
            fprintf(stderr, "must enter the ENABLE_INDEXING. exits.\n");
            exit(-1);
        }

        val = iniparser_getboolean(ini, "SYSTEM:ENABLE_STEALING", val_not_found);
        if (val != val_not_found) {
            global_enable_workstealing = val;
        } else {
            fprintf(stderr, "must enter the ENABLE_STEALING. exits.\n");
            exit(-1);
        }

        val = iniparser_getboolean(ini, "SYSTEM:ENABLE_GARBAGE_COLLECT", val_not_found);
        if (val != val_not_found) {
            global_enable_garbage_collect = val;
        } else {
            fprintf(stderr, "must enter the ENABLE_GARBAGE_COLLECT. exits.\n");
            exit(-1);
        }

        val = iniparser_getboolean(ini, "SYSTEM:ENABLE_OPT_PREREAD", val_not_found);
        if (val != val_not_found) {
            global_enable_opt_preread = val;
        } else {
            fprintf(stderr, "must enter the ENABLE_OPT_PREREAD. exits.\n");
            exit(-1);
        }

        val = iniparser_getboolean(ini, "SYSTEM:ENABLE_OPT_VALIDATION", val_not_found);
        if (val != val_not_found) {
            global_enable_opt_validation = val;
        } else {
            fprintf(stderr, "must enter the ENABLE_OPT_VALIDATION. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "SYSTEM:MAX_MSG_SIZE", val_not_found);
        if (val != val_not_found) {
            max_data_size = val;
        } else {
            fprintf(stderr, "must enter the MAX_MSG_SIZE. exits.\n");
            exit(-1);
        }

        str = iniparser_getstring(ini, "SYSTEM:SNAPSHOT_PATH", str_not_found);

        if (strcmp(str, str_not_found) != 0) {
            // analyse snapshot_path to absolute path
            string ori_str = str;
            string str_to_process = str;

            if (str_to_process[0] == '~') {
                string sub = str_to_process.substr(1);

                str_to_process = string(getenv("HOME")) + sub;
            }

            // SNAPSHOT_PATH = string(realpath(str_to_process.c_str(), NULL));
            // throw null pointer to a directory that do not exists
            SNAPSHOT_PATH = str_to_process;

            if (node.get_world_rank() == MASTER_RANK)
                printf("given SNAPSHOT_PATH = %s, processed = %s\n", ori_str.c_str(), SNAPSHOT_PATH.c_str());
        } else {
            // fprintf(stderr, "must enter the SNAPSHOT_PATH. exits.\n");
            // exit(-1);
            SNAPSHOT_PATH = "";
        }

        val = iniparser_getint(ini, "GC:ERASE_V_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            Erase_V_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the ERASE_V_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:ERASE_OUTE_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            Erase_OUTE_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the ERASE_OUTE_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:ERASE_INE_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            Erase_INE_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the ERASE_INE_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:VMVCC_GC_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            VMVCC_GC_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the VMVCC_GC_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:VPMVCC_GC_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            VPMVCC_GC_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the VPMVCC_GC_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:EPMVCC_GC_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            EPMVCC_GC_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the EPMVCC_GC_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:EMVCC_GC_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            EMVCC_GC_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the EMVCC_GC_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:TOPO_ROW_GC_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            Topo_Row_GC_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the TOPO_ROW_GC_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:TOPO_ROW_DEFRAG_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            Topo_Row_Defrag_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the TOPO_ROW_DEFRAG_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:VP_ROW_GC_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            VP_Row_GC_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the VP_ROW_GC_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:VP_ROW_DEFRAG_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            VP_Row_Defrag_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the VP_ROW_DEFRAG_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:EP_ROW_GC_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            EP_Row_GC_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the EP_ROW_GC_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:EP_ROW_DEFRAG_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            EP_Row_Defrag_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the EP_ROW_DEFRAG_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:TOPO_INDEX_GC_RATIO", val_not_found);
        if (val != val_not_found && val <= 100 && val >= 0) {
            Topo_Index_GC_RATIO = val;
        } else {
            fprintf(stderr, "must enter the TOPO_INDEX_GC_RATIO and value in range [0, 100]. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:PROP_INDEX_GC_RATIO", val_not_found);
        if (val != val_not_found && val <= 100 && val >= 0) {
            Prop_Index_GC_RATIO = val;
        } else {
            fprintf(stderr, "must enter the PROP_INDEX_GC_RATIO and value in range [0, 100]. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:PROP_INDEX_GC_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            Prop_Index_GC_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the PROP_INDEX_GC_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        val = iniparser_getint(ini, "GC:RCT_GC_T_THRESHOLD", val_not_found);
        if (val != val_not_found) {
            RCT_GC_Task_THRESHOLD = val;
        } else {
            fprintf(stderr, "must enter the RCT_GC_T_THRESHOLD. exits.\n");
            exit(-1);
        }

        iniparser_freedict(ini);

        trx_table_sz = MiB2B(trx_table_sz_mb);  // this should be shared by master and workers,workers need to this to compute trx_num_total_buckets...
        min_bt_buffer_sz = 64 * global_num_workers;
        if (node.get_world_rank() != MASTER_RANK) {
            // Workers
            kvstore_sz = 0;
            kvstore_offset = 0;

            // three more threads
            send_buffer_sz = (global_num_threads + extra_send_buf_count) * MiB2B(global_per_send_buffer_sz_mb);
            send_buffer_offset = kvstore_offset + kvstore_sz;

            recv_buffer_sz = (global_num_workers - 1) * global_num_threads * MiB2B(global_per_recv_buffer_sz_mb);
            recv_buffer_offset = send_buffer_offset + send_buffer_sz;

            local_head_buffer_sz = (global_num_workers - 1) * global_num_threads * sizeof(uint64_t);
            local_head_buffer_offset = recv_buffer_sz + recv_buffer_offset;

            remote_head_buffer_sz = (global_num_workers - 1) * global_num_threads * sizeof(uint64_t);
            remote_head_buffer_offset = local_head_buffer_sz + local_head_buffer_offset;

            // only one thread (GC) will read MIN_BT from master
            min_bt_buffer_offset = remote_head_buffer_sz + remote_head_buffer_offset;

            ts_sync_buffer_sz = 64;
            ts_sync_buffer_offset = min_bt_buffer_offset + min_bt_buffer_sz;

            trx_table_offset = ts_sync_buffer_sz + ts_sync_buffer_offset;

            dgram_send_buffer_sz = MiB2B(global_per_send_buffer_sz_mb);
            dgram_send_buffer_offset = trx_table_offset + trx_table_sz;

            dgram_recv_buffer_sz = MiB2B(global_per_recv_buffer_sz_mb);
            dgram_recv_buffer_offset = dgram_send_buffer_sz + dgram_send_buffer_offset;

            conn_buf_sz = kvstore_sz + send_buffer_sz + recv_buffer_sz + local_head_buffer_sz + remote_head_buffer_sz + min_bt_buffer_sz + ts_sync_buffer_sz + trx_table_sz;
            dgram_buf_sz = dgram_recv_buffer_sz + dgram_send_buffer_sz;
        }

        buffer_sz = conn_buf_sz + dgram_buf_sz;

        trx_num_total_buckets = trx_table_sz / (ASSOCIATIVITY * sizeof(TidStatus));
        trx_num_main_buckets = trx_num_total_buckets * MI_RATIO / 100;
        trx_num_indirect_buckets = trx_num_total_buckets - trx_num_main_buckets;
        trx_num_slots = trx_num_total_buckets * ASSOCIATIVITY;

        // init hdfs
        hdfs_init(HDFS_HOST_ADDRESS, HDFS_PORT);
        LOG(INFO) << "[Config] rank " << node.get_world_rank() << " " << DebugString();
    }

    string DebugString() const {
        std::stringstream ss;
        ss << "HDFS_HOST_ADDRESS : " << HDFS_HOST_ADDRESS << endl;
        ss << "HDFS_PORT : " << HDFS_PORT << endl;
        ss << "HDFS_INPUT_PATH : " << HDFS_INPUT_PATH << endl;
        ss << "HDFS_INDEX_PATH : " << HDFS_INDEX_PATH << endl;
        ss << "HDFS_VTX_SUBFOLDER : " << HDFS_VTX_SUBFOLDER << endl;
        ss << "HDFS_VP_SUBFOLDER : " << HDFS_VP_SUBFOLDER << endl;
        ss << "HDFS_EP_SUBFOLDER : " << HDFS_EP_SUBFOLDER << endl;
        ss << "HDFS_OUTPUT_PATH : " << HDFS_OUTPUT_PATH << endl;
        ss << "SNAPSHOT_PATH : " << SNAPSHOT_PATH << endl;

        ss << "kvstore_sz : " << kvstore_sz << endl;
        ss << "send_buffer_sz : " << send_buffer_sz << endl;
        ss << "recv_buffer_sz : " << recv_buffer_sz << endl;
        ss << "local_head_buffer_sz : " << local_head_buffer_sz << endl;
        ss << "remote_head_buffer_sz : " << remote_head_buffer_sz << endl;
        ss << "trx_table_sz_mb : " << trx_table_sz_mb << endl;
        ss << "dgram_send_buffer_sz : " << dgram_send_buffer_sz << endl;
        ss << "dgram_recv_buffer_sz : " << dgram_recv_buffer_sz << endl;
        ss << "trx_num_total_buckets : " << trx_num_total_buckets << endl;
        ss << "trx_num_main_buckets : " << trx_num_main_buckets << endl;
        ss << "trx_num_indirect_buckets : " << trx_num_indirect_buckets<< endl;
        ss << "trx_num_slots : " << trx_num_slots << endl;

        ss << "global_num_workers : " << global_num_workers << endl;
        ss << "global_num_threads : " << global_num_threads << endl;
        ss << "global_vertex_property_kv_sz_gb : " << global_vertex_property_kv_sz_gb << endl;
        ss << "global_edge_property_kv_sz_gb : " << global_edge_property_kv_sz_gb << endl;
        ss << "global_per_send_buffer_sz_mb : " << global_per_send_buffer_sz_mb << endl;
        ss << "global_per_recv_buffer_sz_mb : " << global_per_recv_buffer_sz_mb << endl;

        ss << "global_use_rdma : " << global_use_rdma << endl;
        ss << "global_enable_caching : " << global_enable_caching << endl;
        ss << "global_enable_core_binding : " << global_enable_core_binding << endl;
        ss << "global_enable_actor_division : " << global_enable_actor_division << endl;
        ss << "global_enable_workstealing : " << global_enable_workstealing << endl;

        return ss.str();
    }
};

#endif /* CONFIG_HPP_ */
