/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

#include <memory>

#include "utils/config.hpp"
#include "utils/unit.hpp"

#include "glog/logging.h"


class Buffer {
public:
    Buffer(Node & node) : node_(node){
        config_ = Config::GetInstance();
		if (config_->global_use_rdma) {
			buffer_ = new char[config_->buffer_sz];
			memset(buffer_, 0, config_->buffer_sz);
			config_->kvstore = buffer_ + config_->kvstore_offset;
			config_->send_buf = buffer_ + config_->send_buffer_offset;
			config_->recv_buf = buffer_ + config_->recv_buffer_offset;
			config_->local_head_buf = buffer_ + config_->local_head_buffer_offset;
			config_->remote_head_buf = buffer_ + config_->remote_head_buffer_offset;
		} else {
			buffer_ = new char[config_->kvstore_sz];
			memset(buffer_, 0, config_->kvstore_sz);
			config_->kvstore = buffer_ + config_->kvstore_offset;
		}
    }

    ~Buffer() {
        delete[] buffer_;
    }

    inline char* GetBuf() {
        return buffer_;
    }

    inline uint64_t GetBufSize(){
    	return config_->buffer_sz;
    }

    inline uint64_t GetVPStoreSize(){
    	return GiB2B(config_->global_vertex_property_kv_sz_gb);
    }

    inline uint64_t GetVPStoreOffset(){
    	return config_->kvstore_offset;
    }

    inline uint64_t GetEPStoreSize(){
    	return GiB2B(config_->global_edge_property_kv_sz_gb);
    }

    inline uint64_t GetEPStoreOffset(){
    	return config_->kvstore_offset + GiB2B(config_->global_vertex_property_kv_sz_gb);
    }

    inline char* GetSendBuf(int index) {
		assert(config_->global_use_rdma);
        CHECK_LE(index, config_->global_num_threads);
        return config_->send_buf + index * MiB2B(config_->global_per_send_buffer_sz_mb);
    }

    inline uint64_t GetSendBufSize(){
		assert(config_->global_use_rdma);
    	return MiB2B(config_->global_per_send_buffer_sz_mb);
    }

    inline uint64_t GetSendBufOffset(int index) {
		assert(config_->global_use_rdma);
    	CHECK_LE(index, config_->global_num_threads);
    	return config_->send_buffer_offset + index * MiB2B(config_->global_per_send_buffer_sz_mb);
    }

    inline char* GetRecvBuf(int tid, int nid){
		assert(config_->global_use_rdma);
        CHECK_LT(tid, config_->global_num_threads);
        CHECK_LT(nid, config_->global_num_machines);
        return config_->recv_buf + (tid * config_->global_num_machines + nid) * MiB2B(config_->global_per_recv_buffer_sz_mb);
    }

    inline uint64_t GetRecvBufSize(){
		assert(config_->global_use_rdma);
    	return MiB2B(config_->global_per_recv_buffer_sz_mb);
    }

    inline uint64_t GetRecvBufOffset(int tid, int nid) {
		assert(config_->global_use_rdma);
        CHECK_LT(tid, config_->global_num_threads);
        CHECK_LT(nid, config_->global_num_machines);
        return config_->recv_buffer_offset + (tid * config_->global_num_machines + nid) * MiB2B(config_->global_per_recv_buffer_sz_mb);
    }

	inline char* GetLocalHeadBuf(int tid, int nid){
		assert(config_->global_use_rdma);
		CHECK_LT(tid, config_->global_num_threads);
        CHECK_LT(nid, config_->global_num_machines);
        return config_->local_head_buf + (tid * config_->global_num_machines + nid) * sizeof(uint64_t);
    }

    inline uint64_t GetLocalHeadBufSize(){
		assert(config_->global_use_rdma);
    	return MiB2B(config_->local_head_buffer_sz);
    }

    inline uint64_t GetLocalHeadBufOffset(int tid, int nid) {
		assert(config_->global_use_rdma);
		CHECK_LT(tid, config_->global_num_threads);
        CHECK_LT(nid, config_->global_num_machines);
        return config_->local_head_buffer_offset + (tid * config_->global_num_machines + nid) * sizeof(uint64_t);
    }

	inline char* GetRemoteHeadBuf(int tid, int nid){
		assert(config_->global_use_rdma);
        CHECK_LT(tid, config_->global_num_threads);
        CHECK_LT(nid, config_->global_num_machines);
        return config_->remote_head_buf + (tid * config_->global_num_machines + nid) * sizeof(uint64_t);
    }

    inline uint64_t GetRemoteHeadBufSize(){
		assert(config_->global_use_rdma);
    	return MiB2B(config_->remote_head_buffer_sz);
    }

    inline uint64_t GetRemoteHeadBufOffset(int tid, int nid) {
		assert(config_->global_use_rdma);
        CHECK_LT(tid, config_->global_num_threads);
        CHECK_LT(nid, config_->global_num_machines);
        return config_->remote_head_buffer_offset + (tid * config_->global_num_machines + nid) * sizeof(uint64_t);
    }

private:
    // layout: (kv-store) | send_buffer | recv_buffer | local_head_buffer | remote_head_buffer
    char* buffer_;
    Config* config_;
    Node & node_;
};
