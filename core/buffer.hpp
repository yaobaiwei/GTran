/*
 * buffer.hpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */
//

#pragma once

#include <memory>

#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "core/ring_buffer.hpp"

#include "glog/logging.h"


class Buffer {
public:
    Buffer(Config * config) : config_(config){
        buffer_ = new char[config_->buffer_sz];
        memset(buffer_, 0, config_->buffer_sz);
    	config_->kvstore = buffer_ + config_->kvstore_offset;
    	config_->send_buf = buffer_ + config_->send_buffer_offset;
    	config_->recv_buf = buffer_ + config_->recv_buffer_offset;
    }

    ~Buffer() {
        delete[] buffer_;
    }

    void Init() {
        for (int i = 0; i < config_->global_num_threads; i ++)
            rdma_send_buffer_.push_back(buffer_ + config_->send_buffer_offset + i * MiB2B(config_->global_per_send_buffer_sz_mb));

        for (int i = 0; i < config_->global_num_machines; i ++)
            rdma_recv_buffer_.emplace_back(
            		std::move(new RingBuffer(buffer_ + GetRecvBufOffset(i), MiB2B(config_->global_per_recv_buffer_sz_mb)))
            );
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
        CHECK_LT(index, config_->global_num_threads);
        return rdma_send_buffer_[index];
    }

    inline uint64_t GetSendBufSize(){
    	return MiB2B(config_->global_per_send_buffer_sz_mb);
    }

    inline int GetSendBufOffset(int index) {
    	CHECK_LT(index, config_->global_num_threads);
    	return config_->send_buffer_offset + index * MiB2B(config_->global_per_send_buffer_sz_mb);
    }

    char* GetRecvBuf(int index){
        CHECK_LT(index, config_->global_num_machines);
        return buffer_ + config_->recv_buffer_offset + index * MiB2B(config_->global_per_recv_buffer_sz_mb);
    }

    inline uint64_t GetRecvBufSize(){
    	return MiB2B(config_->global_per_recv_buffer_sz_mb);
    }

    int GetRecvBufOffset(int index) {
        CHECK_LT(index, config_->global_num_machines);
        return config_->recv_buffer_offset + index * MiB2B(config_->global_per_recv_buffer_sz_mb);
    }

    // Check corresponding buffer 
    bool CheckRecvBuf(int id) {
        return rdma_recv_buffer_[id]->Check();
    }

    void FetchMsgFromRecvBuf(int id, obinstream & um) {
        rdma_recv_buffer_[id]->Pop(um);
    }

private:
    // layout: (kv-store) | send_buffer | recv_buffer
    char* buffer_;
    Config* config_;

    // TODO: check overflow for rdma_send_buffer
    std::vector<char*> rdma_send_buffer_;
    std::vector<std::unique_ptr<RingBuffer>> rdma_recv_buffer_;
};

