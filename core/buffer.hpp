/*
 * buffer.hpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "core/message.hpp"
#include "core/id_mapper.hpp"
#include "core/ring_buffer.hpp"
#include "storage/data_store.hpp"

#include "glog/logging.h"

template <class M>
class Buffer {

public:
    typedef Message<M> Msg;
    typedef RingBuffer<Msg> Rbf;

    Buffer(Config * config, AbstractIdMapper* id_mapper) : config_(config), id_mapper_(id_mapper) {}

    ~Buffer() {
        delete[] buffer_;
    }

    void InitBuf() {
        buffer_ = new char[config_->buffer_sz];
        memset(buffer_, 0, config_->buffer_sz);
    	config_->datastore = buffer_ + config_->datastore_offset;
    	config_->send_buf = buffer_ + config_->send_buffer_offset;
    	config_->recv_buf = buffer_ + config_->recv_buffer_offset;
    }

    void SetStorage() {
    	data_store_.Set(buffer_, config_, id_mapper_);
    }

    void SetBuf() {
        for (int i = 0; i < config_->global_num_threads; i ++)
            rdma_send_buffer_.push_back(buffer_ + config_->send_buffer_offset + i * MiB2B(config_->global_per_send_buffer_sz_mb));

        for (int i = 0; i < config_->global_num_machines; i ++)
            rdma_recv_buffer_.emplace_back(
            		std::move(new Rbf(buffer_ + GetRecvBufOffset(i), MiB2B(config_->global_per_recv_buffer_sz_mb)))
            );
    }

    inline char* GetBuf() {
        return buffer_;
    }

    char * GetDatastore(){
    	return config_->datastore;
    }

    inline uint64_t GetDatastoreOffset(){
    	return config_->datastore_offset;
    }

    char* GetSendBuf(int index) {
        CHECK_LT(index, config_->global_num_threads - 1);
        return rdma_send_buffer_[index];
    }

    int GetSendBufOffset(int index) {
    	CHECK_LT(index, config_->global_num_threads - 1);
    	return config_->send_buffer_offset + index * MiB2B(config_->global_per_send_buffer_sz_mb);
    }

    char* GetRecvBuf(int index){
        CHECK_LT(index, config_->global_num_machines - 1);
        return buffer_ + config_->recv_buffer_offset + index * MiB2B(config_->global_per_recv_buffer_sz_mb);
    }

    int GetRecvBufOffset(int index) {
        CHECK_LT(index, config_->global_num_machines - 1);
        return config_->recv_buffer_offset + index * MiB2B(config_->global_per_recv_buffer_sz_mb);
    }

    // Check corresponding buffer 
    bool CheckRecvBuf(int id) {
        return rdma_recv_buffer_[id]->Check();
    }

    void FetchMsgFromRecvBuf(int id, Msg & msg) {
        rdma_recv_buffer_[id]->Pop(msg);
    }

private:
    // layout: (data-store) | send_buffer | recv_buffer
    char* buffer_;
    Config* config_;
    AbstractIdMapper* id_mapper_;

    DataStore data_store_;
    // TODO: check overflow for rdma_send_buffer
    std::vector<char*> rdma_send_buffer_;
    std::vector<std::unique_ptr<Rbf>> rdma_recv_buffer_;
};

