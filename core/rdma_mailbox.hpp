/*
 * rdma_mailbox.hpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include <vector>
#include <string>
#include <mutex>

#include "core/buffer.hpp"
#include "core/message.hpp"
#include "utils/config.hpp"
#include "base/rdma.hpp"
#include "base/serialization.hpp"
#include "base/abstract_mailbox.hpp"
#include "utils/global.hpp"

#include "glog/logging.h"

// #include "rdma/rdma.hpp"
template <class M>
class RdmaMailbox : public AbstractMailbox<M> {
public:
    RdmaMailbox(Config* config, AbstractIdMapper* id_mapper, Buffer<M>* buffer) :
        config_(config), id_mapper_(id_mapper), buffer_(buffer) {
    	scheduler_ = 0;
        // Do some checking
        CHECK_NOTNULL(id_mapper);
        CHECK_NOTNULL(buffer);
    }

    virtual ~RdmaMailbox() {}

    void Init(std::string host_fname) {
        // Init RDMA
    	RDMA_init(config_->global_num_machines, get_node_id(), buffer_->GetBuf(), buffer_->GetBufSize(), host_fname);
        // init the scheduler
        scheduler_ = 0;
    }

    //local read
    Message<M> Recv() {
        // Only one thread can enter the handler
        std::lock_guard<std::mutex> lk(recv_mu_);
        
        while (true) {
        	Message<M> msg_to_recv;
            int machine_id = (scheduler_++) % config_->global_num_machines;
            if (buffer_->CheckRecvBuf(machine_id)){
                buffer_->FetchMsgFromRecvBuf(machine_id, msg_to_recv);
                return msg_to_recv;
            }
        }
    }

    bool TryRecv(Message<M> & msg){
    	// Only one thread can enter the handler
		std::lock_guard<std::mutex> lk(recv_mu_);

		for (int machine_id = 0; machine_id < config_->global_num_machines; machine_id++) {
			if (buffer_->CheckRecvBuf(machine_id)){
				buffer_->FetchMsgFromRecvBuf(machine_id, msg);
				return true;
			}
		}
		return false;
    }

    // When sent to the same recv buffer, the consistency relies on
    // the lock in the id_mapper
    virtual int Send(const int t_id, const int dst_nid, const Message<M>& msg) {
        // get the recv buffer by index
    	ibinstream im;
    	im << msg;
    	size_t data_sz = im.size();

    	uint64_t msg_sz = sizeof(uint64_t) + ceil(data_sz, sizeof(uint64_t)) + sizeof(uint64_t);
    	uint64_t off = id_mapper_->GetAndIncrementRdmaRingBufferOffset(msg.meta.recver_node, msg_sz);
        char * recv_buf_ptr = buffer_->GetRecvBuf(msg.meta.recver_node);
        uint64_t rbf_sz = MiB2B(config_->global_per_recv_buffer_sz_mb);

        if(get_node_id() == dst_nid){
        	// write msg to the local physical-queue
			*((uint64_t *)(recv_buf_ptr + off % rbf_sz)) = data_sz;       // header
			off += sizeof(uint64_t);

			if (off / rbf_sz == (off + data_sz - 1) / rbf_sz ) { // data
				memcpy(recv_buf_ptr + (off % rbf_sz), im.get_buf(), data_sz);
			} else {
				uint64_t _sz = rbf_sz - (off % rbf_sz);
				memcpy(recv_buf_ptr + (off % rbf_sz), im.get_buf(), _sz);
				memcpy(recv_buf_ptr, im.get_buf() + _sz, data_sz - _sz);
			}
			off += ceil(data_sz, sizeof(uint64_t));
			*((uint64_t *)(recv_buf_ptr + off % rbf_sz)) = data_sz;       // footer
        }else{
        	//TODO
        	//????
        	//why we must follow Wukong's design to copy send-data to send-buf first???
        	//????
        	// prepare RDMA buffer for RDMA-WRITE
			char *rdma_buf = buffer_->GetSendBuf(t_id);

			*((uint64_t *)rdma_buf) = data_sz;  // header
			rdma_buf += sizeof(uint64_t);

			memcpy(rdma_buf, im.get_buf(), data_sz);    // data
			rdma_buf += ceil(data_sz, sizeof(uint64_t));

			*((uint64_t*)rdma_buf) = data_sz;   // footer

			// write msg to the remote physical-queue
			RDMA &rdma = RDMA::get_rdma();
			uint64_t rdma_off = buffer_->GetRecvBufOffset(get_node_id());
			if (off / rbf_sz == (off + msg_sz - 1) / rbf_sz ) {
				//rdma.dev->RdmaWriteSelective(tid, msg.meta.recver_node, buffer_->GetSendBuffer(t_id), (uint64_t)msg.meta.size, (uint64_t)recv_buffer_offset);
				rdma.dev->RdmaWriteSelective(dst_nid, buffer_->GetSendBuf(t_id), msg_sz, rdma_off + (off % rbf_sz));
			} else {
				uint64_t _sz = rbf_sz - (off % rbf_sz);
				rdma.dev->RdmaWriteSelective(dst_nid, buffer_->GetSendBuf(t_id), _sz, rdma_off + (off % rbf_sz));
				rdma.dev->RdmaWriteSelective(dst_nid, buffer_->GetSendBuf(t_id) + _sz, msg_sz - _sz, rdma_off);
			}
        }
        // Reset the send buffer
        memset(buffer_->GetSendBuf(t_id), 0, MiB2B(config_->global_per_send_buffer_sz_mb));
    }


    virtual void Start(){}
    virtual void Stop(){}

private:
    Config* config_;
    AbstractIdMapper* id_mapper_;
    Buffer<M>* buffer_;

    // round-robin scheduler
    int scheduler_;

    // lock for recv 
    std::mutex recv_mu_;
};
  
