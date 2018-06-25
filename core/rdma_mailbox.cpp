/*
 * rdma_mailbox.cpp
 *
 *  Created on: Jun 5, 2018
 *      Author: Hongzhi Chen
 */

#include "core/rdma_mailbox.hpp"


void RdmaMailbox::Init(std::string host_fname) {
	// Init RDMA
	RDMA_init(node_.get_local_size(), node_.get_local_rank(), buffer_->GetBuf(), buffer_->GetBufSize(), host_fname);
	// init the scheduler
	scheduler_ = 0;
}

Message RdmaMailbox::Recv() {
	// Only one thread can enter the handler
	std::lock_guard<std::mutex> lk(recv_mu_);

	while (true) {
		Message msg_to_recv;
		int machine_id = (scheduler_++) % node_.get_local_size();
		if(buffer_->CheckRecvBuf(machine_id)){
			obinstream um;
			buffer_->FetchMsgFromRecvBuf(machine_id, um);
			um >> msg_to_recv;
			return msg_to_recv;
		}
	}
}


bool RdmaMailbox::TryRecv(Message & msg) {
	// Only one thread can enter the handler
	std::lock_guard<std::mutex> lk(recv_mu_);

	for (int machine_id = 0; machine_id < node_.get_local_size(); machine_id++) {
		if (buffer_->CheckRecvBuf(machine_id)){
			obinstream um;
			buffer_->FetchMsgFromRecvBuf(machine_id, um);
			um >> msg;
			return true;
		}
	}
	return false;
}


int RdmaMailbox::Send(const int t_id, const Message & msg) {
	// get the recv buffer by index
	ibinstream im;
	im << msg;
	size_t data_sz = im.size();

	int dst_nid = msg.meta.recver;
	int src_nid = msg.meta.sender;
	uint64_t msg_sz = sizeof(uint64_t) + ceil(data_sz, sizeof(uint64_t)) + sizeof(uint64_t);
	uint64_t off = id_mapper_->GetAndIncrementRdmaRingBufferOffset(dst_nid, msg_sz);
	uint64_t rbf_sz = MiB2B(config_->global_per_recv_buffer_sz_mb);

	if(src_nid == dst_nid){
		char * recv_buf_ptr = buffer_->GetRecvBuf(src_nid);
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
		// prepare RDMA buffer for RDMA-WRITE
		//TODO
		//why we must follow Wukong's design to copy send-data to send-buf first???
		char *rdma_buf = buffer_->GetSendBuf(t_id);

		*((uint64_t *)rdma_buf) = data_sz;  // header
		rdma_buf += sizeof(uint64_t);

		memcpy(rdma_buf, im.get_buf(), data_sz);    // data
		rdma_buf += ceil(data_sz, sizeof(uint64_t));

		*((uint64_t*)rdma_buf) = data_sz;   // footer

		// write msg to the remote physical-queue
		RDMA &rdma = RDMA::get_rdma();
		uint64_t rdma_off = buffer_->GetRecvBufOffset(src_nid);
		if (off / rbf_sz == (off + msg_sz - 1) / rbf_sz ) {
			rdma.dev->RdmaWrite(dst_nid, buffer_->GetSendBuf(t_id), msg_sz, rdma_off + (off % rbf_sz));
		} else {
			uint64_t _sz = rbf_sz - (off % rbf_sz);
			rdma.dev->RdmaWrite(dst_nid, buffer_->GetSendBuf(t_id), _sz, rdma_off + (off % rbf_sz));
			rdma.dev->RdmaWrite(dst_nid, buffer_->GetSendBuf(t_id) + _sz, msg_sz - _sz, rdma_off);
		}
		// Reset the send buffer
		memset(buffer_->GetSendBuf(t_id), 0, MiB2B(config_->global_per_send_buffer_sz_mb));
	}
}
