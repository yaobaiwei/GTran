/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)

*/

#include "core/rdma_mailbox.hpp"


void RdmaMailbox::Init(vector<Node> & nodes) {
	// Init RDMA
	RDMA_init(node_.get_local_size(), config_->global_num_threads + 1, node_.get_local_rank(), buffer_->GetBuf(), buffer_->GetBufSize(), nodes);

	int nrbfs = config_->global_num_machines * config_->global_num_threads;

	rmetas = (rbf_rmeta_t *)malloc(sizeof(rbf_rmeta_t) * nrbfs);
	memset(rmetas, 0, sizeof(rbf_rmeta_t) * nrbfs);
	for (int i = 0; i < nrbfs; i++) {
		rmetas[i].tail = 0;
		pthread_spin_init(&rmetas[i].lock, 0);
	}

	lmetas = (rbf_lmeta_t *)malloc(sizeof(rbf_lmeta_t) * nrbfs);
	memset(lmetas, 0, sizeof(rbf_lmeta_t) * nrbfs);
	for (int i = 0; i < nrbfs; i++) {
		lmetas[i].head = 0;
		pthread_spin_init(&lmetas[i].lock, 0);
	}

	recv_locks = (pthread_spinlock_t *)malloc(sizeof(pthread_spinlock_t) * config_->global_num_threads);
	for (int i = 0; i < config_->global_num_threads; i++) {
		pthread_spin_init(&recv_locks[i], 0);
	}

	schedulers = (scheduler_t *)malloc(sizeof(scheduler_t) * config_->global_num_threads);
	memset(schedulers, 0, sizeof(scheduler_t) * config_->global_num_threads);

	pending_msgs.resize(config_->global_num_threads + 1);
}

bool RdmaMailbox::IsBufferFull(int dst_nid, int dst_tid, uint64_t tail, uint64_t msg_sz) {
	static uint64_t old_head = 0;
    uint64_t rbf_sz = MiB2B(config_->global_per_recv_buffer_sz_mb);
    uint64_t head = *(uint64_t *)buffer_->GetRemoteHeadBuf(dst_tid, dst_nid);

    return rbf_sz < (tail - head + msg_sz);
}

void RdmaMailbox::Sweep(int tid) {
	if(pending_msgs[tid].size() == 0){
		return;
	}
	bool success = false;	// for time measurement only

	//timer::start_timer(tid);
	for (auto it = pending_msgs[tid].begin(); it != pending_msgs[tid].end();){
        if (SendData(tid, *it)){
            it = pending_msgs[tid].erase(it);
			success = true;
		}else{
			it++;
		}
	}
	if(success){
		//timer::stop_timer(tid);
	}
}

int RdmaMailbox::Send(int tid, const Message & msg) {
	mailbox_data_t data;
	data.dst_nid = msg.meta.recver_nid;
	data.dst_tid = msg.meta.recver_tid;

	//timer::start_timer(tid + config_->global_num_threads);
	data.stream << msg;
	//timer::stop_timer(tid + config_->global_num_threads);

	pending_msgs[tid].push_back(move(data));
}

int RdmaMailbox::Send(int tid, const mailbox_data_t & data) {
	pending_msgs[tid].push_back(data);
}

bool RdmaMailbox::SendData(int tid, const mailbox_data_t& data) {
	int dst_nid = data.dst_nid;
	int dst_tid = data.dst_tid;

	size_t data_sz = data.stream.size();
	uint64_t msg_sz = sizeof(uint64_t) + ceil(data_sz, sizeof(uint64_t)) + sizeof(uint64_t);

	rbf_rmeta_t *rmeta = &rmetas[dst_nid * config_->global_num_threads + dst_tid];

	pthread_spin_lock(&rmeta->lock);
	 // detect overflow
	if (IsBufferFull(dst_nid, dst_tid, rmeta->tail, msg_sz)) {
        pthread_spin_unlock(&rmeta->lock);
        return false;
    }
	// update tail
    uint64_t off = rmeta->tail;
    rmeta->tail += msg_sz;
	pthread_spin_unlock(&rmeta->lock);

	uint64_t rbf_sz = MiB2B(config_->global_per_recv_buffer_sz_mb);

	if(node_.get_local_rank() == dst_nid){
		char * recv_buf_ptr = buffer_->GetRecvBuf(dst_tid, node_.get_local_rank());

		*((uint64_t *)(recv_buf_ptr + off % rbf_sz)) = data_sz;		// header
		off += sizeof(uint64_t);

		if (off / rbf_sz == (off + data_sz - 1) / rbf_sz ) { 		// data
			memcpy(recv_buf_ptr + (off % rbf_sz), data.stream.get_buf(), data_sz);
		} else {
			uint64_t _sz = rbf_sz - (off % rbf_sz);
			memcpy(recv_buf_ptr + (off % rbf_sz), data.stream.get_buf(), _sz);
			memcpy(recv_buf_ptr, data.stream.get_buf() + _sz, data_sz - _sz);
		}
		off += ceil(data_sz, sizeof(uint64_t));
		*((uint64_t *)(recv_buf_ptr + off % rbf_sz)) = data_sz;  	// footer
	}else{
		char *rdma_buf = buffer_->GetSendBuf(tid);

		*((uint64_t *)rdma_buf) = data_sz;  // header
		rdma_buf += sizeof(uint64_t);

		memcpy(rdma_buf, data.stream.get_buf(), data_sz);    // data
		rdma_buf += ceil(data_sz, sizeof(uint64_t));

		*((uint64_t*)rdma_buf) = data_sz;   // footer

		RDMA &rdma = RDMA::get_rdma();
		uint64_t rdma_off = buffer_->GetRecvBufOffset(dst_tid, node_.get_local_rank());
		pthread_spin_lock(&rmeta->lock);
		if (off / rbf_sz == (off + msg_sz - 1) / rbf_sz ) {
			rdma.dev->RdmaWrite(dst_tid, dst_nid, buffer_->GetSendBuf(tid), msg_sz, rdma_off + (off % rbf_sz));
		} else {
			uint64_t _sz = rbf_sz - (off % rbf_sz);
			rdma.dev->RdmaWrite(dst_tid, dst_nid, buffer_->GetSendBuf(tid), _sz, rdma_off + (off % rbf_sz));
			rdma.dev->RdmaWrite(dst_tid, dst_nid, buffer_->GetSendBuf(tid) + _sz, msg_sz - _sz, rdma_off);
		}
		pthread_spin_unlock(&rmeta->lock);
	}
	return true;
}

void RdmaMailbox::Recv(int tid, Message & msg) {
	while (true) {
		int machine_id = (schedulers[tid].rr_cnt++) % node_.get_local_size();
		if(CheckRecvBuf(tid, machine_id)){
			obinstream um;
			FetchMsgFromRecvBuf(tid, machine_id, um);
			um >> msg;
		}
	}
}


bool RdmaMailbox::TryRecv(int tid, Message & msg) {
	pthread_spin_lock(&recv_locks[tid]);
	for (int i = 0; i < node_.get_local_size(); i++) {
		int machine_id = (schedulers[tid].rr_cnt++) % node_.get_local_size();
		if (CheckRecvBuf(tid, machine_id)){
			obinstream um;
			FetchMsgFromRecvBuf(tid, machine_id, um);
			pthread_spin_unlock(&recv_locks[tid]);

			//timer::start_timer(tid + config_->global_num_threads);
			um >> msg;
			//timer::stop_timer(tid + config_->global_num_threads);
			return true;
		}
	}
	pthread_spin_unlock(&recv_locks[tid]);
	return false;
}

bool RdmaMailbox::CheckRecvBuf(int tid, int nid){
	rbf_lmeta_t *lmeta = &lmetas[tid * config_->global_num_machines + nid];
	char * rbf = buffer_->GetRecvBuf(tid, nid);
	uint64_t rbf_sz = buffer_->GetRecvBufSize();
	volatile uint64_t msg_size = *(volatile uint64_t *)(rbf + lmeta->head % rbf_sz);  // header
	return msg_size != 0;
}

void RdmaMailbox::FetchMsgFromRecvBuf(int tid, int nid, obinstream & um) {
	rbf_lmeta_t *lmeta = &lmetas[tid * config_->global_num_machines + nid];
	char * rbf = buffer_->GetRecvBuf(tid, nid);
	uint64_t rbf_sz = buffer_->GetRecvBufSize();
	volatile uint64_t pop_msg_size = *(volatile uint64_t *)(rbf + lmeta->head % rbf_sz);  // header

	uint64_t to_footer = sizeof(uint64_t) + ceil(pop_msg_size, sizeof(uint64_t));
	volatile uint64_t * footer = (volatile uint64_t *)(rbf + (lmeta->head  + to_footer) % rbf_sz); // footer

	if (pop_msg_size) {
		// Make sure RDMA trans is done
		while (*footer != pop_msg_size) {
			_mm_pause();
			assert(*footer == 0 || *footer == pop_msg_size);
		}

		// IF it is a ring(rare situation)
		uint64_t start = (lmeta->head + sizeof(uint64_t)) % rbf_sz;
		uint64_t end = (lmeta->head + sizeof(uint64_t) + pop_msg_size) % rbf_sz;
		if (start > end) {
			char* tmp_buf = new char[pop_msg_size];
			memcpy(tmp_buf, rbf + start, pop_msg_size - end);
			memcpy(tmp_buf + pop_msg_size - end, rbf, end);

			//register tmp_buf into obinstream,
			//the obinstream will charge the memory of buf, including memory release
			um.assign(tmp_buf, pop_msg_size, 0);

			// clean
			memset(rbf + start, 0, pop_msg_size - end);
			memset(rbf, 0, ceil(end, sizeof(uint64_t)));
		}
		else {
			char* tmp_buf = new char[pop_msg_size];
			memcpy(tmp_buf, rbf + start, pop_msg_size);

			um.assign(tmp_buf, pop_msg_size, 0);

			// clean the data
			memset(rbf + start, 0, ceil(pop_msg_size, sizeof(uint64_t)));
		}

		//clear header and footer
		*(uint64_t *)(rbf + lmeta->head % rbf_sz) = 0;
		*footer = 0;

		// advance the pointer
		lmeta->head += 2 * sizeof(uint64_t) + ceil(pop_msg_size, sizeof(uint64_t));

		// update heads of ring buffer to writer to help it detect overflow
		//timer::start_timer(tid);
        const uint64_t threshold = rbf_sz / 16;
		char *head = buffer_->GetLocalHeadBuf(tid, nid);
        if (lmeta->head - *(uint64_t *)head > threshold) {
            *(uint64_t *)head = lmeta->head;
            if (node_.get_local_rank() == nid) {
                *(uint64_t *)buffer_->GetRemoteHeadBuf(tid, nid) = lmeta->head;
            } else {
				rbf_rmeta_t *rmeta = &rmetas[nid * config_->global_num_threads + tid];
				RDMA &rdma = RDMA::get_rdma();
                uint64_t off = buffer_->GetRemoteHeadBufOffset(tid, node_.get_local_rank());
				pthread_spin_lock(&rmeta->lock);
                rdma.dev->RdmaWrite(tid, nid, head, sizeof(uint64_t), off);
				pthread_spin_unlock(&rmeta->lock);
            }
        }
		//timer::stop_timer(tid);
	}
}
