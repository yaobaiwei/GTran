/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*        Modified by Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

/*
 * Copyright (c) 2016 Shanghai Jiao Tong University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/projects/wukong
 *
 */

#pragma once

#pragma GCC diagnostic warning "-fpermissive"

#include <algorithm>
#include <fstream>      // std::ifstream
#include <iostream>     // std::cout
#include <map>
#include <string>
#include <vector>
#include "base/node.hpp"
#include "glog/logging.h"

struct rdma_mem_t{
    char* mem_conn;
    uint64_t mem_conn_sz;    // For one-sided read/write
    char* mem_dgram;
    uint64_t mem_dgram_sz;    // For two-sided send/recv
    char* mem_dgram_recv;
    uint64_t mem_dgram_recv_sz;    // For post recv
};

#ifdef HAS_RDMA

#include "utils/timer.hpp"
#include "rdmalib/rdmaio.hpp"

using namespace rdmaio;

struct rdma_header_t {
    int nid;
    int packet_num;
    int packet_id;
    int total_len;
    int data_len;

    rdma_header_t(int nid_, int packet_num_, int packet_id_, int total_len_, int data_len_) :
                nid(nid_), packet_num(packet_num_), packet_id(packet_id_),
                total_len(total_len_), data_len(data_len_) {}
};

struct rdma_msg_t {
    vector<bool> received_packet;
    char* data;
    int total_len;
    int total_packet;
    int received_count;

    rdma_msg_t() : data(NULL), total_len(0), total_packet(0), received_count(0) {}
};

#define RDMA_MTU_SZ 4096
// Each receive request will receive at most RDMA_UD_RECV_BUF_SZ data
const int RDMA_UD_RECV_BUF_SZ = RDMA_MTU_SZ + 40;
const int RDMA_PACK_HEADER_SZ = sizeof(rdma_header_t);
const int RDMA_PACK_DATA_SZ = RDMA_MTU_SZ - RDMA_PACK_HEADER_SZ;

class RDMA {
    class RDMA_Device {
     public:
        RdmaCtrl* ctrl = NULL;

        RDMA_Device(int num_workers, int num_threads, int nid, rdma_mem_t mem_info, vector<Node> & nodes, Node & master)
                : num_threads_(num_threads), nid_(nid) {
            // record IPs of ndoes
            vector<string> ipset;
            for (const auto & node : nodes)
                ipset.push_back(node.ibname);
            // Master nid = #workders
            ipset.push_back(master.ibname);

            int rdma_port = nodes[0].rdma_port;
            // initialization of new librdma
            // nid, ipset, port, thread_id-no use, enable single memory region
            ctrl = new RdmaCtrl(nid, ipset, rdma_port, true);
            ctrl->open_device();
            ctrl->set_dgram_mr(mem_info.mem_dgram, mem_info.mem_dgram_sz);
            ctrl->register_dgram_mr();

            Qp* ud_qp;
            // Init QP connection
            ctrl->set_connect_mr(mem_info.mem_conn, mem_info.mem_conn_sz);
            ctrl->register_connect_mr();
            ctrl->start_server();

            if (nid == num_workers) {  // Master Rank
                // RC connection betweenq
                for (uint j = 0; j < num_threads; ++j) {
                    for (uint i = 0; i < num_workers; ++i) {
                        Qp *qp = ctrl->create_rc_qp(num_threads + j, i, 0, 1);
                        assert(qp != NULL);
                    }
                }

                while (1) {
                    int connected = 0;
                    for (uint j = 0; j < num_threads; ++j) {
                        for (uint i = 0; i < num_workers; ++i) {
                            Qp *qp = ctrl->create_rc_qp(num_threads + j, i, 0, 1);
                            if (qp->inited_) {
                                connected += 1;
                            } else if (qp->connect_rc()) {
                                connected += 1;
                            }
                        }
                    }
                    if (connected == num_workers * num_threads)
                        break;
                    else
                        sleep(1);
                }

                // UD connection between master and workers
                ud_qp = ctrl->create_ud_qp(0, 0, 1, 0);
                assert(ud_qp != NULL);

                while (1) {
                    int connected = 0;
                    for (uint i = 0; i < num_workers; ++i) {
                        if (ud_qp->get_ud_connect_info_specific(i, 0, 0)) {
                            connected += 1;
                        }
                    }
                    if (connected == num_workers)
                        break;
                    else
                        sleep(1);
                }
            } else {
                // create RC connection QP between workers
                for (uint i = 0; i< num_workers; ++i) {
                    if (i == nid) {
                        // skip local qp
                        continue;
                    }
                    for (uint j = 0; j < num_threads * 2; ++j) {
                        Qp *qp = ctrl->create_rc_qp(j, i, 0, 1);
                        assert(qp != NULL);
                    }
                }

                // connect those RC QP
                while (1) {
                    int connected = 0;
                    for (uint i = 0; i< num_workers; ++i) {
                        if (i == nid) {
                            continue;
                        }
                        for (uint j = 0; j < num_threads * 2; ++j) {
                            Qp *qp = ctrl->create_rc_qp(j, i, 0, 1);
                            if (qp->inited_) {
                                connected += 1;
                            } else if (qp->connect_rc()) {
                                connected += 1;
                            }
                        }
                    }
                    if (connected == (num_workers - 1) * num_threads * 2)
                        break;
                    else
                        sleep(1);
                }

                // create RC connection QP to the master
                for (uint j = 0; j < num_threads; ++ j) {
                    Qp * qp = ctrl -> create_rc_qp(num_threads + j, num_workers, 0 , 1);
                    CHECK(qp != NULL);
                }

                while (1) {
                    int connected = 0;
                    for (uint j = 0; j < num_threads; ++ j) {
                        Qp * qp = ctrl -> create_rc_qp(num_threads + j, num_workers, 0 , 1);
                        if (qp->inited_) {
                            connected += 1;
                        } else if (qp->connect_rc()) {
                            connected += 1;
                        }
                    }
                    if (connected == num_threads)
                        break;
                    else
                        sleep(1);
                }

                // UD connection between master and workers
                // Each worker will only have one send/recv qp
                ud_qp = ctrl->create_ud_qp(0, 0, 1, 0);
                assert(ud_qp != NULL);

                while (1) {
                    int connected = 0;
                    for (uint i = 0; i <= num_workers; ++i) {
                        if (ud_qp->get_ud_connect_info_specific(i, 0, 0)) {
                            connected += 1;
                        }
                    }
                    if (connected == num_workers + 1)
                        break;
                    else
                        sleep(1);
                }
            }
            // Maximum number of outstanding work request
            int max_wr = min(UD_MAX_RECV_SIZE, static_cast<int>(mem_info.mem_dgram_recv_sz / RDMA_UD_RECV_BUF_SZ));
            // Post recv wr for each worker
            for (int i = 0; i < max_wr; i++) {
                char* buf_addr = mem_info.mem_dgram_recv + i * RDMA_UD_RECV_BUF_SZ;
                Qp::IOStatus status = ud_qp->ud_post_recv(buf_addr, RDMA_UD_RECV_BUF_SZ);
                assert(status == Qp::IOStatus::IO_SUCC);
            }
        }

        int RdmaRecv(char*& received, uint64_t& received_len) {
            Qp* qp = ctrl->get_ud_qp(0, 0);
            Qp::IOStatus status;
            while (1) {
                uint64_t length, rid;
                status = qp->poll_recv_completion(length, &rid);
                if (status != Qp::IOStatus::IO_SUCC) {
                    return 1;
                }
                char* buf_addr = reinterpret_cast<char*>(rid);
                // skip GRH header
                buf_addr += 40;

                // Read header
                rdma_header_t* header = reinterpret_cast<rdma_header_t*>(buf_addr);
                buf_addr += RDMA_PACK_HEADER_SZ;

                // Get msg collector, reset if it is first packet from nid
                rdma_msg_t& msg = msg_map_[header->nid];
                if (msg.data == NULL) {
                    msg.total_len = header->total_len;
                    msg.total_packet = header->packet_num;
                    msg.received_packet.resize(header->packet_num);
                    msg.data = new char[msg.total_len];
                    std::fill(msg.received_packet.begin(), msg.received_packet.end(), false);
                }

                // Collect not received packet only
                if (!msg.received_packet[header->packet_id]) {
                    memcpy(msg.data + header->packet_id * RDMA_PACK_DATA_SZ, buf_addr, header->data_len);
                    msg.received_count += 1;
                    msg.received_packet[header->packet_id] = true;
                }
                // re-post wr
                status = qp->ud_post_recv(reinterpret_cast<char*>(rid), RDMA_UD_RECV_BUF_SZ);
                if (status != Qp::IOStatus::IO_SUCC) {
                    return 1;
                }
                // Return full data when collection done
                if (msg.received_count == msg.total_packet) {
                    received = msg.data;
                    received_len = msg.total_len;
                    msg.data = NULL;
                    msg.total_len = 0;
                    msg.received_count = 0;
                    break;
                }
            }
            return 0;
        }

        int RdmaSend(int dst_nid, char* send_buf, char* local, int len) {
            Qp* qp = ctrl->get_ud_qp(0, 0);
            Qp::IOStatus status;
            int num_packet = len / RDMA_PACK_DATA_SZ + 1;
            int last_packet_len = len % RDMA_PACK_DATA_SZ;
            RdmaReq* reqs = new RdmaReq[num_packet];
            for (int i = 0; i < num_packet; i++) {
                char* buf = send_buf + i * RDMA_MTU_SZ;
                int sz = (i == num_packet - 1) ? last_packet_len : RDMA_PACK_DATA_SZ;
                rdma_header_t header(nid_, num_packet, i, len, sz);
                // set header
                memcpy(buf, reinterpret_cast<char*>(&header), RDMA_PACK_HEADER_SZ);
                // set data
                memcpy(buf + RDMA_PACK_HEADER_SZ, local + i * RDMA_PACK_DATA_SZ, sz);
                reqs[i].buf = buf;
                reqs[i].length = sz + RDMA_PACK_HEADER_SZ;
            }
            status = qp->ud_post_doorbell(dst_nid, 0, reqs, num_packet, IBV_SEND_SIGNALED);
            delete reqs;
            if (status != Qp::IOStatus::IO_SUCC) {
                return 1;
            }
            status = qp->poll_completions(num_packet);
            if (status != Qp::IOStatus::IO_SUCC) {
                return 1;
            }
            return 0;
        }

        // 0 on success, -1 otherwise
        int RdmaRead(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            // virtual tid for read
            int vir_tid = dst_tid + num_threads_;

            Qp* qp = ctrl->get_rc_qp(vir_tid, dst_nid);
            qp->rc_post_send(IBV_WR_RDMA_READ, local, size, off, IBV_SEND_SIGNALED);
            if (!qp->first_send())
                qp->poll_completion();

            qp->poll_completion();
            return 0;
            // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_READ);
        }

        int RdmaWrite(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            Qp* qp = ctrl->get_rc_qp(dst_tid, dst_nid);

            // int flags = (qp->first_send() ? IBV_SEND_SIGNALED : 0);
            int flags = IBV_SEND_SIGNALED;
            qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);

            // if (qp->need_poll())
            qp->poll_completion();

            return 0;
            // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_WRITE);
        }

        int RdmaWriteSelective(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            Qp* qp = ctrl->get_rc_qp(dst_tid, dst_nid);
            int flags = (qp->first_send() ? IBV_SEND_SIGNALED : 0);
            // int flags = IBV_SEND_SIGNALED;

            qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);

            if (qp->need_poll())
                qp->poll_completion();
            return 0;
            // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_WRITE);
        }

        int RdmaWriteNonSignal(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            Qp* qp = ctrl->get_rc_qp(dst_tid, dst_nid);
            int flags = 0;
            qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);
            return 0;
        }

     private:
        int num_threads_;
        int nid_;
        uint64_t dgram_recv_sz_;
        map<int, rdma_msg_t> msg_map_;
    };

 public:
    RDMA_Device *dev = NULL;

    RDMA() { }

    ~RDMA() { if (dev != NULL) delete dev; }

    void init_dev(int num_workers, int num_threads, int nid, rdma_mem_t mem_info, vector<Node> & nodes, Node & master) {
        dev = new RDMA_Device(num_workers, num_threads, nid, mem_info, nodes, master);
    }

    inline static bool has_rdma() { return true; }

    static RDMA &get_rdma() {
        static RDMA rdma;
        return rdma;
    }
};

void RDMA_init(int num_workers, int num_threads, int nid, rdma_mem_t mem_info, vector<Node> & nodes, Node & master);

#else

class RDMA {
    class RDMA_Device {
     public:
        RDMA_Device(int num_workers, int num_threads, int nid, rdma_mem_t mem_info,
                vector<Node> & nodes, Node & master) {
            cout << "This system is compiled without RDMA support." << endl;
            assert(false);
        }

        int RdmaRead(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t remote_offset) {
            cout << "This system is compiled without RDMA support." << endl;
            assert(false);
            return 0;
        }

        int RdmaWrite(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t remote_offset) {
            cout << "This system is compiled without RDMA support." << endl;
            assert(false);
            return 0;
        }

        int RdmaWriteSelective(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t remote_offset) {
            cout << "This system is compiled without RDMA support." << endl;
            assert(false);
            return 0;
        }

        int RdmaWriteNonSignal(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            cout << "This system is compiled without RDMA support." << endl;
            assert(false);
            return 0;
        }
    };

 public:
    RDMA_Device *dev = NULL;

    RDMA() { }

    ~RDMA() { }

    void init_dev(int num_workers, int num_threads, int nid, rdma_mem_t mem_info, vector<Node> & nodes, Node & master) {
        dev = new RDMA_Device(num_workers, num_threads, nid, mem_info, nodes, master);
    }

    inline static bool has_rdma() { return false; }

    static RDMA &get_rdma() {
        static RDMA rdma;
        return rdma;
    }
};

void RDMA_init(int num_workers, int num_threads, int nid, rdma_mem_t mem_info, vector<Node> & nodes, Node & master);

#endif
