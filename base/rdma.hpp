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

#include <vector>
#include <string>
#include <iostream>     // std::cout
#include <fstream>      // std::ifstream
#include "base/node.hpp"

using namespace std;

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

class RDMA {
    class RDMA_Device {
    public:
        RdmaCtrl* ctrl = NULL;

        RDMA_Device(int num_workers, int num_threads, int nid, rdma_mem_t mem_info, vector<Node> & nodes, Node & master) : num_threads_(num_threads){
            // record IPs of ndoes
            vector<string> ipset;
            for(const auto & node: nodes)
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

            // Init QP connection
            if(nid == num_workers){
                ctrl->start_server();
                // UD connection between master and workers
                Qp* qp = ctrl->create_ud_qp(0, 0, 1, 0);
                assert(qp != NULL);

                while (1) {
                    int connected = 0;
                    for (uint i = 0; i < num_workers; ++i) {
                        if (qp->get_ud_connect_info_specific(i, 0, 0)) {
                            connected += 1;
                        }
                    }
                    if (connected == num_workers)
                        break;
                    else
                    sleep(1);
                }

                dgram_recv_sz_ = mem_info.mem_dgram_recv_sz / num_workers;
                // Post recv wr for each worker
                for(int i = 0; i < num_workers; i++){
                    char* buf_addr = mem_info.mem_dgram_recv + i * dgram_recv_sz_;
                    Qp::IOStatus status = qp->ud_post_recv(buf_addr, dgram_recv_sz_);
                    assert(status == Qp::IOStatus::IO_SUCC);
                }
            }else{
                ctrl->set_connect_mr(mem_info.mem_conn, mem_info.mem_conn_sz);
                ctrl->register_connect_mr();
                ctrl->start_server();
                // RC connection between workers
                for (uint j = 0; j < num_threads * 2; ++j) {
                    for (uint i = 0; i < num_workers; ++i) {
                        Qp *qp = ctrl->create_rc_qp(j, i, 0, 1);
                        assert(qp != NULL);
                    }
                }

                while (1) {
                    int connected = 0;
                    for (uint j = 0; j < num_threads * 2; ++j) {
                        for (uint i = 0; i < num_workers; ++i) {
                            Qp *qp = ctrl->create_rc_qp(j, i, 0, 1);
                            if (qp->inited_) connected += 1;
                            else {
                                if (qp->connect_rc()) {
                                    connected += 1;
                                }
                            }
                        }
                    }
                    if (connected == num_workers * num_threads * 2)
                        break;
                    else
                        sleep(1);
                }
                // UD connection between master and workers
                // Each worker will only have one send/recv qp
                Qp* qp = ctrl->create_ud_qp(0, 0, 1, 0);
                assert(qp != NULL);

                while (1) {
                    if (qp->get_ud_connect_info_specific(num_workers, 0, 0))
                        break;
                    else
                        sleep(1);
                }
                dgram_recv_sz_ = mem_info.mem_dgram_recv_sz;
                Qp::IOStatus status = qp->ud_post_recv(mem_info.mem_dgram_recv, dgram_recv_sz_);
                assert(status == Qp::IOStatus::IO_SUCC);
            }
        }

        int RdmaRecv(char*& received, uint64_t& received_len){
            Qp* qp = ctrl->get_ud_qp(0, 0);
            Qp::IOStatus status;

            uint64_t rid;
            status = qp->poll_recv_completion(received_len, &rid);
            if(status != Qp::IOStatus::IO_SUCC){
                return 1;
            }
            char* buf_addr = (char*)rid;
            // RDMA recv will carry 40 bytes GRH header information
            assert(received_len > 40);
            received_len -= 40; // remove GRH header
            received = new char[received_len];
            memcpy(received, buf_addr + 40, received_len);

            status = qp->ud_post_recv(buf_addr, dgram_recv_sz_);
            return status == Qp::IOStatus::IO_SUCC ? 0 : 1;
        }

        int RdmaSend(int dst_nid, char* local, int len){
            Qp* qp = ctrl->get_ud_qp(0, 0);
            Qp::IOStatus status;
            status = qp->ud_post_send(dst_nid, 0, local, len, IBV_SEND_SIGNALED);
            if(status != Qp::IOStatus::IO_SUCC){
                return 1;
            }
            status = qp->poll_completion();
            return status == Qp::IOStatus::IO_SUCC ? 0 : 1;
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

            // if(qp->need_poll())
            qp->poll_completion();

            return 0;
            // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_WRITE);
        }

        int RdmaWriteSelective(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            Qp* qp = ctrl->get_rc_qp(dst_tid, dst_nid);
            int flags = (qp->first_send() ? IBV_SEND_SIGNALED : 0);
            // int flags = IBV_SEND_SIGNALED;

            qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);

            if (qp->need_poll()) qp->poll_completion();
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
        uint64_t dgram_recv_sz_;
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
        RDMA_Device(int num_workers, int num_threads, int nid, rdma_mem_t mem_info, vector<Node> & nodes, Node & master) {
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
        dev = new RDMA_Device(num_nodes, num_threads, nid, mem_info, nodes, master);
    }

    inline static bool has_rdma() { return false; }

    static RDMA &get_rdma() {
        static RDMA rdma;
        return rdma;
    }
};

void RDMA_init(int num_workers, int num_threads, int nid, rdma_mem_t mem_info, vector<Node> & nodes, Node & master);

#endif
