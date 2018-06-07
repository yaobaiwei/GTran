/*
 * rdma.cpp
 *
 *  Created on: Jun 7, 2018
 *      Author: Hongzhi Chen
 */

#include "base/rdma.hpp"

#ifdef HAS_RDMA

void RDMA_init(int num_nodes, int nid, char *mem, uint64_t mem_sz, string ipfn) {
    uint64_t t = timer::get_usec();

    // init RDMA device
    RDMA &rdma = RDMA::get_rdma();
    rdma.init_dev(num_nodes, nid, mem, mem_sz, ipfn);

    t = timer::get_usec() - t;
    cout << "INFO: initializing RMDA done (" << t / 1000  << " ms)" << endl;
}

#else

void RDMA_init(int num_nodes, int nid, char *mem, uint64_t mem_sz, string ipfn) {
    std::cout << "This system is compiled without RDMA support." << std::endl;
}

#endif


