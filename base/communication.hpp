//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef COMMUNICATION_HPP_
#define COMMUNICATION_HPP_

#include <mpi.h>

#include <vector>
#include "base/serialization.hpp"
#include "utils/global.hpp"

using namespace std;
//============================================

int all_sum(int my_copy);

long long master_sum_LL(long long my_copy);

long long all_sum_LL(long long my_copy);

char all_bor(char my_copy);

bool all_lor(bool my_copy);

bool all_land(bool my_copy);

//============================================

void pregel_send(void* buf, int size, int dst, int tag);

int pregel_recv(void* buf, int size, int src, int tag); //return the actual source, since "src" can be MPI_ANY_SOURCE

//============================================

void send_ibinstream(ibinstream& m, int dst, int tag);

obinstream recv_obinstream(int src, int tag);

//============================================
//obj-level send/recv
template <class T>
void send_data(const T& data, int dst, int tag);

template <class T>
void send_data(const T& data, int dst);

template <class T>
T recv_data(int src, int tag);

template <class T>
T recv_data(int src);

//============================================
//all-to-all
template <class T>
void all_to_all(std::vector<T>& to_exchange);

template <class T>
void all_to_all(vector<vector<T*> > & to_exchange);

template <class T, class T1>
void all_to_all(vector<T>& to_send, vector<T1>& to_get);

template <class T, class T1>
void all_to_all_cat(std::vector<T>& to_exchange1, std::vector<T1>& to_exchange2);

template <class T, class T1, class T2>
void all_to_all_cat(std::vector<T>& to_exchange1, std::vector<T1>& to_exchange2, std::vector<T2>& to_exchange3);

//============================================
//scatter
template <class T>
void master_scatter(vector<T>& to_send);

template <class T>
void slave_scatter(T& to_get);

//================================================================
//gather
template <class T>
void master_gather(vector<T>& to_get);

template <class T>
void slave_gather(T& to_send);

//================================================================
//bcast
template <class T>
void master_bcast(T& to_send);

template <class T>
void slave_bcast(T& to_get);

#include "communication.tpp"

#endif /* COMMUNICATION_HPP_ */
