//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu

#include "base/communication.hpp"

//============================================
int all_sum(int my_copy)
{
	int tmp;
	MPI_Allreduce(&my_copy, &tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
	return tmp;
}

long long master_sum_LL(long long my_copy)
{
	long long tmp = 0;
	MPI_Reduce(&my_copy, &tmp, 1, MPI_LONG_LONG_INT, MPI_SUM, MASTER_RANK, MPI_COMM_WORLD);
	return tmp;
}

long long all_sum_LL(long long my_copy)
{
	long long tmp = 0;
	MPI_Allreduce(&my_copy, &tmp, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
	return tmp;
}

char all_bor(char my_copy)
{
	char tmp;
	MPI_Allreduce(&my_copy, &tmp, 1, MPI_BYTE, MPI_BOR, MPI_COMM_WORLD);
	return tmp;
}

bool all_lor(bool my_copy)
{
	bool tmp;
	MPI_Allreduce(&my_copy, &tmp, 1, MPI_BYTE, MPI_LOR, MPI_COMM_WORLD);
	return tmp;
}

bool all_land(bool my_copy)
{
	bool tmp;
	MPI_Allreduce(&my_copy, &tmp, 1, MPI_CHAR, MPI_LAND, MPI_COMM_WORLD);
	return tmp;
}

//============================================
void pregel_send(void* buf, int size, int dst, int tag)
{
	MPI_Send(buf, size, MPI_CHAR, dst, tag, MPI_COMM_WORLD);
}

int pregel_recv(void* buf, int size, int src, int tag) //return the actual source, since "src" can be MPI_ANY_SOURCE
{
	MPI_Status status;
	MPI_Recv(buf, size, MPI_CHAR, src, tag, MPI_COMM_WORLD, &status);
	return status.MPI_SOURCE;
}

//============================================
void send_ibinstream(ibinstream& m, int dst, int tag)
{
	size_t size = m.size();
	pregel_send(&size, sizeof(size_t), dst, tag);
	pregel_send(m.get_buf(), m.size(), dst, tag);
}

//TODO
//OPT Performance, avoid copy before return
obinstream recv_obinstream(int src, int tag)
{
	size_t size;
	src = pregel_recv(&size, sizeof(size_t), src, tag); //must receive the content (paired with the msg-size) from the msg-size source
	char* buf = new char[size];
	pregel_recv(buf, size, src, tag);
	return obinstream(buf, size);
}

//============================================
