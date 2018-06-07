//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu
//Acknowledgements: this code is implemented by referencing pregel-mpi (https://code.google.com/p/pregel-mpi/) by Chuntao Hong.


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
void pregel_send(void* buf, int size, int dst, int tag = COMMUN_CHANNEL)
{
	MPI_Send(buf, size, MPI_CHAR, dst, tag, MPI_COMM_WORLD);
}

int pregel_recv(void* buf, int size, int src, int tag = COMMUN_CHANNEL) //return the actual source, since "src" can be MPI_ANY_SOURCE
{
	MPI_Status status;
	MPI_Recv(buf, size, MPI_CHAR, src, tag, MPI_COMM_WORLD, &status);
	return status.MPI_SOURCE;
}

//============================================
void send_ibinstream(ibinstream& m, int dst, int tag = COMMUN_CHANNEL)
{
	size_t size = m.size();
	pregel_send(&size, sizeof(size_t), dst, tag);
	pregel_send(m.get_buf(), m.size(), dst, tag);
}

//TODO
//OPT Performance, avoid copy before return
obinstream recv_obinstream(int src, int tag = COMMUN_CHANNEL)
{
	size_t size;
	src = pregel_recv(&size, sizeof(size_t), src, tag); //must receive the content (paired with the msg-size) from the msg-size source
	char* buf = new char[size];
	pregel_recv(buf, size, src, tag);
	return obinstream(buf, size);
}

//============================================
//obj-level send/recv
template <class T>
void send_data(const T& data, int dst, int tag)
{
	ibinstream m;
	m << data;
	send_ibinstream(m, dst, tag);
}

template <class T>
void send_data(const T& data, int dst)
{
	ibinstream m;
	m << data;
	send_ibinstream(m, dst);
}

template <class T>
T recv_data(int src, int tag)
{
	obinstream um = recv_obinstream(src, tag);
	T data;
	um >> data;
	return data;
}

template <class T>
T recv_data(int src)
{
	obinstream um = recv_obinstream(src);
	T data;
	um >> data;
	return data;
}
//============================================
//all-to-all
template <class T>
void all_to_all(std::vector<T>& to_exchange)
{
	//for each to_exchange[i]
	//send out *to_exchange[i] to i
	//save received data in *to_exchange[i]
	int np = get_num_nodes();
	int me = get_node_id();
	for (int i = 0; i < np; i++)
	{
		int partner = (i - me + np) % np;
		if (me != partner)
		{
			if (me < partner)
			{
				//send
				ibinstream m;
				m << to_exchange[partner];
				send_ibinstream(m, partner);

				//receive
				obinstream um = recv_obinstream(partner);
				um >> to_exchange[partner];
			}
			else
			{
				//receive
				obinstream um = recv_obinstream(partner);
				T received;
				um >> received;

				//send
				ibinstream m;
				m << to_exchange[partner];
				send_ibinstream(m, partner);
				to_exchange[partner] = received;
			}
		}
	}
}

template <class T>
void all_to_all(vector<vector<T*> > & to_exchange)
{
	int np = get_num_nodes();
	int me = get_node_id();
	for (int i = 0; i < np; i++)
	{
		int partner = (i - me + np) % np;
		if (me != partner)
		{
			if (me < partner)
			{
				//send
				ibinstream * m = new ibinstream;
				*m << to_exchange[partner];
				for(int k = 0; k < to_exchange[partner].size(); k++)
					delete to_exchange[partner][k];
				vector<T*>().swap(to_exchange[partner]);

				send_ibinstream(*m, partner);
				delete m;

				//receive
				obinstream um = recv_obinstream(partner);
				um >> to_exchange[partner];
			}
			else
			{
				//receive
				obinstream um = recv_obinstream(partner);

				//send
				ibinstream * m = new ibinstream;
				*m << to_exchange[partner];
				for(int k = 0; k < to_exchange[partner].size(); k++)
					delete to_exchange[partner][k];
				vector<T*>().swap(to_exchange[partner]);

				send_ibinstream(*m, partner);
				delete m;

				um >> to_exchange[partner];
			}
		}
	}
}

template <class T, class T1>
void all_to_all(vector<T>& to_send, vector<T1>& to_get)
{
	//for each to_exchange[i]
	//send out *to_exchange[i] to i
	//save received data in *to_exchange[i]
	int np = get_num_nodes();
	int me = get_node_id();
	for (int i = 0; i < np; i++)
	{
		int partner = (i - me + np) % np;
		if (me != partner)
		{
			if (me < partner)
			{
				//send
				ibinstream m;
				m << to_send[partner];
				send_ibinstream(m, partner);

				//receive
				obinstream um = recv_obinstream(partner);
				um >> to_get[partner];
			}
			else
			{
				//receive
				obinstream um = recv_obinstream(partner);
				T1 received;
				um >> received;

				//send
				ibinstream m;
				m << to_send[partner];
				send_ibinstream(m, partner);
				to_get[partner] = received;
			}
		}
	}
}

template <class T, class T1>
void all_to_all_cat(std::vector<T>& to_exchange1, std::vector<T1>& to_exchange2)
{
	//for each to_exchange[i]
	//send out *to_exchange[i] to i
	//save received data in *to_exchange[i]
	int np = get_num_nodes();
	int me = get_node_id();
	for (int i = 0; i < np; i++)
	{
		int partner = (i - me + np) % np;
		if (me != partner)
		{
			if (me < partner)
			{
				//send
				ibinstream m;
				m << to_exchange1[partner];
				m << to_exchange2[partner];

				send_ibinstream(m, partner);

				//receive
				obinstream um = recv_obinstream(partner);

				um >> to_exchange1[partner];
				um >> to_exchange2[partner];
			}
			else
			{
				//receive
				obinstream um = recv_obinstream(partner);

				T received1;
				T1 received2;
				um >> received1;
				um >> received2;
				//send
				ibinstream m;
				m << to_exchange1[partner];
				m << to_exchange2[partner];

				send_ibinstream(m, partner);

				to_exchange1[partner] = received1;
				to_exchange2[partner] = received2;
			}
		}
	}
}

template <class T, class T1, class T2>
void all_to_all_cat(std::vector<T>& to_exchange1, std::vector<T1>& to_exchange2, std::vector<T2>& to_exchange3)
{
	//for each to_exchange[i]
	//send out *to_exchange[i] to i
	//save received data in *to_exchange[i]
	int np = get_num_nodes();
	int me = get_node_id();
	for (int i = 0; i < np; i++)
	{
		int partner = (i - me + np) % np;
		if (me != partner)
		{
			if (me < partner)
			{
				//send
				ibinstream m;
				m << to_exchange1[partner];
				m << to_exchange2[partner];
				m << to_exchange3[partner];

				send_ibinstream(m, partner);

				//receive
				obinstream um = recv_obinstream(partner);

				um >> to_exchange1[partner];
				um >> to_exchange2[partner];
				um >> to_exchange3[partner];
			}
			else
			{
				//receive
				obinstream um = recv_obinstream(partner);

				T received1;
				T1 received2;
				T2 received3;
				um >> received1;
				um >> received2;
				um >> received3;
				//send
				ibinstream m;
				m << to_exchange1[partner];
				m << to_exchange2[partner];
				m << to_exchange3[partner];

				send_ibinstream(m, partner);

				to_exchange1[partner] = received1;
				to_exchange2[partner] = received2;
				to_exchange3[partner] = received3;
			}
		}
	}
}

//============================================
//scatter
template <class T>
void master_scatter(vector<T>& to_send)
{
	//scatter
	int* sendcounts = new int[get_num_nodes()];
	int recvcount;
	int* sendoffset = new int[get_num_nodes()];

	ibinstream m;

	int size = 0;
	for (int i = 0; i < get_num_nodes(); i++)
	{
		if (i == _my_rank)
		{
			sendcounts[i] = 0;
		}
		else
		{
			m << to_send[i];
			sendcounts[i] = m.size() - size;
			size = m.size();
		}
	}

	MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

	for (int i = 0; i < get_num_nodes(); i++)
	{
		sendoffset[i] = (i == 0 ? 0 : sendoffset[i - 1] + sendcounts[i - 1]);
	}
	char* sendbuf = m.get_buf(); //ibinstream will delete it
	char* recvbuf;

	MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);

	delete[] sendcounts;
	delete[] sendoffset;
}

template <class T>
void slave_scatter(T& to_get)
{
	//scatter
	int* sendcounts;
	int recvcount;
	int* sendoffset;

	MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

	char* sendbuf;
	char* recvbuf = new char[recvcount]; //obinstream will delete it

	MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);

	obinstream um(recvbuf, recvcount);
	um >> to_get;
}

//================================================================
//gather
template <class T>
void master_gather(vector<T>& to_get)
{
	//gather
	int sendcount = 0;
	int* recvcounts = new int[get_num_nodes()];
	int* recvoffset = new int[get_num_nodes()];

	MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

	for (int i = 0; i < get_num_nodes(); i++)
	{
		recvoffset[i] = (i == 0 ? 0 : recvoffset[i - 1] + recvcounts[i - 1]);
	}

	char* sendbuf;
	int recv_tot = recvoffset[get_num_nodes() - 1] + recvcounts[get_num_nodes() - 1];
	char* recvbuf = new char[recv_tot]; //obinstream will delete it

	MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);

	obinstream um(recvbuf, recv_tot);
	for (int i = 0; i < get_num_nodes(); i++)
	{
		if (i == _my_rank)
			continue;
		um >> to_get[i];
	}

	delete[] recvcounts;
	delete[] recvoffset;
}

template <class T>
void slave_gather(T& to_send)
{
	//gather
	int sendcount;
	int* recvcounts;
	int* recvoffset;

	ibinstream m;
	m << to_send;
	sendcount = m.size();

	MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

	char* sendbuf = m.get_buf(); //ibinstream will delete it
	char* recvbuf;

	MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
}

//================================================================
//bcast
template <class T>
void master_bcast(T& to_send)
{
	//broadcast
	ibinstream m;
	m << to_send;
	int size = m.size();

	MPI_Bcast(&size, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

	char* sendbuf = m.get_buf();
	MPI_Bcast(sendbuf, size, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
}

template <class T>
void slave_bcast(T& to_get)
{
	//broadcast
	int size;
	MPI_Bcast(&size, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

	char* recvbuf = new char[size]; //obinstream will delete it
	MPI_Bcast(recvbuf, size, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);

	obinstream um(recvbuf, size);
	um >> to_get;
}
