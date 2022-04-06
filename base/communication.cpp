// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "base/communication.hpp"

int all_sum(int my_copy, MPI_Comm world) {
    int tmp;
    MPI_Allreduce(&my_copy, &tmp, 1, MPI_INT, MPI_SUM, world);
    return tmp;
}

long long master_sum_LL(long long my_copy, MPI_Comm world) {
    long long tmp = 0;
    MPI_Reduce(&my_copy, &tmp, 1, MPI_LONG_LONG_INT, MPI_SUM, MASTER_RANK, world);
    return tmp;
}

long long all_sum_LL(long long my_copy, MPI_Comm world) {
    long long tmp = 0;
    MPI_Allreduce(&my_copy, &tmp, 1, MPI_LONG_LONG_INT, MPI_SUM, world);
    return tmp;
}

char all_bor(char my_copy, MPI_Comm world) {
    char tmp;
    MPI_Allreduce(&my_copy, &tmp, 1, MPI_BYTE, MPI_BOR, world);
    return tmp;
}

bool all_lor(bool my_copy, MPI_Comm world) {
    bool tmp;
    MPI_Allreduce(&my_copy, &tmp, 1, MPI_BYTE, MPI_LOR, world);
    return tmp;
}

bool all_land(bool my_copy, MPI_Comm world) {
    bool tmp;
    MPI_Allreduce(&my_copy, &tmp, 1, MPI_CHAR, MPI_LAND, world);
    return tmp;
}

// size = #char / sizeof(uint64_t)
void send(void* buf, int size, int dst, MPI_Comm world, int tag) {
    MPI_Send(buf, size, MPI_UINT64_T, dst, tag, world);
}

// return the actual source, since "src" can be MPI_ANY_SOURCE
int recv(void* buf, int size, int src, MPI_Comm world, int tag) {
    MPI_Status status;
    MPI_Recv(buf, size, MPI_UINT64_T, src, tag, world, &status);
    return status.MPI_SOURCE;
}

void send_ibinstream(ibinstream& m, int dst, MPI_Comm world, int tag) {
    size_t size = m.size();
    send(&size, 1, dst, world, tag);

    size_t msg_size = ceil(size, sizeof(uint64_t)) / sizeof(uint64_t);
    send(m.get_buf(), msg_size, dst, world, tag);
}

void recv_obinstream(obinstream& m, int src, MPI_Comm world, int tag) {
    size_t size;
    // must receive the content (paired with the msg-size) from the msg-size source
    src = recv(&size, 1, src, world, tag);

    size_t msg_sz = ceil(size, sizeof(uint64_t)) / sizeof(uint64_t);
    char* buf = new char[msg_sz * sizeof(uint64_t)];
    recv(buf, msg_sz, src, world, tag);
    m.assign(buf, size, 0);
}
