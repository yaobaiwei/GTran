/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include "utils/global.hpp"

void InitMPIComm(int* argc, char*** argv, Node & node) {
    int provided;
    MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided != MPI_THREAD_MULTIPLE) {
        printf("MPI do not Support Multiple thread\n");
        exit(0);
    }
    int num_nodes;
    int rank;
    MPI_Comm_size(MPI_COMM_WORLD, &num_nodes);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    node.set_world_rank(rank);
    node.set_world_size(num_nodes);

    if (rank == MASTER_RANK)
        node.set_color(0);
    else
        node.set_color(1);

    MPI_Comm_split(MPI_COMM_WORLD, node.get_color(), node.get_world_rank(), &(node.local_comm));

    int local_rank, local_size;
    MPI_Comm_rank(node.local_comm, &local_rank);
    MPI_Comm_size(node.local_comm, &local_size);

    node.set_local_rank(local_rank);
    node.set_local_size(local_size);

    int name_len;
    char hostname[MPI_MAX_PROCESSOR_NAME];
    MPI_Get_processor_name(hostname, &name_len);
    node.hostname = std::move(string(hostname));
}

void worker_finalize(Node & node) {
    MPI_Comm_free(&(node.local_comm));
}

void worker_barrier(Node & node) {
    MPI_Barrier(node.local_comm);
}

void node_finalize() {
    MPI_Finalize();
}

void node_barrier() {
    MPI_Barrier(MPI_COMM_WORLD);
}

// ============================

void mk_dir(const char *dir) {
    char tmp[256];
    char *p = NULL;
    size_t len;

    snprintf(tmp, sizeof(tmp), "%s", dir);
    len = strlen(tmp);
    if (tmp[len - 1] == '/') tmp[len - 1] = '\0';
    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, S_IRWXU);
            *p = '/';
        }
    }
    mkdir(tmp, S_IRWXU);
}

void rm_dir(string path) {
    DIR* dir = opendir(path.c_str());
    struct dirent * file;
    while ((file = readdir(dir)) != NULL) {
        if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0)
            continue;
        string filename = path + "/" + file->d_name;
        remove(filename.c_str());
    }
    if (rmdir(path.c_str()) == -1) {
        perror("The following error occurred");
        exit(-1);
    }
}

void check_dir(string path, bool force_write) {
    if (access(path.c_str(), F_OK) == 0) {
        if (force_write) {
            rm_dir(path);
            mk_dir(path.c_str());
        } else {
            cout << path <<  " already exists!" << endl;
            exit(-1);
        }
    } else {
        mk_dir(path.c_str());
    }
}
// =========================================================
