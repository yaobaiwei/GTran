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

#include "base/node.hpp"
#include "base/node_util.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"
#include "driver/master.hpp"
#include "driver/worker.hpp"

#include "glog/logging.h"


int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);

    Node my_node;
    InitMPIComm(&argc, &argv, my_node);

    string cfg_fname = argv[1];
    CHECK(!cfg_fname.empty());

    vector<Node> nodes = ParseFile(cfg_fname);
    CHECK(CheckUniquePort(nodes));
    Node & node = GetNodeById(nodes, my_node.get_world_rank());
    Node master = GetNodeById(nodes, 0);

    my_node.ibname = node.ibname;
    my_node.tcp_port = node.tcp_port;
    my_node.rdma_port = node.rdma_port;
    cout << my_node.DebugString();
    nodes.erase(nodes.begin());  // delete the master info in nodes (array) for rdma init

    // set my_node as the shared static Node instance
    my_node.InitialLocalWtime();
    Node::StaticInstance(&my_node);

    Config* config = Config::GetInstance();
    config->Init();

    // Erase redundant nodes
    if (nodes.size() > config->global_num_workers) {
        nodes.erase(nodes.begin() + config->global_num_workers, nodes.end());
    }
    assert(nodes.size() == config->global_num_workers);

    cout  << "DONE -> Config->Init()" << endl;

    if (my_node.get_world_rank() == MASTER_RANK) {
        Master master(my_node);
        master.Init();
        master.Start();
    } else {
        Worker worker(my_node, nodes);
        worker.Init();
        worker.Start();

        worker_barrier(my_node);
        worker_finalize(my_node);
    }

    node_barrier();
    node_finalize();

    return 0;
}
