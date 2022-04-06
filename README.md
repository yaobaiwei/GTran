# G-Tran


G-Tran is an *Remote Direct Memory Access (RDMA)*-enabled distributed in-memory graph database with serializable and snapshot isolation support.

Graph data are abundant today in both industrial applications and academic research. In order to support efficient graph data storage and management, graph databases have become an essential infrastructure. Most of existing graph databases are built upon NoSQL store (e.g., JanusGraph, ArangoDB) or graph-native store (e.g., Neo4j, TigerGraph) but without tailored graph transactional optimizations, which leads to relatively low throughput and high latency for graph OLTP workloads. We summarize the challenges of distributed graph transaction processing as follows:

- Graph data can easily result in poor locality for reads and writes in databases after continuous updates due to its irregularity.
- Due to the power-law distribution on vertex degree and the small world phenomenon of most real-world graphs, the cost of traversal-based queries can be very high after multi-hops (e.g., ≥ 2) fan-out, which may lead to higher contention and lower throughput in transaction processing.
- Latency and scalability is another critical issue, where network bandwidth, contention likelihood, and CPU overheads are the main bottlenecks. Traditional centralized system architecture (i.e., assigning a master node as the global coordinator) may limit the scalability for graph OLTP.

It motivates us to prototype **G-Tran**, a high-performance distributed graph database with new system architecture, graph data layout and concurrency control protocol. To address above challenges in graph OLTP, generally we provide:

- a graph-native data store with efficient data and memory layouts, which offers good data locality and fast data access for read/write graph transactions under frequent updates.
- a fully decentralized system architecture by leveraging the benefits of RDMA to avoid the bottleneck from centralized transaction coordinating, and each worker executes distributed transactions under the MPP (i.e., massive parallel processing) model.
- a multi-version-based optimistic concurrency control (MV-OCC) protocol, which is specifically designed to reduce the abort rate and CPU overheads in concurrent graph transaction processing.


**Please check the [paper](docs/G-Tran_arXiv.pdf) for more details.**

## Getting Started

**Requirements**

To install G-Tran's dependencies (G++[ver 5.2.0], MPI, JDK, HDFS2), please follow the instructions in [here](http://www.cse.cuhk.edu.hk/systems/gminer/deploy.html).
In addition, we also request the following libraries:
* [ZeroMQ](https://zeromq.org/download/)
* [GLog](https://github.com/google/glog)
* [Libibverbs-1.2.0](https://git.kernel.org/pub/scm/libs/infiniband/libibverbs.git)
* [Intel TBB](https://github.com/intel/tbb)
* [Intel MKL](https://software.intel.com/en-us/articles/intelr-mkl-and-c-template-libraries)`[Major version: 2017; Minor version: 0; Update version: 4]`

**Build**

Please manually SET the dependency paths for above libraries in CMakeLists.txt at the root directory.

```bash
$ export GTRAN_HOME=/path/to/gtran_root  # must configure this ENV
$ cd $GTRAN_HOME
$ ./auto-build.sh
```
**How to run**

Please follow this [**tutorial**](docs/Tutorial.md).


## Academic Paper

[**CoRR 2021**] [G-Tran: Making Distributed Graph Transactions Fast](https://arxiv.org/abs/2105.04449). Hongzhi Chen, Changji Li, Chenguang Zheng, Chenghuan Huang, Juncheng Fang, James Cheng, Jian Zhang

## License

Copyright 2020 Husky Data Lab, CUHK
