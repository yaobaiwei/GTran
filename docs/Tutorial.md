## G-Tran Tutorial

### Sample datasets

In the repo, we already provide a sample dataset at folder `/data`, which matches with the toy graph presented in this [**picture**](../data/graph-example-1.jpg).
The fold contains four subfolders: `/vertices`, `/vtx_property`, `/edge_property` and `/index`, which stores the data of vertices (graph topology), the properties of vertices, the properties of edges and the index information respectively.

#### Input format

* The files in `/vertices`: each file contains a number of lines, where each line represents one vertex as well as its adjacency list.
	```
	{vid} \tab {num_in_neighbors} \tab {neighbor1} \s {neighbor2} \s ... {neighborN} \s {num_out_neighbors} \tab {neighbor1} \s {neighbor2} \s ... {neighborN} \s
	```

* The files in `/vtx_property`: each file contains a number of lines, where each line represents one vertex as well as its **v_label** and property information (formed by a list of key-value pair <**vp_key**, val>).
	```
	{vid} \tab {label} \tab [vp_key1:val,vp_key2:val...]
	```

* The files in `/edge_property`: each file contains a number of lines, where each line represents one edge as well as its **e_label** and property information (formed by a list of key-value pair <**ep_key**, val>).
	```
	{in_vid} \tab {out_vid} \tab {label} \tab [ep_key1:val,ep_key2:val...]
	```

* In subfolder `/index`, there are four files named `vtx_label`, `edge_label`, `vtx_property_index`, `edge_property_index`.

	The files `vtx_label` and `edge_label` are used to record the *string-id* maps which converts the **v_label** and **e_label** from `string` to `int` stored in the memory for data compression. Each line of these two files follows the format: 
	```bash
	#string \tab int
	#for example:
	person  1
	software    2
	```

	The files `vtx_property_index` and `edge_property_index` are used to record the *string-id* maps which converts the **vp_key** and **ep_key** from `string` to `int` stored in the memory for data compression. Each line of these two files follows the format: 
	```bash
	#string \tab int
	#for example:
	name	1
	age 	2
	lang	3
	```

### Uploading the dataset to HDFS
G-Tran reads data from HDFS, and it will handle the graph partition automatically. Users need to upload their data onto HDFS based on the format  sample `/data` as we described above.

However, we cannot use the command `hadoop fs -put {local-file} {hdfs-path}` directly. Otherwise, the data will be loaded by one G-Tran process only, while the other processes are just waiting there until data loading is finished. We support parallel read to speed up the load processing, and it has no influence on the performance of graph partitioning and OLAP querying.

To enable it, we suggest to upload graph data (i.e., `/vertices`, `/vtx_property`, `/edge_property`) onto HDFS by running our `put` program with two arguments **{local-file}** and **{hdfs-path}**.
```bash
$ cd $GTRAN_HOME
$ mpiexec -n 1 $GTRAN_HOME/release/put /local_path/to/dataset/{local-file} /hdfs_path/to/dataset/
```


### Configuring and running G-Tran

**1** Edit `gtran-conf.ini`, `machine.cfg` and `ib.cfg`

**gtran-conf.ini:**
```bash
#ini file for example
#new line to end this ini

[HDFS]
HDFS_HOST_ADDRESS = master      #the hostname of hdfs name node
HDFS_PORT = 9000		        #the port of HDFS
HDFS_INPUT_PATH = /hdfs_path/to/input/
HDFS_INDEX_PATH = /hdfs_path/to/input/index/
HDFS_VTX_SUBFOLDER = /hdfs_path/to/input/vertices/
HDFS_VP_SUBFOLDER = /hdfs_path/to/input/vtx_property/
HDFS_EP_SUBFOLDER = /hdfs_path/to/input/edge_property/
HDFS_OUTPUT_PATH = /hdfs_path/to/input/output/

[SYSTEM]
ISOLATION_LEVEL = SERIALIZABLE  	# i.e., SERIALIZABLE or SNAPSHOT
NUM_THREADS = 20                	# num of local computing threads
NUM_GC_CONSUMER = 2             	# num of threads to execute GC
NUM_PARSER_THREADS = 2          	# num of threads to process query parser, suggested value: 1 or 2
VTX_P_KV_SZ_GB = 2              	# the size of KVS allocated for VTX Property, unit in #GB
EDGE_P_KV_SZ_GB = 1             	# the size of KVS allocated for EDGE Property, unit in #GB
PER_SEND_BUF_SZ_MB = 2          	# the size of send-buff for each thread, unit in #MB
PER_RECV_BUF_SZ_MB = 64         	# the size of recv-buff for each worker, unit in #MB
VE_ROW_POOL_SIZE = 20000000     	# the capacity of ConcurrentMemPool<VertexEdgeRow> in data store
VP_ROW_POOL_SIZE = 12000000     	# the capacity of ConcurrentMemPool<VertexPropertyRow> in data store 
EP_ROW_POOL_SIZE = 15000000     	# the capacity of ConcurrentMemPool<EdgePropertyRow> in data store
V_MVCC_POOL_SIZE = 10000000     	# the capacity of ConcurrentMemPool<VertexMVCCItem> in data store
E_MVCC_POOL_SIZE = 100000000     	# the capacity of ConcurrentMemPool<EdgeMVCCItem> in data store
VP_MVCC_POOL_SIZE = 80000000    	# the capacity of ConcurrentMemPool<VPropertyMVCCItem> in data store
EP_MVCC_POOL_SIZE = 20000000    	# the capacity of ConcurrentMemPool<EPropertyMVCCItem> in data store
PREDICT_CONTAINER_USAGE = true  	# to output the prediction of the size of above ConcurrentMemPools, please do not set to false unless you know what you do 
TRX_TABLE_SZ_MB = 1024          	# the size of TransactionStatusTable allocated on each worker
USE_RDMA = true                 	# if enable RDMA, set false to use TCP for commun
ENABLE_MEM_POOL_UTILIZATION_RECORD = true  	# to set if report the mem pool util during GC 
ENABLE_CACHE = true             	#if enable cache in experts of transaction processing 
ENABLE_CORE_BIND = true         	#if enable core-bind, see more details in our GTran proj
ENABLE_EXPERT_DIVISION = true   	#if enable expert division for logical thread regions, only useful when core-bind is on.
ENABLE_STEP_REORDER = true      	#if enable query-step reorder for query optimization
ENABLE_INDEXING = true          	#if enable index construction
ENABLE_STEALING = true          	#if enable index construction
ENABLE_GARBAGE_COLLECT = true   	#if enable GC, please do not set to false unless you know what you do
ENABLE_OPT_PREREAD = true       	#if enable OPT(pre-read) in our transaction processing protocol, please do not set to false unless you know what you do
ENABLE_OPT_VALIDATION = true    	#if enable OPT(optimistic-validation) in our transaction processing protocol, please do not set to false unless you know what you do
MAX_MSG_SIZE = 65536            	#(bytes), the upper-bound of message size for splitting
SNAPSHOT_PATH = ~/tmp/gtran_snapshot 	# the local path to store the graph snapshot on disk, to avoid repeatedly data loading when reboot the system.

[GC]
#TODO(Aaron), fill in the annotations for the below variables
ERASE_V_T_THRESHOLD = 1000
ERASE_OUTE_T_THRESHOLD = 2000
ERASE_INE_T_THRESHOLD = 2000
VMVCC_GC_T_THRESHOLD = 2000
VPMVCC_GC_T_THRESHOLD = 2000
EPMVCC_GC_T_THRESHOLD = 2000
EMVCC_GC_T_THRESHOLD = 2000
TOPO_ROW_GC_T_THRESHOLD = 2000
TOPO_ROW_DEFRAG_T_THRESHOLD = 2000
VP_ROW_GC_T_THRESHOLD = 2000
VP_ROW_DEFRAG_T_THRESHOLD = 2000
EP_ROW_GC_T_THRESHOLD = 2000
EP_ROW_DEFRAG_T_THRESHOLD = 2000
TOPO_INDEX_GC_RATIO = 50
PROP_INDEX_GC_RATIO = 20
PROP_INDEX_GC_T_THRESHOLD = 100
RCT_GC_T_THRESHOLD = 100

```

**machine.cfg (one per line):**
```bash
w1
w2
w3
w4
w5
.......
```

**ib.cfg (one per line):**
```bash
#format: hostname:ibname:tcp_port:ib_port
w1:ib1:19935:-1
w2:ib2:19935:19936
w3:ib3:19935:19936
w4:ib4:19935:19936
w5:ib5:19935:19936
.......
```

**2** To start the G-Tran servers, the user only needs to execute the script we provide as follow:

```bash
$ sh $GTRAN_HOME/start-server.sh {$NUM_of_Server+1} machine.cfg ib.cfg

#sample log
[hzchen@master GTran]$ sh ./start-server.sh 5 machine.cfg ib.cfg
Node: { world_rank = 0 world_size = 5 local_rank = 0 local_size = 1 color = 0 hostname = worker1 ibname = ib1}
Node: { world_rank = 1 world_size = 5 local_rank = 0 local_size = 4 color = 1 hostname = worker2 ibname = ib2}
Node: { world_rank = 3 world_size = 5 local_rank = 2 local_size = 4 color = 1 hostname = worker3 ibname = ib3}
Node: { world_rank = 2 world_size = 5 local_rank = 1 local_size = 4 color = 1 hostname = worker4 ibname = ib4}
Node: { world_rank = 4 world_size = 5 local_rank = 3 local_size = 4 color = 1 hostname = worker5 ibname = ib5}
given SNAPSHOT_PATH = /home/hzchen/tmp/gtran_snapshot, processed = /home/hzchen/tmp/gtran_snapshot
DONE -> Config->Init() 
DONE -> Config->Init()
DONE -> Config->Init()
DONE -> Config->Init()
DONE -> Config->Init()
local_rank_ == 0 node 0: cores: [ 0 2 4 6], [ 8 10], [ 12 14], [ 1 3], [ 5 7 9], [ 11 13 15], threads: [ 0 4 8 12 16 20 24 28], [ 1 5 17 21], [ 9 13 25 29], [ 2 6 18 22], [ 10 14 3 26 30 19], [ 7 11 15 23 27 31], 
local_rank_ == 1 node 1: cores: [ 0 2 4 6], [ 8 10], [ 12 14], [ 1 3], [ 5 7 9], [ 11 13 15], threads: [ 0 4 8 12 16 20 24 28], [ 1 5 17 21], [ 9 13 25 29], [ 2 6 18 22], [ 10 14 3 26 30 19], [ 7 11 15 23 27 31], 
local_rank_ == 2 node 2: cores: [ 0 2 4 6], [ 8 10], [ 12 14], [ 1 3], [ 5 7 9], [ 11 13 15], threads: [ 0 4 8 12 16 20 24 28], [ 1 5 17 21], [ 9 13 25 29], [ 2 6 18 22], [ 10 14 3 26 30 19], [ 7 11 15 23 27 31], 
local_rank_ == 3 node 3: cores: [ 0 2 4 6], [ 8 10], [ 12 14], [ 1 3], [ 5 7 9], [ 11 13 15], threads: [ 0 4 8 12 16 20 24 28], [ 1 5 17 21], [ 9 13 25 29], [ 2 6 18 22], [ 10 14 3 26 30 19], [ 7 11 15 23 27 31], 
Worker0: DONE -> Init Core Affinity 
Worker2: DONE -> Init Core Affinity
Worker1: DONE -> Init Core Affinity 
Worker3: DONE -> Init Core Affinity 
Worker0: DONE -> Register RDMA MEM, SIZE = 12773753312 
Worker3: DONE -> Register RDMA MEM, SIZE = 12773753312 
Worker1: DONE -> Register RDMA MEM, SIZE = 12773753312 
Worker2: DONE -> Register RDMA MEM, SIZE = 12773753312 
[librdma] : listener binding: tcp://*:19936 
[librdma] : listener binding: tcp://*:19936 
[librdma] : listener binding: tcp://*:19936 
[librdma] : listener binding: tcp://*:19936
INFO: initializing RDMA done (1168 ms)
Worker3: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
INFO: initializing RDMA done (1210 ms)
Worker0: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
INFO: initializing RDMA done (1175 ms)
Worker1: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
INFO: initializing RDMA done (1164 ms)
Worker2: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
Worker3: DONE -> DataStore->Init()
Worker1: DONE -> DataStore->Init()
Worker0: DONE -> DataStore->Init()
Worker2: DONE -> DataStore->Init()
INFO: initializing RDMA done (1168 ms)
Worker3: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
INFO: initializing RDMA done (1210 ms)
Worker0: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
INFO: initializing RDMA done (1175 ms)
Worker1: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
INFO: initializing RDMA done (1164 ms)
Worker2: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
Worker3: DONE -> DataStore->Init()
Worker1: DONE -> DataStore->Init()
Worker0: DONE -> DataStore->Init()
Worker2: DONE -> DataStore->Init()
Node 1 get_string_indexes() DONE !
Node 1 Get_vertices() DONE !
Node 1 Get_vplist() DONE !
Node 1 Get_eplist() DONE !
Node 3 get_string_indexes() DONE !
Node 3 Get_vertices() DONE !
Node 3 Get_vplist() DONE !
Node 3 Get_eplist() DONE !
Node 2 get_string_indexes() DONE !
Node 2 Get_vertices() DONE !
Node 2 Get_vplist() DONE !
Node 2 Get_eplist() DONE !
Node 0 get_string_indexes() DONE !
Node 0 Get_vertices() DONE !
Node 0 Get_vplist() DONE !
Node 0 Get_eplist() DONE !
get_vertices snapshot->TestRead('datastore_v_table')
Shuffle snapshot->TestRead('vkvstore')
Shuffle snapshot->TestRead('ekvstore')
Worker1: DONE -> DataStore->Shuffle()
Worker2: DONE -> DataStore->Shuffle()
Worker3: DONE -> DataStore->Shuffle()
Worker0: DONE -> DataStore->Shuffle()
DataConverter snapshot->TestRead('datastore_v_table')
Worker1: DONE -> Datastore->DataConverter()
Worker3: DONE -> Datastore->DataConverter()
Worker2: DONE -> Datastore->DataConverter()
Worker0: DONE -> Datastore->DataConverter()
Worker0: DONE -> Parser_->LoadMapping()
Worker2: DONE -> Parser_->LoadMapping()
Worker1: DONE -> Parser_->LoadMapping()
Worker3: DONE -> Parser_->LoadMapping()
Worker1: DONE -> expert_adapter->Start()
Worker3: DONE -> expert_adapter->Start()
Worker0: DONE -> expert_adapter->Start()
Worker2: DONE -> expert_adapter->Start()
GTran Servers Are All Ready ...

```

3. To start the G-Tran client, the user only needs to execute the script we provide as follow:

```bash
$ sh $GTRAN_HOME/start-client.sh ib.cfg

#sample log
[hzchen@master GTran]$ sh ./start-client.sh ib.cfg 
DONE -> Client->Init()
GTran> help
GTran commands: 
    help                display general help infomation
    help index          display help infomation for building index
    help config         display help infomation for setting config
    help emu            display help infomation for running emulation of througput test
    quit                quit from console
    GTran <args>       run Gremlin-Like queries
        -q <query> [<args>] a single query input by user
           -o <file>           output results into <file>
        -f <file> [<args>]  a single query from <file>
           -o <file>           output results into <file>
        -b <file> [<args>]  a set of queries configured by <file> (batch-mode)
           -o <file>           output results into <file>


GTran> GTran -q g.V().hasKey("<http://dbpedia.org/property/publisher>").hasLabel("link").has("<http://dbpedia.org/property/language>", "Irish")
[Client] Processing query : g.V().hasKey("<http://dbpedia.org/property/publisher>").hasLabel("link").has("<http://dbpedia.org/property/language>", "Irish")
[Client] Client just posted a REQ
[Client] Client 1 recvs a REP: get available worker_node0

[Client] Client posts the query to worker_node0

[Client] result: Query 'g.V().hasKey("<http://dbpedia.org/property/publisher>").hasLabel("link").has("<http://dbpedia.org/property/language>", "Irish")' result:
=>26693946
=>26693940
=>26668218
=>14265685
=>27713332
=>15486153
=>28085459
=>15513537
=>28249935
=>15819014
=>16591506
=>21324100
=>20466007
=>21352851
=>22472949
=>22411388
=>23789152
=>24437478
=>24437479
[Timer] 1309.06 ms for ProcessQuery
[Timer] 1319 ms for whole process.

```

4. Advanced Usage

G-Tran also supports index construction on vertices and on vtx_property/edge_property to speedup the query. And we provide a command in client console to evaluate the throughput performance too, please follow this [**doc**](./HOW_TO_RUN.txt) for reference.
