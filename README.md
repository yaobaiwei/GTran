# GQuery
Distributed Online Property Graph Query System

This is the actors branch!

Please never push your commits into master branch, instead using pull request to contribute your code!

I will review your code in the PR before merging, please make sure your code in the PR can be compiled and runnable.

Don't forget pull the lastest master branch before you want to PR to me!


noted by Hongzhi CHEN

# Modification

## scripts

### server-manual.py

#### 作用

在传入参数中获取节点数量。比如，python server-manual.py -n 7就是6个worker和一个master

根据gq-hfs.txt创建hostfile，包括给mpi用的manual-nodes.cfg和给gquery用的manual_conf

最后执行命令
``` python
os.system("export GQUERY_HOME=${PWD}; mpirun -n " + str(nnodes) + " -f manual-nodes.cfg ./release/server manual_conf")
```

### gq-hfs.txt

每行是一个数字。比如：

```
30
29
28
27
26
25
24
23
22
21

```

如果python server-manual.py -n 3的话，w30就会作为master，w29和w28作为client。

### start-server-small-amazon-7-nodes.sh

```
source runtime_environment.sh
./script/use_small.sh
python server-manual.py -n 7
```

### script/use_small.sh

```
rm gquery-conf.ini
ln -s small.gquery-conf.ini gquery-conf.ini
```

### start_client.sh

```
#使用刚刚生成的manual_conf来运行client
source runtime_environment.sh
./release/client ./manual_conf
```

## Optimization

### ActorCache

depend on core/message.hpp

### MKLUtil

depend on TidMapper and UglyThreadSafeMap

### MPISnapshot

depend on MPIConfigNamer and base/serialization.hpp

## Basic Utilization

### TidMapper

no dependency

### CPUInfoUtil

no dependency

### ConsoleUtil

no dependency

### MPIConfigNamer

no dependency

### MPIProfiler

no dependency

### UglyThreadSafeMap

no dependency


