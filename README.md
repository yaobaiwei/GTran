# GQuery
Distributed Online Property Graph Query System

This is the master branch!

Please never push your commits into master branch, instead using pull request to contribute your code!

I will review your code in the PR before merging, please make sure your code in the PR can be compiled and runnable.

Don't forget pull the lastest master branch before you want to PR to me!

noted by Hongzhi CHEN

# Build & Run

## Before

Set environment variable GQUERY_HOME. If you are in the working directory, you can run:
```
export GQUERT_HOME=$PWD
```

## Build

```
mkdir build; cd build; cmake ..; make -j;
```

## run server

```
sh start-server.sh $Process_NUM $Name_of_Machine.cfg $Name_of_IB.cfg
```

## run client

```
sh start-client.sh $Name_of_IB.cfg
```

# other files

> you may edit those files according to your need

## machine.cfg

machine file required by mpirun

## ib_conf

Ethernet and Infiniband hostnames and ports specification that required by gquery

## gquery-conf.ini

Detailed configurations.
