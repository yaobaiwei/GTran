# GTran

# Build & Run

## Before

Set environment variable GTRAN_HOME. If you are in the working directory, you can run:
```
export GTRAN_HOME=$PWD
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

Ethernet and Infiniband hostnames and ports specification that required by gtran

## gtran-conf.ini

Detailed configurations.
