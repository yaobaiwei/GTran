#Copyright 2018 Husky Data Lab, CUHK
#Authors: Hongzhi Chen

cmake_minimum_required(VERSION 3.3.0)
project(GQUERY)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/release/)

set(CMAKE_C_COMPILER   "/data/opt/brew/bin/mpicc")
set(CMAKE_CXX_COMPILER "/data/opt/brew/bin/mpic++")
set(CMAKE_BUILD_TYPE Release CACHE STRING "set build type to release")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -m64")

set(COMMON_LINK_LIBS "-lrt")

set(ZMQ_ROOT "/data/opt")
set(GLOG_ROOT "/data/hzchen/lib/glog")
set(HDFS2_ROOT "/data/opt/hadoop-2.6.0")
set(IBVERB_ROOT "/data/miao/lib/libibverbs-1.2.0")
set(RDMA_ROOT "/data/miao/lib/librdma-1.0.0")

add_definitions(-DUSE_HDFS2_)  #use hdfs v2.x.x by defalut
add_definitions(-DHAS_RDMA)  #use RDMA

find_package(Threads)

# External Dependencies
include(cmake/modules/glog.cmake)
include(cmake/modules/hdfs.cmake)
include(cmake/modules/ibverbs.cmake)
include(cmake/modules/librdma.cmake)
include(cmake/modules/zmq.cmake)

set(GQUERY_EXTERNAL_INCLUDES  ${GLOG_INCLUDE_DIR} ${HDFS2_INCLUDE_DIR} ${IBVERB_INCLUDE_DIR} ${RDMA_INCLUDE_DIR} ${ZMQ_INCLUDE_DIR})
set(GQUERY_EXTERNAL_LIBRARIES ${GLOG_LIBRARIES} ${HDFS2_LIBRARIES} ${IBVERB_LIBRARIES} ${RDMA_LIBRARIES} ${ZMQ_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} ${COMMON_LINK_LIBS})

add_subdirectory(base)
add_subdirectory(core)
add_subdirectory(storage)
add_subdirectory(utils)

add_subdirectory(driver)