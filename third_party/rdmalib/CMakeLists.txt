include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})

file(GLOB rdma-src-files
	rdmaio.cpp
    )

add_library(rdma-objs OBJECT ${rdma-src-files})
