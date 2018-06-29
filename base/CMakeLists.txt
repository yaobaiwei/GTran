include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})
 
file(GLOB base-src-files
	rdma.cpp
	node_util.cpp
    sarray_binstream.cpp
    type.cpp
    serialization.cpp
   	communication.cpp
   	client_connection.cpp
    )

add_library(base-objs OBJECT ${base-src-files})

#target_link_libraries(base-objs ${GQUERY_EXTERNAL_LIBRARIES})
