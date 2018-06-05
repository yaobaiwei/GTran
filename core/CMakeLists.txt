include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})
 
file(GLOB core-src-files
    rdma_mailbox.cpp
    )

add_library(core-objs OBJECT ${core-src-files})

#target_link_libraries(core-objs ${GQUERY_EXTERNAL_LIBRARIES})
