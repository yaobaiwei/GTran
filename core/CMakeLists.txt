include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})

file(GLOB core-src-files
    exec_plan.cpp
    message.cpp
    rdma_mailbox.cpp
    tcp_mailbox.cpp
    parser.cpp
    RCT.cpp
    transactions_table.cpp
    worker_master_message.cpp
    bplustree.c
    common.cpp
    )

# add a OBJECT library called core-objs
add_library(core-objs OBJECT ${core-src-files})

#target_link_libraries(core-objs ${GQUERY_EXTERNAL_LIBRARIES})
