include_directories(${PROJECT_SOURCE_DIR} ${GTRAN_EXTERNAL_INCLUDES})
 
file(GLOB layout-src-files
    data_storage.cpp
    hdfs_data_loader.cpp
    garbage_collector.cpp
    gc_consumer.cpp
    gc_producer.cpp
    index_store.cpp
    mpi_snapshot_manager.cpp
    mvcc_definition.cpp
    mvcc_value_store.cpp
    pmt_rct_table.cpp
    topology_row_list.cpp
    )

add_library(layout-objs OBJECT ${layout-src-files})

#target_link_libraries(layout-objs ${GTRAN_EXTERNAL_LIBRARIES})
