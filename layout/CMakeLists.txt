include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})
 
file(GLOB layout-src-files
    data_storage.cpp
    hdfs_data_loader.cpp
    gc_executor.cpp
    layout_type.cpp
    mpi_snapshot_manager.cpp
    mvcc_definition.cpp
    mvcc_value_store.cpp
    pmt_rct_table.cpp
    topology_row_list.cpp
    )

add_library(layout-objs OBJECT ${layout-src-files})

#target_link_libraries(layout-objs ${GQUERY_EXTERNAL_LIBRARIES})
