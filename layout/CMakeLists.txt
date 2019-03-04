include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})
 
file(GLOB layout-src-files
    data_storage.cpp
    edge_property_row.cpp
    hdfs_data_loader.cpp
    layout_type.cpp
    mvcc_kv_store.cpp
    pmt_rct_table.cpp
    vtx_edge_row.cpp
    vtx_property_row.cpp
    )

add_library(layout-objs OBJECT ${layout-src-files})

#target_link_libraries(layout-objs ${GQUERY_EXTERNAL_LIBRARIES})
