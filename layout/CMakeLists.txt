include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})
 
file(GLOB layout-src-files
    pmt_rct_table.cpp
    )

add_library(layout-objs OBJECT ${layout-src-files})
