include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})
 
file(GLOB utils-src-files
    global.cpp
    hdfs_core.cpp
    )

add_library(utils-objs OBJECT ${utils-src-files})

#target_link_libraries(utils-objs ${GQUERY_EXTERNAL_LIBRARIES})
