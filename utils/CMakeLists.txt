include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})

add_subdirectory(iniparser)

file(GLOB utils-src-files
    global.cpp
    hdfs_core.cpp
	timer.cpp
    console_util.cpp
    tid_mapper.cpp
    )

add_library(utils-objs OBJECT ${utils-src-files})
