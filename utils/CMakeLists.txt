include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})

add_subdirectory(iniparser)

file(GLOB utils-src-files
    console_util.cpp
    distributed_clock.cpp
    hdfs_core.cpp
    global.cpp
    mkl_util.cpp
    timer.cpp
    write_prior_rwlock.cpp
    )

add_library(utils-objs OBJECT ${utils-src-files})
