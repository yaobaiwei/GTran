include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})

file(GLOB mvcc-src-files
	   concurrent_ll.cpp
    )

add_library(mvcc-objs OBJECT ${mvcc-src-files})
