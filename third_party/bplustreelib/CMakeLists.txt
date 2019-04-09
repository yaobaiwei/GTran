include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})

file(GLOB bptree-src-files
    bplustree.cpp
    )
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++11 -fpermissive")
add_library(bptree-objs OBJECT ${bptree-src-files})
