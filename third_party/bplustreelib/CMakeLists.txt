include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})

file(GLOB bptree-src-files
    bplustree.c
    )

add_library(bptree-objs OBJECT ${bptree-src-files})
