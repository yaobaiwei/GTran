include_directories(${PROJECT_SOURCE_DIR} ${GTRAN_EXTERNAL_INCLUDES})

file(GLOB expert-src-files
    add_edge_expert.cpp
    barrier_expert.cpp
    terminate_expert.cpp
    drop_expert.cpp
    expert_object.cpp
    expert_validation_object.cpp
    status_expert.cpp
    validation_expert.cpp
    )

add_library(expert-objs OBJECT ${expert-src-files})
