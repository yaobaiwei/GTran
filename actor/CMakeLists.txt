include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})

file(GLOB actor-src-files
    actor_object.cpp
    actor_validation_object.cpp
    add_edge_actor.cpp
    barrier_actor.cpp
    commit_actor.cpp
    drop_actor.cpp
    status_actor.cpp
    validation_actor.cpp
    )

add_library(actor-objs OBJECT ${actor-src-files})
