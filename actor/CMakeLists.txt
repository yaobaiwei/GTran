include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})
 
file(GLOB actor-src-files
	actor_object.cpp
	actor_validation_object.cpp
    )

add_library(actor-objs OBJECT ${actor-src-files})
