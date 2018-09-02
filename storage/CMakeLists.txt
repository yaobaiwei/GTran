include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})
 
file(GLOB store-src-files
	layout.cpp
	data_store.cpp
    ekvstore.cpp
    vkvstore.cpp
	tcp_helper.cpp
    )

add_library(store-objs OBJECT ${store-src-files})

#target_link_libraries(store-objs ${GQUERY_EXTERNAL_LIBRARIES})
