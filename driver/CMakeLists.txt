include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})
 
add_library(all-deps STATIC
  $<TARGET_OBJECTS:iniparser-objs>
  $<TARGET_OBJECTS:store-objs>
  $<TARGET_OBJECTS:core-objs>
  $<TARGET_OBJECTS:utils-objs>
  $<TARGET_OBJECTS:base-objs>
  $<TARGET_OBJECTS:actor-objs>
  $<TARGET_OBJECTS:rdma-objs>
  $<TARGET_OBJECTS:layout-objs>
  $<TARGET_OBJECTS:bptree-objs>
  )

add_executable(client client.cpp )
target_link_libraries(client all-deps)
target_link_libraries(client ${GQUERY_EXTERNAL_LIBRARIES})

add_executable(server server.cpp)
target_link_libraries(server all-deps)
target_link_libraries(server ${GQUERY_EXTERNAL_LIBRARIES})
