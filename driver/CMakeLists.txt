include_directories(${PROJECT_SOURCE_DIR} ${GQUERY_EXTERNAL_INCLUDES})
 
add_library(all-deps STATIC
  $<TARGET_OBJECTS:base-objs>
  $<TARGET_OBJECTS:core-objs>
  $<TARGET_OBJECTS:store-objs>
  $<TARGET_OBJECTS:utils-objs>
  $<TARGET_OBJECTS:iniparser-objs>
  )
  
add_executable(driver gquery.cpp)
target_link_libraries(driver all-deps)
target_link_libraries(driver ${GQUERY_EXTERNAL_LIBRARIES})
