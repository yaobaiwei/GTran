include_directories(${PROJECT_SOURCE_DIR} ${GQS_EXTERNAL_INCLUDE})

file(GLOB storage-src-files
  abstract_storage.hpp
  storage.hpp
  abstract_cache.hpp)

add_library(storage-objs OBJECT ${storage-src-files})
set_property(TARGET storage-objs PROPERTY CXX_STANDARD 11)
add_dependencies(storage-objs ${external_project_dependencies})

