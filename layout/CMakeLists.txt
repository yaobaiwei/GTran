# Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_directories(${PROJECT_SOURCE_DIR} ${GTRAN_EXTERNAL_INCLUDES})
 
file(GLOB layout-src-files
    data_storage.cpp
    hdfs_data_loader.cpp
    garbage_collector.cpp
    gc_consumer.cpp
    gc_producer.cpp
    index_store.cpp
    mpi_snapshot_manager.cpp
    mvcc_definition.cpp
    mvcc_value_store.cpp
    pmt_rct_table.cpp
    topology_row_list.cpp
    )

add_library(layout-objs OBJECT ${layout-src-files})

#target_link_libraries(layout-objs ${GTRAN_EXTERNAL_LIBRARIES})
