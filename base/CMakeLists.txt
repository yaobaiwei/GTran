# Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
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

file(GLOB base-src-files
    cpuinfo_util.cpp
    rdma.cpp
    node_util.cpp
    type.cpp
    serialization.cpp
    communication.cpp
    client_connection.cpp
    predicate.cpp
    )

add_library(base-objs OBJECT ${base-src-files})

#target_link_libraries(base-objs ${GTRAN_EXTERNAL_LIBRARIES})
