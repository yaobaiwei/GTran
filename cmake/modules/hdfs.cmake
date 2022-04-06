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

IF (HDFS2_INCLUDE_DIR)
    SET (HDFS2_FIND_QUIETLY TRUE)
ENDIF (HDFS2_INCLUDE_DIR)

message(${HDFS2_ROOT})

# find includes
FIND_PATH (HDFS2_INCLUDE_DIR
    NAMES hdfs.h
    PATHS ${HDFS2_ROOT}/include
)

# find lib
SET(HDFS2_NAME hdfs)

FIND_LIBRARY(HDFS2_LIBRARIES
    NAMES ${HDFS2_NAME}
    PATHS ${HDFS2_ROOT}/lib ${HDFS2_ROOT}/lib/native
    NO_DEFAULT_PATH
)

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("HDFS2" DEFAULT_MSG HDFS2_INCLUDE_DIR HDFS2_LIBRARIES)

mark_as_advanced (HDFS2_INCLUDE_DIR HDFS2_LIBRARIES)
