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

IF (GLOG_INCLUDE_DIR)
    SET (GLOG_FIND_QUIETLY TRUE)
ENDIF (GLOG_INCLUDE_DIR)

message(${GLOG_ROOT})

# find includes
FIND_PATH (GLOG_INCLUDE_DIR
    NAMES logging.h
    PATHS ${GLOG_ROOT}/include ${GLOG_ROOT}/include/glog
)

# find lib
SET(GLOG_NAME glog)

FIND_LIBRARY(GLOG_LIBRARIES
    NAMES ${GLOG_NAME}
    PATHS ${GLOG_ROOT}/lib
    NO_DEFAULT_PATH
)

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("GLOG" DEFAULT_MSG GLOG_INCLUDE_DIR GLOG_LIBRARIES)

mark_as_advanced (GLOG_INCLUDE_DIR GLOG_LIBRARIES)
