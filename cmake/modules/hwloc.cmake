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

IF (HWLOC_INCLUDE_DIR)
    SET (HWLOC_FIND_QUIETLY TRUE)
ENDIF (HWLOC_INCLUDE_DIR)

message(${HWLOC_ROOT})

# find includes
FIND_PATH (HWLOC_INCLUDE_DIR
    NAMES hwloc.h
    PATHS ${HWLOC_ROOT}/include
)

# find lib
SET(HWLOC_NAME hwloc)

FIND_LIBRARY(HWLOC_LIBRARIES
    NAMES ${HWLOC_NAME}
    PATHS ${HWLOC_ROOT}/lib
    NO_DEFAULT_PATH
)

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("HWLOC" DEFAULT_MSG HWLOC_INCLUDE_DIR HWLOC_LIBRARIES)

mark_as_advanced (HWLOC_INCLUDE_DIR HWLOC_LIBRARIES)
