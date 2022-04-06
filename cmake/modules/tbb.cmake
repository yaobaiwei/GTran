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

IF (TBB_INCLUDE_DIR)
    SET (TBB_FIND_QUIETLY TRUE)
ENDIF (TBB_INCLUDE_DIR)

message(${TBB_ROOT})

# find includes
FIND_PATH (TBB_INCLUDE_DIR
    NAMES tbb.h
    PATHS ${TBB_ROOT}/include/tbb
)

# find lib
SET(TBB_NAME tbb)

FIND_LIBRARY(TBB_LIBRARIES
    NAMES ${TBB_NAME}
    PATHS ${TBB_ROOT}/build/linux_intel64_gcc_cc5.2.0_libc_kernel2.6.32_release
    NO_DEFAULT_PATH
)

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("TBB" DEFAULT_MSG TBB_INCLUDE_DIR TBB_LIBRARIES)

mark_as_advanced (TBB_INCLUDE_DIR TBB_LIBRARIES)
