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

IF (IBVERB_INCLUDE_DIR)
    SET (IBVERB_FIND_QUIETLY TRUE)
ENDIF (IBVERB_INCLUDE_DIR)

message(${IBVERB_ROOT})

# find includes
FIND_PATH (IBVERB_INCLUDE_DIR
    NAMES verbs.h
    PATHS ${IBVERB_ROOT}/include ${IBVERB_ROOT}/include/infiniband
)

# find lib
SET(IBVERB_NAME ibverbs)

FIND_LIBRARY(IBVERB_LIBRARIES
    NAMES ${IBVERB_NAME}
    PATHS ${IBVERB_ROOT}/lib
    NO_DEFAULT_PATH
)

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("IBVERB" DEFAULT_MSG IBVERB_INCLUDE_DIR IBVERB_LIBRARIES)

mark_as_advanced (IBVERB_INCLUDE_DIR IBVERB_LIBRARIES)
