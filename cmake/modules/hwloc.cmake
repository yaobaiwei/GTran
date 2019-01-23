### HWLOC ###

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
