### RDMA ###

IF (RDMA_INCLUDE_DIR)
    SET (RDMA_FIND_QUIETLY TRUE)
ENDIF (RDMA_INCLUDE_DIR)

message(${RDMA_ROOT})

# find includes
FIND_PATH (RDMA_INCLUDE_DIR
    NAMES rdmaio.h
    HINTS
    ${RDMA_ROOT}/include
)

# find lib
SET(RDMA_NAME rdma)

FIND_LIBRARY(RDMA_LIBRARIES
    NAMES ${RDMA_NAME}
    PATHS ${RDMA_ROOT}/lib
)

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("RDMA" DEFAULT_MSG RDMA_INCLUDE_DIR RDMA_LIBRARIES)

mark_as_advanced (RDMA_INCLUDE_DIR RDMA_LIBRARIES)
