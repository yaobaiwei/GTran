### GLOG ###

include (GNUInstallDirs)

if(GLOG_SEARCH_PATH)
    # Note: if using GLOG_SEARCH_PATH, the customized format is not activated.
    find_path(GLOG_INCLUDE_DIR NAMES glog/logging.h PATHS ${GLOG_SEARCH_PATH} NO_SYSTEM_ENVIRONMENT_PATH)
    find_library(GLOG_LIBRARY NAMES glog PATHS ${GLOG_SEARCH_PATH} NO_SYSTEM_ENVIRONMENT_PATH)
    message(STATUS "Found GLog in search path ${GLOG_SEARCH_PATH}")
    message(STATUS "  (Headers)       ${GLOG_INCLUDE_DIR}")
    message(STATUS "  (Library)       ${GLOG_LIBRARY}")
else(GLOG_SEARCH_PATH)
    include(ExternalProject)
    set(THIRDPARTY_DIR ${PROJECT_BINARY_DIR}/third_party)
    ExternalProject_Add(
        glog
        GIT_REPOSITORY "https://github.com/google/glog"
        GIT_TAG v0.3.5
        PREFIX ${THIRDPARTY_DIR}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${PROJECT_BINARY_DIR}
        CMAKE_ARGS -DWITH_GFLAGS=OFF
        CMAKE_ARGS -DBUILD_TESTING=OFF
        CMAKE_ARGS -DBUILD_SHARED_LIBS=OFF
        UPDATE_COMMAND ""
    )
    list(APPEND external_project_dependencies glog)
    set(GLOG_INCLUDE_DIR "${PROJECT_BINARY_DIR}/include")
    if(WIN32)
        set(GLOG_LIBRARY "${PROJECT_BINARY_DIR}/lib/libglog.lib")
    else(WIN32)
        set(GLOG_LIBRARY "${PROJECT_BINARY_DIR}/lib/libglog.a")
    endif(WIN32)
    message(STATUS "GLog will be built as a third party")
    message(STATUS "  (Headers should be)       ${GLOG_INCLUDE_DIR}")
    message(STATUS "  (Library should be)       ${GLOG_LIBRARY}")
endif(GLOG_SEARCH_PATH)
