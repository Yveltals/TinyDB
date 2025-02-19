cmake_minimum_required(VERSION 3.9)
project(tinydb VERSION 1.0.0 LANGUAGES C CXX)

# C++ standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

set(TINYDB_PLATFORM_NAME TINYDB_PLATFORM_POSIX)

include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
check_library_exists(zstd zstd_compress "" HAVE_ZSTD)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)

include(CheckCXXSymbolExists)
check_cxx_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)
check_cxx_symbol_exists(F_FULLFSYNC "fcntl.h" HAVE_FULLFSYNC)
check_cxx_symbol_exists(O_CLOEXEC "fcntl.h" HAVE_O_CLOEXEC)


set(TINYDB_PUBLIC_INCLUDE_DIR "include/tinydb")
set(TINYDB_PORT_CONFIG_DIR "include/port")

include_directories(
  "${PROJECT_BINARY_DIR}/include"
  "."
)


# Must be included before CMAKE_INSTALL_INCLUDEDIR is used.
include(GNUInstallDirs)

add_library(tinydb "")
target_sources(tinydb
  PRIVATE
    "util/arena.cc"
    "util/arena.h"
    "util/hash.h"
    "util/hash.cc"
    "util/coding.h"
    "util/coding.cc"
    "util/cache.cc"
    # "util/env_posix.cc"
    # "util/posix_logger.h"

  PUBLIC
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/c.h"
    "${TINYDB_PUBLIC_INCLUDE_DIR}/cache.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/comparator.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/db.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/dumpfile.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/env.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/export.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/filter_policy.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/iterator.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/options.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/slice.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/status.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/table_builder.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/table.h"
  #   "${TINYDB_PUBLIC_INCLUDE_DIR}/write_batch.h"
)


target_include_directories(tinydb
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(tinydb
  PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

if(HAVE_CRC32C)
  target_link_libraries(tinydb crc32c)
endif(HAVE_CRC32C)
if(HAVE_SNAPPY)
  target_link_libraries(tinydb snappy)
endif(HAVE_SNAPPY)
if(HAVE_ZSTD)
  target_link_libraries(tinydb zstd)
endif(HAVE_ZSTD)
if(HAVE_TCMALLOC)
  target_link_libraries(tinydb tcmalloc)
endif(HAVE_TCMALLOC)