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

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wthread-safety HAVE_CLANG_THREAD_SAFETY)

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
    "db/skiplist.h"
    "db/memtable.h"
    "db/memtable.cc"
    "db/dbformat.h"
    "db/dbformat.cc"
    "db/log_reader.h"
    "db/log_reader.cc"
    "db/log_writer.h"
    "db/log_writer.cc"
    "db/db_impl.h"
    "db/db_impl.cc"
    "db/write_batch_internal.h"
    "db/write_batch.cc"
    "db/filename.h"
    "db/filename.cc"
    "db/table_cache.h"
    "db/table_cache.cc"
    "table/block_builder.h"
    "table/block_builder.cc"
    "table/block.h"
    "table/block.cc"
    "table/filter_block.h"
    "table/filter_block.cc"
    "table/format.h"
    "table/format.cc"
    "table/iterator.cc"
    "table/table_builder.cc"
    "table/table.cc"
    "table/two_level_iterator.h"
    "table/two_level_iterator.cc"
    "table/iterator_wrapper.h"
    "util/arena.cc"
    "util/arena.h"
    "util/hash.h"
    "util/hash.cc"
    "util/coding.h"
    "util/coding.cc"
    "util/cache.cc"
    "util/bloom.cc"
    "util/logging.h"
    "util/logging.cc"
    "util/status.cc"
    "util/env.cc"

  PUBLIC
    "${TINYDB_PUBLIC_INCLUDE_DIR}/cache.h"
    "${TINYDB_PUBLIC_INCLUDE_DIR}/comparator.h"
    "${TINYDB_PUBLIC_INCLUDE_DIR}/db.h"
    "${TINYDB_PUBLIC_INCLUDE_DIR}/env.h"
    "${TINYDB_PUBLIC_INCLUDE_DIR}/filter_policy.h"
    "${TINYDB_PUBLIC_INCLUDE_DIR}/iterator.h"
    "${TINYDB_PUBLIC_INCLUDE_DIR}/options.h"
    "${TINYDB_PUBLIC_INCLUDE_DIR}/slice.h"
    "${TINYDB_PUBLIC_INCLUDE_DIR}/status.h"
    "${TINYDB_PUBLIC_INCLUDE_DIR}/table_builder.h"
    "${TINYDB_PUBLIC_INCLUDE_DIR}/table.h"
    "${TINYDB_PUBLIC_INCLUDE_DIR}/write_batch.h"
)


target_include_directories(tinydb
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(tinydb
  PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

if(HAVE_SNAPPY)
  target_link_libraries(tinydb snappy)
endif(HAVE_SNAPPY)
if(HAVE_ZSTD)
  target_link_libraries(tinydb zstd)
endif(HAVE_ZSTD)
if(HAVE_TCMALLOC)
  target_link_libraries(tinydb tcmalloc)
endif(HAVE_TCMALLOC)


option(TINYDB_BUILD_TESTS "Build unit tests" OFF)

if(TINYDB_BUILD_TESTS)
  enable_testing()

  # Prevent overriding the parent project's compiler/linker settings on Windows.
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(install_gtest OFF)
  set(install_gmock OFF)
  set(build_gmock ON)

  # This project is tested using GoogleTest.
  add_subdirectory("third_party/googletest")

  # GoogleTest triggers a missing field initializers warning.
  if(TINYDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)
    set_property(TARGET gtest
        APPEND PROPERTY COMPILE_OPTIONS -Wno-missing-field-initializers)
    set_property(TARGET gmock
        APPEND PROPERTY COMPILE_OPTIONS -Wno-missing-field-initializers)
  endif(TINYDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)

  add_executable(tinydb_tests "")
  target_sources(tinydb_tests
    PRIVATE
      "db/log_test.cc"
  )
  target_link_libraries(tinydb_tests tinydb gmock gtest gtest_main)
  target_compile_definitions(tinydb_tests
    PRIVATE
      ${TINYDB_PLATFORM_NAME}=1
  )

  add_test(NAME "tinydb_tests" COMMAND "tinydb_tests")

endif(TINYDB_BUILD_TESTS)