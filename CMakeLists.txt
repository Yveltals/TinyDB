cmake_minimum_required(VERSION 3.9)
project(tinydb VERSION 1.0.0 LANGUAGES C CXX)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

include(CheckLibraryExists)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wthread-safety HAVE_CLANG_THREAD_SAFETY)

include(GNUInstallDirs)

include_directories(
  "${PROJECT_BINARY_DIR}/include"
  "."
)

# ###########################################################
# TinyDB
# ###########################################################
add_library(tinydb "")
target_sources(tinydb
  PRIVATE
  "common/comparator.h"
  "common/filter_policy.h"
  "common/iterator.h"
  "common/options.h"
  "common/slice.h"
  "common/status.h"
  "common/status.cc"
  "db/db.h"
  "db/db_impl.h"
  "db/db_impl.cc"
  "db/dbformat.h"
  "db/dbformat.cc"
  "util/filename.h"
  "util/filename.cc"
  "log/log_reader.h"
  "log/log_reader.cc"
  "log/log_writer.h"
  "log/log_writer.cc"
  "db/memtable.h"
  "db/memtable.cc"
  "db/skiplist.h"
  "db/snapshot.h"
  "db/table_cache.h"
  "db/table_cache.cc"
  "db/version_edit.h"
  "db/version_edit.cc"
  "db/version_set.h"
  "db/version_set.cc"
  "db/write_batch.h"
  "db/write_batch.cc"
  "table/block_builder.h"
  "table/block_builder.cc"
  "table/block.h"
  "table/block.cc"
  "table/filter_block.h"
  "table/filter_block.cc"
  "table/format.h"
  "table/format.cc"
  "table/table.h"
  "table/table.cc"
  "table/table_builder.h"
  "table/table_builder.cc"
  "iterator/iterator_base.h"
  "iterator/iterator_block.h"
  "iterator/iterator_level_files.h"
  "iterator/iterator_merger.h"
  "iterator/iterator_merger.cc"
  "iterator/iterator_two_level.h"
  "iterator/iterator_two_level.cc"
  "util/arena.cc"
  "util/arena.h"
  "util/bloom.cc"
  "util/cache.h"
  "util/cache.cc"
  "util/coding.h"
  "util/coding.cc"
  "util/file.h"
  "util/file.cc"
  "util/thread_pool.h"
  "util/hash.h"
  "util/hash.cc"
  "log/logging.h"
  "log/logging.cc"
)

target_include_directories(tinydb
  PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(tinydb
  PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

if(HAVE_CLANG_THREAD_SAFETY)
  target_compile_options(tinydb
    PUBLIC
    -Werror -Wthread-safety)
endif(HAVE_CLANG_THREAD_SAFETY)

if(HAVE_TCMALLOC)
  target_link_libraries(tinydb tcmalloc)
endif(HAVE_TCMALLOC)

# ###########################################################
# Gtest
# ###########################################################
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
  add_test(NAME "tinydb_tests" COMMAND "tinydb_tests")
endif(TINYDB_BUILD_TESTS)