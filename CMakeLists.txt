cmake_minimum_required(VERSION 3.9)
project(tinydb VERSION 1.0.0 LANGUAGES C CXX)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wthread-safety HAVE_CLANG_THREAD_SAFETY)

include(GNUInstallDirs)

include_directories(
  "${PROJECT_BINARY_DIR}/include"
  "."
)

add_subdirectory("third_party/fmt")

# ###########################################################
# TinyDB
# ###########################################################
add_library(tinydb "")
target_sources(tinydb
  PRIVATE
  "common/status.cc"
  "db/db_impl.cc"
  "db/dbformat.cc"
  "db/memtable.cc"
  "db/table_cache.cc"
  "db/version_edit.cc"
  "db/version_set.cc"
  "db/write_batch.cc"
  "iterator/iterator_merger.cc"
  "iterator/iterator_two_level.cc"
  "log/log_reader.cc"
  "log/log_writer.cc"
  "log/logging.cc"
  "table/block.cc"
  "table/block_builder.cc"
  "table/filter_block.cc"
  "table/format.cc"
  "table/table.cc"
  "table/table_builder.cc"
  "util/arena.cc"
  "util/bloom.cc"
  "util/cache.cc"
  "util/coding.cc"
  "util/file.cc"
  "util/filename.cc"
  "util/hash.cc"
)

target_include_directories(tinydb
  PUBLIC
  "${PROJECT_SOURCE_DIR}/common"
  "${PROJECT_SOURCE_DIR}/db"
  "${PROJECT_SOURCE_DIR}/iterator"
  "${PROJECT_SOURCE_DIR}/log"
  "${PROJECT_SOURCE_DIR}/table"
  "${PROJECT_SOURCE_DIR}/util"
)

set_target_properties(tinydb
  PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

if(HAVE_CLANG_THREAD_SAFETY)
  target_compile_options(tinydb
    PUBLIC
    -Werror -Wthread-safety)
endif(HAVE_CLANG_THREAD_SAFETY)

target_link_libraries(tinydb PRIVATE fmt::fmt)

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

# ###########################################################
# Only For Test
# ###########################################################
option(TEST_BUILD "Build test for debug" OFF)

if(TEST_BUILD)
  add_executable(test "")
  target_sources(test
    PRIVATE
    "test.cc"
  )
  target_include_directories(test
    PUBLIC
    "${PROJECT_SOURCE_DIR}/log"
  )
  target_link_libraries(test fmt::fmt)
endif(TEST_BUILD)