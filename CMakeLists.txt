# Windows 64-bit:
#
# Prerequisites:
#     Tested with Visual Studio 2019. Start the Developer Command Prompt via 
#     Tools->Command Line->Developer Command Prompt. 
#     Be sure to use 64 bit version of Developer Command Prompt.
#     Run the build commands from within the Developer Command Prompt window to 
#     have paths to the compiler and runtime libraries set.
#     You must have git.exe in your %PATH% environment variable.
#
# To build VidarDB for Windows:
#
# 1. Update paths to third-party libraries in thirdparty.inc file
# 2. Create a new directory "build" for build artifacts and go into it.
# 3. Run cmake to generate project files for Windows, add more options to enable 
#    required third-party libraries. See thirdparty.inc for more information.
#        sample command: cmake ..
# 4. Then build the project in release mode (you may want to add /m[:<N>] flag 
#    to run msbuild in <N> parallel threads or simply /m to use all avail cores)
#        msbuild vidardb.sln /p:Configuration=Release
#
# Linux:
#
# 1. C++11 required, and build-essential.
# 2. mkdir build; cd build
# 3. cmake ..
# 4. make -j(number of cores)

cmake_minimum_required(VERSION 2.6)
project(vidardb)

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")

if(WIN32)
  include(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty.inc)
else()
  option(WITH_JEMALLOC "build with JeMalloc" OFF)
  if(WITH_JEMALLOC)
    find_package(JeMalloc REQUIRED)
    add_definitions(-DVIDARDB_JEMALLOC)
    include_directories(${JEMALLOC_INCLUDE_DIR})
  endif()
  if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    # FreeBSD has jemaloc as default malloc
    add_definitions(-DVIDARDB_JEMALLOC)
    set(WITH_JEMALLOC ON)
  endif()
  # Use Snappy if installed, not find is also okay
  option(WITH_SNAPPY "build with SNAPPY" ON)
  if(WITH_SNAPPY)
    find_package(Snappy)
    if(Snappy_FOUND)
      add_definitions(-DSNAPPY)
      list(APPEND THIRDPARTY_LIBS Snappy::snappy)
    endif()
  endif()
endif()

if(WIN32)
  execute_process(COMMAND powershell -Command "Get-Date -format MM_dd_yyyy" OUTPUT_VARIABLE DATE)
  execute_process(COMMAND powershell -Command "Get-Date -format HH:mm:ss" OUTPUT_VARIABLE TIME)
  string(REGEX REPLACE "(..)_(..)_..(..).*" "\\1/\\2/\\3" DATE "${DATE}")
  string(REGEX REPLACE "(..):(.....).*" " \\1:\\2" TIME "${TIME}")
  set(GIT_DATE_TIME "${DATE} ${TIME}")
else()
  execute_process(COMMAND date "+%Y/%m/%d %H:%M:%S" OUTPUT_VARIABLE DATETIME)
  string(REGEX REPLACE "\n" "" DATETIME ${DATETIME})
  set(GIT_DATE_TIME "${DATETIME}")
endif()

find_package(Git)

if (GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
  if(WIN32)
    execute_process(COMMAND $ENV{COMSPEC} /C ${GIT_EXECUTABLE} -C ${CMAKE_CURRENT_SOURCE_DIR} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  else()
    execute_process(COMMAND ${GIT_EXECUTABLE} -C ${CMAKE_CURRENT_SOURCE_DIR} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  endif()
else()
  set(GIT_SHA 0)
endif()

string(REGEX REPLACE "[^0-9a-f]+" "" GIT_SHA "${GIT_SHA}")

if (NOT WIN32)
  execute_process(COMMAND
      "./build_tools/version.sh" "full"
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      OUTPUT_VARIABLE VIDARDB_VERSION
  )
  string(STRIP "${VIDARDB_VERSION}" VIDARDB_VERSION)
  execute_process(COMMAND
      "./build_tools/version.sh" "major"
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      OUTPUT_VARIABLE VIDARDB_VERSION_MAJOR
  )
  string(STRIP "${VIDARDB_VERSION_MAJOR}" VIDARDB_VERSION_MAJOR)
endif()

set(BUILD_VERSION_CC ${CMAKE_BINARY_DIR}/build_version.cc)
configure_file(util/build_version.cc.in ${BUILD_VERSION_CC} @ONLY)
add_library(build_version OBJECT ${BUILD_VERSION_CC})
target_include_directories(build_version PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/util)
if(WIN32)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo  /EHsc /GS /Gd /GR /GF /fp:precise /Zc:wchar_t /Zc:forScope /errorReport:queue")

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /d2Zi+ /W3 /wd4127 /wd4800 /wd4996 /wd4351")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wextra -Wall")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare -Wshadow -Wno-unused-parameter -Wno-unused-variable -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
  if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -march=native")
    include(CheckCXXCompilerFlag)
    CHECK_CXX_COMPILER_FLAG("-momit-leaf-frame-pointer" HAVE_OMIT_LEAF_FRAME_POINTER)
    if(HAVE_OMIT_LEAF_FRAME_POINTER)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -momit-leaf-frame-pointer")
    endif()
  endif()
endif()

option(FAIL_ON_WARNINGS "Treat compile warnings as errors" ON)
if(FAIL_ON_WARNINGS)
  if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /WX")
  else() # assume GCC
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
  endif()
endif()

if(WIN32)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /RTC1 /Gm- /MDd")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /Oxt /Zp8 /Gm- /Gy /MD")

  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /DEBUG")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")
endif()

if(CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp")
endif()

# no debug info, that will make our library big
add_definitions(-DNDEBUG)

if(CMAKE_SYSTEM_NAME MATCHES "Cygwin")
  add_definitions(-fno-builtin-memcmp -DCYGWIN)
elseif(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  add_definitions(-DOS_MACOSX)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES arm)
    add_definitions(-DIOS_CROSS_COMPILE)
  endif()
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
  add_definitions(-DOS_LINUX)
elseif(CMAKE_SYSTEM_NAME MATCHES "SunOS")
  add_definitions(-DOS_SOLARIS)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  add_definitions(-DOS_FREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "NetBSD")
  add_definitions(-DOS_NETBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "OpenBSD")
  add_definitions(-DOS_OPENBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "DragonFly")
  add_definitions(-DOS_DRAGONFLYBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "Android")
  add_definitions(-DOS_ANDROID)
elseif(CMAKE_SYSTEM_NAME MATCHES "Windows")
  add_definitions(-DWIN32 -DOS_WIN -D_MBCS -DWIN64 -DNOMINMAX)
endif()

if(NOT WIN32)
  add_definitions(-DVIDARDB_PLATFORM_POSIX -DVIDARDB_LIB_IO_POSIX)
endif()

option(WITH_FALLOCATE "build with fallocate" ON)

if(WITH_FALLOCATE)
  include(CheckCSourceCompiles)
  CHECK_C_SOURCE_COMPILES("
#include <fcntl.h>
#include <linux/falloc.h>
int main() {
 int fd = open(\"/dev/null\", 0);
 fallocate(fd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, 0, 1024);
}
" HAVE_FALLOCATE)
  if(HAVE_FALLOCATE)
    add_definitions(-DVIDARDB_FALLOCATE_PRESENT)
  endif()
endif()

include(CheckFunctionExists)
CHECK_FUNCTION_EXISTS(malloc_usable_size HAVE_MALLOC_USABLE_SIZE)
if(HAVE_MALLOC_USABLE_SIZE)
  add_definitions(-DVIDARDB_MALLOC_USABLE_SIZE)
endif()

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)
find_package(Threads REQUIRED)

# Main library source code

set(SOURCES
        db/auto_roll_logger.cc
        db/builder.cc
        db/column_family.cc
        db/compaction.cc
        db/compaction_iterator.cc
        db/compaction_job.cc
        db/compaction_picker.cc
        db/convenience.cc
        db/db_filesnapshot.cc
        db/dbformat.cc
        db/db_impl.cc
        db/db_impl_debug.cc
        db/db_impl_readonly.cc
        db/db_info_dumper.cc
        db/db_iter.cc
        db/event_helpers.cc
        db/file_indexer.cc
        db/filename.cc
        db/flush_job.cc
        db/flush_scheduler.cc
        db/forward_iterator.cc
        db/internal_stats.cc
        db/log_reader.cc
        db/log_writer.cc
        memtable/memtable_allocator.cc
        memtable/memtable.cc
        memtable/memtable_list.cc
        db/repair.cc
        db/snapshot_impl.cc
        db/table_cache.cc
        db/table_properties_collector.cc
        db/transaction_log_impl.cc
        db/version_builder.cc
        db/version_edit.cc
        db/version_set.cc
        db/wal_manager.cc
        db/write_batch.cc
        db/write_controller.cc
        db/write_thread.cc
        memtable/skiplistrep.cc
        port/stack_trace.cc
        table/adaptive_table_factory.cc
        table/block_based_table_builder.cc
        table/block_based_table_factory.cc
        table/block_based_table_reader.cc
        table/column_table_builder.cc
        table/column_table_factory.cc
        table/column_table_reader.cc
        table/block_builder.cc
        table/block.cc
        table/column_block_builder.cc
        table/flush_block_policy.cc
        table/format.cc
        table/get_context.cc
        table/iterator.cc
        table/merger.cc
        table/meta_blocks.cc
        table/sst_file_writer.cc
        table/table_properties.cc
        table/two_level_iterator.cc
        util/arena.cc
        util/build_version.cc
        util/cache.cc
        util/coding.cc
        util/comparator.cc
        util/splitter.cc
        util/compaction_job_stats_impl.cc
        util/concurrent_arena.cc
        util/crc32c.cc
        util/delete_scheduler.cc
        util/env.cc
        util/threadpool.cc
        util/sst_file_manager_impl.cc
        util/file_util.cc
        util/file_reader_writer.cc
        util/hash.cc
        util/histogram.cc
        util/instrumented_mutex.cc
        util/iostats_context.cc
        util/event_logger.cc

        util/log_buffer.cc
        util/logging.cc
        util/murmurhash.cc
        util/mutable_cf_options.cc
        util/options.cc
        util/options_helper.cc
        util/options_parser.cc
        util/options_sanity_check.cc
        util/perf_context.cc
        util/perf_level.cc
        util/random.cc
        util/slice.cc
        util/statistics.cc
        util/status.cc
        util/status_message.cc
        util/string_util.cc
        util/sync_point.cc
        util/thread_local.cc
        util/thread_status_impl.cc
        util/thread_status_updater.cc
        util/thread_status_updater_debug.cc
        util/thread_status_util.cc
        util/thread_status_util_debug.cc
        utilities/write_batch_with_index/write_batch_with_index.cc
        utilities/write_batch_with_index/write_batch_with_index_internal.cc
        utilities/transactions/transaction_db_mutex_impl.cc
        utilities/transactions/transaction_util.cc
        utilities/transactions/transaction_base.cc
        utilities/transactions/transaction_impl.cc
        utilities/transactions/transaction_lock_mgr.cc
        utilities/transactions/transaction_db_impl.cc
        $<TARGET_OBJECTS:build_version>)

if(WIN32)
  list(APPEND SOURCES
    port/win/io_win.cc
    port/win/env_win.cc
    port/win/env_default.cc
    port/win/port_win.cc
    port/win/win_logger.cc
    port/win/xpress_win.cc)
else()
  list(APPEND SOURCES
    port/port_posix.cc
    util/env_posix.cc
    util/io_posix.cc)
endif()

if(WIN32)
  set(SYSTEM_LIBS ${SYSTEM_LIBS} Shlwapi.lib Rpcrt4.lib)
  set(VIDARDB_STATIC_LIB vidardblib${ARTIFACT_SUFFIX})
  set(VIDARDB_IMPORT_LIB vidardb${ARTIFACT_SUFFIX})
  set(LIBS ${VIDARDB_STATIC_LIB} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
else()
  set(SYSTEM_LIBS ${CMAKE_THREAD_LIBS_INIT})
  set(VIDARDB_STATIC_LIB vidardb${ARTIFACT_SUFFIX})
  set(VIDARDB_SHARED_LIB vidardb-shared)
  set(VIDARDB_IMPORT_LIB ${VIDARDB_SHARED_LIB})
  set(LIBS ${VIDARDB_SHARED_LIB} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

  add_library(${VIDARDB_SHARED_LIB} SHARED ${SOURCES})
  target_link_libraries(${VIDARDB_SHARED_LIB}
    ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
  set_target_properties(${VIDARDB_SHARED_LIB} PROPERTIES
                        LINKER_LANGUAGE CXX
                        VERSION ${VIDARDB_VERSION}
                        SOVERSION ${VIDARDB_VERSION_MAJOR}
                        CXX_STANDARD 11
                        OUTPUT_NAME "vidardb")
endif()

add_library(${VIDARDB_STATIC_LIB} STATIC ${SOURCES})
target_link_libraries(${VIDARDB_STATIC_LIB}
  ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

if(WIN32)
  set_target_properties(${VIDARDB_STATIC_LIB} PROPERTIES
    COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${VIDARDB_STATIC_LIB}.pdb")
endif()

if(WIN32)
  add_library(${VIDARDB_IMPORT_LIB} SHARED ${SOURCES})
  target_link_libraries(${VIDARDB_IMPORT_LIB}
    ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
  set_target_properties(${VIDARDB_IMPORT_LIB} PROPERTIES
    COMPILE_FLAGS "-DVIDARDB_DLL -DVIDARDB_LIBRARY_EXPORTS /Fd${CMAKE_CFG_INTDIR}/${VIDARDB_IMPORT_LIB}.pdb")
endif()

# Installation and packaging for Linux
if (NOT WIN32)
install(TARGETS ${VIDARDB_STATIC_LIB} COMPONENT devel ARCHIVE DESTINATION lib)
install(TARGETS ${VIDARDB_SHARED_LIB} COMPONENT runtime DESTINATION lib)
install(DIRECTORY "${PROJECT_SOURCE_DIR}/include/vidardb/"
        COMPONENT devel
        DESTINATION include/vidardb)
set(CMAKE_INSTALL_PREFIX /usr/local)
endif()
