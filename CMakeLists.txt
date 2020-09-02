cmake_minimum_required(VERSION 3.13 FATAL_ERROR)

project(flaviadb VERSION 0.1)

set(CMAKE_VERBOSE_MAKEFILE off)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED on)

include_directories(/usr/local/include/hsql)
include_directories(/usr/include/readline)
include_directories(include)

add_executable(flaviadb src/main.cc src/Table.cc src/filestruct.cc src/printutils.cc
               src/Where.cc src/Processor.cc)
add_executable(query_run src/query_run.cc src/Table.cc src/filestruct.cc src/printutils.cc
               src/Where.cc src/Processor.cc)

target_link_libraries(flaviadb readline sqlparser)
target_link_libraries(query_run sqlparser)

target_compile_options(flaviadb PRIVATE -Wall -Wextra)
