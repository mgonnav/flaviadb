cmake_minimum_required(VERSION 3.13 FATAL_ERROR)

project(flaviadb VERSION 0.1)

set(CMAKE_VERBOSE_MAKEFILE off)
set(CMAKE_CXX_STANDARD 17)

include_directories(/usr/local/include/hsql)
include_directories(/usr/include/readline)
include_directories(include)

add_executable(flaviadb main.cc src/Table.cc src/filestruct.cc src/printutils.cc
               src/Where.cc)
add_executable(query_run query_run.cc src/Table.cc src/filestruct.cc src/printutils.cc
               src/Where.cc)

target_link_libraries(flaviadb readline sqlparser)
target_link_libraries(query_run sqlparser)

target_compile_options(flaviadb PRIVATE -Wall -Wextra)
