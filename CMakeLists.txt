cmake_minimum_required(VERSION 3.25)

project(elog
    VERSION 0.1.0
    DESCRIPTION "Distributed event log with dynamic consumer rebalancing"
    LANGUAGES C CXX)   # C only for the SQLite amalgamation

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# All executables land in one directory so the functional tests can find them.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(Warnings)
include(Sanitizers)
include(Dependencies)   # after Sanitizers, so third-party code is instrumented too

add_subdirectory(src)
