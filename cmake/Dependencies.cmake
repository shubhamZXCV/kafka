# Third-party dependencies, pinned to exact versions and verified by hash (LLD §2.3).
#
# Targets provided:
#   nlohmann_json::nlohmann_json   control-plane JSON codec
#   msgpack-cxx                    record and data-plane MessagePack codec
#   spdlog::spdlog                 logging
#   crc32c                         CRC32C (hardware-accelerated)
#   elog_sqlite3                   SQLite amalgamation for the coordinator store

include(FetchContent)

# SYSTEM (on each declaration below): their headers are included as system headers, so our
# strict warnings apply to our code only.
#
# Let our plain set() calls override option() defaults inside dependencies,
# even those that declare an old cmake_minimum_required.
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

FetchContent_Declare(nlohmann_json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
    URL_HASH SHA256=d6c65aca6b1ed68e7a182f4757257b107ae403032760ed6ef121c9d55e81757d
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM)

set(MSGPACK_USE_BOOST OFF)   # header-only, no Boost: defines MSGPACK_NO_BOOST
set(MSGPACK_BUILD_DOCS OFF)
FetchContent_Declare(msgpack
    URL https://github.com/msgpack/msgpack-c/releases/download/cpp-6.1.1/msgpack-cxx-6.1.1.tar.gz
    URL_HASH SHA256=5fd555742e37bbd58d166199e669f01f743c7b3c6177191dd7b31fb0c37fa191
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM)

FetchContent_Declare(spdlog
    URL https://github.com/gabime/spdlog/archive/refs/tags/v1.14.1.tar.gz
    URL_HASH SHA256=1586508029a7d0670dfcb2d97575dcdc242d3868a259742b69f100801ab4e16b
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM)

set(CRC32C_BUILD_TESTS OFF)
set(CRC32C_BUILD_BENCHMARKS OFF)
set(CRC32C_USE_GLOG OFF)
set(CRC32C_INSTALL OFF)
FetchContent_Declare(crc32c
    URL https://github.com/google/crc32c/archive/refs/tags/1.1.2.tar.gz
    URL_HASH SHA256=ac07840513072b7fcebda6e821068aa04889018f24e10e46181068fb214d7e56
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM)

FetchContent_Declare(sqlite3
    URL https://www.sqlite.org/2024/sqlite-amalgamation-3460100.zip
    URL_HASH SHA256=77823cb110929c2bcb0f5d48e4833b5c59a8a6e40cdea3936b99e199dbbe5784
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM)

FetchContent_MakeAvailable(nlohmann_json msgpack spdlog crc32c sqlite3)

# SQLite ships as a single C file with no CMake support, so we build it here.
# THREADSAFE=2: one connection per thread (the coordinator event loop owns it).
find_package(Threads REQUIRED)
add_library(elog_sqlite3 STATIC ${sqlite3_SOURCE_DIR}/sqlite3.c)
target_include_directories(elog_sqlite3 SYSTEM PUBLIC ${sqlite3_SOURCE_DIR})
target_compile_definitions(elog_sqlite3 PUBLIC
    SQLITE_THREADSAFE=2
    SQLITE_OMIT_LOAD_EXTENSION)
target_link_libraries(elog_sqlite3 PUBLIC Threads::Threads)
