add_library(elog_common STATIC
    config.cc
    crc32c.cc
    events.cc
    logging.cc
    metrics.cc
    murmur3.cc
    socket.cc
    version.cc)

target_include_directories(elog_common PUBLIC include)
target_link_libraries(elog_common
    PUBLIC spdlog::spdlog
    PRIVATE crc32c)
elog_set_warnings(elog_common)
target_compile_definitions(elog_common PRIVATE ELOG_VERSION="${PROJECT_VERSION}")

# Vendored MurmurHash3 is kept unchanged, so its warnings (e.g. implicit fallthrough) are
# not ours to fix.
set_source_files_properties(murmur3.cc PROPERTIES COMPILE_OPTIONS "-w")
