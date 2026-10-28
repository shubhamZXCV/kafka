add_library(elog_common STATIC
    config.cc
    logging.cc
    socket.cc
    version.cc)

target_include_directories(elog_common PUBLIC include)
target_link_libraries(elog_common PUBLIC spdlog::spdlog)
elog_set_warnings(elog_common)
target_compile_definitions(elog_common PRIVATE ELOG_VERSION="${PROJECT_VERSION}")
