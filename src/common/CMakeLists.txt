add_library(elog_common STATIC
    version.cc)

target_include_directories(elog_common PUBLIC include)
elog_set_warnings(elog_common)
target_compile_definitions(elog_common PRIVATE ELOG_VERSION="${PROJECT_VERSION}")
