add_library(elog_storage STATIC
    segment.cc)

target_include_directories(elog_storage PUBLIC include)
target_link_libraries(elog_storage PUBLIC elog_common elog_protocol)
elog_set_warnings(elog_storage)
