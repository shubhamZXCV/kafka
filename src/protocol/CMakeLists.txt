add_library(elog_protocol STATIC
    api_keys.cc
    error_codes.cc
    frame.cc
    record_batch.cc)

target_include_directories(elog_protocol PUBLIC include)
target_link_libraries(elog_protocol PUBLIC elog_common)
elog_set_warnings(elog_protocol)
