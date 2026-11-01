add_library(elog_storage STATIC
    checkpoint_file.cc
    log_verifier.cc
    partition_log.cc
    segment.cc
    sparse_index.cc)

target_include_directories(elog_storage PUBLIC include)
target_link_libraries(elog_storage PUBLIC elog_common elog_protocol)
elog_set_warnings(elog_storage)
