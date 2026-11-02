add_library(elog_client STATIC
    assignment_state.cc
    consumer.cc
    fetcher.cc
    membership_client.cc
    metadata_cache.cc
    producer.cc
    rpc_connection.cc)

target_include_directories(elog_client PUBLIC include)
target_link_libraries(elog_client PUBLIC elog_protocol)
elog_set_warnings(elog_client)
