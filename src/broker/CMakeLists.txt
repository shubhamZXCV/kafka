add_library(elog_broker_lib STATIC
    broker_config.cc
    broker_scheduler.cc
    broker_server.cc
    coordinator_agent.cc
    fetch_handler.cc
    produce_handler.cc
    replica.cc
    replica_fetch_handler.cc
    replica_fetcher.cc
    replica_manager.cc
    request_dispatcher.cc)

target_include_directories(elog_broker_lib PUBLIC include)
target_link_libraries(elog_broker_lib PUBLIC elog_storage elog_protocol)
elog_set_warnings(elog_broker_lib)

add_executable(elog-broker main.cc)
target_link_libraries(elog-broker PRIVATE elog_broker_lib)
elog_set_warnings(elog-broker)
