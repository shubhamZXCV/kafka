add_library(elog_tools_common STATIC cli.cc)
target_include_directories(elog_tools_common PUBLIC include)
target_link_libraries(elog_tools_common PUBLIC elog_common)
elog_set_warnings(elog_tools_common)

add_executable(elog-producer elog_producer.cc)
target_link_libraries(elog-producer PRIVATE elog_tools_common elog_client)
elog_set_warnings(elog-producer)

add_executable(elog-consumer elog_consumer.cc)
target_link_libraries(elog-consumer PRIVATE elog_tools_common elog_client)
elog_set_warnings(elog-consumer)

add_executable(elogctl elogctl.cc)
target_link_libraries(elogctl PRIVATE elog_tools_common elog_client elog_storage
    nlohmann_json::nlohmann_json)
elog_set_warnings(elogctl)
