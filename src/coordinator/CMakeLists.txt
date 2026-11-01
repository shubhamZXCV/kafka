add_library(elog_coordinator_lib STATIC
    coordinator_config.cc
    coordinator_server.cc
    db.cc
    event_loop.cc
    metadata_manager.cc
    repository.cc
    schema.cc)

target_include_directories(elog_coordinator_lib PUBLIC include)
target_link_libraries(elog_coordinator_lib
    PUBLIC elog_protocol
    PRIVATE elog_sqlite3 nlohmann_json::nlohmann_json)
elog_set_warnings(elog_coordinator_lib)

add_executable(elog-coordinator main.cc)
target_link_libraries(elog-coordinator PRIVATE elog_coordinator_lib)
elog_set_warnings(elog-coordinator)
