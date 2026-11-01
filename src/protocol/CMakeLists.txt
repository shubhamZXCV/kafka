add_library(elog_protocol STATIC
    api_keys.cc
    app_value.cc
    error_codes.cc
    error_response.cc
    frame.cc
    frame_server.cc
    json_codec.cc
    msgpack_codec.cc
    record_batch.cc)

target_include_directories(elog_protocol PUBLIC include)
target_link_libraries(elog_protocol
    PUBLIC elog_common
    PRIVATE msgpack-cxx nlohmann_json::nlohmann_json)
elog_set_warnings(elog_protocol)
