# Modules and their allowed dependencies (LLD §2.2). No cycles.
#
#   common       (no project dependencies)
#   protocol     -> common
#   storage      -> common, protocol
#   broker       -> storage, protocol
#   coordinator  -> protocol
#   client       -> protocol
#   tools        -> client, storage
#
# A module is added here when its first source file lands.

add_subdirectory(common)
add_subdirectory(protocol)
add_subdirectory(storage)
