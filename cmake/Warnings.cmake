# Compiler warnings for project targets (not third-party code).
# Usage: elog_set_warnings(<target>)

option(ELOG_WARNINGS_AS_ERRORS "Treat compiler warnings as errors (CI)" OFF)

function(elog_set_warnings target)
    target_compile_options(${target} PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wshadow
        -Wnon-virtual-dtor
        -Wold-style-cast
        -Woverloaded-virtual)
    if(ELOG_WARNINGS_AS_ERRORS)
        target_compile_options(${target} PRIVATE -Werror)
    endif()
endfunction()
