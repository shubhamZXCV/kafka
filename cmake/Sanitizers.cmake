# Sanitizers, applied to everything in the build (including third-party code,
# which ThreadSanitizer needs to avoid false reports).
#
#   ELOG_SANITIZE=""                    no sanitizers (release)
#   ELOG_SANITIZE="address;undefined"   dev preset
#   ELOG_SANITIZE="thread"              tsan preset

set(ELOG_SANITIZE "" CACHE STRING "Sanitizers to enable: address, undefined, thread")

if(ELOG_SANITIZE)
    if("thread" IN_LIST ELOG_SANITIZE AND "address" IN_LIST ELOG_SANITIZE)
        message(FATAL_ERROR "ThreadSanitizer cannot be combined with AddressSanitizer")
    endif()

    list(JOIN ELOG_SANITIZE "," sanitizers)
    message(STATUS "Sanitizers enabled: ${sanitizers}")

    # -O1 keeps sanitized builds fast enough for the functional tests.
    add_compile_options(-fsanitize=${sanitizers} -fno-omit-frame-pointer -O1 -g)
    add_link_options(-fsanitize=${sanitizers})
endif()
