message(STATUS "linking connectorx")

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/connectorx")
set(CX_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/../connector-x")

add_library(_connectorx SHARED IMPORTED GLOBAL)
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
    set_property(TARGET _connectorx PROPERTY IMPORTED_LOCATION "${CX_PROJECT_DIR}/target/debug/libconnectorx_cpp.so")
else()
    set_property(TARGET _connectorx PROPERTY IMPORTED_LOCATION "${CX_PROJECT_DIR}/target/release/libconnectorx_cpp.so")
endif()

set_property(TARGET _connectorx PROPERTY INTERFACE_INCLUDE_DIRECTORIES "${LIBRARY_DIR}/include")

add_library(ch_contrib::connectorx ALIAS _connectorx)
