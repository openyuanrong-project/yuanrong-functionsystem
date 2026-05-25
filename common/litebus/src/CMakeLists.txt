# include DIRECTIVES FOR LITEBUS LIBRARY (generates, e.g., -I/path/to/thing on Linux).
######################################################################################
if(${HTTP_ENABLED} STREQUAL "on")
  set(LITEBUS_INCLUDE_DIR ${LITEBUS_INCLUDE_DIR} ${HTTP_PARSER_INCLUDE_DIR})
endif(${HTTP_ENABLED} STREQUAL "on")

if(${HTTP_ENABLED} STREQUAL "on")
  link_directories(${HTTP_PARSER_LIB_DIR})
endif(${HTTP_ENABLED} STREQUAL "on")

# add object lib to avoid duplicate compile for a single source file
#######################################################################
add_library(litebus_obj OBJECT litebus.cpp)
add_dependencies(litebus_obj securec openssl)
target_include_directories(litebus_obj PUBLIC ${LITEBUS_INCLUDE_DIR})

# THE LITEBUS LIBRARY (generates, e.g., liblitebus.so, etc., on Linux).
#######################################################################
if(${STATIC_LIB} STREQUAL "on")

#######################################################################
add_library(${LITEBUS_TARGET}_static STATIC $<TARGET_OBJECTS:litebus_obj> )
add_dependencies(${LITEBUS_TARGET}_static securec openssl)
target_link_libraries(${LITEBUS_TARGET}_static ${LINK_LIBS} ${crypto_LIB_A} ${ssl_LIB_A})
set_target_properties(${LITEBUS_TARGET}_static PROPERTIES OUTPUT_NAME ${LITEBUS_TARGET})

if(DEFINED DEPEND_PATH)
  add_custom_command(TARGET ${LITEBUS_TARGET}_static POST_BUILD
                     COMMAND ${CMAKE_COMMAND} -E create_symlink lib${LITEBUS_TARGET}.a liblitebus.a
  )
endif()

#######################################################################
endif(${STATIC_LIB} STREQUAL "on")

if(${SSL_ENABLED} STREQUAL "on")
  set(LINK_LIBS ${LINK_LIBS} ${crypto_LIB} ${ssl_LIB} ${securec_LIB} ${yrlogs_LIB})
else()
  set(LINK_LIBS ${LINK_LIBS} ${securec_LIB} ${yrlogs_LIB})
endif(${SSL_ENABLED} STREQUAL "on")

if(${HTTP_ENABLED} STREQUAL "on")
  set(LINK_LIBS ${LINK_LIBS} ${HTTP_PARSER_DFLAG})
endif(${HTTP_ENABLED} STREQUAL "on")

add_library(litebus_shared SHARED $<TARGET_OBJECTS:litebus_obj>)
target_link_libraries(litebus_shared ${LINK_LIBS} util)
target_include_directories(litebus_shared PUBLIC ${LITEBUS_INCLUDE_DIR})

set_target_properties(
    litebus_shared PROPERTIES
    OUTPUT_NAME litebus
    VERSION ${LITEBUS_PACKAGE_VERSION}
    SOVERSION ${LITEBUS_PACKAGE_VERSION}
    LINK_FLAGS -s
)

install(TARGETS litebus_shared DESTINATION lib)

#copy lib to depend path (internal use)
#set(DEPEND_PATH "${PROJECT_SOURCE_DIR}/output1")
if(DEFINED DEPEND_PATH)
  set(DEPEND_LIB_PATH ${DEPEND_PATH}/LITEBUS/lib)
  set(DEPEND_INCLUDE_PATH ${DEPEND_PATH}/LITEBUS/include)

  add_custom_target(litebus_all ALL COMMENT "================= litebus_all =====================")
  if(${STATIC_LIB} STREQUAL "on")
    add_dependencies(litebus_all litebus_shared ${LITEBUS_TARGET}_static)
  endif(${STATIC_LIB} STREQUAL "on")
  add_dependencies(litebus_all litebus_shared)
  add_custom_command(TARGET litebus_all POST_BUILD
                     COMMAND ${CMAKE_COMMAND} -P ${PROJECT_SOURCE_DIR}/cmake/MakeDirectory.cmake ${DEPEND_LIB_PATH} ${DEPEND_INCLUDE_PATH}
                     COMMAND ${CMAKE_COMMAND} -DLITEBUS_COPYTO="${DEPEND_LIB_PATH}" -P ${PROJECT_SOURCE_DIR}/cmake/CopyLibToPath.cmake
                     COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/include/litebus.hpp ${DEPEND_INCLUDE_PATH}
                     COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/include/litebus.h ${DEPEND_INCLUDE_PATH}
                     COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/include/actor ${DEPEND_INCLUDE_PATH}/actor
                     COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/include/async ${DEPEND_INCLUDE_PATH}/async
                     COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/include/timer ${DEPEND_INCLUDE_PATH}/timer
                     COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/include/exec ${DEPEND_INCLUDE_PATH}/exec
                     COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/include/utils ${DEPEND_INCLUDE_PATH}/utils)
  if(${HTTP_ENABLED} STREQUAL "on")
    add_custom_command(TARGET litebus_all POST_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/include/httpd ${DEPEND_INCLUDE_PATH}/httpd)
  endif(${HTTP_ENABLED} STREQUAL "on")
  if(${SSL_ENABLED} STREQUAL "on")
    add_custom_command(TARGET litebus_all POST_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/include/ssl ${DEPEND_INCLUDE_PATH}/ssl)
  endif(${SSL_ENABLED} STREQUAL "on")

endif()

add_subdirectory(actor)
add_subdirectory(async)
add_subdirectory(evloop)
add_subdirectory(exec)
add_subdirectory(httpd)
add_subdirectory(iomgr)
add_subdirectory(ssl)
add_subdirectory(tcp)
add_subdirectory(timer)
add_subdirectory(udp)
add_subdirectory(utils)
