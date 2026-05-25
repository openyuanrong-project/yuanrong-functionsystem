# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

aux_source_directory(${CMAKE_CURRENT_LIST_DIR} PROMETHEUS_PULL_EXPORTER)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/../common/ COMMON_SOURCES)

set(TARGET_NAME observability-prometheus-pull-exporter)

add_library(${TARGET_NAME} SHARED ${COMMON_SOURCES} ${PROMETHEUS_PULL_EXPORTER})

target_include_directories(${TARGET_NAME} PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>")

target_link_libraries(${TARGET_NAME}
    PUBLIC
        observability-metrics-headers
        ${securec_LIB}
        ${litebus_ALL_LIB}
        ${ssl_LIB}
        ${crypto_LIB}
        pthread)

install(
    TARGETS ${TARGET_NAME}
    LIBRARY
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
