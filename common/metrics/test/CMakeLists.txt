# Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.

message(STATUS "LLT is enabled")
include(GoogleTest)
enable_testing()

add_subdirectory(unit)
add_subdirectory(integration)

add_library(api_test SHARED api/meter_provider_test.cpp)

target_link_libraries(api_test PUBLIC observability-metrics-headers pthread)

add_executable(metrics_test
        main.cpp
        sdk/meter_provider_sdk_test.cpp
        exporters/prometheus_pull_exporter_test.cpp
        exporters/prometheus_push_exporter_test.cpp
        exporters/file_exporter_test.cpp
        exporters/ostream_exporter_test.cpp
        plugin/dynamic_load_test.cpp)
target_link_libraries(
        metrics_test
        PRIVATE ${gtest_LIB_A} ${gmock_LIB_A} api_test observability-metrics-sdk)

gtest_discover_tests(metrics_test)

add_library(example_exporter SHARED
        plugin/example_exporter.cpp
        plugin/factory_impl.cpp)
target_link_libraries(example_exporter observability-metrics-headers)

add_executable(plugin_example
        plugin/main.cpp
        )
target_link_libraries(plugin_example PUBLIC observability-metrics-headers ${CMAKE_DL_LIBS})
