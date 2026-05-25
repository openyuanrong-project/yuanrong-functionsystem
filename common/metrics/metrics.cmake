# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(src_dir ${ROOT_DIR}/common/metrics)
get_filename_component(ABSOLUTE_src_dir ${src_dir} ABSOLUTE)
set(src_dir ${ABSOLUTE_src_dir})

message(STATUS "metrics src dir: ${src_dir}")
if (NOT EXISTS ${src_dir}/output)
    if (EXISTS ${src_dir})
        message(STATUS "begin build metrics")
        execute_process(COMMAND bash build.sh -x ${DOWNLOAD_OPENSRC} -T ${THIRDPARTY_SRC_DIR} WORKING_DIRECTORY ${src_dir})
    endif()
endif()

set(metrics_INCLUDE_DIR ${src_dir}/output/include)
set(metrics_LIB_DIR ${src_dir}/output/lib)
set(metrics_LIB ${metrics_LIB_DIR}/libobservability-metrics.so)
set(metrics_sdk_LIB ${metrics_LIB_DIR}/libobservability-metrics-sdk.so)
set(metrics_file_exporter_LIB ${metrics_LIB_DIR}/libobservability-metrics-file-exporter.so)
set(metrics_exporter_ostream_LIB ${metrics_LIB_DIR}/libobservability-metrics-exporter-ostream.so)
set(metrics_prometheus_push_exporter_LIB ${metrics_LIB_DIR}/libobservability-prometheus-push-exporter.so)
set(metrics_prometheus_pull_exporter_LIB ${metrics_LIB_DIR}/libobservability-prometheus-pull-exporter.so)
set(metrics_opentelemetry_exporter_LIB ${metrics_LIB_DIR}/libobservability-metrics-opentelemetry-exporter.so)

include_directories(${metrics_INCLUDE_DIR})

message(STATUS "metrics include dir: ${metrics_INCLUDE_DIR}")

install(FILES
    ${metrics_LIB}
    ${metrics_sdk_LIB}
    ${metrics_file_exporter_LIB}
    ${metrics_exporter_ostream_LIB}
    ${metrics_prometheus_push_exporter_LIB}
    ${metrics_prometheus_pull_exporter_LIB}
    ${metrics_opentelemetry_exporter_LIB}
    DESTINATION lib)
