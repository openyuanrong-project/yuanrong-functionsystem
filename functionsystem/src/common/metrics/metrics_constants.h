/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FUNCTIONSYSTEM_METRICS_CONSTANT_H
#define FUNCTIONSYSTEM_METRICS_CONSTANT_H

#include <string>
#include <unordered_map>

namespace functionsystem {
namespace metrics {

const std::string UNKNOWN_INSTRUMENT_NAME = "unknown_instrument_name";
const std::string YR_INSTANCE_RUNNING_DURATION = "yr_instance_running_duration";
const std::string YR_APP_INSTANCE_BILLING_INVOKE_LATENCY = "yr_app_instance_billing_invoke_latency";
const std::string YR_METRICS_KEY = "YR_Metrics";
const std::string YR_POD_RESOURCE("yr_pod_resource");
const std::string YR_NODE_CPU_USAGE = "yr_node_cpu_usage";
const std::string YR_NODE_MEMORY_USAGE = "yr_node_memory_usage";
const std::string YR_NODE_MEMORY_CAPACITY = "yr_node_memory_capacity";
const std::string YR_NODE_DISK_USAGE = "yr_node_disk_usage";
const std::string YR_NODE_DISK_CAPACITY = "yr_node_disk_capacity";
const std::string YR_NODE_NPU = "yr_node_npu";
const std::string YR_INSTANCE_CPU_USAGE = "yr_instance_cpu_usage";
const std::string YR_INSTANCE_NPU = "yr_instance_npu";


// alarm
const std::string K8S_ALARM("yr_k8s_alarm");
const std::string SCHEDULER_ALARM("yr_proxy_alarm");
const std::string ETCD_ALARM("yr_etcd_alarm");
const std::string TOKEN_ROTATION_FAILURE_ALARM("yr_token_rotation_failure_alarm");
const std::string S3_ALARM("yr_obs_alarm");
const std::string POD_ALARM("yr_pod_alarm");
const std::string ELECTION_ALARM("yr_election_alarm");
const std::string METASTORE_ALARM("yr_metastore_alarm");
const std::string YR_APP_INSTANCE_STATUS("yr_app_instance_status");
const std::string YR_INSTANCE_EXIT_LATENCY("yr_instance_exit_latency");
const std::string YR_INSTANCE_MEMORY_USAGE("yr_instance_memory_usage");

const std::string FILE_EXPORTER("fileExporter");
const std::string PROMETHEUS_PULL_EXPORTER("prometheusPullExporter");
const std::string PROMETHEUS_PUSH_EXPORTER("prometheusPushExporter");
const std::string OPENTELEMETRY_EXPORTER("opentelemetryExporter");

const uint32_t SIZE_MEGA_BYTES = 1024 * 1024;  // 1 MB
const uint32_t INSTANCE_RUNNING_DURATION_COLLECT_INTERVAL = 15;  // unit:second
const uint32_t INSTANCE_METRICS_COLLECT_INTERVAL = 15;  // unit:second
const uint32_t POD_RESOURCE_COLLECT_INTERVAL = 15;  // unit:second

const int TOKEN_ROTATION_FAILURE_TIMES_THRESHOLD = 3;

const uint32_t MONOPOLY_INSTANCE_COUNT = 1;

enum class YRInstrument {
    UNKNOWN_INSTRUMENT = 0,
    YR_INSTANCE_RUNNING_DURATION = 1,
    YR_APP_INSTANCE_BILLING_INVOKE_LATENCY = 2,
    YR_K8S_ALARM = 3,
    YR_SCHEDULER_ALARM = 4,
    YR_ETCD_ALARM = 5,
    YR_TOKEN_ROTATION_FAILURE_ALARM = 6,
    YR_S3_ALARM = 7,
    YR_POD_ALARM = 8,
    YR_POD_RESOURCE = 9,
    YR_ELECTION_ALARM = 10,
    YR_METASTORE_ALARM = 11,
    YR_APP_INSTANCE_STATUS = 12,
    YR_INSTANCE_EXIT_LATENCY = 13,
    YR_NODE_CPU_USAGE = 14,
    YR_NODE_MEMORY_USAGE = 15,
    YR_NODE_MEMORY_CAPACITY = 16,
    YR_NODE_DISK_USAGE = 17,
    YR_NODE_DISK_CAPACITY = 18,
    YR_NODE_NPU = 19,
    YR_INSTANCE_MEMORY_USAGE = 20,
    YR_INSTANCE_CPU_USAGE = 21,
    YR_INSTANCE_NPU = 22,
};

enum class AlarmLevel { OFF, NOTICE, INFO, MINOR, MAJOR, CRITICAL };

const std::unordered_map<std::string, YRInstrument> INSTRUMENT_DESC_2_ENUM = {
    { YR_INSTANCE_RUNNING_DURATION, YRInstrument::YR_INSTANCE_RUNNING_DURATION },
    { YR_APP_INSTANCE_BILLING_INVOKE_LATENCY, YRInstrument::YR_APP_INSTANCE_BILLING_INVOKE_LATENCY },
    { K8S_ALARM, YRInstrument::YR_K8S_ALARM },
    { SCHEDULER_ALARM, YRInstrument::YR_SCHEDULER_ALARM },
    { ETCD_ALARM, YRInstrument::YR_ETCD_ALARM },
    { TOKEN_ROTATION_FAILURE_ALARM, YRInstrument::YR_TOKEN_ROTATION_FAILURE_ALARM },
    { S3_ALARM, YRInstrument::YR_S3_ALARM },
    { POD_ALARM, YRInstrument::YR_POD_ALARM },
    { YR_POD_RESOURCE, YRInstrument::YR_POD_RESOURCE },
    { ELECTION_ALARM, YRInstrument::YR_ELECTION_ALARM },
    { METASTORE_ALARM, YRInstrument::YR_METASTORE_ALARM },
    { YR_APP_INSTANCE_STATUS, YRInstrument::YR_APP_INSTANCE_STATUS },
    { YR_INSTANCE_EXIT_LATENCY, YRInstrument::YR_INSTANCE_EXIT_LATENCY },
    { YR_NODE_CPU_USAGE, YRInstrument::YR_NODE_CPU_USAGE },
    { YR_NODE_MEMORY_USAGE, YRInstrument::YR_NODE_MEMORY_USAGE },
    { YR_NODE_MEMORY_CAPACITY, YRInstrument::YR_NODE_MEMORY_CAPACITY },
    { YR_NODE_DISK_USAGE, YRInstrument::YR_NODE_DISK_USAGE },
    { YR_NODE_DISK_CAPACITY, YRInstrument::YR_NODE_DISK_CAPACITY },
    { YR_NODE_NPU, YRInstrument::YR_NODE_NPU },
    { YR_INSTANCE_MEMORY_USAGE, YRInstrument::YR_INSTANCE_MEMORY_USAGE },
    { YR_INSTANCE_CPU_USAGE, YRInstrument::YR_INSTANCE_CPU_USAGE },
    { YR_INSTANCE_NPU, YRInstrument::YR_INSTANCE_NPU },
};

const std::unordered_map<YRInstrument, std::string> ENUM_2_INSTRUMENT_DESC = {
    { YRInstrument::YR_INSTANCE_RUNNING_DURATION, YR_INSTANCE_RUNNING_DURATION },
    { YRInstrument::YR_APP_INSTANCE_BILLING_INVOKE_LATENCY, YR_APP_INSTANCE_BILLING_INVOKE_LATENCY },
    { YRInstrument::YR_K8S_ALARM, K8S_ALARM },
    { YRInstrument::YR_SCHEDULER_ALARM, SCHEDULER_ALARM },
    { YRInstrument::YR_ETCD_ALARM, ETCD_ALARM },
    { YRInstrument::YR_TOKEN_ROTATION_FAILURE_ALARM, TOKEN_ROTATION_FAILURE_ALARM },
    { YRInstrument::YR_S3_ALARM, S3_ALARM },
    { YRInstrument::YR_POD_ALARM, POD_ALARM },
    { YRInstrument::YR_POD_RESOURCE, YR_POD_RESOURCE },
    { YRInstrument::YR_ELECTION_ALARM, ELECTION_ALARM },
    { YRInstrument::YR_METASTORE_ALARM, METASTORE_ALARM },
    { YRInstrument::YR_APP_INSTANCE_STATUS, YR_APP_INSTANCE_STATUS },
    { YRInstrument::YR_INSTANCE_EXIT_LATENCY, YR_INSTANCE_EXIT_LATENCY },
    { YRInstrument::YR_NODE_CPU_USAGE, YR_NODE_CPU_USAGE},
    { YRInstrument::YR_NODE_MEMORY_USAGE, YR_NODE_MEMORY_USAGE},
    { YRInstrument::YR_NODE_MEMORY_CAPACITY, YR_NODE_MEMORY_CAPACITY},
    { YRInstrument::YR_NODE_DISK_USAGE, YR_NODE_DISK_USAGE},
    { YRInstrument::YR_NODE_DISK_CAPACITY, YR_NODE_DISK_CAPACITY},
    { YRInstrument::YR_NODE_NPU, YR_NODE_NPU},
    { YRInstrument::YR_INSTANCE_MEMORY_USAGE, YR_INSTANCE_MEMORY_USAGE },
    { YRInstrument::YR_INSTANCE_CPU_USAGE, YR_INSTANCE_CPU_USAGE },
    { YRInstrument::YR_INSTANCE_NPU, YR_INSTANCE_NPU },
};
}
}

#endif // FUNCTIONSYSTEM_METRICS_CONSTANT_H
