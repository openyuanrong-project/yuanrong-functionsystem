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

#include "metrics/exporters/prometheus_push_exporter/prometheus_push_exporter.h"

#include "metrics/exporters/common/prometheus_text_serializer.h"

#include <functional>
#include <future>
#include <iostream>
#include <nlohmann/json.hpp>

#include "common/logs/log.h"
#include "metrics/sdk/metric_processor.h"
#include "metrics/exporters/http_exporter/curl_helper.h"
#include "exporters/http_exporter/http_heartbeat_client.h"

namespace observability::exporters::metrics {
static const char CONTENT_TYPE[] = "Content-Type: text/plain; version=0.0.4; charset=utf-8";
namespace MetricsSdk = observability::sdk::metrics;
namespace MetricsExporter = observability::exporters::metrics;

PrometheusPushExporter::PrometheusPushExporter(const std::string &config)
{
    PrometheusPushExportOptions options;
    try {
        auto configJson = nlohmann::json::parse(config);
        if (configJson.find("endpoint") != configJson.end()) {
            options.endpoint = configJson.at("endpoint");
        }
        if (configJson.find("jobName") != configJson.end()) {
            options.jobName = configJson.at("jobName");
        }
        if (configJson.find("heartbeatInterval") != configJson.end()) {
            options.heartbeatInterval = std::max(configJson.at("heartbeatInterval").get<uint32_t>(),
                MetricsSdk::DEFAULT_HEARTBEAT_INTERVAL);
        }
        if (configJson.find("heartbeatUrl") != configJson.end()) {
            options.heartbeatUrl = configJson.at("heartbeatUrl");
        }
    } catch (std::exception &e) {
        std::cerr << "failed to parse PrometheusPushExportOptions" << std::endl;
        return;
    }
    options.sslConfig.Parse(config);
    Init(options);
}

PrometheusPushExporter::PrometheusPushExporter(const PrometheusPushExportOptions &options)
{
    Init(options);
}

void PrometheusPushExporter::Init(const PrometheusPushExportOptions &options)
{
    curlHelper_ = std::make_shared<CurlHelper>();
    serializer_ = std::make_shared<PrometheusTextSerializer>();
    jobUrl_ = options.endpoint + "/metrics/job/" + options.jobName;
    method_ = HttpRequestMethod::POST;
    if (curlHelper_) {
        curlHelper_->SetHttpHeader(CONTENT_TYPE);
        curlHelper_->SetSSLConfig(options.sslConfig);
    }
    if (!options.heartbeatUrl.empty()) {
        heartbeatParam_.heartbeatUrl = options.endpoint + options.heartbeatUrl;
    }
    heartbeatParam_.heartbeatInterval = std::max(options.heartbeatInterval, MetricsSdk::DEFAULT_HEARTBEAT_INTERVAL);
    heartbeatParam_.sslConfig = options.sslConfig;
    heartbeatParam_.httpHeader = CONTENT_TYPE;
}

}  // namespace observability::exporters::metrics
