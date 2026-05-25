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

#ifndef OBSERVABILITY_EXPORTERS_METRICS_HTTP_EXPORTER_H
#define OBSERVABILITY_EXPORTERS_METRICS_HTTP_EXPORTER_H

#include <map>
#include <atomic>

#include "metrics/exporters/exporter.h"
#include "curl_helper.h"
#include "metrics/exporters/common/prometheus_text_serializer.h"

namespace observability::exporters::metrics {

struct HeartbeatParam {
    std::string heartbeatUrl;
    uint32_t heartbeatInterval;
    std::string httpHeader;
    SSLConfig sslConfig;
    HttpRequestMethod method = HttpRequestMethod::GET;
};

class HttpHeartBeatClient;

namespace MetricsSdk = observability::sdk::metrics;
namespace MetricsExporter = observability::exporters::metrics;

class HttpExporter : public observability::exporters::metrics::Exporter {
public:
    MetricsExporter::ExportResult Export(
        const std::vector<MetricsSdk::MetricData> &metricDataVec) noexcept override;
    MetricsSdk::AggregationTemporality GetAggregationTemporality(
        MetricsSdk::InstrumentType instrumentType) const noexcept override;

    bool ForceFlush(std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept override;

    bool Shutdown(std::chrono::microseconds timeout = std::chrono::microseconds(0)) noexcept override;

    void RegisterOnHealthChangeCb(const std::function<void(bool)> &onChange) noexcept override;

protected:
    std::shared_ptr<CurlHelper> curlHelper_;
    std::shared_ptr<Serializer> serializer_;
    std::string jobUrl_;
    HttpRequestMethod method_;
    std::atomic<bool> healthy_ = true;
    std::shared_ptr<HttpHeartBeatClient> httpHeartBeatClient_;
    HeartbeatParam heartbeatParam_;

private:
    observability::exporters::metrics::ExportResult PushMetrics(HttpRequestMethod method,
                                                                const std::ostringstream &oss);
    void StartMonitor();
};
}  // namespace observability::exporters::metrics
#endif // OBSERVABILITY_EXPORTERS_METRICS_HTTP_EXPORTER_H
