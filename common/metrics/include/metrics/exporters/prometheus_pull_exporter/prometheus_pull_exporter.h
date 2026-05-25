/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#ifndef OBSERVABILITY_EXPORTERS_PROMETHEUS_PULL_EXPORTER_H
#define OBSERVABILITY_EXPORTERS_PROMETHEUS_PULL_EXPORTER_H

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <netdb.h>
#include <string>
#include <thread>

#include <openssl/ssl.h>

#include "metrics/exporters/common/ssl_config.h"
#include "metrics/exporters/exporter.h"
#include "metrics/exporters/common/prometheus_text_serializer.h"

namespace observability::exporters::metrics {

struct PrometheusPullExportOptions {
    std::string ip = "0.0.0.0";
    uint16_t port = 31539;
    std::string metricsPath = "/metrics";
    SSLConfig sslConfig;
    bool mutualTlsEnable = false;
};

class PrometheusPullExporter final : public Exporter {
public:
    explicit PrometheusPullExporter(const std::string &config);
    explicit PrometheusPullExporter(const PrometheusPullExportOptions &options);
    ~PrometheusPullExporter() override;

    ExportResult Export(const std::vector<observability::sdk::metrics::MetricData> &data) noexcept override;

    observability::sdk::metrics::AggregationTemporality GetAggregationTemporality(
        observability::sdk::metrics::InstrumentType instrumentType) const noexcept override;

    bool ForceFlush(std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept override;

    bool Shutdown(std::chrono::microseconds timeout = std::chrono::microseconds(0)) noexcept override;

    void RegisterOnHealthChangeCb(const std::function<void(bool)> &onChange) noexcept override;

private:
    void Init(const PrometheusPullExportOptions &options);
    bool Start();
    bool PrepareSslContext();
    int OpenServerSocket();
    bool BindServerSocket(addrinfo *result, int &serverFd);
    void CleanupSslContext();
    void Stop();
    void NotifyHealthChange(bool healthy) const noexcept;
    void ServeLoop();
    void HandleHttpSession(int clientFd);
    void HandleHttpsSession(int clientFd);
    std::string BuildHttpResponse(const std::string &method, const std::string &target) const;
    std::string BuildHttpResponse(int statusCode, const std::string &body) const;
    SSL_CTX *BuildSslContext();
    bool LoadCertificateAndKey(SSL_CTX *ctx);
    bool LoadCertificateChainFromData(SSL_CTX *ctx);
    bool LoadPrivateKeyFromData(SSL_CTX *ctx);
    bool LoadRootCertsFromData(SSL_CTX *ctx);
    std::string RenderMetrics() const;
    void MergeMetricData(const observability::sdk::metrics::MetricData &metric);
    static bool ParseRequest(const std::string &request, std::string &method, std::string &target);
    static bool WriteAll(int fd, const std::string &data);
    static bool WriteAll(SSL *ssl, const std::string &data);
    static std::string ReadRequest(int fd);
    static std::string ReadRequest(SSL *ssl);

    PrometheusPullExportOptions options_;
    std::shared_ptr<Serializer> serializer_;
    mutable std::mutex callbackMutex_{};
    mutable std::mutex cacheMutex_{};
    std::function<void(bool)> onHealthChange_{};
    std::map<std::string, observability::sdk::metrics::MetricData> metricCache_{};
    int serverFd_ = -1;
    SSL_CTX *sslContext_ = nullptr;
    std::thread worker_{};
    std::atomic<bool> running_{false};
};

}  // namespace observability::exporters::metrics

#endif  // OBSERVABILITY_EXPORTERS_PROMETHEUS_PULL_EXPORTER_H
