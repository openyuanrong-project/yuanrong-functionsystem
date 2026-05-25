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

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <list>
#include <optional>
#include <utility>
#include <sstream>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "metrics/plugin/dynamic_load.h"
#include "metrics/sdk/metric_exporter.h"

namespace MetricsSdk = observability::sdk::metrics;

namespace {
constexpr const char *LIB_NAME = "libobservability-prometheus-pull-exporter.so";
constexpr size_t MAX_ATTEMPTS = 10;
constexpr int COMMAND_BUFFER_SIZE = 4096;
constexpr int BITS_PER_BYTE = 8;
constexpr int BITS_PER_BASE64_CHAR = 6;
constexpr int BASE64_GROUP_SIZE = 4;
constexpr int BASE64_VALUE_MASK = (1 << BITS_PER_BASE64_CHAR) - 1;
constexpr int RETRY_INTERVAL_MS = 20;
constexpr int EXPORTER_READY_TIMEOUT_SEC = 2;
constexpr int RESET_REQUEST_WAIT_MS = 100;

const MetricsSdk::InstrumentDescriptor INSTRUMENT_DESCRIPTOR = {
    .name = "test_metric",
    .description = "test metric desc",
    .unit = "ms",
    .type = MetricsSdk::InstrumentType::COUNTER,
    .valueType = MetricsSdk::InstrumentValueType::DOUBLE
};
const std::list<std::pair<std::string, std::string>> POINT_LABELS = { std::pair{ "instance_id", "ins001" },
                                                                      std::pair{ "job_id", "job001" } };
const std::vector<MetricsSdk::PointData> POINT_DATA = { { .labels = POINT_LABELS, .value = static_cast<double>(10) } };
const MetricsSdk::MetricData METRIC_DATA = {
    .instrumentDescriptor = INSTRUMENT_DESCRIPTOR,
    .aggregationTemporality = MetricsSdk::AggregationTemporality::CUMULATIVE,
    .collectionTs = std::chrono::system_clock::now(),
    .pointData = POINT_DATA
};
const MetricsSdk::InstrumentDescriptor GAUGE_DESCRIPTOR = {
    .name = "test_gauge",
    .description = "test gauge desc",
    .unit = "count",
    .type = MetricsSdk::InstrumentType::GAUGE,
    .valueType = MetricsSdk::InstrumentValueType::UINT64
};
const std::vector<MetricsSdk::PointData> GAUGE_POINT_DATA = { { .labels = {}, .value = static_cast<uint64_t>(7) } };
const MetricsSdk::MetricData GAUGE_DATA = {
    .instrumentDescriptor = GAUGE_DESCRIPTOR,
    .aggregationTemporality = MetricsSdk::AggregationTemporality::CUMULATIVE,
    .collectionTs = std::chrono::system_clock::now(),
    .pointData = GAUGE_POINT_DATA
};

void RetainExporter(const std::shared_ptr<observability::exporters::metrics::Exporter> &exporter)
{
    // Keep dlopen handles alive across tests. Unloading one exporter plugin before loading another can make weak
    // plugin hooks resolve to the wrong shared object in this single-process test binary.
    static std::vector<std::shared_ptr<observability::exporters::metrics::Exporter>> exporters;
    exporters->push_back(exporter);
}

std::string GetLibraryPath()
{
    if (const char *libDir = std::getenv("OBSERVABILITY_METRICS_LIB_DIR"); libDir != nullptr && libDir[0] != '\0') {
        const std::string filePath = std::string(libDir) + "/" + LIB_NAME;
        if (access(filePath.c_str(), R_OK) == 0) {
            return filePath;
        }
    }

    char path[1024] = {0};
    const ssize_t len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if (len != -1) {
        const std::string executablePath(path, static_cast<size_t>(len));
        const auto slashPos = executablePath.find_last_of('/');
        if (slashPos != std::string::npos) {
            const std::string directoryPath = executablePath.substr(0, slashPos);
            const std::string filePath = directoryPath + "/../lib/" + LIB_NAME;
            if (access(filePath.c_str(), R_OK) == 0) {
                return filePath;
            }
        }
    }
    return "";
}

sockaddr *ToSockAddr(sockaddr_in &addr)
{
    return static_cast<sockaddr *>(static_cast<void *>(&addr));
}

std::optional<uint16_t> GetAvailablePort()
{
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0);
    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1 || bind(fd, ToSockAddr(addr), sizeof(addr)) != 0) {
        close(fd);
        return std::nullopt;
    }

    socklen_t addrLen = sizeof(addr);
    if (getsockname(fd, ToSockAddr(addr), &addrLen) != 0) {
        close(fd);
        return std::nullopt;
    }

    const uint16_t port = ntohs(addr.sin_port);
    close(fd);
    return port;
}

std::optional<std::pair<int, uint16_t>> OpenMockListeningSocket()
{
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }

    int reuse = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, static_cast<socklen_t>(sizeof(reuse)));
    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0);
    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1 ||
        bind(fd, ToSockAddr(addr), sizeof(addr)) != 0 ||
        listen(fd, 1) != 0) {
        close(fd);
        return std::nullopt;
    }

    socklen_t addrLen = sizeof(addr);
    if (getsockname(fd, ToSockAddr(addr), &addrLen) != 0) {
        close(fd);
        return std::nullopt;
    }
    return std::make_pair(fd, ntohs(addr.sin_port));
}

std::optional<std::string> SendHttpRequest(uint16_t port, const std::string &request)
{
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1 ||
        connect(fd, ToSockAddr(addr), sizeof(addr)) != 0 ||
        send(fd, request.data(), request.size(), 0) < 0) {
        close(fd);
        return std::nullopt;
    }

    std::string response;
    char buffer[1024] = {0};
    ssize_t readSize = 0;
    while ((readSize = recv(fd, buffer, sizeof(buffer), 0)) > 0) {
        response.append(buffer, static_cast<size_t>(readSize));
    }
    close(fd);
    return response;
}

bool SendResetHttpRequest(uint16_t port)
{
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }

    linger resetOnClose {};
    resetOnClose.l_onoff = 1;
    resetOnClose.l_linger = 0;
    (void)setsockopt(fd, SOL_SOCKET, SO_LINGER, &resetOnClose, static_cast<socklen_t>(sizeof(resetOnClose)));

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    const std::string request = "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1 ||
        connect(fd, ToSockAddr(addr), sizeof(addr)) != 0 ||
        send(fd, request.data(), request.size(), 0) < 0) {
        close(fd);
        return false;
    }
    close(fd);
    return true;
}

std::string RunCommand(const std::string &command)
{
    std::string output;
    FILE *pipe = popen(command.c_str(), "r");
    if (pipe == nullptr) {
        return output;
    }
    char buffer[COMMAND_BUFFER_SIZE] = {0};
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        output += buffer;
    }
    (void)pclose(pipe);
    return output;
}

bool GenerateSelfSignedCert(const std::string &certFile, const std::string &keyFile)
{
    std::ostringstream command;
    command << "openssl req -x509 -newkey rsa:2048 -nodes -days 1 " <<
        "-subj /CN=127.0.0.1 " <<
        "-keyout " << keyFile << " " <<
        "-out " << certFile << " >/dev/null 2>&1";
    return std::system(command.str().c_str()) == 0;
}

std::string ReadFile(const std::string &file)
{
    std::ifstream stream(file, std::ios::in | std::ios::binary);
    std::ostringstream content;
    content << stream.rdbuf();
    return content.str();
}

std::string Base64Encode(const std::string &input)
{
    static constexpr char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string output;
    int val = 0;
    int valb = -BITS_PER_BASE64_CHAR;
    for (const auto ch : input) {
        val = (val << BITS_PER_BYTE) + static_cast<unsigned char>(ch);
        valb += BITS_PER_BYTE;
        while (valb >= 0) {
            output.push_back(table[(val >> valb) & BASE64_VALUE_MASK]);
            valb -= BITS_PER_BASE64_CHAR;
        }
    }
    if (valb > -BITS_PER_BASE64_CHAR) {
        output.push_back(table[((val << BITS_PER_BYTE) >> (valb + BITS_PER_BYTE)) & BASE64_VALUE_MASK]);
    }
    while (output.size() % BASE64_GROUP_SIZE != 0) {
        output.push_back('=');
    }
    return output;
}

std::string SendHttpsRequest(uint16_t port, const std::string &path)
{
    std::ostringstream command;
    command << "printf 'GET " << path <<
        " HTTP/1.1\\r\\nHost: 127.0.0.1\\r\\nConnection: close\\r\\n\\r\\n' | " <<
        "openssl s_client -connect 127.0.0.1:" << port <<
        " -quiet -ign_eof 2>/dev/null";
    return RunCommand(command.str());
}

std::optional<std::string> HttpGet(uint16_t port, const std::string &path)
{
    return SendHttpRequest(port, "GET " + path + " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
}

std::optional<std::string> HttpHead(uint16_t port, const std::string &path)
{
    return SendHttpRequest(port, "HEAD " + path + " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
}

bool WaitForExporterReady(uint16_t port, const std::string &path, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (HttpGet(port, path).has_value()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_INTERVAL_MS));
    }
    return false;
}

std::shared_ptr<observability::exporters::metrics::Exporter> LoadReadyExporter(const std::string &libraryPath,
    nlohmann::json &jsonConfig, std::string &error)
{
    for (size_t attempt = 0; attempt < MAX_ATTEMPTS; ++attempt) {
        const auto port = GetAvailablePort();
        if (!port.has_value()) {
            continue;
        }

        jsonConfig = {
            { "ip", "127.0.0.1" },
            { "port", port.value() },
            { "metricsPath", "/metrics" }
        };

        auto exporter = observability::plugin::metrics::LoadExporterFromLibrary(libraryPath, jsonConfig.dump(), error);
        if (exporter == nullptr) {
            continue;
        }
        RetainExporter(exporter);
        if (WaitForExporterReady(port.value(), jsonConfig["metricsPath"].get<std::string>(),
                                 std::chrono::seconds(EXPORTER_READY_TIMEOUT_SEC))) {
            return exporter;
        }
        (void)exporter->Shutdown(std::chrono::microseconds());
    }
    return nullptr;
}

std::shared_ptr<observability::exporters::metrics::Exporter> LoadExporter(
    const std::string &libraryPath, const nlohmann::json &jsonConfig, std::string &error)
{
    auto exporter = observability::plugin::metrics::LoadExporterFromLibrary(libraryPath, jsonConfig.dump(), error);
    if (exporter != nullptr) {
        RetainExporter(exporter);
    }
    return exporter;
}
}  // namespace

namespace observability::test::exporter {

class PrometheusPullExporterTest : public ::testing::Test {};

TEST_F(PrometheusPullExporterTest, ExportAndPull)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());

    std::string error;
    nlohmann::json jsonConfig;
    auto exporter = LoadReadyExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);

    std::vector<MetricsSdk::MetricData> vec = { METRIC_DATA };
    EXPECT_EQ(exporter->Export(vec), observability::exporters::metrics::ExportResult::SUCCESS);

    const auto response = HttpGet(jsonConfig["port"].get<uint16_t>(), jsonConfig["metricsPath"].get<std::string>());
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(response->find("200 OK"), std::string::npos);
    EXPECT_EQ(response->find("Server:"), std::string::npos);
    EXPECT_NE(response->find("# TYPE test_metric counter"), std::string::npos);
    EXPECT_NE(response->find("test_metric{instance_id=\"ins001\",job_id=\"job001\"} 10"), std::string::npos);

    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, CustomMetricsPathAndMultipleMetricTypes)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());
    const auto port = GetAvailablePort();
    ASSERT_TRUE(port.has_value());

    std::string error;
    nlohmann::json jsonConfig = {
        { "ip", "127.0.0.1" },
        { "port", port.value() },
        { "metricsPath", "/custom-metrics" }
    };
    auto exporter = observability::plugin::metrics::LoadExporterFromLibrary(libraryPath, jsonConfig.dump(), error);
    ASSERT_NE(exporter, nullptr);
    RetainExporter(exporter);
    ASSERT_TRUE(WaitForExporterReady(port.value(), "/custom-metrics",
                                     std::chrono::seconds(EXPORTER_READY_TIMEOUT_SEC)));

    EXPECT_EQ(exporter->Export({ METRIC_DATA, GAUGE_DATA }), observability::exporters::metrics::ExportResult::SUCCESS);

    const auto response = HttpGet(port.value(), "/custom-metrics");
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(response->find("test_metric{instance_id=\"ins001\",job_id=\"job001\"} 10"), std::string::npos);
    EXPECT_NE(response->find("# TYPE test_gauge gauge"), std::string::npos);
    EXPECT_NE(response->find("test_gauge 7"), std::string::npos);

    const auto defaultPathResponse = HttpGet(port.value(), "/metrics");
    ASSERT_TRUE(defaultPathResponse.has_value());
    EXPECT_NE(defaultPathResponse->find("404 Not Found"), std::string::npos);
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, GetAggregationTemporality)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());

    std::string error;
    nlohmann::json jsonConfig;
    auto exporter = LoadReadyExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);

    EXPECT_EQ(exporter->GetAggregationTemporality(MetricsSdk::InstrumentType::GAUGE),
              MetricsSdk::AggregationTemporality::CUMULATIVE);
    EXPECT_EQ(exporter->GetAggregationTemporality(MetricsSdk::InstrumentType::COUNTER),
              MetricsSdk::AggregationTemporality::CUMULATIVE);
    EXPECT_EQ(exporter->GetAggregationTemporality(MetricsSdk::InstrumentType::HISTOGRAM),
              MetricsSdk::AggregationTemporality::CUMULATIVE);
    EXPECT_TRUE(exporter->ForceFlush(std::chrono::microseconds()));
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, ExportSkipsEmptyPointData)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());

    std::string error;
    nlohmann::json jsonConfig;
    auto exporter = LoadReadyExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);

    MetricsSdk::MetricData emptyPointMetric = METRIC_DATA;
    emptyPointMetric.pointData.clear();
    EXPECT_EQ(exporter->Export({ emptyPointMetric }), observability::exporters::metrics::ExportResult::SUCCESS);

    const auto response = HttpGet(jsonConfig["port"].get<uint16_t>(), jsonConfig["metricsPath"].get<std::string>());
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(response->find("200 OK"), std::string::npos);
    EXPECT_EQ(response->find("test_metric"), std::string::npos);
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, InvalidListenAddressKeepsExporterUnhealthy)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());

    std::string error;
    nlohmann::json jsonConfig = {
        { "ip", "invalid.invalid" },
        { "port", 0 },
        { "metricsPath", "/metrics" }
    };
    auto exporter = observability::plugin::metrics::LoadExporterFromLibrary(libraryPath, jsonConfig.dump(), error);
    ASSERT_NE(exporter, nullptr);
    RetainExporter(exporter);

    EXPECT_EQ(exporter->Export({ METRIC_DATA }), observability::exporters::metrics::ExportResult::FAILURE);
    EXPECT_TRUE(exporter->ForceFlush(std::chrono::microseconds()));
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, MockOccupiedPortKeepsExporterUnhealthy)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());
    const auto mockSocket = OpenMockListeningSocket();
    ASSERT_TRUE(mockSocket.has_value());

    std::string error;
    nlohmann::json jsonConfig = {
        { "ip", "127.0.0.1" },
        { "port", mockSocket->second },
        { "metricsPath", "/metrics" }
    };
    auto exporter = observability::plugin::metrics::LoadExporterFromLibrary(libraryPath, jsonConfig.dump(), error);
    close(mockSocket->first);
    ASSERT_NE(exporter, nullptr);
    RetainExporter(exporter);

    EXPECT_EQ(exporter->Export({ METRIC_DATA }), observability::exporters::metrics::ExportResult::FAILURE);
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, MockResetClientDoesNotBreakExporter)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());

    std::string error;
    nlohmann::json jsonConfig;
    auto exporter = LoadReadyExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);
    EXPECT_EQ(exporter->Export({ METRIC_DATA }), observability::exporters::metrics::ExportResult::SUCCESS);

    ASSERT_TRUE(SendResetHttpRequest(jsonConfig["port"].get<uint16_t>()));
    std::this_thread::sleep_for(std::chrono::milliseconds(RESET_REQUEST_WAIT_MS));

    const auto response = HttpGet(jsonConfig["port"].get<uint16_t>(), "/metrics");
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(response->find("test_metric{instance_id=\"ins001\",job_id=\"job001\"} 10"), std::string::npos);
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, SslConfigFailuresKeepExporterUnhealthy)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());

    const auto mutualTlsPort = GetAvailablePort();
    ASSERT_TRUE(mutualTlsPort.has_value());
    std::string error;
    nlohmann::json missingRootConfig = {
        { "ip", "127.0.0.1" },
        { "port", mutualTlsPort.value() },
        { "isSSLEnable", true },
        { "mutualTlsEnable", true },
        { "certFile", "/dev/null" },
        { "keyFile", "/dev/null" }
    };
    auto missingRootExporter = observability::plugin::metrics::LoadExporterFromLibrary(
        libraryPath, missingRootConfig.dump(), error);
    ASSERT_NE(missingRootExporter, nullptr);
    RetainExporter(missingRootExporter);
    EXPECT_EQ(missingRootExporter->Export({ METRIC_DATA }), observability::exporters::metrics::ExportResult::FAILURE);

    const auto invalidCertPort = GetAvailablePort();
    ASSERT_TRUE(invalidCertPort.has_value());
    nlohmann::json invalidCertConfig = {
        { "ip", "127.0.0.1" },
        { "port", invalidCertPort.value() },
        { "isSSLEnable", true },
        { "certFile", "/dev/null" },
        { "keyFile", "/dev/null" },
        { "passphrase", "test-passphrase" }
    };
    auto invalidCertExporter = observability::plugin::metrics::LoadExporterFromLibrary(
        libraryPath, invalidCertConfig.dump(), error);
    ASSERT_NE(invalidCertExporter, nullptr);
    RetainExporter(invalidCertExporter);
    EXPECT_EQ(invalidCertExporter->Export({ METRIC_DATA }), observability::exporters::metrics::ExportResult::FAILURE);
}

TEST_F(PrometheusPullExporterTest, CertificateDataMissingPairKeepsExporterUnhealthy)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());
    const auto port = GetAvailablePort();
    ASSERT_TRUE(port.has_value());

    std::string error;
    nlohmann::json jsonConfig = {
        { "ip", "127.0.0.1" },
        { "port", port.value() },
        { "isSSLEnable", true },
        { "certFile", "/dev/null" },
        { "keyFile", "/dev/null" },
        { "certData", Base64Encode("not a certificate") }
    };
    auto exporter = LoadExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);

    EXPECT_EQ(exporter->Export({ METRIC_DATA }), observability::exporters::metrics::ExportResult::FAILURE);
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, InvalidCertificateDataKeepsExporterUnhealthy)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());
    const auto port = GetAvailablePort();
    ASSERT_TRUE(port.has_value());

    std::string error;
    nlohmann::json jsonConfig = {
        { "ip", "127.0.0.1" },
        { "port", port.value() },
        { "isSSLEnable", true },
        { "certData", Base64Encode("not a certificate") },
        { "keyData", Base64Encode("not a private key") }
    };
    auto exporter = LoadExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);

    EXPECT_EQ(exporter->Export({ METRIC_DATA }), observability::exporters::metrics::ExportResult::FAILURE);
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, InvalidPrivateKeyDataKeepsExporterUnhealthy)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());
    const auto port = GetAvailablePort();
    ASSERT_TRUE(port.has_value());

    const std::string filePrefix = "/tmp/prometheus_pull_exporter_test_invalid_key_" + std::to_string(getpid()) +
        "_" + std::to_string(port.value());
    const std::string certFile = filePrefix + ".crt";
    const std::string keyFile = filePrefix + ".key";
    ASSERT_TRUE(GenerateSelfSignedCert(certFile, keyFile));
    const auto certData = Base64Encode(ReadFile(certFile));
    ASSERT_FALSE(certData.empty());
    (void)unlink(certFile.c_str());
    (void)unlink(keyFile.c_str());

    std::string error;
    nlohmann::json jsonConfig = {
        { "ip", "127.0.0.1" },
        { "port", port.value() },
        { "isSSLEnable", true },
        { "certData", certData },
        { "keyData", Base64Encode("not a private key") }
    };
    auto exporter = LoadExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);

    EXPECT_EQ(exporter->Export({ METRIC_DATA }), observability::exporters::metrics::ExportResult::FAILURE);
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, InvalidRootCertificateDataKeepsExporterUnhealthy)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());
    const auto port = GetAvailablePort();
    ASSERT_TRUE(port.has_value());

    const std::string filePrefix = "/tmp/prometheus_pull_exporter_test_invalid_root_" + std::to_string(getpid()) +
        "_" + std::to_string(port.value());
    const std::string certFile = filePrefix + ".crt";
    const std::string keyFile = filePrefix + ".key";
    ASSERT_TRUE(GenerateSelfSignedCert(certFile, keyFile));
    const auto certData = Base64Encode(ReadFile(certFile));
    const auto keyData = Base64Encode(ReadFile(keyFile));
    ASSERT_FALSE(certData.empty());
    ASSERT_FALSE(keyData.empty());
    (void)unlink(certFile.c_str());
    (void)unlink(keyFile.c_str());

    std::string error;
    nlohmann::json jsonConfig = {
        { "ip", "127.0.0.1" },
        { "port", port.value() },
        { "isSSLEnable", true },
        { "mutualTlsEnable", true },
        { "rootCertData", Base64Encode("not a root certificate") },
        { "certData", certData },
        { "keyData", keyData }
    };
    auto exporter = LoadExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);

    EXPECT_EQ(exporter->Export({ METRIC_DATA }), observability::exporters::metrics::ExportResult::FAILURE);
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, MockHttpsClientPullsMetrics)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());
    const auto port = GetAvailablePort();
    ASSERT_TRUE(port.has_value());

    const std::string filePrefix = "/tmp/prometheus_pull_exporter_test_" + std::to_string(getpid()) + "_" +
        std::to_string(port.value());
    const std::string certFile = filePrefix + ".crt";
    const std::string keyFile = filePrefix + ".key";
    ASSERT_TRUE(GenerateSelfSignedCert(certFile, keyFile));

    std::string error;
    nlohmann::json jsonConfig = {
        { "ip", "127.0.0.1" },
        { "port", port.value() },
        { "metricsPath", "/metrics" },
        { "isSSLEnable", true },
        { "certFile", certFile },
        { "keyFile", keyFile }
    };
    auto exporter = observability::plugin::metrics::LoadExporterFromLibrary(libraryPath, jsonConfig.dump(), error);
    ASSERT_NE(exporter, nullptr);
    RetainExporter(exporter);

    EXPECT_EQ(exporter->Export({ METRIC_DATA }), observability::exporters::metrics::ExportResult::SUCCESS);
    const auto response = SendHttpsRequest(port.value(), "/metrics");
    EXPECT_NE(response.find("HTTP/1.1 200 OK"), std::string::npos);
    EXPECT_NE(response.find("test_metric{instance_id=\"ins001\",job_id=\"job001\"} 10"), std::string::npos);
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
    (void)unlink(certFile.c_str());
    (void)unlink(keyFile.c_str());
}

TEST_F(PrometheusPullExporterTest, MockHttpsClientPullsMetricsWithCertificateData)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());
    const auto port = GetAvailablePort();
    ASSERT_TRUE(port.has_value());

    const std::string filePrefix = "/tmp/prometheus_pull_exporter_test_data_" + std::to_string(getpid()) + "_" +
        std::to_string(port.value());
    const std::string certFile = filePrefix + ".crt";
    const std::string keyFile = filePrefix + ".key";
    ASSERT_TRUE(GenerateSelfSignedCert(certFile, keyFile));

    const auto certData = Base64Encode(ReadFile(certFile));
    const auto keyData = Base64Encode(ReadFile(keyFile));
    ASSERT_FALSE(certData.empty());
    ASSERT_FALSE(keyData.empty());
    (void)unlink(certFile.c_str());
    (void)unlink(keyFile.c_str());

    std::string error;
    nlohmann::json jsonConfig = {
        { "ip", "127.0.0.1" },
        { "port", port.value() },
        { "metricsPath", "/metrics" },
        { "isSSLEnable", true },
        { "certData", certData },
        { "keyData", keyData }
    };
    auto exporter = observability::plugin::metrics::LoadExporterFromLibrary(libraryPath, jsonConfig.dump(), error);
    ASSERT_NE(exporter, nullptr);
    RetainExporter(exporter);

    EXPECT_EQ(exporter->Export({ METRIC_DATA }), observability::exporters::metrics::ExportResult::SUCCESS);
    const auto response = SendHttpsRequest(port.value(), "/metrics");
    EXPECT_NE(response.find("HTTP/1.1 200 OK"), std::string::npos);
    EXPECT_NE(response.find("test_metric{instance_id=\"ins001\",job_id=\"job001\"} 10"), std::string::npos);
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, InvalidRequestReturnsBadRequest)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());

    std::string error;
    nlohmann::json jsonConfig;
    auto exporter = LoadReadyExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);

    const auto response = SendHttpRequest(jsonConfig["port"].get<uint16_t>(), "GET /metrics\r\n\r\n");
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(response->find("400 Bad Request"), std::string::npos);

    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, HeadRequestReturnsHeadersOnly)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());

    std::string error;
    nlohmann::json jsonConfig;
    auto exporter = LoadReadyExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);

    const auto response = HttpHead(jsonConfig["port"].get<uint16_t>(), jsonConfig["metricsPath"].get<std::string>());
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(response->find("200 OK"), std::string::npos);
    EXPECT_NE(response->find("Content-Length: 0"), std::string::npos);
    EXPECT_EQ(response->find("# TYPE"), std::string::npos);

    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, NotFoundAndMethodNotAllowed)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());

    std::string error;
    nlohmann::json jsonConfig;
    auto exporter = LoadReadyExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);

    const auto notFound = HttpGet(jsonConfig["port"].get<uint16_t>(), "/not-found");
    ASSERT_TRUE(notFound.has_value());
    EXPECT_NE(notFound->find("404 Not Found"), std::string::npos);

    const auto methodNotAllowed = SendHttpRequest(
        jsonConfig["port"].get<uint16_t>(),
        "POST " + jsonConfig["metricsPath"].get<std::string>() +
            " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    ASSERT_TRUE(methodNotAllowed.has_value());
    EXPECT_NE(methodNotAllowed->find("405 Method Not Allowed"), std::string::npos);

    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, EmptyExportAndShutdownBehavior)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());

    std::string error;
    nlohmann::json jsonConfig;
    auto exporter = LoadReadyExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);

    EXPECT_EQ(exporter->Export({}), observability::exporters::metrics::ExportResult::EMPTY_DATA);
    EXPECT_TRUE(WaitForExporterReady(jsonConfig["port"].get<uint16_t>(), "/metrics",
                                     std::chrono::seconds(EXPORTER_READY_TIMEOUT_SEC)));

    std::vector<MetricsSdk::MetricData> vec = { METRIC_DATA };
    EXPECT_EQ(exporter->Export(vec), observability::exporters::metrics::ExportResult::SUCCESS);

    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
    EXPECT_EQ(exporter->Export(vec), observability::exporters::metrics::ExportResult::FAILURE);
}

TEST_F(PrometheusPullExporterTest, DefaultMetricsPathAndHealthCallback)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());

    const auto port = GetAvailablePort();
    ASSERT_TRUE(port.has_value());

    std::string error;
    nlohmann::json jsonConfig = {
        { "ip", "127.0.0.1" },
        { "port", port.value() }
    };
    auto exporter = observability::plugin::metrics::LoadExporterFromLibrary(libraryPath, jsonConfig.dump(), error);
    ASSERT_NE(exporter, nullptr);
    RetainExporter(exporter);

    std::vector<bool> healthEvents;
    exporter->RegisterOnHealthChangeCb([&healthEvents](bool healthy) { healthEvents.push_back(healthy); });

    EXPECT_TRUE(WaitForExporterReady(port.value(), "/metrics", std::chrono::seconds(EXPORTER_READY_TIMEOUT_SEC)));
    ASSERT_FALSE(healthEvents.empty());
    EXPECT_TRUE(healthEvents.front());

    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
    ASSERT_GE(healthEvents.size(), 2U);
    EXPECT_FALSE(healthEvents.back());
}

TEST_F(PrometheusPullExporterTest, MergeMetricDataReplacesSameLabelsAndAppendsDifferentLabels)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());

    std::string error;
    nlohmann::json jsonConfig;
    auto exporter = LoadReadyExporter(libraryPath, jsonConfig, error);
    ASSERT_NE(exporter, nullptr);
    const auto port = jsonConfig["port"].get<uint16_t>();

    MetricsSdk::MetricData baseMetric = METRIC_DATA;
    EXPECT_EQ(exporter->Export({ baseMetric }), observability::exporters::metrics::ExportResult::SUCCESS);

    MetricsSdk::MetricData replaceMetric = METRIC_DATA;
    replaceMetric.pointData = { { .labels = POINT_LABELS, .value = static_cast<double>(20) } };
    EXPECT_EQ(exporter->Export({ replaceMetric }), observability::exporters::metrics::ExportResult::SUCCESS);

    auto response = HttpGet(port, "/metrics");
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(response->find("test_metric{instance_id=\"ins001\",job_id=\"job001\"} 20"), std::string::npos);
    EXPECT_EQ(response->find("test_metric{instance_id=\"ins001\",job_id=\"job001\"} 10"), std::string::npos);

    const std::list<std::pair<std::string, std::string>> otherLabels = { std::pair{"instance_id", "ins002"} };
    MetricsSdk::MetricData appendMetric = METRIC_DATA;
    appendMetric.pointData = { { .labels = otherLabels, .value = static_cast<double>(30) } };
    EXPECT_EQ(exporter->Export({ appendMetric }), observability::exporters::metrics::ExportResult::SUCCESS);
    response = HttpGet(port, "/metrics");
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(response->find("test_metric{instance_id=\"ins002\"} 30"), std::string::npos);

    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));
}

TEST_F(PrometheusPullExporterTest, ParseRequestAndSslContextValidation)
{
    const auto libraryPath = GetLibraryPath();
    ASSERT_FALSE(libraryPath.empty());
    const auto port = GetAvailablePort();
    ASSERT_TRUE(port.has_value());

    std::string error;
    nlohmann::json jsonConfig = {
        { "ip", "127.0.0.1" },
        { "port", port.value() },
        { "metricsPath", "/metrics" }
    };
    auto exporter = observability::plugin::metrics::LoadExporterFromLibrary(libraryPath, jsonConfig.dump(), error);
    ASSERT_NE(exporter, nullptr);
    RetainExporter(exporter);

    auto invalidResponse = SendHttpRequest(port.value(), "GET /metrics HTTP/1.1 extra\r\n\r\n");
    ASSERT_TRUE(invalidResponse.has_value());
    EXPECT_NE(invalidResponse->find("400 Bad Request"), std::string::npos);
    auto response = SendHttpRequest(port.value(), "GET /metrics HTTP/2.0\r\n\r\n");
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(response->find("400 Bad Request"), std::string::npos);
    EXPECT_TRUE(exporter->Shutdown(std::chrono::microseconds()));

    const auto sslPort = GetAvailablePort();
    ASSERT_TRUE(sslPort.has_value());
    nlohmann::json sslConfig = {
        { "ip", "127.0.0.1" },
        { "port", sslPort.value() },
        { "isSSLEnable", true }
    };
    auto sslExporter = observability::plugin::metrics::LoadExporterFromLibrary(libraryPath, sslConfig.dump(), error);
    ASSERT_NE(sslExporter, nullptr);
    RetainExporter(sslExporter);
    EXPECT_EQ(sslExporter->Export({ METRIC_DATA }), observability::exporters::metrics::ExportResult::FAILURE);
    EXPECT_TRUE(sslExporter->Shutdown(std::chrono::microseconds()));
}

}  // namespace observability::test::exporter
