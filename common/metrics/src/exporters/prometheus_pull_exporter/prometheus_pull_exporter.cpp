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

#include "metrics/exporters/prometheus_pull_exporter/prometheus_pull_exporter.h"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <locale>
#include <sstream>
#include <utility>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/pem.h>
#include <nlohmann/json.hpp>
#include <securec.h>

#include "common/logs/log.h"

namespace observability::exporters::metrics {

namespace {
constexpr std::chrono::milliseconds ACCEPT_INTERVAL(50);
constexpr std::chrono::seconds SESSION_TIMEOUT(30);
constexpr int HTTP_STATUS_OK = 200;
constexpr int HTTP_STATUS_BAD_REQUEST = 400;
constexpr int HTTP_STATUS_NOT_FOUND = 404;
constexpr int HTTP_STATUS_METHOD_NOT_ALLOWED = 405;
constexpr const char *CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";
constexpr const char *BAD_REQUEST_BODY = "bad request\n";
constexpr size_t BUFFER_SIZE = 4096;
constexpr size_t MAX_REQUEST_SIZE = 16384;

bool SameLabels(const sdk::metrics::PointLabels &lhs, const sdk::metrics::PointLabels &rhs)
{
    return lhs == rhs;
}

void CloseFd(int &fd)
{
    if (fd >= 0) {
        close(fd);
        fd = -1;
    }
}

template <typename T>
sockaddr *ToSockAddr(T &addr)
{
    return static_cast<sockaddr *>(static_cast<void *>(&addr));
}

std::string BuildStatusLine(int statusCode)
{
    switch (statusCode) {
        case HTTP_STATUS_BAD_REQUEST:
            return "HTTP/1.1 400 Bad Request\r\n";
        case HTTP_STATUS_OK:
            return "HTTP/1.1 200 OK\r\n";
        case HTTP_STATUS_NOT_FOUND:
            return "HTTP/1.1 404 Not Found\r\n";
        case HTTP_STATUS_METHOD_NOT_ALLOWED:
            return "HTTP/1.1 405 Method Not Allowed\r\n";
        default:
            return "HTTP/1.1 500 Internal Server Error\r\n";
    }
}

bool ResolveBoundPort(int serverFd, uint16_t &port)
{
    sockaddr_in addr {};
    socklen_t addrLen = sizeof(addr);
    if (getsockname(serverFd, ToSockAddr(addr), &addrLen) != 0) {
        return false;
    }
    port = ntohs(addr.sin_port);
    return true;
}

void ShutdownSslSession(SSL *ssl)
{
    int ret = SSL_shutdown(ssl);
    if (ret == 0) {
        ret = SSL_shutdown(ssl);
    }
    if (ret != 1) {
        METRICS_LOG_WARN("Prometheus pull exporter ssl shutdown incomplete, ssl error {}",
                         SSL_get_error(ssl, ret));
    }
}

int PasswordCallback(char *buf, int size, int, void *userdata)  // NOLINT
{
    if (buf == nullptr || userdata == nullptr || size <= 0) {
        return 0;
    }
    const auto *pass = static_cast<const SensitiveData *>(userdata);
    const auto passLen = static_cast<int>(pass->GetSize());
    if (passLen <= 0 || passLen >= size) {
        return 0;
    }
    if (memcpy_s(buf, static_cast<size_t>(size), pass->GetData(), static_cast<size_t>(passLen)) != EOK) {
        return 0;
    }
    buf[passLen] = '\0';
    return passLen;
}
}  // namespace

PrometheusPullExporter::PrometheusPullExporter(const std::string &config)
{
    PrometheusPullExportOptions parsedOptions;
    try {
        const auto configJson = nlohmann::json::parse(config);
        if (configJson.contains("ip")) {
            parsedOptions.ip = configJson.at("ip").get<std::string>();
        }
        if (configJson.contains("port")) {
            parsedOptions.port = configJson.at("port").get<uint16_t>();
        }
        if (configJson.contains("metricsPath")) {
            parsedOptions.metricsPath = configJson.at("metricsPath").get<std::string>();
        }
        if (configJson.contains("mutualTlsEnable")) {
            parsedOptions.mutualTlsEnable = configJson.at("mutualTlsEnable").get<bool>();
        }
    } catch (const std::exception &e) {
        METRICS_LOG_ERROR("Failed to parse PrometheusPullExportOptions, error {}", e.what());
    }
    parsedOptions.sslConfig.Parse(config);
    Init(parsedOptions);
}

PrometheusPullExporter::PrometheusPullExporter(const PrometheusPullExportOptions &options)
{
    Init(options);
}

PrometheusPullExporter::~PrometheusPullExporter()
{
    Stop();
}

void PrometheusPullExporter::Init(const PrometheusPullExportOptions &options)
{
    options_ = options;
    serializer_ = std::make_shared<PrometheusTextSerializer>();
    if (options_.metricsPath.empty()) {
        options_.metricsPath = "/metrics";
    }
    if (!Start()) {
        METRICS_LOG_ERROR("Failed to start prometheus pull exporter on {}:{}", options_.ip, options_.port);
    }
}

ExportResult PrometheusPullExporter::Export(const std::vector<sdk::metrics::MetricData> &data) noexcept
{
    if (data.empty()) {
        return ExportResult::EMPTY_DATA;
    }
    if (!running_.load()) {
        return ExportResult::FAILURE;
    }
    for (const auto &metric : data) {
        if (metric.pointData.empty()) {
            continue;
        }
        MergeMetricData(metric);
    }
    return ExportResult::SUCCESS;
}

sdk::metrics::AggregationTemporality PrometheusPullExporter::GetAggregationTemporality(
    sdk::metrics::InstrumentType /* instrumentType */) const noexcept
{
    return sdk::metrics::AggregationTemporality::CUMULATIVE;
}

bool PrometheusPullExporter::ForceFlush(std::chrono::microseconds /* timeout */) noexcept
{
    return true;
}

bool PrometheusPullExporter::Shutdown(std::chrono::microseconds /* timeout */) noexcept
{
    Stop();
    return true;
}

void PrometheusPullExporter::RegisterOnHealthChangeCb(const std::function<void(bool)> &onChange) noexcept
{
    {
        std::lock_guard<std::mutex> lock(callbackMutex_);
        onHealthChange_ = onChange;
    }
    NotifyHealthChange(running_.load());
}

bool PrometheusPullExporter::Start()
{
    if (running_.load()) {
        return true;
    }
    if (!PrepareSslContext()) {
        return false;
    }

    int serverFd = OpenServerSocket();
    if (serverFd < 0) {
        METRICS_LOG_ERROR("Bind/listen prometheus pull exporter {}:{} failed", options_.ip, options_.port);
        CleanupSslContext();
        return false;
    }
    if (!ResolveBoundPort(serverFd, options_.port)) {
        METRICS_LOG_ERROR("Resolve prometheus pull exporter bound port failed");
        CloseFd(serverFd);
        CleanupSslContext();
        return false;
    }

    serverFd_ = serverFd;
    running_.store(true);
    worker_ = std::thread([this]() { ServeLoop(); });
    NotifyHealthChange(true);
    return true;
}

bool PrometheusPullExporter::PrepareSslContext()
{
    if (!options_.sslConfig.isSSLEnable_) {
        return true;
    }
    sslContext_ = BuildSslContext();
    options_.sslConfig.passphrase_.Clear();
    return sslContext_ != nullptr;
}

int PrometheusPullExporter::OpenServerSocket()
{
    addrinfo hints {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    addrinfo *result = nullptr;
    const std::string port = std::to_string(options_.port);
    const int getAddrInfoRet = getaddrinfo(options_.ip.c_str(), port.c_str(), &hints, &result);
    if (getAddrInfoRet != 0) {
        METRICS_LOG_ERROR("Getaddrinfo for prometheus pull exporter {}:{} failed, error {}", options_.ip,
                          options_.port, gai_strerror(getAddrInfoRet));
        CleanupSslContext();
        return -1;
    }
    int serverFd = -1;
    const bool bindSuccess = BindServerSocket(result, serverFd);
    freeaddrinfo(result);
    return bindSuccess ? serverFd : -1;
}

bool PrometheusPullExporter::BindServerSocket(addrinfo *result, int &serverFd)
{
    for (addrinfo *it = result; it != nullptr; it = it->ai_next) {
        serverFd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (serverFd < 0) {
            continue;
        }
        int reuse = 1;
        (void)setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &reuse, static_cast<socklen_t>(sizeof(reuse)));
        const int flags = fcntl(serverFd, F_GETFL, 0);
        if (flags >= 0) {
            (void)fcntl(serverFd, F_SETFL, flags | O_NONBLOCK);
        }
        if (bind(serverFd, it->ai_addr, it->ai_addrlen) == 0 && listen(serverFd, SOMAXCONN) == 0) {
            return true;
        }
        CloseFd(serverFd);
    }
    return false;
}

void PrometheusPullExporter::CleanupSslContext()
{
    if (sslContext_ != nullptr) {
        SSL_CTX_free(sslContext_);
        sslContext_ = nullptr;
    }
}

void PrometheusPullExporter::Stop()
{
    const bool wasRunning = running_.exchange(false);
    if (!wasRunning) {
        return;
    }
    CloseFd(serverFd_);
    if (worker_.joinable()) {
        worker_.join();
    }
    if (sslContext_ != nullptr) {
        SSL_CTX_free(sslContext_);
        sslContext_ = nullptr;
    }
    NotifyHealthChange(false);
}

void PrometheusPullExporter::NotifyHealthChange(bool healthy) const noexcept
{
    std::function<void(bool)> callback;
    {
        std::lock_guard<std::mutex> lock(callbackMutex_);
        callback = onHealthChange_;
    }
    if (callback != nullptr) {
        callback(healthy);
    }
}

void PrometheusPullExporter::ServeLoop()
{
    while (running_.load()) {
        sockaddr_storage addr {};
        socklen_t addrLen = sizeof(addr);
        const int clientFd = accept(serverFd_, ToSockAddr(addr), &addrLen);
        if (!running_.load()) {
            if (clientFd >= 0) {
                close(clientFd);
            }
            break;
        }
        if (clientFd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::sleep_for(ACCEPT_INTERVAL);
                continue;
            }
            METRICS_LOG_WARN("Accept prometheus pull exporter request failed, errno {}", errno);
            continue;
        }

        timeval timeout {};
        timeout.tv_sec = static_cast<decltype(timeout.tv_sec)>(SESSION_TIMEOUT.count());
        timeout.tv_usec = 0;
        (void)setsockopt(clientFd, SOL_SOCKET, SO_RCVTIMEO, &timeout, static_cast<socklen_t>(sizeof(timeout)));
        (void)setsockopt(clientFd, SOL_SOCKET, SO_SNDTIMEO, &timeout, static_cast<socklen_t>(sizeof(timeout)));
        if (options_.sslConfig.isSSLEnable_) {
            HandleHttpsSession(clientFd);
        } else {
            HandleHttpSession(clientFd);
        }
        close(clientFd);
    }
}

void PrometheusPullExporter::HandleHttpSession(int clientFd)
{
    const auto request = ReadRequest(clientFd);
    std::string method;
    std::string target;
    const auto response = ParseRequest(request, method, target) ? BuildHttpResponse(method, target)
                                                                : BuildHttpResponse(HTTP_STATUS_BAD_REQUEST,
                                                                                    BAD_REQUEST_BODY);
    if (!WriteAll(clientFd, response)) {
        METRICS_LOG_WARN("Write prometheus pull exporter http response failed");
    }
}

void PrometheusPullExporter::HandleHttpsSession(int clientFd)
{
    SSL *ssl = SSL_new(sslContext_);
    if (ssl == nullptr) {
        METRICS_LOG_WARN("Create prometheus pull exporter ssl handle failed");
        return;
    }
    if (SSL_set_fd(ssl, clientFd) != 1 || SSL_accept(ssl) != 1) {
        METRICS_LOG_WARN("Prometheus pull exporter ssl handshake failed, error {}", ERR_get_error());
        SSL_free(ssl);
        return;
    }

    const auto request = ReadRequest(ssl);
    std::string method;
    std::string target;
    const auto response = ParseRequest(request, method, target) ? BuildHttpResponse(method, target)
                                                                : BuildHttpResponse(HTTP_STATUS_BAD_REQUEST,
                                                                                    BAD_REQUEST_BODY);
    if (!WriteAll(ssl, response)) {
        METRICS_LOG_WARN("Write prometheus pull exporter https response failed");
    }

    ShutdownSslSession(ssl);
    SSL_free(ssl);
}

std::string PrometheusPullExporter::BuildHttpResponse(const std::string &method, const std::string &target) const
{
    int statusCode = HTTP_STATUS_OK;
    std::string body;
    if (method != "GET" && method != "HEAD") {
        statusCode = HTTP_STATUS_METHOD_NOT_ALLOWED;
        body = "method not allowed\n";
    } else if (target != options_.metricsPath) {
        statusCode = HTTP_STATUS_NOT_FOUND;
        body = "not found\n";
    } else if (method != "HEAD") {
        body = RenderMetrics();
    }

    return BuildHttpResponse(statusCode, body);
}

std::string PrometheusPullExporter::BuildHttpResponse(int statusCode, const std::string &body) const
{
    std::ostringstream oss;
    oss << BuildStatusLine(statusCode)
        << "Content-Type: " << CONTENT_TYPE << "\r\n"
        << "Connection: close\r\n"
        << "Content-Length: " << body.size() << "\r\n\r\n"
        << body;
    return oss.str();
}

SSL_CTX *PrometheusPullExporter::BuildSslContext()
{
    const bool hasCertData = !options_.sslConfig.certData_.Empty() && !options_.sslConfig.keyData_.Empty();
    const bool hasCertFile = !options_.sslConfig.certFile_.empty() && !options_.sslConfig.keyFile_.empty();
    if (!hasCertData && !hasCertFile) {
        METRICS_LOG_ERROR("Prometheus pull exporter ssl enabled but certFile/keyFile is empty");
        return nullptr;
    }
    if (options_.mutualTlsEnable && options_.sslConfig.rootCertFile_.empty() &&
        options_.sslConfig.rootCertData_.Empty()) {
        METRICS_LOG_ERROR("Prometheus pull exporter mutual tls enabled but rootCertFile is empty");
        return nullptr;
    }

    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    if (ctx == nullptr) {
        METRICS_LOG_ERROR("Create prometheus pull exporter ssl context failed");
        return nullptr;
    }

    if (!options_.sslConfig.passphrase_.Empty()) {
        SSL_CTX_set_default_passwd_cb_userdata(ctx, &options_.sslConfig.passphrase_);
        SSL_CTX_set_default_passwd_cb(ctx, PasswordCallback);
    }

    SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);
    if (!LoadCertificateAndKey(ctx)) {
        SSL_CTX_free(ctx);
        return nullptr;
    }
    if (!LoadRootCertsFromData(ctx)) {
        SSL_CTX_free(ctx);
        return nullptr;
    }
    if (options_.sslConfig.rootCertData_.Empty() && !options_.sslConfig.rootCertFile_.empty() &&
        SSL_CTX_load_verify_locations(ctx, options_.sslConfig.rootCertFile_.c_str(), nullptr) != 1) {
        METRICS_LOG_ERROR("Load prometheus pull exporter root cert failed");
        SSL_CTX_free(ctx);
        return nullptr;
    }
    if (options_.mutualTlsEnable) {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    }
    return ctx;
}

bool PrometheusPullExporter::LoadCertificateAndKey(SSL_CTX *ctx)
{
    if (!options_.sslConfig.certData_.Empty() || !options_.sslConfig.keyData_.Empty()) {
        if (options_.sslConfig.certData_.Empty() || options_.sslConfig.keyData_.Empty()) {
            METRICS_LOG_ERROR("Prometheus pull exporter certificate data or key data is empty");
            return false;
        }
        return LoadCertificateChainFromData(ctx) && LoadPrivateKeyFromData(ctx);
    }
    if (SSL_CTX_use_certificate_chain_file(ctx, options_.sslConfig.certFile_.c_str()) != 1 ||
        SSL_CTX_use_PrivateKey_file(ctx, options_.sslConfig.keyFile_.c_str(), SSL_FILETYPE_PEM) != 1) {
        METRICS_LOG_ERROR("Load prometheus pull exporter certificate or key failed");
        return false;
    }
    return true;
}

bool PrometheusPullExporter::LoadCertificateChainFromData(SSL_CTX *ctx)
{
    BIO *bio = BIO_new_mem_buf(options_.sslConfig.certData_.GetData(),
                               static_cast<int>(options_.sslConfig.certData_.GetSize()));
    if (bio == nullptr) {
        METRICS_LOG_ERROR("Create prometheus pull exporter certificate bio failed");
        return false;
    }
    X509 *cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    if (cert == nullptr) {
        METRICS_LOG_ERROR("Parse prometheus pull exporter certificate data failed");
        BIO_free(bio);
        return false;
    }
    if (SSL_CTX_use_certificate(ctx, cert) != 1) {
        METRICS_LOG_ERROR("Load prometheus pull exporter certificate data failed");
        X509_free(cert);
        BIO_free(bio);
        return false;
    }
    X509_free(cert);

    for (X509 *chainCert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr); chainCert != nullptr;
         chainCert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr)) {
        if (SSL_CTX_add_extra_chain_cert(ctx, chainCert) != 1) {
            METRICS_LOG_ERROR("Load prometheus pull exporter certificate chain data failed");
            X509_free(chainCert);
            BIO_free(bio);
            return false;
        }
    }
    ERR_clear_error();
    BIO_free(bio);
    return true;
}

bool PrometheusPullExporter::LoadPrivateKeyFromData(SSL_CTX *ctx)
{
    BIO *bio = BIO_new_mem_buf(options_.sslConfig.keyData_.GetData(),
                               static_cast<int>(options_.sslConfig.keyData_.GetSize()));
    if (bio == nullptr) {
        METRICS_LOG_ERROR("Create prometheus pull exporter private key bio failed");
        return false;
    }
    void *callbackData = options_.sslConfig.passphrase_.Empty() ? nullptr : &options_.sslConfig.passphrase_;
    EVP_PKEY *privateKey = PEM_read_bio_PrivateKey(bio, nullptr, PasswordCallback, callbackData);
    BIO_free(bio);
    if (privateKey == nullptr) {
        METRICS_LOG_ERROR("Parse prometheus pull exporter private key data failed");
        return false;
    }
    if (SSL_CTX_use_PrivateKey(ctx, privateKey) != 1) {
        METRICS_LOG_ERROR("Load prometheus pull exporter private key data failed");
        EVP_PKEY_free(privateKey);
        return false;
    }
    EVP_PKEY_free(privateKey);
    return true;
}

bool PrometheusPullExporter::LoadRootCertsFromData(SSL_CTX *ctx)
{
    if (options_.sslConfig.rootCertData_.Empty()) {
        return true;
    }
    BIO *bio = BIO_new_mem_buf(options_.sslConfig.rootCertData_.GetData(),
                               static_cast<int>(options_.sslConfig.rootCertData_.GetSize()));
    if (bio == nullptr) {
        METRICS_LOG_ERROR("Create prometheus pull exporter root cert bio failed");
        return false;
    }
    X509_STORE *store = SSL_CTX_get_cert_store(ctx);
    if (store == nullptr) {
        METRICS_LOG_ERROR("Get prometheus pull exporter cert store failed");
        BIO_free(bio);
        return false;
    }
    bool loaded = false;
    for (X509 *rootCert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr); rootCert != nullptr;
         rootCert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr)) {
        if (X509_STORE_add_cert(store, rootCert) != 1) {
            METRICS_LOG_ERROR("Load prometheus pull exporter root cert data failed");
            X509_free(rootCert);
            BIO_free(bio);
            return false;
        }
        loaded = true;
        X509_free(rootCert);
    }
    ERR_clear_error();
    BIO_free(bio);
    if (!loaded) {
        METRICS_LOG_ERROR("Prometheus pull exporter root cert data is empty");
    }
    return loaded;
}

std::string PrometheusPullExporter::RenderMetrics() const
{
    std::ostringstream oss;
    std::lock_guard<std::mutex> lock(cacheMutex_);
    for (const auto &item : metricCache_) {
        if (serializer_ != nullptr) {
            serializer_->Serialize(oss, item.second);
        }
    }
    return oss.str();
}

void PrometheusPullExporter::MergeMetricData(const sdk::metrics::MetricData &metric)
{
    std::lock_guard<std::mutex> lock(cacheMutex_);
    auto &cachedMetric = metricCache_[metric.instrumentDescriptor.name];
    if (cachedMetric.instrumentDescriptor.name.empty()) {
        cachedMetric = metric;
        return;
    }
    cachedMetric.instrumentDescriptor = metric.instrumentDescriptor;
    cachedMetric.aggregationTemporality = metric.aggregationTemporality;
    cachedMetric.collectionTs = metric.collectionTs;
    for (const auto &point : metric.pointData) {
        const auto it = std::find_if(cachedMetric.pointData.begin(), cachedMetric.pointData.end(),
                                     [&point](const sdk::metrics::PointData &cachedPoint) {
                                         return SameLabels(cachedPoint.labels, point.labels);
                                     });
        if (it == cachedMetric.pointData.end()) {
            cachedMetric.pointData.push_back(point);
        } else {
            *it = point;
        }
    }
}

bool PrometheusPullExporter::ParseRequest(const std::string &request, std::string &method, std::string &target)
{
    const auto lineEnd = request.find("\r\n");
    if (lineEnd == std::string::npos) {
        return false;
    }
    const std::string requestLine = request.substr(0, lineEnd);
    std::istringstream iss(requestLine);
    std::string version;
    std::string extra;
    if (!(iss >> method >> target >> version) || (iss >> extra)) {
        return false;
    }
    return version == "HTTP/1.1" || version == "HTTP/1.0";
}

bool PrometheusPullExporter::WriteAll(int fd, const std::string &data)
{
    size_t written = 0;
    while (written < data.size()) {
        const auto ret = send(fd, data.data() + written, data.size() - written, 0);
        if (ret <= 0) {
            return false;
        }
        written += static_cast<size_t>(ret);
    }
    return true;
}

bool PrometheusPullExporter::WriteAll(SSL *ssl, const std::string &data)
{
    size_t written = 0;
    while (written < data.size()) {
        const int ret = SSL_write(ssl, data.data() + written, static_cast<int>(data.size() - written));
        if (ret <= 0) {
            return false;
        }
        written += static_cast<size_t>(ret);
    }
    return true;
}

std::string PrometheusPullExporter::ReadRequest(int fd)
{
    std::string request;
    char buffer[BUFFER_SIZE] = {0};
    while (request.size() < MAX_REQUEST_SIZE) {
        const auto ret = recv(fd, buffer, sizeof(buffer), 0);
        if (ret <= 0) {
            break;
        }
        request.append(buffer, static_cast<size_t>(ret));
        if (request.find("\r\n\r\n") != std::string::npos) {
            break;
        }
    }
    return request;
}

std::string PrometheusPullExporter::ReadRequest(SSL *ssl)
{
    std::string request;
    char buffer[BUFFER_SIZE] = {0};
    while (request.size() < MAX_REQUEST_SIZE) {
        const int ret = SSL_read(ssl, buffer, static_cast<int>(sizeof(buffer)));
        if (ret <= 0) {
            break;
        }
        request.append(buffer, static_cast<size_t>(ret));
        if (request.find("\r\n\r\n") != std::string::npos) {
            break;
        }
    }
    return request;
}

}  // namespace observability::exporters::metrics
