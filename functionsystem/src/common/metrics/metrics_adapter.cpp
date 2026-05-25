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

#include "metrics_adapter.h"

#include <chrono>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/logs/logging.h"
#include "common/metadata/metadata.h"
#include "common/resource_view/resource_type.h"
#include "common/resource_view/resource_tool.h"
#include "common/status/status.h"
#include "metrics/api/metric_data.h"
#include "metrics/api/null.h"
#include "metrics/api/provider.h"
#include "metrics/plugin/dynamic_library_handle_unix.h"
#include "metrics/sdk/batch_export_processor.h"
#include "metrics/sdk/immediately_export_processor.h"
#include "metrics/sdk/meter_provider.h"
#include "metrics_constants.h"
#include "metrics_context.h"
#include "metrics_utils.h"
#include "utils/os_utils.hpp"

namespace functionsystem {
namespace metrics {
namespace MetricsExporters = observability::exporters::metrics;
namespace MetricsPlugin = observability::plugin::metrics;

const std::unordered_set<std::string> SYSTEM_FUNCTION_NAME = { "0-system-faasscheduler", "0-system-faasfrontend",
                                                               "0-system-faascontroller", "0-system-faasmanager" };
const std::string IMMEDIATELY_EXPORT = "immediatelyExport";
const std::string BATCH_EXPORT = "batchExport";
const std::unordered_set<std::string> SENSITIVE_CONFIG_KEYS = {
    "rootCertData", "certData", "keyData", "passphrase"
};

static nlohmann::json RedactSensitiveMetricConfig(nlohmann::json config)
{
    if (!config.is_object()) {
        return config;
    }
    for (auto &[key, value] : config.items()) {
        if (SENSITIVE_CONFIG_KEYS.find(key) != SENSITIVE_CONFIG_KEYS.end()) {
            value = "***";
        } else if (value.is_object()) {
            value = RedactSensitiveMetricConfig(value);
        }
    }
    return config;
}

static std::string GetLibraryPath(const std::string &exporterType)
{
    char path[1024];
    std::string filePath;
    ssize_t len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if (len != -1) {
        path[len] = '\0';
        auto directoryPath = path;
        auto fileName = strrchr(path, '/');
        if (fileName) {
            *fileName = '\0';
        }
        std::string libPath = std::string(directoryPath) + "/../lib/";
        char realLibPath[PATH_MAX] = { 0 };
        if (realpath(libPath.c_str(), realLibPath) == nullptr) {
            YRLOG_WARN("failed to get real path of library {}, errno: {}, {}", libPath, errno,
                       litebus::os::Strerror(errno));
            return "";
        }
        if (exporterType == FILE_EXPORTER) {
            filePath = std::string(realLibPath) + "/libobservability-metrics-file-exporter.so";
        } else if (exporterType == PROMETHEUS_PULL_EXPORTER) {
            filePath = std::string(realLibPath) + "/libobservability-prometheus-pull-exporter.so";
        } else if (exporterType == PROMETHEUS_PUSH_EXPORTER) {
            filePath = std::string(realLibPath) + "/libobservability-prometheus-push-exporter.so";
        } else if (exporterType == OPENTELEMETRY_EXPORTER) {
            filePath = std::string(realLibPath) + "/libobservability-metrics-opentelemetry-exporter.so";
        }
        YRLOG_INFO("exporter {} get library path: {}", exporterType, filePath);
    }
    return filePath;
}

const MetricsSdk::ExportConfigs MetricsAdapter::BuildExportConfigs(const nlohmann::json &exporterValue)
{
    try {
        YRLOG_DEBUG("Start to build export config {}", exporterValue.dump());
    } catch (std::exception &e) {
        YRLOG_ERROR("dump exporterValue failed, error: {}", e.what());
    }
    MetricsSdk::ExportConfigs exportConfigs;
    if (exporterValue.contains("batchSize")) {
        exportConfigs.batchSize = exporterValue.at("batchSize");
    }
    if (exporterValue.contains("batchIntervalSec")) {
        exportConfigs.batchIntervalSec = exporterValue.at("batchIntervalSec");
    }
    if (exporterValue.contains("failureQueueMaxSize")) {
        exportConfigs.failureQueueMaxSize = exporterValue.at("failureQueueMaxSize");
    }
    if (exporterValue.contains("failureDataDir")) {
        exportConfigs.failureDataDir = exporterValue.at("failureDataDir");
    }
    if (exporterValue.contains("failureDataFileMaxCapacity")) {
        exportConfigs.failureDataFileMaxCapacity = exporterValue.at("failureDataFileMaxCapacity");
    }
    if (exporterValue.contains("enabledInstruments")) {
        for (auto &it : exporterValue.at("enabledInstruments").items()) {
            YRLOG_INFO("Enabled instrument: {}", it.value());
            exportConfigs.enabledInstruments.insert(it.value());
            enabledInstruments_.insert(GetInstrumentEnum(it.value()));
        }
    }
    return exportConfigs;
}

std::shared_ptr<MetricsExporters::Exporter> MetricsAdapter::InitFileExporter(
    const std::string &backendKey, const std::string &backendName, const nlohmann::json &exporterValue,
    const std::function<std::string(std::string)> &getFileName)
{
    YRLOG_DEBUG("add exporter {} for backend {} of {}", FILE_EXPORTER, backendKey, backendName);
    if (exporterValue.find("enable") == exporterValue.end() || !exporterValue.at("enable").get<bool>()) {
        YRLOG_DEBUG("metrics exporter {} for backend {} of {} is not enabled", FILE_EXPORTER, backendKey, backendName);
        return nullptr;
    }
    std::string initConfig;
    if (exporterValue.find("initConfig") != exporterValue.end()) {
        auto initConfigJson = exporterValue.at("initConfig");
        if (initConfigJson.find("fileDir") == initConfigJson.end() ||
            initConfigJson.at("fileDir").get<std::string>().empty()) {
            YRLOG_DEBUG("not find the metrics exporter file path, use the log path: {}", GetContextValue("log_dir"));
            initConfigJson["fileDir"] = GetContextValue("log_dir");
        }
        if (!litebus::os::ExistPath(initConfigJson.at("fileDir")) &&
            litebus::os::Mkdir(initConfigJson.at("fileDir")).IsSome()) {
            YRLOG_ERROR("failed to mkdir({}) for exporter {} for backend {} of {}, msg: {}",
                        initConfigJson.at("fileDir"), FILE_EXPORTER, backendKey, backendName,
                        litebus::os::Strerror(errno));
            return nullptr;
        }
        if (initConfigJson.find("fileName") == initConfigJson.end() ||
            initConfigJson.at("fileName").get<std::string>().empty()) {
            initConfigJson["fileName"] = getFileName(backendName);
        }
        try {
            initConfig = initConfigJson.dump();
        } catch (std::exception &e) {
            YRLOG_ERROR("dump initConfigJson failed, error: {}", e.what());
        }
    }
    YRLOG_INFO("metrics exporter {} for backend {} of {}, init config: {}", FILE_EXPORTER, backendKey, backendName,
               initConfig);
    std::string error;
    return MetricsPlugin::LoadExporterFromLibrary(GetLibraryPath(FILE_EXPORTER), initConfig, error);
}

std::shared_ptr<MetricsExporters::Exporter> MetricsAdapter::InitHttpExporter(const std::string &httpExporterType,
    const std::string &backendName, const nlohmann::json &exporterValue, const SSLCertConfig &sslCertConfig)
{
    YRLOG_DEBUG("add exporter {} for backend {}", httpExporterType, backendName);
    if (exporterValue.find("enable") == exporterValue.end() || !exporterValue.at("enable").get<bool>()) {
        YRLOG_DEBUG("metrics exporter {} for backend {} is not enabled", httpExporterType, backendName);
        return nullptr;
    }
    std::string initConfig;
    if (exporterValue.find("initConfig") != exporterValue.end()) {
        auto initConfigJson = exporterValue.at("initConfig");
        initConfigJson["jobName"] = metricsContext_.GetAttr("component_name");
        if (metricsContext_.GetAttr("component_name") == "function_agent") {
            YRLOG_INFO("begin to refresh jobName, agent_id is {}", metricsContext_.GetAttr("agent_id"));
            initConfigJson["jobName"] = metricsContext_.GetAttr("agent_id"); // distinguish muti agents in the same node
        }
        std::string ip;
        // default use local address
        if (auto opt = litebus::os::GetEnv("IP_ADDRESS"); opt.IsSome()) {
            ip = opt.Get();
        }
        if (initConfigJson.find("ip") != initConfigJson.end()) {
            ip = initConfigJson.at("ip").get<std::string>();
        }
        if (!ip.empty() && initConfigJson.find("port") != initConfigJson.end()) {
            initConfigJson["endpoint"] =
                ip + ":" + std::to_string(initConfigJson.at("port").get<int>());
        }

        try {
            YRLOG_INFO("metrics http exporter for backend {}, initConfig: {}", backendName,
                       RedactSensitiveMetricConfig(initConfigJson).dump());
        } catch (std::exception &e) {
            YRLOG_ERROR("dump initConfigJson failed, error: {}", e.what());
        }

        if (sslCertConfig.isMetricsSSLEnable) {
            initConfigJson["isSSLEnable"] = true;
            initConfigJson["rootCertFile"] = sslCertConfig.rootCertFile;
            initConfigJson["certFile"] = sslCertConfig.certFile;
            initConfigJson["keyFile"] = sslCertConfig.keyFile;
        }
        try {
            initConfig = initConfigJson.dump();
        } catch (std::exception &e) {
            YRLOG_ERROR("dump initConfigJson failed, error: {}", e.what());
        }
    }
    std::string error;
    return MetricsPlugin::LoadExporterFromLibrary(GetLibraryPath(httpExporterType), initConfig, error);
}

std::shared_ptr<MetricsExporters::Exporter> MetricsAdapter::InitOtelExporter(
    const std::string &backendName, const nlohmann::json &exporterValue)
{
    YRLOG_DEBUG("add exporter {} for backend {}", OPENTELEMETRY_EXPORTER, backendName);
    if (exporterValue.find("enable") == exporterValue.end() || !exporterValue.at("enable").get<bool>()) {
        YRLOG_DEBUG("metrics exporter {} for backend {} is not enabled", OPENTELEMETRY_EXPORTER, backendName);
        return nullptr;
    }
    std::string initConfig;
    if (exporterValue.find("initConfig") != exporterValue.end()) {
        auto initConfigJson = exporterValue.at("initConfig");
        // Set default endpoint if not provided
        if (initConfigJson.find("endpoint") == initConfigJson.end()) {
            initConfigJson["endpoint"] = "http://localhost:4318/v1/metrics";
        }

        // Set default protocol if not provided
        if (initConfigJson.find("protocol") == initConfigJson.end()) {
            initConfigJson["protocol"] = "http";
        }

        // Set default timeout if not provided
        if (initConfigJson.find("timeout") == initConfigJson.end()) {
            constexpr int kDefaultTimeoutMs = 10000;  // 10 seconds
            initConfigJson["timeout"] = kDefaultTimeoutMs;
        }

        // Add service.name attribute as header for OTLP
        if (initConfigJson.find("headers") == initConfigJson.end()) {
            initConfigJson["headers"] = nlohmann::json::object();
        }
        initConfigJson["headers"]["service.name"] = metricsContext_.GetAttr("component_name");

        try {
            YRLOG_INFO("metrics opentelemetry exporter for backend {}, initConfig: {}",
                       backendName, initConfigJson.dump());
        } catch (std::exception &e) {
            YRLOG_ERROR("dump initConfigJson failed, error: {}", e.what());
        }

        try {
            initConfig = initConfigJson.dump();
        } catch (std::exception &e) {
            YRLOG_ERROR("dump initConfigJson failed, error: {}", e.what());
        }
    }
    std::string error;
    return MetricsPlugin::LoadExporterFromLibrary(GetLibraryPath(OPENTELEMETRY_EXPORTER), initConfig, error);
}

void MetricsAdapter::SetImmediatelyExporters(const std::shared_ptr<observability::sdk::metrics::MeterProvider> &mp,
                                             const std::string &backendName, const nlohmann::json &exporters,
                                             const std::function<std::string(std::string)> &getFileName,
                                             const SSLCertConfig &sslCertConfig)
{
    RETURN_IF_NULL(mp);
    std::string exportModeDesc = IMMEDIATELY_EXPORT;
    for (auto &[key, value] : exporters.items()) {
        if (key == FILE_EXPORTER) {
            auto &&exporter = InitFileExporter(exportModeDesc, backendName, value, getFileName);
            if (exporter == nullptr) {
                continue;
            }
            auto exportConfigs = BuildExportConfigs(value);
            exportConfigs.exporterName = metricsContext_.GetAttr("component_name") + "_" + key;
            exportConfigs.exportMode = MetricsSdk::ExportMode::IMMEDIATELY;
            auto processor =
                    std::make_shared<MetricsSdk::ImmediatelyExportProcessor>(std::move(exporter), exportConfigs);
            mp->AddMetricProcessor(std::move(processor));
        } else if (key == PROMETHEUS_PUSH_EXPORTER || key == PROMETHEUS_PULL_EXPORTER) {
            auto &&exporter = InitHttpExporter(key, backendName, value, sslCertConfig);
            if (exporter == nullptr) {
                continue;
            }
            auto exportConfigs = BuildExportConfigs(value);
            exportConfigs.exporterName = metricsContext_.GetAttr("component_name") + "_" + key;
            exportConfigs.exportMode = MetricsSdk::ExportMode::IMMEDIATELY;
            auto processor =
                std::make_shared<MetricsSdk::ImmediatelyExportProcessor>(std::move(exporter), exportConfigs);
            mp->AddMetricProcessor(std::move(processor));
        } else if (key == OPENTELEMETRY_EXPORTER) {
            auto &&exporter = InitOtelExporter(backendName, value);
            if (exporter == nullptr) {
                continue;
            }
            auto exportConfigs = BuildExportConfigs(value);
            exportConfigs.exporterName = metricsContext_.GetAttr("component_name") + "_" + key;
            exportConfigs.exportMode = MetricsSdk::ExportMode::IMMEDIATELY;
            auto processor =
                std::make_shared<MetricsSdk::ImmediatelyExportProcessor>(std::move(exporter), exportConfigs);
            mp->AddMetricProcessor(std::move(processor));
        } else {
            YRLOG_WARN("unknown exporter name: {}", key);
        }
    }
}

void MetricsAdapter::SetBatchExporters(const std::shared_ptr<observability::sdk::metrics::MeterProvider> &mp,
                                       const std::string &backendName, const nlohmann::json &exporters,
                                       const std::function<std::string(std::string)> &getFileName,
                                       const SSLCertConfig &sslCertConfig)
{
    RETURN_IF_NULL(mp);
    std::string exportModeDesc = BATCH_EXPORT;
    for (auto &[key, value] : exporters.items()) {
        if (key == FILE_EXPORTER) {
            auto &&exporter = InitFileExporter(exportModeDesc, backendName, value, getFileName);
            if (exporter == nullptr) {
                YRLOG_ERROR("Failed to init exporter {}", key);
                continue;
            }
            auto exportConfigs = BuildExportConfigs(value);
            exportConfigs.exporterName = metricsContext_.GetAttr("component_name") + "_" + key;
            exportConfigs.exportMode = MetricsSdk::ExportMode::BATCH;
            auto processor = std::make_shared<MetricsSdk::BatchExportProcessor>(std::move(exporter), exportConfigs);
            mp->AddMetricProcessor(std::move(processor));
        } else if (key == PROMETHEUS_PUSH_EXPORTER) {
            auto &&exporter = InitHttpExporter(key, backendName, value, sslCertConfig);
            if (exporter == nullptr) {
                YRLOG_ERROR("Failed to init exporter {}", key);
                continue;
            }
            auto exportConfigs = BuildExportConfigs(value);
            exportConfigs.exporterName = metricsContext_.GetAttr("component_name") + "_" + key;
            exportConfigs.exportMode = MetricsSdk::ExportMode::BATCH;
            auto processor = std::make_shared<MetricsSdk::BatchExportProcessor>(std::move(exporter), exportConfigs);
            mp->AddMetricProcessor(std::move(processor));
        } else if (key == OPENTELEMETRY_EXPORTER) {
            auto &&exporter = InitOtelExporter(backendName, value);
            if (exporter == nullptr) {
                YRLOG_ERROR("Failed to init exporter {}", key);
                continue;
            }
            auto exportConfigs = BuildExportConfigs(value);
            exportConfigs.exporterName = metricsContext_.GetAttr("component_name") + "_" + key;
            exportConfigs.exportMode = MetricsSdk::ExportMode::BATCH;
            auto processor = std::make_shared<MetricsSdk::BatchExportProcessor>(std::move(exporter), exportConfigs);
            mp->AddMetricProcessor(std::move(processor));
        } else {
            YRLOG_WARN("unknown exporter name: {}", key);
        }
    }
}

void MetricsAdapter::InitExport(const MetricsSdk::ExportMode &exportMode,
                                const std::shared_ptr<MetricsSdk::MeterProvider> &mp,
                                const nlohmann::json &backendValue,
                                const std::function<std::string(std::string)> &getFileName,
                                const SSLCertConfig &sslCertConfig)
{
    std::string exportModeDesc = GetExportModeDesc(exportMode);
    YRLOG_DEBUG("metrics add backend {}", exportModeDesc);
    if (backendValue.find("enable") == backendValue.end() || !backendValue.at("enable").get<bool>()) {
        YRLOG_DEBUG("metrics backend {} is not enabled", exportModeDesc);
        return;
    }
    std::string backendName;
    if (backendValue.find("name") != backendValue.end()) {
        backendName = backendValue.at("name");
        YRLOG_DEBUG("metrics add backend {} of {}", exportModeDesc, backendName);
        enabledBackends_.insert(backendName);
    }
    if (backendValue.find("custom") != backendValue.end()) {
        auto custom = backendValue.at("custom");
        if (custom.find("labels") != custom.end()) {
            auto labels = custom.at("labels");
            for (auto &label : labels.items()) {
                YRLOG_DEBUG("metrics backend {} of {} add custom label, key: {}, value: {}", exportModeDesc,
                            backendName, label.key(), label.value());
                metricsContext_.SetAttr(label.key(), label.value());
            }
        }
    }
    if (backendValue.find("exporters") != backendValue.end()) {
        for (auto &[index, exporters] : backendValue.at("exporters").items()) {
            YRLOG_DEBUG("metrics add exporter index({}) for backend {}", index, backendName);
            if (exportMode == MetricsSdk::ExportMode::IMMEDIATELY) {
                SetImmediatelyExporters(mp, backendName, exporters, getFileName, sslCertConfig);
            } else if (exportMode == MetricsSdk::ExportMode::BATCH) {
                SetBatchExporters(mp, backendName, exporters, getFileName, sslCertConfig);
            } else {
                YRLOG_ERROR("Unknown exporter index({}) for backend {}", index, backendName);
            }
        }
    }
}

MetricsAdapter::~MetricsAdapter() noexcept
{
}

void MetricsAdapter::CleanMetrics() noexcept
{
    std::shared_ptr<MetricsApi::NullMeterProvider> null = nullptr;
    MetricsApi::Provider::SetMeterProvider(null);
}

void MetricsAdapter::InitMetricsFromJson(const nlohmann::json &json,
                                         const std::function<std::string(std::string)> &getFileName,
                                         const SSLCertConfig &sslCertConfig)
{
    if (json.find("backends") == json.end()) {
        YRLOG_DEBUG("metrics backends is none");
        return;
    }
    if (json.find("enabledMetrics") != json.end()) {
        for (auto &it : json.at("enabledMetrics").items()) {
            YRLOG_INFO("Enabled instrument: {}", it.value());
            enabledInstruments_.insert(GetInstrumentEnum(it.value()));
        }
    }
    auto mp = std::make_shared<MetricsSdk::MeterProvider>();

    auto backends = json.at("backends");
    for (auto &[index, backend] : backends.items()) {
        YRLOG_DEBUG("metrics add backend index({})", index);
        for (auto &[key, value] : backend.items()) {
            if (key == IMMEDIATELY_EXPORT) {
                InitExport(MetricsSdk::ExportMode::IMMEDIATELY, mp, value, getFileName, sslCertConfig);
            } else if (key == BATCH_EXPORT) {
                InitExport(MetricsSdk::ExportMode::BATCH, mp, value, getFileName, sslCertConfig);
            } else {
                YRLOG_WARN("unknown backend key: {}", key);
            }
        }
    }
    metricsContext_.SetEnabledInstruments(enabledInstruments_);

    MetricsApi::Provider::SetMeterProvider(mp);
}

void MetricsAdapter::InitAlarmGauge()
{
    if (alarmGauge_ != nullptr) {
        return;
    }
    if (enabledBackends_.find("CAAS_Alarm") == enabledBackends_.end()) {
        return;
    }
    auto provider = MetricsApi::Provider::GetMeterProvider();
    if (provider == nullptr) {
        return;
    }
    std::shared_ptr<MetricsApi::Meter> meter = provider->GetMeter("alarm_meter");
    if (meter == nullptr) {
        return;
    }
    alarmGauge_ = meter->CreateUInt64Gauge("alarm_meter_gauge");
}

void MetricsAdapter::AlarmGaugeLabelsAddContextAttr(MetricsApi::MetricLabels &labels)
{
    labels.emplace_back(std::make_pair<std::string, std::string>("site", metricsContext_.GetAttr("site")));
    labels.emplace_back(std::make_pair<std::string, std::string>("tenant_id", metricsContext_.GetAttr("tenant_id")));
    labels.emplace_back(
        std::make_pair<std::string, std::string>("application_id", metricsContext_.GetAttr("application_id")));
    labels.emplace_back(std::make_pair<std::string, std::string>("service_id", metricsContext_.GetAttr("service_id")));
}

void MetricsAdapter::AlarmGaugeLabelsAddBaseAttr(const std::string &id, const std::string &name,
                                                 const std::string &level, MetricsApi::MetricLabels &labels)
{
    labels.emplace_back(std::pair{ "id", id });
    labels.emplace_back(std::pair{ "name", name });
    labels.emplace_back(std::pair{ "level", level });
}

void MetricsAdapter::AlarmGaugeLabelsAddTimeStamp(const std::string &start, const std::string &end,
                                                  MetricsApi::MetricLabels &labels)
{
    labels.emplace_back(std::pair{ "start_timestamp", start });
    labels.emplace_back(std::pair{ "end_timestamp", end });
}

void MetricsAdapter::AddAlarmCommonAttrs(MetricsApi::AlarmInfo &alarmInfo)
{
    alarmInfo.customOptions["site"] = metricsContext_.GetAttr("site");
    alarmInfo.customOptions["application_id"] = metricsContext_.GetAttr("application_id");
    alarmInfo.customOptions["service_id"] = metricsContext_.GetAttr("service_id");
    alarmInfo.customOptions["tenant_id"] = metricsContext_.GetAttr("tenant_id");
    alarmInfo.customOptions["clear_type"] = "ADAC";
}

void MetricsAdapter::HandleElectionAlarm(bool isFiring, const std::string &msg)
{
    if (enabledInstruments_.find(YRInstrument::YR_ELECTION_ALARM) == enabledInstruments_.end()) {
        YRLOG_DEBUG("election alarm is not enabled");
        return;
    }
    MetricsApi::AlarmInfo electionAlarmInfo;
    AddAlarmCommonAttrs(electionAlarmInfo);
    electionAlarmInfo.id = ContainStr(msg, "/yr/leader/function-master") ?
        "YuanrongMasterElection00001" : "YuanrongIamElection00001";
    electionAlarmInfo.alarmName = ELECTION_ALARM;
    electionAlarmInfo.alarmSeverity = MetricsApi::AlarmSeverity::MAJOR;
    electionAlarmInfo.customOptions["source_tag"] = GetSourceTag() + "YuanrongElectionAlarm";
    electionAlarmInfo.cause = msg;
    if (!isFiring) {
        if (!metricsContext_.IsAlarmExisted(electionAlarmInfo.id)) {
            return;
        }
        electionAlarmInfo.startsAt = 0;
        electionAlarmInfo.endsAt = GetCurrentTimeInMilliSec();
        electionAlarmInfo.customOptions["op_type"] = "resolved";
        metricsContext_.EraseAlarm(electionAlarmInfo.id);
    } else {
        electionAlarmInfo.startsAt = GetCurrentTimeInMilliSec();
        electionAlarmInfo.endsAt = 0;
        nlohmann::json annotations;
        annotations["err_msg"] = msg;
        electionAlarmInfo.customOptions["annotations"] = annotations.dump();
        electionAlarmInfo.customOptions["op_type"] = "firing";
        metricsContext_.SetAlarm(electionAlarmInfo.id, electionAlarmInfo);
    }
    alarmHandler_.SendElectionAlarm(electionAlarmInfo);
}

void MetricsAdapter::ElectionFiring(const std::string &msg)
{
    HandleElectionAlarm(true, msg);
}

void MetricsAdapter::ElectionFiringResolved(const std::string &msg)
{
    HandleElectionAlarm(false, msg);
}

bool MetricsAdapter::HasMetastoreAlarmError()
{
    if (enabledInstruments_.find(YRInstrument::YR_METASTORE_ALARM) == enabledInstruments_.end()) {
        YRLOG_DEBUG("Metastore alarm is not enabled");
        return true;
    }
    if (metricsContext_.GetAttr("component_name") != "function_proxy") {
        YRLOG_DEBUG("current component({}) is not function proxy, do not send metastore alarm",
                    metricsContext_.GetAttr("component_name"));
        return true;
    }
    return false;
}

bool MetricsAdapter::HasEtcdAlarmError()
{
    if (enabledInstruments_.find(YRInstrument::YR_ETCD_ALARM) == enabledInstruments_.end()) {
        YRLOG_DEBUG("Etcd alarm is not enabled");
        return true;
    }
    if (metricsContext_.GetAttr("component_name") != "function_master") {
        YRLOG_DEBUG("current component({}) is not function master, do not send etcd alarm",
                    metricsContext_.GetAttr("component_name"));
        return true;
    }
    return false;
}

void MetricsAdapter::HandleBackendStorageAlarm(bool isFiring, const AlarmLevel level, const std::string &msg,
                                               const std::string &backend)
{
    YRLOG_DEBUG("storage backend is {}", backend);
    if (backend == METASTORE_BACKEND ? HasMetastoreAlarmError() : HasEtcdAlarmError()) {
        return;
    }

    MetricsApi::AlarmInfo alarmInfo;
    AddAlarmCommonAttrs(alarmInfo);
    alarmInfo.id = "Yuanrong" + backend + "Connection00001";
    alarmInfo.alarmName = backend == METASTORE_BACKEND ? METASTORE_ALARM : ETCD_ALARM;
    alarmInfo.alarmSeverity = alarmHandler_.GetAlarmLevel(level);
    alarmInfo.customOptions["source_tag"] = GetSourceTag() + "Yuanrong" + backend + "Connection";
    alarmInfo.cause = msg;
    if (!isFiring) {
        if (!metricsContext_.IsAlarmExisted(alarmInfo.id)) {
            YRLOG_DEBUG("No alarm({}) sent before, ignore resolve firing", alarmInfo.id);
            return;
        }
        alarmInfo.startsAt = 0;
        alarmInfo.endsAt = GetCurrentTimeInMilliSec();
        alarmInfo.customOptions["op_type"] = "resolved";
        metricsContext_.EraseAlarm(alarmInfo.id);
    } else {
        alarmInfo.startsAt = GetCurrentTimeInMilliSec();
        alarmInfo.endsAt = 0;
        nlohmann::json annotations;
        annotations["err_msg"] = msg;
        alarmInfo.customOptions["annotations"] = annotations.dump();
        alarmInfo.customOptions["op_type"] = "firing";
        metricsContext_.SetAlarm(alarmInfo.id, alarmInfo);
    }
    alarmHandler_.SendBackendStorageAlarm(alarmInfo, backend);
}

void MetricsAdapter::StorageBackendUnhealthyFiring(const AlarmLevel level, const std::string &errMsg,
    const std::string &backend)
{
    HandleBackendStorageAlarm(true, level, "failed to connect to " + backend + ", " + errMsg, backend);
}

void MetricsAdapter::StorageBackendUnhealthyResolved(const AlarmLevel level, const std::string &backend)
{
    YRLOG_DEBUG("Resolve {} alarm", backend);
    HandleBackendStorageAlarm(false, level, "connect to " + backend + " successfully; resolve alarm.", backend);
}

void MetricsAdapter::StsUnhealthyFiring(const std::string &errMsg)
{
    InitAlarmGauge();
    if (alarmGauge_ == nullptr) {
        return;
    }
    MetricsApi::MetricLabels labels;
    AlarmGaugeLabelsAddContextAttr(labels);
    AlarmGaugeLabelsAddBaseAttr("InitStsSdkErr00001", "InitStsSdkErr", "major", labels);

    labels.emplace_back(std::make_pair<std::string, std::string>("source_tag", GetSourceTag() + "|InitStsSdkErr"));
    labels.emplace_back(std::make_pair<std::string, std::string>("op_type", "firing"));
    labels.emplace_back(std::make_pair<std::string, std::string>("details", "Init sts err: " + errMsg));
    labels.emplace_back(std::make_pair<std::string, std::string>("clear_type", "ADAC"));
    AlarmGaugeLabelsAddTimeStamp(GetSystemTimeStampNowStr(), "0", labels);

    alarmGauge_->Set(1, labels);
}

std::string MetricsAdapter::GetSourceTag() const
{
    std::string sourceTag;
    auto podName = litebus::os::GetEnv("POD_NAME");
    if (podName.IsSome()) {
        sourceTag = sourceTag + podName.Get() + "|";
    } else {
        YRLOG_WARN("env POD_NAME is empty, source tag may be incomplete");
    }
    auto podIP = litebus::os::GetEnv("POD_IP");
    if (podIP.IsSome()) {
        sourceTag = sourceTag + podIP.Get() + "|";
    } else {
        YRLOG_WARN("env POD_IP is empty, source tag may be incomplete");
    }
    auto clusterName = litebus::os::GetEnv("CLUSTER_NAME");
    if (clusterName.IsSome()) {
        sourceTag = sourceTag + clusterName.Get();
    } else {
        YRLOG_WARN("env CLUSTER_NAME is empty, source tag may be incomplete");
    }
    return sourceTag;
}

void MetricsAdapter::SendK8sAlarm(const std::string &locationInfo)
{
    if (enabledInstruments_.find(YRInstrument::YR_K8S_ALARM) == enabledInstruments_.end()) {
        YRLOG_DEBUG("k8s alarm is not enabled");
        return;
    }
    if (metricsContext_.GetAttr("component_name") == "function_master") {
        YRLOG_DEBUG("{} send k8s alarm", metricsContext_.GetAttr("component_name"));
        alarmHandler_.SendK8sAlarm(locationInfo);
    }
}

void MetricsAdapter::SendSchedulerAlarm(const std::string &locationInfo)
{
    if (enabledInstruments_.find(YRInstrument::YR_SCHEDULER_ALARM) == enabledInstruments_.end()) {
        YRLOG_DEBUG("scheduler alarm is not enabled");
        return;
    }
    if (metricsContext_.GetAttr("component_name") == "function_master") {
        YRLOG_DEBUG("{} sends scheduler alarm", metricsContext_.GetAttr("component_name"));
        alarmHandler_.SendSchedulerAlarm(locationInfo);
    }
}

void MetricsAdapter::SendTokenRotationFailureAlarm()
{
    if (enabledInstruments_.find(YRInstrument::YR_TOKEN_ROTATION_FAILURE_ALARM) == enabledInstruments_.end()) {
        YRLOG_DEBUG("oidc token alarm is not enabled");
        return;
    }
    alarmHandler_.SendTokenRotationFailureAlarm();
}

void MetricsAdapter::SendS3Alarm()
{
    if (enabledInstruments_.find(YRInstrument::YR_S3_ALARM) == enabledInstruments_.end()) {
        YRLOG_DEBUG("s3 alarm is not enabled");
        return;
    }
    alarmHandler_.SendS3Alarm();
}

void MetricsAdapter::SendPodAlarm(const std::string &podName, const std::string &cause)
{
    if (enabledInstruments_.find(YRInstrument::YR_POD_ALARM) == enabledInstruments_.end()) {
        YRLOG_DEBUG("pod alarm is not enabled");
        return;
    }
    alarmHandler_.SendPodAlarm(podName, cause);
}

void MetricsAdapter::InitObservableCounter(const struct MeterTitle &title, int interval, MetricsApi::CallbackPtr &cb,
                                           observability::sdk::metrics::InstrumentValueType observableType)
{
    if (observableInstrumentMap_.find(title.name) != observableInstrumentMap_.end()) {
        YRLOG_DEBUG("{} ObservableCounter exists, type({})", title.name, static_cast<uint32_t>(observableType));
        return;
    }
    auto provider = MetricsApi::Provider::GetMeterProvider();
    if (provider == nullptr) {
        return;
    }
    std::shared_ptr<MetricsApi::Meter> meter = provider->GetMeter("observable_instrument_meter");
    if (meter == nullptr) {
        return;
    }

    std::shared_ptr<MetricsApi::ObservableInstrument> observableInstrument;
    switch (observableType) {
        case observability::sdk::metrics::InstrumentValueType::UINT64:
            observableInstrument =
                meter->CreateUint64ObservableCounter(title.name, title.description, title.unit, interval, cb);
            break;

        // not support yet
        case observability::sdk::metrics::InstrumentValueType::INT64:
        case observability::sdk::metrics::InstrumentValueType::DOUBLE:
        default:
            // observe uint64 for default
            observableInstrument =
                meter->CreateUint64ObservableCounter(title.name, title.description, title.unit, interval, cb);
    }

    observableInstrumentMap_[title.name] = std::move(observableInstrument);
    YRLOG_DEBUG("InitObservableCounter for {}", title.name);
}

void MetricsAdapter::InitObservableGauge(const struct MeterTitle &title, int interval, MetricsApi::CallbackPtr &cb,
                                         observability::sdk::metrics::InstrumentValueType observableType)
{
    if (observableInstrumentMap_.find(title.name) != observableInstrumentMap_.end()) {
        YRLOG_DEBUG("{} ObservableGauge exists, type({})", title.name, static_cast<uint32_t>(observableType));
        return;
    }
    auto provider = MetricsApi::Provider::GetMeterProvider();
    if (provider == nullptr) {
        return;
    }

    std::shared_ptr<MetricsApi::Meter> meter = provider->GetMeter("observable_instrument_meter");
    if (meter == nullptr) {
        return;
    }

    std::shared_ptr<MetricsApi::ObservableInstrument> observableInstrument;
    switch (observableType) {
        case observability::sdk::metrics::InstrumentValueType::DOUBLE:
            observableInstrument =
                meter->CreateDoubleObservableGauge(title.name, title.description, title.unit, interval, cb);
            break;

        // not support yet
        case observability::sdk::metrics::InstrumentValueType::UINT64:
        case observability::sdk::metrics::InstrumentValueType::INT64:
        default:
            // observe double for default
            observableInstrument =
                meter->CreateDoubleObservableGauge(title.name, title.description, title.unit, interval, cb);
    }

    observableInstrumentMap_[title.name] = std::move(observableInstrument);
    YRLOG_DEBUG("InitObservableGauge for {}", title.name);
}

void MetricsAdapter::InitDoubleGauge(const struct MeterTitle &title)
{
    if (doubleGaugeMap_.find(title.name) != doubleGaugeMap_.end()) {
        return;
    }
    auto provider = MetricsApi::Provider::GetMeterProvider();
    if (provider == nullptr) {
        return;
    }
    std::shared_ptr<MetricsApi::Meter> meter = provider->GetMeter("gauge_meter");
    if (meter == nullptr) {
        return;
    }
    auto gauge = meter->CreateDoubleGauge(title.name, title.description, title.unit);
    doubleGaugeMap_[title.name] = std::move(gauge);
}

void MetricsAdapter::ReportGauge(const struct MeterTitle &title, struct MeterData &data)
{
    std::list<std::string> contextAttrs = { "node_id", "ip" };
    ReportDoubleGauge(title, data, contextAttrs);
}

std::vector<std::string> MetricsAdapter::ConvertNodeLabels(const NodeLabelsType &nodeLabels)
{
    std::vector<std::string> poolLabels;
    for (auto &keyIt : nodeLabels) {
        auto key = keyIt.first;
        for (auto &valueIt : keyIt.second) {
            if (valueIt.second > 0) {
                poolLabels.emplace_back(key + ":" + valueIt.first);
            }
        }
    }
    return poolLabels;
}


void MetricsAdapter::RegisterPhysicalMetricsCounter()
{
    RegisterPhysicalNodeCPUUtilization();
    RegisterPhysicalNodeMemoryUsage();
    RegisterPhysicalNodeMemoryCapacity();
    RegisterPhysicalNodeDiskUsage();
    RegisterPhysicalNodeDiskCapacity();
    RegisterPhysicalNodeNpu();
}

void MetricsAdapter::RegisterPhysicalNodeCPUUtilization()
{
    if (enabledInstruments_.find(YRInstrument::YR_NODE_CPU_USAGE) == enabledInstruments_.end()) {
        YRLOG_DEBUG("yr node cpu usage is not enabled");
        return;
    }
    MeterTitle meterTitle{ GetInstrumentDesc(YRInstrument::YR_NODE_CPU_USAGE), "yr node cpu usage", "percent" };
    LabelType baseLabelMap = {};
    MetricsApi::CallbackPtr cb = std::bind(&MetricsAdapter::CollectGeneralMetricsNodeData, this, std::placeholders::_1,
                                           "CPU", true, baseLabelMap);
    InitObservableGauge(meterTitle, INSTANCE_RUNNING_DURATION_COLLECT_INTERVAL, cb,
                        observability::sdk::metrics::InstrumentValueType::DOUBLE);
}


void MetricsAdapter::RegisterPhysicalNodeMemoryUsage()
{
    if (enabledInstruments_.find(YRInstrument::YR_NODE_MEMORY_USAGE) == enabledInstruments_.end()) {
        YRLOG_DEBUG("yr node memory usage is not enabled");
        return;
    }
    MeterTitle meterTitle{ GetInstrumentDesc(YRInstrument::YR_NODE_MEMORY_USAGE), "yr node memory usage", "MB" };
    LabelType baseLabelMap = {};
    MetricsApi::CallbackPtr cb = std::bind(&MetricsAdapter::CollectGeneralMetricsNodeData, this, std::placeholders::_1,
                                           "Memory", true, baseLabelMap);
    InitObservableGauge(meterTitle, INSTANCE_RUNNING_DURATION_COLLECT_INTERVAL, cb,
                        observability::sdk::metrics::InstrumentValueType::DOUBLE);
}

void MetricsAdapter::RegisterPhysicalNodeMemoryCapacity()
{
    if (enabledInstruments_.find(YRInstrument::YR_NODE_MEMORY_CAPACITY) == enabledInstruments_.end()) {
        YRLOG_DEBUG("yr node memory capacity is not enabled");
        return;
    }
    MeterTitle meterTitle{ GetInstrumentDesc(YRInstrument::YR_NODE_MEMORY_CAPACITY), "yr node memory capacity", "MB" };
    LabelType baseLabelMap = {};
    MetricsApi::CallbackPtr cb = std::bind(&MetricsAdapter::CollectGeneralMetricsNodeData, this, std::placeholders::_1,
                                           "Memory", false, baseLabelMap);
    InitObservableGauge(meterTitle, INSTANCE_RUNNING_DURATION_COLLECT_INTERVAL, cb,
                        observability::sdk::metrics::InstrumentValueType::DOUBLE);
}

void MetricsAdapter::RegisterPhysicalNodeDiskUsage()
{
    if (enabledInstruments_.find(YRInstrument::YR_NODE_DISK_USAGE) == enabledInstruments_.end()) {
        YRLOG_DEBUG("yr node disk usage is not enabled");
        return;
    }
    MeterTitle meterTitle{ GetInstrumentDesc(YRInstrument::YR_NODE_DISK_USAGE), "yr node disk usage", "GB" };
    LabelType baseLabelMap = {};
    MetricsApi::CallbackPtr cb = std::bind(&MetricsAdapter::CollectGeneralMetricsNodeData, this, std::placeholders::_1,
                                           "disk", true, baseLabelMap);
    InitObservableGauge(meterTitle, INSTANCE_RUNNING_DURATION_COLLECT_INTERVAL, cb,
                        observability::sdk::metrics::InstrumentValueType::DOUBLE);
}

void MetricsAdapter::RegisterPhysicalNodeDiskCapacity()
{
    if (enabledInstruments_.find(YRInstrument::YR_NODE_DISK_CAPACITY) == enabledInstruments_.end()) {
        YRLOG_DEBUG("yr node disk usage is not enabled");
        return;
    }
    MeterTitle meterTitle{ GetInstrumentDesc(YRInstrument::YR_NODE_DISK_CAPACITY), "yr node disk capacity", "GB" };
    LabelType baseLabelMap = {};
    MetricsApi::CallbackPtr cb = std::bind(&MetricsAdapter::CollectGeneralMetricsNodeData, this, std::placeholders::_1,
                                           "disk", false, baseLabelMap);
    InitObservableGauge(meterTitle, INSTANCE_RUNNING_DURATION_COLLECT_INTERVAL, cb,
                        observability::sdk::metrics::InstrumentValueType::DOUBLE);
}

void MetricsAdapter::CollectGeneralMetricsNodeData(MetricsApi::ObserveResult obRes, const std::string &reourceName,
                                                    bool isUageMetrics, LabelType baseLabelMap)
{
    std::vector<std::pair<MetricsApi::MetricLabels, double>> vec;
    auto physicalMetrics = metricsContext_.GetPhysicalMetrics();
    YRLOG_DEBUG_COUNT_60("Collect CollectAgentGeneralMetricsData agentID is {}", physicalMetrics.agentID);
    if (!physicalMetrics.agentID.empty()) { // physicalMetrics has been set
        MetricsApi::MetricLabels labels;
        metrics::LabelType labelMap = baseLabelMap;
        labelMap["agent_id"] = physicalMetrics.agentID;
        labelMap["node_id"] = physicalMetrics.nodeID;

        double value = -1.0;
        if (isUageMetrics) {
            auto targetReource = physicalMetrics.unit.actualuse().resources().find(reourceName);
            if (targetReource != physicalMetrics.unit.actualuse().resources().end()) {
                value = targetReource->second.scalar().value();
            }
        } else { // limit metrics
            auto targetReource = physicalMetrics.unit.capacity().resources().find(reourceName);
            if (targetReource != physicalMetrics.unit.capacity().resources().end()) {
                value = targetReource->second.scalar().value();
            }
        }

        for (auto iter = labelMap.begin(); iter != labelMap.end(); ++iter) {
            labels.emplace_back(*iter);
        }
        vec.push_back(std::pair{ labels, value });
    }

    if (std::holds_alternative<std::shared_ptr<MetricsApi::ObserveResultT<double>>>(obRes)) {
        std::get<std::shared_ptr<MetricsApi::ObserveResultT<double>>>(obRes)->Observe(vec);
    }
}

void MetricsAdapter::RegisterPhysicalNodeNpu()
{
    if (enabledInstruments_.find(YRInstrument::YR_NODE_NPU) == enabledInstruments_.end()) {
        YRLOG_DEBUG("yr node npu metrics is not enabled");
        return;
    }

    MeterTitle meterTitle{ GetInstrumentDesc(YRInstrument::YR_NODE_NPU),
                           "yr node npu", "MB" };
    MetricsApi::CallbackPtr cb =
       std::bind(&MetricsAdapter::CollectPhysicalNodeNPUData, this, std::placeholders::_1);
    InitObservableGauge(meterTitle, INSTANCE_RUNNING_DURATION_COLLECT_INTERVAL, cb,
                          observability::sdk::metrics::InstrumentValueType::DOUBLE);
}

void MetricsAdapter::CollectPhysicalNodeNPUData(MetricsApi::ObserveResult obRes)
{
    std::vector<std::pair<MetricsApi::MetricLabels, double>> vec;
    auto physicalMetrics = metricsContext_.GetPhysicalMetrics();
    YRLOG_DEBUG("Collect PhysicalNPUData agentID is {}", physicalMetrics.agentID);
    nlohmann::json result = nlohmann::json::object();
    if (!physicalMetrics.agentID.empty()) {  // physicalMetrics has been set
        MetricsApi::MetricLabels labels;
        metrics::LabelType labelMap = { { "agent_id", physicalMetrics.agentID },
                                        { "node_id", physicalMetrics.nodeID } };
        double cardCnt = 0;
        for (auto &item : physicalMetrics.unit.actualuse().resources()) {
            if (item.first.length() < NPU_REOURCE_NAME_.length()
                || item.first.substr(0, NPU_REOURCE_NAME_.length())
                       != NPU_REOURCE_NAME_) {  // check if resourceName starts with "NPU"
                continue;
            }

            cardCnt = item.second.scalar().value();  // npu card cnt
            auto npuType =
                item.second.heterogeneousinfo().find(PRODUCT_MODEL_KEY_)->second;  // card type name like Ascend310
            auto npuVec = item.second.vectors().values();  // Capacity and usage of the HBM and memory
            google::protobuf::RepeatedField<double> capacities, usages, realIDs;

            ExtractValueFromVec(npuVec, HBM_KEY_, physicalMetrics.nodeID, capacities);
            ExtractValueFromVec(npuVec, USED_HBM_KEY_, physicalMetrics.nodeID, usages);
            if (capacities.empty() || capacities.empty()) {
                ExtractValueFromVec(npuVec, Memory_KEY_, physicalMetrics.nodeID, capacities);
                ExtractValueFromVec(npuVec, USED_MEMORY_KEY_, physicalMetrics.nodeID, usages);
            }
            ExtractValueFromVec(npuVec, IDS_KEY_, physicalMetrics.nodeID, realIDs);

            for (int i = 0;
                 realIDs.size() == capacities.size() && capacities.size() == usages.size() && i < capacities.size();
                 ++i) {
                nlohmann::json entry;
                entry["used"] = usages[i];
                entry["capacity"] = capacities[i];
                entry["type"] = npuType;
                result[std::to_string(static_cast<int>(realIDs.Get(i)))] = entry;
            }
        }
        YRLOG_DEBUG_COUNT_60("in collect npu info details is {}", result.dump());
        labelMap.emplace("details", result.dump());
        for (auto iter = labelMap.begin(); iter != labelMap.end(); ++iter) {
            labels.emplace_back(*iter);
        }
        vec.push_back(std::pair{ labels, cardCnt });
    }
    if (std::holds_alternative<std::shared_ptr<MetricsApi::ObserveResultT<double>>>(obRes)) {
        std::get<std::shared_ptr<MetricsApi::ObserveResultT<double>>>(obRes)->Observe(vec);
    }
}


void MetricsAdapter::RegisterInstanceMetrics()
{
    RegisterInstCPUUsage();
    RegisterInstMemoryUsage();
    RegisterInstNPUUsage();
}

void MetricsAdapter::RegisterInstCPUUsage()
{
    if (enabledInstruments_.find(YRInstrument::YR_INSTANCE_CPU_USAGE) == enabledInstruments_.end()) {
        YRLOG_DEBUG("YR_INSTANCE_CPU_USAGE is not enabled");
        return;
    }

    MeterTitle meterTitle{ "yr_instance_cpu_usage", "The percent of cpu used by the instance", "percent" };
    MetricsApi::CallbackPtr cb = std::bind(&MetricsAdapter::CollectInstanceMetrics, this, std::placeholders::_1, "CPU");
    InitObservableGauge(meterTitle, INSTANCE_METRICS_COLLECT_INTERVAL, cb,
                          observability::sdk::metrics::InstrumentValueType::DOUBLE);
}

void MetricsAdapter::RegisterInstMemoryUsage()
{
    if (enabledInstruments_.find(YRInstrument::YR_INSTANCE_MEMORY_USAGE) == enabledInstruments_.end()) {
        YRLOG_DEBUG("YR_INSTANCE_MEMORY_USAGE is not enabled");
        return;
    }

    MeterTitle meterTitle{ "yr_instance_memory_usage", "The amount of memory used by the instance", "KB" };
    MetricsApi::CallbackPtr cb =
        std::bind(&MetricsAdapter::CollectInstanceMetrics, this, std::placeholders::_1, "Memory");

    InitObservableGauge(meterTitle, INSTANCE_METRICS_COLLECT_INTERVAL, cb,
                        observability::sdk::metrics::InstrumentValueType::DOUBLE);
}

void MetricsAdapter::RegisterInstNPUUsage()
{
    if (enabledInstruments_.find(YRInstrument::YR_INSTANCE_NPU) == enabledInstruments_.end()) {
        YRLOG_DEBUG("YR_INSTANCE_NPU is not enabled");
        return;
    }
    MeterTitle meterTitle{ "yr_instance_npu", "The amount of npu used by the instance", "MB" };
    MetricsApi::CallbackPtr cb = std::bind(&MetricsAdapter::CollectInstanceMetrics, this, std::placeholders::_1, "NPU");

    InitObservableGauge(meterTitle, INSTANCE_METRICS_COLLECT_INTERVAL, cb,
                          observability::sdk::metrics::InstrumentValueType::DOUBLE);
}

void MetricsAdapter::CollectInstanceMetrics(MetricsApi::ObserveResult obRes, const std::string &reourceName)
{
    auto instanceMetricsMap = metricsContext_.GetInstanceMetricsMap();
    YRLOG_DEBUG_COUNT_60("collect instanceMetricsMap size: {}", instanceMetricsMap.size());
    std::vector<std::pair<MetricsApi::MetricLabels, double>> vec;

    for (auto [instanceId, instanceMetrics] : instanceMetricsMap) {
        auto timeStamp =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
        metrics::LabelType labelMap = { { "instance_id", instanceId },
                                        { "node_id", instanceMetrics.nodeId },
                                        { "timestamp", std::to_string(timeStamp) },
                                        { "agent_id", instanceMetrics.agentId }};

        MetricsApi::MetricLabels labels;
        double value = 0;
        auto resources = instanceMetrics.resources.resources();
        if (reourceName == NPU_REOURCE_NAME_) {
            for (auto &item : resources) {
                if (item.first.length() < NPU_REOURCE_NAME_.length()
                    || item.first.substr(0, NPU_REOURCE_NAME_.length())
                           != NPU_REOURCE_NAME_) {  // check if resourceName starts with "NPU"
                    continue;
                }
                value = item.second.scalar().value();        // npu card cnt
                auto details = GenerateInstCardDetails(item.second, instanceMetrics.nodeId);
                labelMap.emplace("details", details);
            }
        } else if (auto it = resources.find(reourceName); it != resources.end()) {
            value = it->second.scalar().value();
        }

        for (auto &iter : labelMap) {
            labels.emplace_back(iter);
        }
        vec.emplace_back(labels, value);
    }

    if (std::holds_alternative<std::shared_ptr<MetricsApi::ObserveResultT<double>>>(obRes)) {
        std::get<std::shared_ptr<MetricsApi::ObserveResultT<double>>>(obRes)->Observe(vec);
    }
}

void MetricsAdapter::ExtractValueFromVec(
    google::protobuf::Map<std::__cxx11::basic_string<char>, resources::Value_Vectors_Category> &vec,
    const std::string &key, std::string &nodeID, google::protobuf::RepeatedField<double> &value)
{
    if (vec.find(key) != vec.end()
        && vec.find(key)->second.vectors().find(nodeID) != vec.find(key)->second.vectors().end()) {
        value = vec.find(key)->second.vectors().find(nodeID)->second.values();
    }
}

std::string MetricsAdapter::GenerateInstCardDetails(const resources::Resource &resource, std::string &nodeID)
{
    if (resource.name().length() < NPU_REOURCE_NAME_.length()
        || resource.name().substr(0, NPU_REOURCE_NAME_.length())
               != NPU_REOURCE_NAME_) {  // check if resourceName starts with "NPU"
        return "";
    }

    auto npuVec = resource.vectors().values();  // Capacity and usage of the HBM and memory
    google::protobuf::RepeatedField<double> usages;
    google::protobuf::RepeatedField<double> realIDs;
    google::protobuf::RepeatedField<double> usedIDs;
    google::protobuf::RepeatedField<double> capacities;
    auto npuType = resource.heterogeneousinfo().find(PRODUCT_MODEL_KEY_)->second;  // card type name like Ascend310

    ExtractValueFromVec(npuVec, HBM_KEY_, nodeID, capacities);
    ExtractValueFromVec(npuVec, USED_HBM_KEY_, nodeID, usages);
    if (capacities.empty() || capacities.empty()) {
        ExtractValueFromVec(npuVec, Memory_KEY_, nodeID, capacities);
        ExtractValueFromVec(npuVec, USED_MEMORY_KEY_, nodeID, usages);
    }

    ExtractValueFromVec(npuVec, IDS_KEY_, nodeID, realIDs);
    ExtractValueFromVec(npuVec, USED_IDS_KEY_, nodeID, usedIDs);

    nlohmann::json result = nlohmann::json::object();
    for (int i = 0; i < usedIDs.size(); ++i) {
        auto usedID = static_cast<int>(usedIDs.Get(i));
        if (usedID < realIDs.size() && usedID < usages.size()) {
            nlohmann::json entry;
            entry["used"] = usages.Get(usedID);
            entry["capacity"] = capacities.Get(usedID);
            entry["type"] = npuType;
            result[std::to_string(static_cast<int>(realIDs.Get(usedID)))] = entry;
        } else {
            YRLOG_WARN("invalid usedID : {}", usedID);
        }
    }
    return result.dump();
}

void MetricsAdapter::RegisterBillingInstanceRunningDuration()
{
    if (enabledInstruments_.find(YRInstrument::YR_INSTANCE_RUNNING_DURATION) == enabledInstruments_.end()) {
        YRLOG_DEBUG("Billing Instance Running Duration is not enabled");
        return;
    }
    MeterTitle meterTitle{ GetInstrumentDesc(YRInstrument::YR_INSTANCE_RUNNING_DURATION),
                           "Billing Instance Running Duration", "milliseconds" };
    MetricsApi::CallbackPtr cb =
        std::bind(&MetricsAdapter::CollectBillingInstanceRunningDuration, this, std::placeholders::_1);
    InitObservableCounter(meterTitle, INSTANCE_RUNNING_DURATION_COLLECT_INTERVAL, cb,
                          observability::sdk::metrics::InstrumentValueType::UINT64);
}

void MetricsAdapter::CollectBillingInstanceRunningDuration(MetricsApi::ObserveResult obRes)
{
    std::vector<std::pair<MetricsApi::MetricLabels, uint64_t>> vec;
    YRLOG_DEBUG("Collect billing instance size: {}, extra instance size {}",
                metricsContext_.GetBillingInstanceMap().size(), metricsContext_.GetExtraBillingInstanceMap().size());
    for (auto i : metricsContext_.GetBillingInstanceMap()) {
        auto data = BuildBillingInstanceRunningDurationData(i.first, i.second, false);
        if (data.second > 0) {
            vec.push_back(data);
        }
    }
    for (auto i : metricsContext_.GetExtraBillingInstanceMap()) {
        auto data = BuildBillingInstanceRunningDurationData(i.first, i.second, true);
        if (data.second > 0) {
            vec.push_back(data);
        }
    }
    if (std::holds_alternative<std::shared_ptr<MetricsApi::ObserveResultT<uint64_t>>>(obRes)) {
        std::get<std::shared_ptr<MetricsApi::ObserveResultT<uint64_t>>>(obRes)->Observe(vec);
    }
}

std::pair<MetricsApi::MetricLabels, uint64_t> MetricsAdapter::BuildBillingInstanceRunningDurationData(
    const std::string &instanceID, const BillingInstanceInfo &billingInstanceInfo, bool extraBilling)
{
    if (billingInstanceInfo.isSystemFunc) {
        YRLOG_DEBUG("{} system function can't bill", instanceID);
        return std::pair{ MetricsApi::MetricLabels{}, 0 };
    }

    auto billingFunctionOption = metricsContext_.GetBillingFunctionOption(instanceID);
    nlohmann::json poolLabelsJson = ConvertNodeLabels(billingFunctionOption.nodeLabels);

    long long reportTimeMillis =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    if (billingInstanceInfo.endTimeMillis > 0) {
        reportTimeMillis = billingInstanceInfo.endTimeMillis;
    }
    metrics::LabelType labelMap;
    try {
        nlohmann::json requiredResourcesJson = billingInstanceInfo.requiredResources;
        labelMap = {
            {"instance_id", instanceID},
            {"cpu_type", billingFunctionOption.cpuType},
            {"xpu_type", billingInstanceInfo.xpuType},
            {"init_ms", std::to_string(billingInstanceInfo.startTimeMillis)},
            {"last_report_ms", std::to_string(billingInstanceInfo.lastReportTimeMillis)},
            {"report_ms", std::to_string(reportTimeMillis)},
            {"pool_label", poolLabelsJson.dump()},
            {"agent_id", billingInstanceInfo.functionAgentId},
            {"required_resources", requiredResourcesJson.dump()}
        };
    } catch (std::exception &e) {
        YRLOG_ERROR("dump labelMap failed, error: {}", e.what());
    }
    for (auto ite : std::as_const(billingInstanceInfo.customCreateOption)) {
        labelMap[ite.first] = ite.second;
    }

    if (reportTimeMillis <= billingInstanceInfo.lastReportTimeMillis) {
        YRLOG_INFO("{} billing instance has invalid reportTimeMillis: {}, lastReportTimeMillis: {}, clear it",
                   instanceID, reportTimeMillis, billingInstanceInfo.lastReportTimeMillis);
        metricsContext_.EraseBillingFunctionOptionItem(instanceID);
        metricsContext_.EraseBillingInstanceItem(instanceID);
        return std::pair{ MetricsApi::MetricLabels{}, 0 };
    }
    uint64_t value = static_cast<uint64_t>(reportTimeMillis - billingInstanceInfo.lastReportTimeMillis);
    MetricsApi::MetricLabels labels;
    for (auto iter = labelMap.begin(); iter != labelMap.end(); ++iter) {
        labels.emplace_back(*iter);
    }
    // Terminated instance: clear info; Non-terminated instance, update last report time
    if (extraBilling) {
        metricsContext_.EraseExtraBillingInstanceItem(instanceID);
    } else {
        if (billingInstanceInfo.endTimeMillis > 0) {
            YRLOG_DEBUG("Terminated billing instance {}, start time {}, end time {}", instanceID,
                        billingInstanceInfo.startTimeMillis, billingInstanceInfo.endTimeMillis);
            metricsContext_.EraseBillingFunctionOptionItem(instanceID);
            metricsContext_.EraseBillingInstanceItem(instanceID);
        } else {
            metricsContext_.SetBillingInstanceReportTime(instanceID, reportTimeMillis);
        }
    }
    return std::pair{ labels, value };
}

void MetricsAdapter::RegisterPodResource(bool logCheckSwitch)
{
    if (enabledInstruments_.find(YRInstrument::YR_POD_RESOURCE) == enabledInstruments_.end()) {
        if (logCheckSwitch) {
            YRLOG_WARN("billing pod resource is not enabled");
        }
        return;
    }
    if (metricsContext_.GetAttr("component_name") != "function_master") {
        YRLOG_DEBUG("current component({}) is not function master, do not report resource info",
                    metricsContext_.GetAttr("component_name"));
        return;
    }
    MeterTitle meterTitle{ GetInstrumentDesc(YRInstrument::YR_POD_RESOURCE), "Pod Resources", "milliseconds" };
    if (observableInstrumentMap_.find(meterTitle.name) != observableInstrumentMap_.end()) {
        YRLOG_DEBUG("pod resource is already running");
        return;
    }
    MetricsApi::CallbackPtr cb = std::bind(&MetricsAdapter::CollectPodResource, this, std::placeholders::_1);
    InitObservableGauge(meterTitle, POD_RESOURCE_COLLECT_INTERVAL, cb,
                        observability::sdk::metrics::InstrumentValueType::DOUBLE);
}

void MetricsAdapter::CollectPodResource(MetricsApi::ObserveResult obRes)
{
    std::vector<std::pair<MetricsApi::MetricLabels, double>> vec;
    auto podResourceMap = metricsContext_.GetPodResourceMap();
    YRLOG_DEBUG("collect pod resource size: {}", podResourceMap.size());
    for (const auto &res : podResourceMap) {
        vec.push_back(BuildPodResourceData(res.first, res.second));
    }

    if (std::holds_alternative<std::shared_ptr<MetricsApi::ObserveResultT<double>>>(obRes)) {
        std::get<std::shared_ptr<MetricsApi::ObserveResultT<double>>>(obRes)->Observe(vec);
    }
}

std::pair<MetricsApi::MetricLabels, double> MetricsAdapter::BuildPodResourceData(const std::string &agentID,
                                                                                 const PodResource &podResource)
{
    nlohmann::json poolLabelsJson = ConvertNodeLabels(podResource.nodeLabels);
    metrics::LabelType labelMap;
    try {
        labelMap = {
            { "allocatable_cpu", GetResourceScalar(podResource.allocatable, resource_view::CPU_RESOURCE_NAME) },
            { "used_cpu", GetResourceUsedScalar(podResource.capacity, podResource.allocatable,
                    resource_view::CPU_RESOURCE_NAME) },
            { "cpu_capacity", GetResourceScalar(podResource.capacity, resource_view::CPU_RESOURCE_NAME) },
            { "allocatable_memory", GetResourceScalar(podResource.allocatable, resource_view::MEMORY_RESOURCE_NAME) },
            { "used_memory", GetResourceUsedScalar(podResource.capacity, podResource.allocatable,
                    resource_view::MEMORY_RESOURCE_NAME) },
            { "memory_capacity", GetResourceScalar(podResource.capacity, resource_view::MEMORY_RESOURCE_NAME) },
            { "pool_label", poolLabelsJson.dump() },
 			{ "agent_id", agentID },
            { "report_ms", std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::system_clock::now().time_since_epoch())
                                              .count()) }
        };
    } catch (std::exception &e) {
        YRLOG_ERROR("dump labelMap failed, error: {}", e.what());
    }

    MetricsApi::MetricLabels labels;
    for (auto iter = labelMap.begin(); iter != labelMap.end(); ++iter) {
        labels.emplace_back(*iter);
    }
    return std::pair{ labels, 0 };
}

void MetricsAdapter::ReportInstanceStatus(const std::string &instanceID,
                                          const resource_view::InstanceInfo &instanceInfo)
{
    if (enabledInstruments_.find(YRInstrument::YR_APP_INSTANCE_STATUS) == enabledInstruments_.end()) {
        YRLOG_DEBUG("YR_APP_INSTANCE_STATUS is not enabled");
        return;
    }

    // Only non-system function indices of the same node are reported
    if (instanceInfo.functionproxyid() != metricsContext_.GetAttr("node_id") || instanceInfo.issystemfunc()) {
        return;
    }

    auto timeStamp =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();

    MeterTitle instanceTitle{
        "yr_app_instance_status",
        "instance status code: 1-Scheduling, 2-Creating, 3-Running, 4-Failed, 5-Exited, 6-Fatal, 7-ScheduleFailed",
        "enum"
    };
    MeterData data{
        static_cast<double>(instanceInfo.instancestatus().code()),
        {
            { "instance_id", instanceID },
            { "function_key", instanceInfo.function() },
            { "timestamp", std::to_string(timeStamp) },
            { "agent_id", instanceInfo.functionagentid() },
            { "parent_id", instanceInfo.parentid() },
        },
    };

    ReportDoubleGauge(instanceTitle, data, { "node_id", "ip" });
}

void MetricsAdapter::ReportBillingInvokeLatency(const std::string &requestID, uint32_t errCode,
                                                long long startTimeMillis, long long endTimeMillis)
{
    YRLOG_DEBUG("{}|report billing invoke latency, errCode: {}", requestID, errCode);
    if (enabledInstruments_.find(YRInstrument::YR_APP_INSTANCE_BILLING_INVOKE_LATENCY) == enabledInstruments_.end()) {
        YRLOG_DEBUG("Billing Invoke Latency is not enabled");
        return;
    }

    auto billingInvokeOption = metricsContext_.GetBillingInvokeOption(requestID);
    YRLOG_DEBUG("billing invoke lantency of function: {}, instanceID: {}", billingInvokeOption.functionName,
                billingInvokeOption.instanceID);
    if (billingInvokeOption.functionName == "" ||
        SYSTEM_FUNCTION_NAME.find(billingInvokeOption.functionName) != SYSTEM_FUNCTION_NAME.end()) {
        YRLOG_WARN("function name {} can't bill", billingInvokeOption.functionName);
        return;
    }

    auto billingFunctionOption = metricsContext_.GetBillingFunctionOption(billingInvokeOption.instanceID);
    nlohmann::json poolLabelsJson = ConvertNodeLabels(billingFunctionOption.nodeLabels);
    try {
        YRLOG_DEBUG("metrics nodeLabels are: {} ", poolLabelsJson.dump());
    } catch (std::exception &e) {
        YRLOG_ERROR("dump poolLabelsJson failed, error: {}", e.what());
    }
    std::string subUrl = "/instanceId/" + billingInvokeOption.instanceID + "/requestId/" +  requestID;
    metrics::LabelType labels;
    try {
         labels = { { "request_id", requestID },
                  { "function_name", billingInvokeOption.functionName },
                  { "status_code", std::to_string(errCode) },
                  { "start_ms", std::to_string(startTimeMillis) },
                  { "end_ms", std::to_string(endTimeMillis) },
                  { "interval_ms", std::to_string(endTimeMillis - startTimeMillis) },
                  { "pool_label", poolLabelsJson.dump() },
                  { "cpu_type", billingFunctionOption.cpuType },
                  { "export_sub_url", subUrl} };
    } catch (std::exception &e) {
        YRLOG_ERROR("dump initConfigJson failed, error: {}", e.what());
    }

    for (auto &ite : std::as_const(billingInvokeOption.invokeOptions)) {
        labels[ite.first] = ite.second;
    }
    for (auto &ite : std::as_const(billingFunctionOption.schedulingExtensions)) {
        labels[ite.first] = ite.second;
    }

    struct MeterData meterData {
        static_cast<double>(endTimeMillis - startTimeMillis), labels
    };
    functionsystem::metrics::MeterTitle meterTitle{
        GetInstrumentDesc(YRInstrument::YR_APP_INSTANCE_BILLING_INVOKE_LATENCY), "", "milliseconds" };
    ReportDoubleGauge(meterTitle, meterData, {});
    metricsContext_.EraseBillingInvokeOptionItem(requestID);
}

void MetricsAdapter::ReportInstanceExitLatency(const Status &status, long long startTimeMillis,
                                               const InstanceInfo &instanceInfo)
{
    if (enabledInstruments_.find(YRInstrument::YR_INSTANCE_EXIT_LATENCY) == enabledInstruments_.end()) {
        YRLOG_DEBUG("YR_INSTANCE_EXIT_LATENCY is not enabled");
        return;
    }

    // Only non-system function indices of the same node are reported
    if (instanceInfo.functionproxyid() != metricsContext_.GetAttr("node_id") || instanceInfo.issystemfunc()) {
        return;
    }

    MeterTitle latencyTitle{ "yr_instance_exit_latency", "Used for counting the exit time of instances",
                             "milliseconds" };

    auto endTimeMillis =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();

    MeterData data{
        static_cast<double>(endTimeMillis - startTimeMillis),
        {
            { "instance_id", instanceInfo.instanceid() },
            { "agent_id", instanceInfo.functionagentid() },
            { "status_code", std::to_string(status.StatusCode()) },
            { "start_ms", std::to_string(startTimeMillis) },
            { "end_ms", std::to_string(endTimeMillis) },
        },
    };

    ReportDoubleGauge(latencyTitle, data, {});
}

void MetricsAdapter::ReportDoubleGauge(const struct MeterTitle &title, struct MeterData &data,
                                       const std::list<std::string> &contextAttrs)
{
    std::lock_guard<std::mutex> l(mutex_);
    InitDoubleGauge(title);
    if (doubleGaugeMap_.find(title.name) == doubleGaugeMap_.end()) {
        return;
    }

    observability::sdk::metrics::PointLabels labels;

    for (const auto &attr : contextAttrs) {
        if (data.labels.find(attr) == data.labels.end()) {
            data.labels[attr] = metricsContext_.GetAttr(attr);
        }
    }

    for (auto iter = data.labels.begin(); iter != data.labels.end(); ++iter) {
        labels.emplace_back(*iter);
    }

    auto it = doubleGaugeMap_.find(title.name);
    it->second->Set(data.value, labels);
}

void MetricsAdapter::TransformGaugeParam(const std::string &name, const std::string &description,
                                         const std::string &unit, const double value)
{
    MeterTitle meterTitle{ name, description, unit };
    MeterData meterData{ value, {} };
    ReportDoubleGauge(meterTitle, meterData, {});
}

void MetricsAdapter::ReportClusterSourceState(const std::shared_ptr<resource_view::ResourceUnit> &unit)
{
    RETURN_IF_NULL(unit);

    if (functionsystem::resource_view::HasValidCPU(unit->capacity())) {
        double capacityCPU =
            unit->capacity().resources().at(functionsystem::resource_view::CPU_RESOURCE_NAME).scalar().value();
        TransformGaugeParam("yr_cluster_cpu_capacity", "", "vmillicore", capacityCPU);
    }

    if (functionsystem::resource_view::HasValidCPU(unit->allocatable())) {
        double allocatableCPU =
            unit->allocatable().resources().at(functionsystem::resource_view::CPU_RESOURCE_NAME).scalar().value();
        TransformGaugeParam("yr_cluster_cpu_allocatable", "", "vmillicore", allocatableCPU);
    }

    if (functionsystem::resource_view::HasValidMemory(unit->capacity())) {
        double capacityMemory =
            unit->capacity().resources().at(functionsystem::resource_view::MEMORY_RESOURCE_NAME).scalar().value();
        TransformGaugeParam("yr_cluster_memory_capacity", "", "mb", capacityMemory);
    }

    if (functionsystem::resource_view::HasValidMemory(unit->allocatable())) {
        double allocatableMemory =
            unit->allocatable().resources().at(functionsystem::resource_view::MEMORY_RESOURCE_NAME).scalar().value();
        TransformGaugeParam("yr_cluster_memory_allocatable", "", "mb", allocatableMemory);
    }

    // 遍历 fragment 中的每个 resourceUnit，只上报 CPU 和 Memory
    for (const auto &[nodeID, resourceUnit] : unit->fragment()) {
        // 上报 CPU capacity
        if (functionsystem::resource_view::HasValidCPU(resourceUnit.capacity())) {
            double value = resourceUnit.capacity().resources().at(
                functionsystem::resource_view::CPU_RESOURCE_NAME).scalar().value();
            MeterTitle meterTitle{ "yr_nodes_cpu_capacity", "", "vmillicore" };
            MeterData meterData{ value, { { "node_id", nodeID } } };
            ReportDoubleGauge(meterTitle, meterData, {});
        }

        // 上报 CPU allocatable
        if (functionsystem::resource_view::HasValidCPU(resourceUnit.allocatable())) {
            double value = resourceUnit.allocatable().resources().at(
                functionsystem::resource_view::CPU_RESOURCE_NAME).scalar().value();
            MeterTitle meterTitle{ "yr_nodes_cpu_allocatable", "", "vmillicore" };
            MeterData meterData{ value, { { "node_id", nodeID } } };
            ReportDoubleGauge(meterTitle, meterData, {});
        }

        // 上报 Memory capacity
        if (functionsystem::resource_view::HasValidMemory(resourceUnit.capacity())) {
            double value = resourceUnit.capacity().resources().at(
                functionsystem::resource_view::MEMORY_RESOURCE_NAME).scalar().value();
            MeterTitle meterTitle{ "yr_nodes_memory_capacity", "", "mb" };
            MeterData meterData{ value, { { "node_id", nodeID } } };
            ReportDoubleGauge(meterTitle, meterData, {});
        }

        // 上报 Memory allocatable
        if (functionsystem::resource_view::HasValidMemory(resourceUnit.allocatable())) {
            double value = resourceUnit.allocatable().resources().at(
                functionsystem::resource_view::MEMORY_RESOURCE_NAME).scalar().value();
            MeterTitle meterTitle{ "yr_nodes_memory_allocatable", "", "mb" };
            MeterData meterData{ value, { { "node_id", nodeID } } };
            ReportDoubleGauge(meterTitle, meterData, {});
        }
    }
}

void MetricsAdapter::SetContextAttr(const std::string &attr, const std::string &value)
{
    metricsContext_.SetAttr(attr, value);
}

std::string MetricsAdapter::GetContextValue(const std::string &attr) const
{
    return metricsContext_.GetAttr(attr);
}

MetricsContext &MetricsAdapter::GetMetricsContext()
{
    return metricsContext_;
}

std::string MetricsAdapter::GetExportModeDesc(const MetricsSdk::ExportMode &mode)
{
    switch (mode) {
        case MetricsSdk::ExportMode::IMMEDIATELY:
            return IMMEDIATELY_EXPORT;
        case MetricsSdk::ExportMode::BATCH:
            return BATCH_EXPORT;
        default:
            return "unknown";
    }
}

std::string MetricsAdapter::GetResourceScalar(const resources::Resources &resource, const std::string &resType)
{
    if (!resource.resources().contains(resType)) {
        return "0";
    }

    return std::to_string(resource.resources().at(resType).scalar().value());
}

std::string MetricsAdapter::GetResourceUsedScalar(const resources::Resources &capacity,
                                                  const resources::Resources &allocatable, const std::string &resType)
{
    if (!capacity.resources().contains(resType)) {
        return "0";
    }
    if (!allocatable.resources().contains(resType)) {
        return GetResourceScalar(capacity, resType);
    }
    return std::to_string(capacity.resources().at(resType).scalar().value()
                          - allocatable.resources().at(resType).scalar().value());
}

}  // namespace metrics
}  // namespace functionsystem
