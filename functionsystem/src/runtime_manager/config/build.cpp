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

#include "build.h"

#include <unordered_set>

#include "common/logs/logging.h"
#include "common/utils/exec_utils.h"
#include "common/constants/constants.h"
#include "utils/os_utils.hpp"
#include "utils/utils.h"

namespace functionsystem::runtime_manager {
const static std::map<std::string, std::string> HANDLER_MAP = {
    { "init", "INIT_HANDLER" },       { "call", "CALL_HANDLER" },         { "checkpoint", "CHECKPOINT_HANDLER" },
    { "recover", "RECOVER_HANDLER" }, { "shutdown", "SHUTDOWN_HANDLER" }, { "signal", "SIGNAL_HANDLER" },
    { "health", "HEALTH_CHECK_HANDLER" },
};

const static std::string HOST_IP = "HOST_IP";
const static std::string POD_IP = "POD_IP";
const static std::string SNUSER_LIB_PATH = "SNUSER_LIB_PATH";
const static std::string REQUEST_ACK_ACC_MAX_SEC = "REQUEST_ACK_ACC_MAX_SEC";
const static std::string POSIX_LISTEN_ADDR = "POSIX_LISTEN_ADDR";
const static std::string YR_RUNTIME_ID = "YR_RUNTIME_ID";
const static std::string INSTANCE_ID_ENV = "INSTANCE_ID";
const static std::string HOME_ENV = "HOME";
const static std::string DATA_SYSTEM_ADDR = "DATASYSTEM_ADDR";
const static std::string YR_DS_ADDRESS = "YR_DS_ADDRESS";
const static std::string YR_DPOSIX_UDS = "YR_DPOSIX_UDS";
const static std::string YR_SERVER_ADDRESS = "YR_SERVER_ADDRESS";
const static std::string DRIVER_SERVER_PORT = "DRIVER_SERVER_PORT";
const static std::string FUNCTION_LIB_PATH = "FUNCTION_LIB_PATH";
const static std::string YR_FUNCTION_LIB_PATH = "YR_FUNCTION_LIB_PATH";
const static std::string LAYER_LIB_PATH = "LAYER_LIB_PATH";
const static std::string PROXY_GRPC_SERVER_PORT = "PROXY_GRPC_SERVER_PORT";
const static std::string CLUSTER_ID = "CLUSTER_ID";
const static std::string ENABLE_DS_CLIENT = "ENABLE_DS_CLIENT";
const static std::string NODE_ID = "NODE_ID";
const static std::string ENABLE_METRICS = "ENABLE_METRICS";
const static std::string METRICS_CONFIG = "METRICS_CONFIG";
const static std::string METRICS_CONFIG_FILE = "METRICS_CONFIG_FILE";
const static std::string RUNTIME_METRICS_CONFIG = "RUNTIME_METRICS_CONFIG";
const static std::string RUNTIME_METRICS_CONFIG_FILE = "RUNTIME_METRICS_CONFIG_FILE";
const static std::string DERICT_RUNTIME_SERVER_PORT = "DERICT_RUNTIME_SERVER_PORT";
const static std::string ENABLE_TRACE = "ENABLE_TRACE";
const static std::string TRACE_CONFIG = "TRACE_CONFIG";
const static std::string RUNTIME_TRACE_CONFIG = "RUNTIME_TRACE_CONFIG";
const static std::string ENABLE_DIS_CONV_CALL_STACK = "ENABLE_DIS_CONV_CALL_STACK";

const static std::string S3_STORAGE_TYPE = "s3";

const static std::string RUNTIME_LAYER_DIR_NAME = "layer";
const static std::string RUNTIME_FUNC_DIR_NAME = "func";
const static std::string GRACEFUL_SHUTDOWN_TIME = "GRACEFUL_SHUTDOWN_TIME";
const static std::string PYTHONUNBUFFERED = "PYTHONUNBUFFERED";

const static std::string ASCEND_RT_VISIBLE_DEVICES = "ASCEND_RT_VISIBLE_DEVICES";
const static std::string CUDA_VISIBLE_DEVICES = "CUDA_VISIBLE_DEVICES";
const static std::string YR_LOG_PREFIX = "YR_LOG_PREFIX";

const std::vector<std::string> PRE_CONFIG_ENV = {
    POSIX_LISTEN_ADDR, POD_IP, INSTANCE_ID_ENV, DATA_SYSTEM_ADDR, DRIVER_SERVER_PORT,
    HOME_ENV, HOST_IP, FUNCTION_LIB_PATH, YR_FUNCTION_LIB_PATH, LAYER_LIB_PATH,
    PROXY_GRPC_SERVER_PORT, CLUSTER_ID, NODE_ID
};

const std::unordered_set<std::string> USER_ENV_OVERWRITE_WHITELIST = {
    PYTHONUNBUFFERED,
    ENABLE_METRICS
};

Envs GenerateEnvs(const RuntimeConfig &config, const std::shared_ptr<messages::StartInstanceRequest> &request,
                  const std::string &port, const std::vector<int> &cardsIDs)
{
    const RuntimeFeatures features;
    return GenerateEnvs(config, request, port, cardsIDs, features);
}

Envs GenerateEnvs(const RuntimeConfig &config, const std::shared_ptr<messages::StartInstanceRequest> &request,
                  const std::string &port, const std::vector<int> &cardsIDs, const RuntimeFeatures &features)
{
    std::map<std::string, std::string> customResourceEnv;
    customResourceEnv[GRACEFUL_SHUTDOWN_TIME] = std::to_string(request->runtimeinstanceinfo().gracefulshutdowntime());
    if (request->runtimeinstanceinfo().runtimeconfig().tlsconfig().dsauthenable()) {
        customResourceEnv["ENABLE_DS_AUTH"] = "true";
    }
    if (request->runtimeinstanceinfo().runtimeconfig().tlsconfig().serverauthenable()) {
        customResourceEnv["ENABLE_SERVER_AUTH"] = "true";
        auto caFile = litebus::os::GetEnv("VERIFY_FILE_PATH");
        customResourceEnv["YR_SSL_ROOT_FILE"] = caFile.IsSome() ? caFile.Get() : "";
        auto certFile = litebus::os::GetEnv("CERTIFICATE_FILE_PATH");
        customResourceEnv["YR_SSL_CERT_FILE"] = certFile.IsSome() ? certFile.Get() : "";
        auto keyFile = litebus::os::GetEnv("PRIVATE_KEY_PATH");
        customResourceEnv["YR_SSL_KEY_FILE"] = keyFile.IsSome() ? keyFile.Get() : "";
    }
    if (request->runtimeinstanceinfo().runtimeconfig().tlsconfig().enableservermode()) {
        customResourceEnv["ENABLE_SERVER_MODE"] = "true";
    }
    if (features.runtimeDirectConnectionEnable) {
        customResourceEnv[RUNTIME_DIRECT_CONNECTION_ENABLE] = "true";
        customResourceEnv[DERICT_RUNTIME_SERVER_PORT] = features.directRuntimeServerPort;
        YRLOG_DEBUG("set RUNTIME_DIRECT_CONNECTION_ENABLE=true");
        YRLOG_DEBUG("set DERICT_RUNTIME_SERVER_PORT={}", customResourceEnv[DERICT_RUNTIME_SERVER_PORT]);
    }
    if (!features.cleanStreamProducerEnable) {
        customResourceEnv[ENABLE_CLEAN_STREAM_PRODUCER] = "false";
        YRLOG_DEBUG("set ENABLE_CLEAN_STREAM_PRODUCER=false");
    }
    if (!request->logprefix().empty()) {
        customResourceEnv[YR_LOG_PREFIX] = request->logprefix();
        // for ds client log reuse
        customResourceEnv["DATASYSTEM_CLIENT_LOG_NAME"] = request->logprefix() + "_ds_client";
        customResourceEnv["DATASYSTEM_CLIENT_ACCESS_LOG_NAME"] = request->logprefix() + "_ds_client_access";
        YRLOG_DEBUG("set YR_LOG_PREFIX: {}", request->logprefix());
    }
    return { GeneratePosixEnvs(config, request, port),
             customResourceEnv,
             GenerateUserEnvs(request->runtimeinstanceinfo(), cardsIDs)};
}

std::map<std::string, std::string> GeneratePosixEnvs(const RuntimeConfig &config,
                                                     const std::shared_ptr<messages::StartInstanceRequest> &request,
                                                     const std::string &port)
{
    const auto &info = request->runtimeinstanceinfo();
    const auto &deploymentConfig = info.deploymentconfig();
    const std::string &deployDir = deploymentConfig.deploydir();
    const std::string &storageType = deploymentConfig.storagetype();
    const std::string &debugServerPort = request->runtimeinstanceinfo().runtimeconfig().debugserverport();

    std::string deployFilePath = deployDir;
    std::string layerPath;
    if (storageType == S3_STORAGE_TYPE && request->scheduleoption().schedpolicyname() != MONOPOLY_SCHEDULE) {
        deployFilePath = deployFilePath.append("/" + RUNTIME_LAYER_DIR_NAME)
                             .append("/" + RUNTIME_FUNC_DIR_NAME)
                             .append("/" + deploymentConfig.bucketid())
                             .append("/" + TransMultiLevelDirToSingle(deploymentConfig.objectid()));
        layerPath = Utils::JoinToString(GenerateLayerPath(info), ",");
    }
    std::stringstream ss;
    // The third-party dependency libraries of functions are stored in the lib directory.
    if (!deployFilePath.empty() && deployFilePath != "/") {
        ss << deployFilePath << ":" << deployFilePath << "/lib";
    } else {
        deployFilePath = "";
    }
    if (info.runtimeconfig().posixenvs().find(ENV_DELEGATE_DOWNLOAD) != info.runtimeconfig().posixenvs().end()) {
        std::string delegateDownload = info.runtimeconfig().posixenvs().find(ENV_DELEGATE_DOWNLOAD)->second;
        ss << ":" << delegateDownload << ":" << delegateDownload
           << "/lib";
    }
    std::string language = info.runtimeconfig().language();
    (void)transform(language.begin(), language.end(), language.begin(), ::tolower);
    if (language.find(CPP_LANGUAGE) != std::string::npos) {
        ss << ":" << config.runtimePath << "/cpp/lib";
    }
    if (language.find(PYTHON_LANGUAGE) != std::string::npos) {
        ss << ":" << config.runtimePath << "/python/yr";
    }
    if (!config.runtimeLdLibraryPath.empty() &&
        request->type() == static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME)) {
        ss << ":" << config.runtimeLdLibraryPath;
    }
    std::string ldLibraryPath = ss.str();
    YRLOG_INFO("{}|{}|start runtime env LD_LIBRARY_PATH: {}", info.traceid(), info.requestid(), ldLibraryPath);

    std::map<std::string, std::string> posixEnvs = {
        { POSIX_LISTEN_ADDR, config.ip + ":" + port },
        { POD_IP, config.ip },
        { SNUSER_LIB_PATH, config.snuserLibDir },
        { YR_RUNTIME_ID, info.runtimeid() },
        { INSTANCE_ID_ENV, info.instanceid() },
        { DATA_SYSTEM_ADDR,
          config.hostIP + ":" + config.dataSystemPort },  // the port of datasystem worker should be configurable.
        { YR_DS_ADDRESS,  // keep same env name for runtime in driver mode and job submission mode
          config.hostIP + ":" + config.dataSystemPort },
        { DRIVER_SERVER_PORT, config.driverServerPort },
        { HOME_ENV, config.runtimeHomeDir },
        { HOST_IP, config.hostIP },
        { FUNCTION_LIB_PATH, deployFilePath },
        { "YR_FUNCTION_LIB_PATH", deployFilePath },
        { LAYER_LIB_PATH, layerPath },
        { LD_LIBRARY_PATH, ldLibraryPath },
        { PROXY_GRPC_SERVER_PORT, config.proxyGrpcServerPort },
        { YR_SERVER_ADDRESS,  // keep same env name for runtime in driver mode and job submission mode
          config.proxyIP + ":" + config.proxyGrpcServerPort },
        { CLUSTER_ID, config.clusterID },
        { NODE_ID, config.nodeID },
        { ENABLE_DIS_CONV_CALL_STACK, config.enableDisConvCallStack ? "true" : "false" },
        { YR_DEBUG_SERVER_PORT, debugServerPort }
    };
    auto addIfValid = [&posixEnvs](const std::string &key, const std::string &value) {
        if (value.empty() || value == "0") {
            return;
        }
        (void)posixEnvs.emplace(std::make_pair(key, value));
    };

    addIfValid(POSIX_LISTEN_ADDR, GetPosixAddress(config, port));
    addIfValid(POD_IP, config.ip);
    addIfValid(YR_RUNTIME_ID, info.runtimeid());
    addIfValid(INSTANCE_ID_ENV, info.instanceid());
    addIfValid(DATA_SYSTEM_ADDR,
               config.hostIP + ":" + config.dataSystemPort);  // the port of datasystem worker should be configurable.
    // keep same env name for runtime in driver mode and job submission mode
    addIfValid(YR_DS_ADDRESS,
               config.hostIP + ":" + config.dataSystemPort);
    addIfValid(DRIVER_SERVER_PORT, config.driverServerPort);
    addIfValid(HOME_ENV, config.runtimeHomeDir);
    addIfValid(HOST_IP, config.hostIP);
    addIfValid(FUNCTION_LIB_PATH, deployFilePath);
    addIfValid("YR_FUNCTION_LIB_PATH", deployFilePath);
    addIfValid(LAYER_LIB_PATH, layerPath);
    addIfValid(LD_LIBRARY_PATH, ldLibraryPath);
    addIfValid(PROXY_GRPC_SERVER_PORT, config.proxyGrpcServerPort);
    // keep same env name for runtime in driver mode and job submission mode
    addIfValid(YR_SERVER_ADDRESS,
               config.proxyIP + ":" + config.proxyGrpcServerPort);
    addIfValid(YR_DPOSIX_UDS, request->runtimeinstanceinfo().runtimeconfig().dposixudspath());
    addIfValid(CLUSTER_ID, config.clusterID);
    addIfValid(NODE_ID, config.nodeID);
    addIfValid(ENABLE_DIS_CONV_CALL_STACK, config.enableDisConvCallStack ? "true" : "");
    addIfValid(YR_DEBUG_SERVER_PORT, debugServerPort);
    addIfValid("YR_JOB_ID", "job-" + Utils::GetJobIDFromTraceID(info.traceid()));

    AddYuanRongEnvs(posixEnvs);

    // for python and posix-custom-runtime set PYTHONUNBUFFERED=1
    if (info.runtimeconfig().language().find(PYTHON_LANGUAGE) != std::string::npos
        || info.runtimeconfig().language() == POSIX_CUSTOM_RUNTIME) {
        (void)posixEnvs.emplace(PYTHONUNBUFFERED, "1");
    }

    // passthrough runtimeconfig.posixenvs: like YR_TENANT_ID, and so on.
    for (const auto &env : info.runtimeconfig().posixenvs()) {
        if (env.first == LD_LIBRARY_PATH) {
            posixEnvs[env.first] = ReplaceDollarContent(env.second, posixEnvs);
            continue;
        }

        // user env_vars can overwrite some default envs
        if (USER_ENV_OVERWRITE_WHITELIST.find(env.first) != USER_ENV_OVERWRITE_WHITELIST.end()) {
            posixEnvs[env.first] = env.second;
        } else {
            posixEnvs.emplace(env.first, env.second);
        }
    }
    // todo(lwy_robb): which should be removed in the future
    auto hookHandler = info.runtimeconfig().hookhandler();
    for (const auto &handlerIter : HANDLER_MAP) {
        auto hookHandlerIter = hookHandler.find(handlerIter.first);
        if (hookHandlerIter != hookHandler.end()) {
            (void)posixEnvs.emplace(std::make_pair(handlerIter.second, hookHandlerIter->second));
        }
    }
    return posixEnvs;
}

std::vector<std::string> GenerateLayerPath(const messages::RuntimeInstanceInfo &info)
{
    std::vector<std::string> layerPath;
    auto layers = info.deploymentconfig().layers();
    std::string deployDir(info.deploymentconfig().deploydir());
    if (auto deployDirIter(info.runtimeconfig().userenvs().find("S3_DEPLOY_DIR"));
        deployDirIter != info.runtimeconfig().userenvs().end()) {
        deployDir = deployDirIter->second;
    }
    for (const auto &layer : layers) {
        std::string path = deployDir + "/" + RUNTIME_LAYER_DIR_NAME + "/" + layer.bucketid() + "/" +
                           TransMultiLevelDirToSingle(layer.objectid());
        (void)layerPath.emplace_back(path);
    }
    return layerPath;
}

std::map<std::string, std::string> GenerateUserEnvs(const ::messages::RuntimeInstanceInfo &info,
                                                    const std::vector<int> &cardsIDs)
{
    std::map<std::string, std::string> envs;
    auto userEnvs = info.runtimeconfig().userenvs();
    for (const auto &envIter : userEnvs) {
        if (envIter.first.find(RUNTIME_ENV_PREFIX, 0) == 0) {
            std::string key = Utils::TrimPrefix(envIter.first, RUNTIME_ENV_PREFIX);
            if (key == "NPU-DEVICE-IDS" || key == "GPU-DEVICE-IDS") {
                auto realIDs = SelectRealIDs(envIter.second, cardsIDs);
                auto visibleDevicesEnvKey = key == "NPU-DEVICE-IDS" ? ASCEND_RT_VISIBLE_DEVICES : CUDA_VISIBLE_DEVICES;
                (void)envs.emplace(std::make_pair(key, envIter.second));
                // XXX_VISIBLE_DEVICES need to set logic id, not physical id
                (void)envs.emplace(std::make_pair(visibleDevicesEnvKey, envIter.second));
                YRLOG_DEBUG("select {} realIDs, mappingIDS: physical[{}], logical[{}]", key, realIDs, envIter.second);
                continue;
            }
            if (IsPreconfiguredEnv(key)) {
                continue;
            }
            (void)envs.emplace(std::make_pair(key, envIter.second));
        }
    }
    return envs;
}

void AddYuanRongEnvs(std::map<std::string, std::string> &envs)
{
    auto addIfPresent = [&envs](const std::string &key, const litebus::Option<std::string> &opt) {
        if (opt.IsNone()) {
            return;
        }
        const auto value = opt.Get();
        if (value.empty()) {
            return;
        }
        (void)envs.emplace(std::make_pair(key, value));
    };
    auto addIfEnabled = [&envs](const std::string &key, const litebus::Option<std::string> &opt) {
        if (opt.IsNone()) {
            return;
        }
        auto value = opt.Get();
        if (value.empty()) {
            return;
        }
        auto lower = value;
        (void)transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        if (lower == "false" || lower == "0") {
            return;
        }
        (void)envs.emplace(std::make_pair(key, value));
    };

    // ENABLE_DS_CLIENT : 当前云助端默认不连接数据系统，后续统一需要支持连接数据系统
    auto enableDsClientOpt = litebus::os::GetEnv(ENABLE_DS_CLIENT);
    addIfEnabled(ENABLE_DS_CLIENT, enableDsClientOpt);

    auto enableMetricsOpt = litebus::os::GetEnv(ENABLE_METRICS);
    {
        std::string metricsValue = "false";
        if (!enableMetricsOpt.IsNone() && !enableMetricsOpt.Get().empty()) {
            auto lower = enableMetricsOpt.Get();
            (void)transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
            if (lower != "false" && lower != "0") {
                metricsValue = enableMetricsOpt.Get();
            }
        }
        (void)envs.emplace(std::make_pair(ENABLE_METRICS, metricsValue));
    }

    auto metricsConfigOpt = litebus::os::GetEnv(RUNTIME_METRICS_CONFIG);
    addIfPresent(METRICS_CONFIG, metricsConfigOpt);

    const auto enableAlarmOpt = litebus::os::GetEnv("ENABLE_DS_HEALTH_CHECK");
    addIfEnabled("ENABLE_DS_HEALTH_CHECK", enableAlarmOpt);

    auto metricsConfigFileOpt = litebus::os::GetEnv(RUNTIME_METRICS_CONFIG_FILE);
    addIfPresent(METRICS_CONFIG_FILE, metricsConfigFileOpt);

    auto enableTraceOpt = litebus::os::GetEnv(ENABLE_TRACE);
    addIfEnabled(ENABLE_TRACE, enableTraceOpt);

    auto runtimeTraceConfigOpt = litebus::os::GetEnv(RUNTIME_TRACE_CONFIG);
    addIfPresent(RUNTIME_TRACE_CONFIG, runtimeTraceConfigOpt);

    auto requestAckAccMaxSecOpt = litebus::os::GetEnv(REQUEST_ACK_ACC_MAX_SEC);
    if (!requestAckAccMaxSecOpt.IsNone()) {
        auto requestAckAccMaxSec = requestAckAccMaxSecOpt.Get();
        (void)envs.emplace(std::make_pair(REQUEST_ACK_ACC_MAX_SEC, requestAckAccMaxSec));
    }
}

std::string SelectRealIDs(const std::string &env, const std::vector<int> &cardsIDs)
{
    auto logicIDs = litebus::strings::Split(env, ",");
    if (cardsIDs.empty()) {
        YRLOG_WARN("real ID doesn't report, cannot select real ID");
        return "";
    }
    std::string newEnvs = "";
    for (auto &id : logicIDs) {
        int idx = 0;
        try {
            idx = std::stoi(id);
        } catch (const std::exception &e) {
            YRLOG_WARN("invalid id: {}", id);
            continue;
        }

        if (idx < 0) {
            YRLOG_WARN("invalid idx: {}", idx);
        } else {
            if (static_cast<std::size_t>(idx) < cardsIDs.size()) {
                newEnvs += std::to_string(cardsIDs[idx]) + ",";
            } else {
                YRLOG_WARN("invalid id: {}, realID size: {}", idx, cardsIDs.size());
            }
        }
    }
    if (!newEnvs.empty()) {
        (void)newEnvs.erase(newEnvs.length() - 1);
    }
    return newEnvs;
}

std::string ReplaceDollarContent(const std::string &source, std::map<std::string, std::string> &env)
{
    auto result = source;
    auto content = source;
    std::map<std::string, std::string> matchesMap;
    const std::regex re(R"((\"?)\$\{(\x20*)(\w+)(\x20*)\}(\"?))");
    std::smatch matches;
    while (std::regex_search(content, matches, re)) {
        matchesMap[matches[0]] = matches[3]; // index 3 is \w+ eg. ${LD_LIBRARY_PATH} is LD_LIBRARY_PATH
        content = matches.suffix().str();
    }

    for (const auto &kv : matchesMap) {
        std::string replaceStr = "";
        if (auto iter = env.find(kv.second); iter != env.end()) {
            replaceStr = iter->second;
        }

        size_t startPos = 0;
        while ((startPos = result.find(kv.first, startPos)) != std::string::npos) {
            result.replace(startPos, kv.first.length(), replaceStr);
            startPos += replaceStr.length();
        }
    }
    return result;
}

bool IsPreconfiguredEnv(std::string& key)
{
    return std::find(PRE_CONFIG_ENV.begin(), PRE_CONFIG_ENV.end(), key) != PRE_CONFIG_ENV.end();
}

std::string GetPosixAddress(const RuntimeConfig config, const std::string &port)
{
    return config.proxyIP + ":" + port;
}
}  // namespace functionsystem::runtime_manager
