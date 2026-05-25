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

#include "utils.h"

#include <nlohmann/json.hpp>
#include <utils/os_utils.hpp>

#include "common/aksk/aksk_util.h"
#include "common/constants/constants.h"
#include "common/create_agent_decision/create_agent_decision.h"
#include "common/crypto/crypto.h"
#include "common/logs/logging.h"
#include "common/metadata/metadata.h"
#include "common/utils/struct_transfer.h"
#include "constants.h"

namespace functionsystem::function_agent {
const static std::string RUNTIME_ENV_PREFIX = "func-";

const std::string DEV_CLUSTER_IPS_KEY = "dev_cluster_ips";  // NOLINT
const std::string CRYPTO_ALGORITHM_STR = "cryptoAlgorithm";
const std::string ENV_KEY = "envKey";
const int CONVERSION = 20;  // MB -> TB
const int DEFAULT_QUOTA = 512;
const int QUOTA_NO_MONITOR = -1;
const std::unordered_set<std::string> DECRYPT_IGNORE_SET = { CRYPTO_ALGORITHM_STR, ENV_KEY };

const std::vector<std::string> DEPLOY_OPTION_KEYS = { CONDA_CONFIG,       CONDA_COMMAND,         CONDA_PREFIX,
                                                      CONDA_DEFAULT_ENV,  CONTAINER_ROOTFS,      CONTAINER_EXTRA_CONFIG,
                                                      CONTAINER_NETWORK, EXEC_PATH };
const std::vector<std::string> POSIX_ENV_KEYS = { YR_APP_MODE,
                                                  YR_WORKING_DIR,
                                                  UNZIPPED_WORKING_DIR,
                                                  ENV_DELEGATE_DOWNLOAD,
                                                  POST_START_EXEC,
                                                  DELEGATE_CONTAINER_ID_KEY,
                                                  ENV_DELEGATE_BOOTSTRAP,
                                                  YR_DEBUG_CONFIG,
                                                  CONDA_PREFIX,
                                                  CONDA_DEFAULT_ENV,
                                                  SHARED_DIRECTORY_PATH };
const std::vector<std::string> USER_ENV_KEYS = { S3_DEPLOY_DIR };

std::shared_ptr<messages::DeployRequest> SetDeployRequestConfig(
    const std::shared_ptr<messages::DeployInstanceRequest> &req, const std::shared_ptr<messages::Layer> &layer)
{
    ASSERT_IF_NULL(req);
    auto deployRequest = std::make_shared<messages::DeployRequest>();
    auto runtimeConfig = SetRuntimeConfig(req);
    deployRequest->mutable_runtimeconfig()->CopyFrom(runtimeConfig);
    deployRequest->set_instanceid(req->instanceid());
    deployRequest->set_schedpolicyname(req->scheduleoption().schedpolicyname());
    if (layer != nullptr) {
        // If Layer is not empty, a layer is constructed.
        auto deploymentConfig = SetDeploymentConfigOfLayer(req, layer);
        deployRequest->mutable_deploymentconfig()->CopyFrom(deploymentConfig);
    } else {
        // If Layer is empty, the main function is constructed.
        SetDeploymentConfig(deployRequest->mutable_deploymentconfig(), req);
    }
    deployRequest->mutable_deploymentconfig()->set_sha256(req->codesha256());
    deployRequest->mutable_deploymentconfig()->set_sha512(req->codesha512());
    return deployRequest;
}

void AddHeteroConfig(const std::shared_ptr<messages::DeployInstanceRequest> &req, messages::RuntimeConfig &runtimeConf)
{
    auto npuIDsIter = req->mutable_createoptions()->find("func-NPU-DEVICE-IDS");
    if (npuIDsIter != req->mutable_createoptions()->end()) {
        (*runtimeConf.mutable_userenvs())["func-NPU-DEVICE-IDS"] = npuIDsIter->second;
    }

    auto gpuIDsIter = req->mutable_createoptions()->find("func-GPU-DEVICE-IDS");
    if (gpuIDsIter != req->mutable_createoptions()->end()) {
        (*runtimeConf.mutable_userenvs())["func-GPU-DEVICE-IDS"] = gpuIDsIter->second;
    }
}

void AddDiskConfig(const std::shared_ptr<messages::DeployInstanceRequest> &req, messages::RuntimeConfig &runtimeConf)
{
    auto envKey = RUNTIME_ENV_PREFIX + resource_view::DISK_MOUNT_POINT;
    auto diskMountPointIter = req->mutable_createoptions()->find(envKey);
    if (diskMountPointIter != req->mutable_createoptions()->end()) {
        (*runtimeConf.mutable_userenvs())[envKey] = diskMountPointIter->second;
    }
}

void AddDefaultEnv(const std::shared_ptr<messages::DeployInstanceRequest> &req, messages::RuntimeConfig &runtimeConf)
{
    if (auto env = litebus::os::GetEnv(POD_NAME); env.IsSome()) {
        (void)runtimeConf.mutable_posixenvs()->insert({ POD_NAME, env.Get() });
    }

    if (auto env = litebus::os::GetEnv(POD_NAMESPACE); env.IsSome()) {
        (void)runtimeConf.mutable_posixenvs()->insert({ POD_NAMESPACE, env.Get() });
    }

    // system function need k8s env
    if (req->instancelevel() == SYSTEM_FUNCTION_INSTANCE_LEVEL) {
        if (auto env = litebus::os::GetEnv(KUBERNETES_SERVICE_HOST); env.IsSome()) {
            (void)runtimeConf.mutable_posixenvs()->insert({ KUBERNETES_SERVICE_HOST, env.Get() });
        }

        if (auto env = litebus::os::GetEnv(KUBERNETES_SERVICE_PORT); env.IsSome()) {
            (void)runtimeConf.mutable_posixenvs()->insert({ KUBERNETES_SERVICE_PORT, env.Get() });
        }

        (void)runtimeConf.mutable_posixenvs()->insert({ GODEBUG_KEY, GODEBUG_VALUE });
    }

    // tenant env
    (void)runtimeConf.mutable_posixenvs()->insert({ YR_TENANT_ID, req->tenantid() });

    // 1. The Delegate-Environment variables in Create-Options have the highest priority and are set first.
    if (auto iter = req->createoptions().find("DELEGATE_ENV_VAR"); iter != req->createoptions().end()) {
        ParseDelegateEnv(iter->second, runtimeConf);
    }

    if (auto env = litebus::os::GetEnv("DELEGATE_ENV_VAR"); env.IsSome()) {
        // 2. use insert api, does not overwrite existing values
        ParseDelegateEnv(env.Get(), runtimeConf);
    }
}

void ParseDelegateEnv(const std::string &value, messages::RuntimeConfig &runtimeConf)
{
    try {
        auto contentJson = nlohmann::json::parse(value);
        for (const auto &item : contentJson.items()) {
            if (!item.value().is_string()) {
                YRLOG_WARN("env key {} from create options is invalid", item.key());
                continue;
            }
            // notice: insert does not overwrite existing values
            (void)runtimeConf.mutable_posixenvs()->insert({ item.key(), item.value() });
        }
    } catch (const std::exception &e) {
        YRLOG_WARN("failed to parson envinfo as json string, exception e.what():{}", e.what());
    }
}

messages::RuntimeConfig SetRuntimeConfig(const std::shared_ptr<messages::DeployInstanceRequest> &req)
{
    ASSERT_IF_NULL(req);
    messages::RuntimeConfig runtimeConf;
    runtimeConf.set_entryfile(req->entryfile());
    runtimeConf.set_language(req->language());
    for (const auto &it : req->hookhandler()) {
        (*runtimeConf.mutable_hookhandler())[it.first] = it.second;
    }
    (*runtimeConf.mutable_resources()) = req->resources();

    AddHeteroConfig(req, runtimeConf);
    AddDiskConfig(req, runtimeConf);
    AddDefaultEnv(req, runtimeConf);
    // Get the specific value from createOptions as posixEnv value.
    for (const std::string &str : POSIX_ENV_KEYS) {
        if (auto iter(req->createoptions().find(str)); iter != req->createoptions().end()) {
            // yuanrong posix envs can override user env keys
            (*runtimeConf.mutable_posixenvs())[str] = iter->second;
        }
    }
    SetDelegateDecryptInfo(req, runtimeConf);
    SetUserEnv(req, runtimeConf);
    SetCreateOptions(req, runtimeConf, { "secretKey", "accessKey", "authToken" });
    SetTLSConfig(req, runtimeConf);
    SetSubDirConfig(req, runtimeConf);
    runtimeConf.set_dposixudspath(req->dposixudspath());
    runtimeConf.mutable_funcmountconfig()->CopyFrom(req->funcmountconfig());
    if (auto mountConfig = req->createoptions().find(DELEGATE_MOUNT); mountConfig != req->createoptions().end()) {
        ParseMountConfig(runtimeConf, mountConfig->second);
    }

    for (const std::string &str : USER_ENV_KEYS) {
        if (auto iter(req->createoptions().find(str)); iter != req->createoptions().end()) {
            (void)runtimeConf.mutable_userenvs()->insert({ str, iter->second });
        }
    }

    return runtimeConf;
}

void SetSubDirConfig(const std::shared_ptr<messages::DeployInstanceRequest> &req, messages::RuntimeConfig &runtimeConf)
{
    if (auto directoryInfo = req->createoptions().find(DELEGATE_DIRECTORY_INFO);
        directoryInfo != req->createoptions().end()) {
        runtimeConf.mutable_subdirectoryconfig()->set_parentdirectory(directoryInfo->second);

        if (auto quota = req->createoptions().find(DELEGATE_DIRECTORY_QUOTA);
            quota == req->createoptions().end() || quota->second.empty()) {
            runtimeConf.mutable_subdirectoryconfig()->set_quota(DEFAULT_QUOTA);
        } else {
            int quotaNum = 0;
            try {
                quotaNum = std::stoi(quota->second);
            } catch (const std::exception &e) {
                YRLOG_WARN("failed to parse DELEGATE_DIRECTORY_QUOTA, exception e.what():{}", e.what());
            }
            if (quotaNum == QUOTA_NO_MONITOR) {
                runtimeConf.mutable_subdirectoryconfig()->set_quota(QUOTA_NO_MONITOR);
            } else if (quotaNum <= 0 || quotaNum > (1 << CONVERSION)) {
                runtimeConf.mutable_subdirectoryconfig()->set_quota(DEFAULT_QUOTA);
            } else {
                runtimeConf.mutable_subdirectoryconfig()->set_quota(quotaNum);
            }
        }
        runtimeConf.mutable_subdirectoryconfig()->set_isenable(true);
    } else {
        runtimeConf.mutable_subdirectoryconfig()->set_isenable(false);
    }
}

void SetDelegateDecryptInfo(const std::shared_ptr<messages::DeployInstanceRequest> &req,
                            messages::RuntimeConfig &runtimeConf)
{
    std::string dataKey = req->has_tenantcredentials() ? req->tenantcredentials().datakey() : "";
    // set delegate decrypt info
    if (auto iter = req->createoptions().find(DELEGATE_DECRYPT); iter != req->createoptions().end()) {
        if (auto decryptData = DecryptDelegateData(iter->second, dataKey); decryptData.IsSome()) {
            (void)runtimeConf.mutable_posixenvs()->insert({ ENV_DELEGATE_DECRYPT, decryptData.Get() });
        }
    }

    // Compatible
    if (auto iter = req->createoptions().find(DELEGATE_ENCRYPT); iter != req->createoptions().end()) {
        if (auto decryptData = DecryptDelegateData(iter->second, dataKey); decryptData.IsSome()) {
            (void)runtimeConf.mutable_posixenvs()->insert({ ENV_DELEGATE_DECRYPT, decryptData.Get() });
        }
    }
}

void SetUserEnv(const std::shared_ptr<messages::DeployInstanceRequest> &req, messages::RuntimeConfig &runtimeConf)
{
    if (!req->envinfo().empty()) {
        // 1. The envs of functions from CLI are encrypted.
        // 2. The envs of functions from Local are not encrypted(NO_CRYPTO).
        auto decryptEnvResult = Crypto::GetInstance().Decrypt(req->envinfo(), req->envkey(), req->cryptoalgorithm());
        if (decryptEnvResult.IsSome()) {
            ParseEnvInfoJson(std::string(decryptEnvResult.Get().GetData(), decryptEnvResult.Get().GetSize()),
                             runtimeConf);
        } else {
            YRLOG_ERROR("failed to decrypt user env");
        }
    }

    if (!req->encrypteduserdata().empty()) {
        // Currently, encrypted_user_data is used only in the FG.
        auto decryptUserData =
            Crypto::GetInstance().Decrypt(req->encrypteduserdata(), req->envkey(), req->cryptoalgorithm());
        if (decryptUserData.IsSome()) {
            (void)runtimeConf.mutable_userenvs()->insert(
                { "func-RUNTIME_USERDATA",
                  std::string(decryptUserData.Get().GetData(), decryptUserData.Get().GetSize()) });
        } else {
            YRLOG_ERROR("failed to decrypt user data");
        }
    }
}

void ParseEnvInfoJson(const std::string &parsedJson, messages::RuntimeConfig &runtimeConf)
{
    try {
        auto contentJson = nlohmann::json::parse(parsedJson);
        for (const auto &item : contentJson.items()) {
            std::string key;
            // check whether prefix "func-" is present, if so, directly assign to the key string
            // if not, add prefix "func-" to the key string
            if (item.key().rfind(RUNTIME_ENV_PREFIX, 0) != 0) {
                key = RUNTIME_ENV_PREFIX + item.key();
            } else {
                key = item.key();
            }
            if (item.value().is_number()) {
                int value = item.value();
                std::string valueStr = std::to_string(value);
                (void)runtimeConf.mutable_userenvs()->insert({ key, valueStr });
            } else {
                (void)runtimeConf.mutable_userenvs()->insert({ key, item.value() });
            }
        }
    } catch (const std::exception &e) {
        YRLOG_WARN("failed to parson envinfo as json string, exception e.what():{}", e.what());
    }
}

void SetCreateOptions(const std::shared_ptr<messages::DeployInstanceRequest> &req, messages::RuntimeConfig &runtimeConf,
                      const std::vector<std::string> &keyList)
{
    auto createOptions = req->createoptions();
    for (const auto &key : keyList) {
        auto it = createOptions.find(key);
        if (it == createOptions.end()) {
            YRLOG_DEBUG("{} not found in createOptions", key);
            continue;
        }
        auto dataOpt = Crypto::GetInstance().Decrypt(it->second);
        if (dataOpt.IsNone()) {
            YRLOG_ERROR("failed to decrypt key: {}", key);
            continue;
        }
        (void)runtimeConf.mutable_userenvs()->insert(
            { key, std::string(dataOpt.Get().GetData(), dataOpt.Get().GetSize()) });
    }
}

void SetTLSConfig(const std::shared_ptr<messages::DeployInstanceRequest> &req, messages::RuntimeConfig &runtimeConf)
{
    runtimeConf.mutable_tlsconfig()->set_enableservermode(req->enableservermode());
    runtimeConf.mutable_tlsconfig()->set_posixport(req->posixport());
    runtimeConf.mutable_tlsconfig()->set_serverauthenable(req->enableauthservercert());
    runtimeConf.mutable_tlsconfig()->set_rootcertdata(req->serverrootcertdata());
    runtimeConf.mutable_tlsconfig()->set_token(req->serverauthtoken());
    runtimeConf.mutable_tlsconfig()->set_salt(req->salt());
    runtimeConf.mutable_tlsconfig()->set_servernameoverride(req->servernameoverride());
    runtimeConf.mutable_tlsconfig()->set_dsauthenable(req->runtimedsauthenable());
    runtimeConf.mutable_tlsconfig()->set_dsencryptenable(req->runtimedsencryptenable());
    runtimeConf.mutable_tlsconfig()->set_accesskey(req->accesskey());
    runtimeConf.mutable_tlsconfig()->set_securitykey(req->securitykey());
    runtimeConf.mutable_tlsconfig()->set_dsclientpublickey(req->runtimedsclientpublickey());
    runtimeConf.mutable_tlsconfig()->set_dsclientprivatekey(req->runtimedsclientprivatekey());
    runtimeConf.mutable_tlsconfig()->set_dsserverpublickey(req->runtimedsserverpublickey());
    SetTenantCredConfig(req, runtimeConf);
    YRLOG_INFO("{}|put credential: {} into runtime", req->tenantid(),
               SensitiveValue(runtimeConf.tlsconfig().tenantcredentials().secretkey()).GetMaskData());
}

void SetTenantCredConfig(const std::shared_ptr<messages::DeployInstanceRequest> &req,
                         messages::RuntimeConfig &runtimeConf)
{
    if (req->tenantcredentials().accesskey().empty() || !req->tenantcredentials().iscredential()) {
        YRLOG_DEBUG("tenant credentials is empty, skip");
        return;
    }

    auto tenantCredentials = runtimeConf.mutable_tlsconfig()->mutable_tenantcredentials();
    auto componentDK = GetComponentDataKey();
    YRLOG_WARN("{}|no component data key, do not decrypt credential", req->requestid());
    tenantCredentials->set_iscredential(req->tenantcredentials().iscredential());
    tenantCredentials->set_accesskey(req->tenantcredentials().accesskey());
    tenantCredentials->set_secretkey(req->tenantcredentials().secretkey());
    tenantCredentials->set_datakey(req->tenantcredentials().datakey());
    return;
}

void SetDeploymentConfig(messages::DeploymentConfig *deploymentConf,
                         const std::shared_ptr<messages::DeployInstanceRequest> &req)
{
    ASSERT_IF_NULL(req);
    deploymentConf->set_bucketid(req->funcdeployspec().bucketid());
    deploymentConf->set_objectid(req->funcdeployspec().objectid());
    deploymentConf->set_bucketurl(req->funcdeployspec().bucketurl());
    auto mutableLayers = deploymentConf->mutable_layers();
    mutableLayers->CopyFrom(req->funcdeployspec().layers());
    deploymentConf->set_deploydir(req->funcdeployspec().deploydir());
    deploymentConf->set_storagetype(req->funcdeployspec().storagetype());
    if (req->funcdeployspec().storagetype() == COPY_STORAGE_TYPE) {
        deploymentConf->set_objectid(req->funcdeployspec().deploydir());
    }

    // Get the specific value from createOptions to deployOptions.
    for (const std::string &str : DEPLOY_OPTION_KEYS) {
        if (auto iter(req->createoptions().find(str)); iter != req->createoptions().end()) {
            (*deploymentConf->mutable_deployoptions())[str] = iter->second;
        }
    }

    for (const std::string &str : VIRTUALENV_KEYS) {
        if (auto iter(req->createoptions().find(str)); iter != req->createoptions().end()) {
            (*deploymentConf->mutable_deployoptions())[str] = iter->second;
        }
    }
}

litebus::Option<std::string> DecryptDelegateData(const std::string &str, const std::string &dataKey)
{
    nlohmann::json parser;
    try {
        parser = nlohmann::json::parse(str);
    } catch (std::exception &) {
        YRLOG_ERROR("Decrypt Delegate data failed.");
        return litebus::None();
    }

    std::string key;  // Encryption Key
    if (parser.find(ENV_KEY) != parser.end()) {
        key = parser.at(ENV_KEY).get<std::string>();
        (void)parser.erase(ENV_KEY);
    }
    std::string algorithm;  // Encryption algorithm
    if (parser.find(CRYPTO_ALGORITHM_STR) != parser.end()) {
        algorithm = parser.at(CRYPTO_ALGORITHM_STR).get<std::string>();
        (void)parser.erase(CRYPTO_ALGORITHM_STR);
    }

    nlohmann::json items;
    for (const auto &item : parser.items()) {
        // Ignore empty value. Otherwise, exception is reported.
        const std::string &cipher = item.value();
        if (cipher.empty()) {
            continue;
        }

        if (auto op = Crypto::GetInstance().Decrypt(cipher, key, algorithm); op.IsSome()) {
            items[item.key()] = std::string(op.Get().GetData(), op.Get().GetSize());
        } else {
            YRLOG_ERROR("Decrypt delegate data: {} failed.", item.value());
        }
    }
    std::string result;
    try {
        result = items.dump();
    } catch (std::exception &e) {
        YRLOG_ERROR("dump items failed");
    }
    if (result == "null") {
        return litebus::None();
    }
    return result;
}

messages::DeploymentConfig SetDeploymentConfigOfLayer(const std::shared_ptr<messages::DeployInstanceRequest> &req,
                                                      const std::shared_ptr<messages::Layer> &layer)
{
    ASSERT_IF_NULL(layer);
    messages::DeploymentConfig deploymentConf;
    deploymentConf.set_bucketid(layer->bucketid());
    deploymentConf.set_objectid(layer->objectid());
    deploymentConf.set_hostname(layer->hostname());
    deploymentConf.set_securitytoken(layer->securitytoken());
    deploymentConf.set_temporaryaccesskey(layer->temporaryaccesskey());
    deploymentConf.set_temporarysecretkey(layer->temporarysecretkey());
    deploymentConf.set_sha256(layer->sha256());
    deploymentConf.set_sha512(layer->sha512());
    deploymentConf.set_deploydir(req->funcdeployspec().deploydir());

    return deploymentConf;
}

void SetStartRuntimeInstanceRequestConfig(const std::unique_ptr<messages::StartInstanceRequest> &startInstanceRequest,
                                          const std::shared_ptr<messages::DeployInstanceRequest> &req)
{
    ASSERT_IF_NULL(req);
    auto runtimeInstanceInfo = SetRuntimeInstanceInfo(req);
    *startInstanceRequest->mutable_runtimeinstanceinfo() = std::move(runtimeInstanceInfo);
    *startInstanceRequest->mutable_scheduleoption() = std::move(req->scheduleoption());
    startInstanceRequest->set_type(!req->has_container() ? static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME)
                                                         : static_cast<int32_t>(EXECUTOR_TYPE::CONTAINER));
}

messages::RuntimeInstanceInfo SetRuntimeInstanceInfo(const std::shared_ptr<messages::DeployInstanceRequest> &req)
{
    ASSERT_IF_NULL(req);
    messages::RuntimeInstanceInfo runtimeInstanceInfo;
    auto runtimeConfig = SetRuntimeConfig(req);
    *runtimeInstanceInfo.mutable_runtimeconfig() = std::move(runtimeConfig);
    SetDeploymentConfig(runtimeInstanceInfo.mutable_deploymentconfig(), req);

    runtimeInstanceInfo.set_instanceid(req->instanceid());
    runtimeInstanceInfo.set_traceid(req->traceid());
    runtimeInstanceInfo.set_requestid(req->requestid());
    runtimeInstanceInfo.set_gracefulshutdowntime(req->gracefulshutdowntime());
    runtimeInstanceInfo.set_warmuptype(req->warmuptype());
    if (req->has_container()) {
        *runtimeInstanceInfo.mutable_container() = std::move(req->container());
    }
    if (req->has_bootstrapconfig()) {
        *runtimeInstanceInfo.mutable_bootstrapconfig() = std::move(req->bootstrapconfig());
    }
    if (req->has_snapshotinfo()) {
        *runtimeInstanceInfo.mutable_snapshotinfo() = std::move(req->snapshotinfo());
    }
    return runtimeInstanceInfo;
}

void SetStopRuntimeInstanceRequest(messages::StopInstanceRequest &stopInstanceRequest,
                                   const std::shared_ptr<messages::KillInstanceRequest> &req)
{
    ASSERT_IF_NULL(req);
    stopInstanceRequest.set_runtimeid(req->runtimeid());
    stopInstanceRequest.set_requestid(req->requestid());
    stopInstanceRequest.set_traceid(req->traceid());
}

std::unordered_map<std::string, std::shared_ptr<messages::Layer>> SetDeployingRequestLayers(
    const messages::FuncDeploySpec &spec)
{
    auto layers = std::unordered_map<std::string, std::shared_ptr<messages::Layer>>();
    for (const auto &layer : spec.layers()) {
        std::string layerDir = litebus::os::Join(spec.deploydir(), "layer");
        std::string bucketDir = litebus::os::Join(layerDir, layer.bucketid());
        std::string objectFile = litebus::os::Join(bucketDir, layer.objectid());
        layers[objectFile] = std::make_shared<messages::Layer>(layer);
    }
    return layers;
}

std::string JoinEntryFile(const std::shared_ptr<messages::DeployInstanceRequest> &req, const std::string &entryFileName)
{
    ASSERT_IF_NULL(req);
    if (req->funcdeployspec().storagetype() == LOCAL_STORAGE_TYPE) {
        return req->funcdeployspec().deploydir();
    }
    auto layerDir = litebus::os::Join(req->funcdeployspec().deploydir(), RUNTIME_LAYER_DIR_NAME);
    auto funcDir = litebus::os::Join(layerDir, RUNTIME_FUNC_DIR_NAME);
    auto bucketDir = litebus::os::Join(funcDir, req->funcdeployspec().bucketid());
    auto objectDir = litebus::os::Join(bucketDir, req->funcdeployspec().objectid());
    return litebus::os::Join(objectDir, entryFileName);
}

bool HasSuffix(const std::string &source, const std::string &suffix)
{
    if (source.length() >= suffix.length()) {
        return (source.compare(source.length() - suffix.length(), suffix.length(), suffix) == 0);
    }
    return false;
}

bool IsDir(const std::string &path)
{
    struct stat buffer {};
    if (stat(path.c_str(), &buffer) != 0) {
        return false;
    }
    if ((buffer.st_mode & S_IFDIR) == 0) {
        return false;
    }
    return true;
}

// Fields splits the string str around each instance of one or more consecutive char ch
std::vector<std::string> Field(const std::string &str, const char &ch)
{
    std::vector<std::string> res;
    size_t i = 0;
    size_t j = 0;
    bool start = true;
    while (j < str.length()) {
        if (str[j] != ch && start) {
            i = j;
            start = !start;
        }

        if (str[j] == ch && !start) {
            res.push_back(str.substr(i, j - i));
            start = !start;
        }

        j++;
    }

    if (!start) {
        res.push_back(str.substr(i, j - i));
    }
    return res;
}

void AddLayer(const std::shared_ptr<messages::DeployInstanceRequest> &req)
{
    auto iter = req->createoptions().find(DELEGATE_LAYER_DOWNLOAD);
    if (iter == req->createoptions().end()) {
        return;
    }

    auto infos = ParseDelegateDownloadInfos(iter->second);
    for (const auto &info : infos) {
        auto layer = req->mutable_funcdeployspec()->add_layers();
        layer->set_appid(info.appID);
        layer->set_bucketid(info.bucketID);
        layer->set_objectid(info.objectID);
        layer->set_hostname(info.hostName);
        layer->set_sha256(info.sha256);
        layer->set_sha512(info.sha512);
        layer->set_securitytoken(info.securityToken);
        layer->set_temporaryaccesskey(info.temporaryAccessKey);
        layer->set_temporarysecretkey(info.temporarySecretKey);
    }
}

void ParseMountConfig(messages::RuntimeConfig &runtimeConfig, const std::string &str)
{
    nlohmann::json parser;
    auto funcMountConfig = runtimeConfig.mutable_funcmountconfig();
    try {
        parser = nlohmann::json::parse(str);
    } catch (std::exception &error) {
        YRLOG_WARN("parse mount configs {} failed, error: {}", str, error.what());
        return;
    }

    if (parser.find(MOUNT_USER) != parser.end()) {
        auto mountUser = funcMountConfig->mutable_funcmountuser();
        const auto &user = parser.at(MOUNT_USER);
        if (user.find(MOUNT_USER_ID) != user.end() && user.at(MOUNT_USER_ID).is_number_integer()) {
            mountUser->set_userid(user.at(MOUNT_USER_ID));
        }
        if (user.find(functionsystem::MOUNT_USER_GROUP_ID) != user.end()
            && user.at(functionsystem::MOUNT_USER_GROUP_ID).is_number_integer()) {
            mountUser->set_groupid(user.at(functionsystem::MOUNT_USER_GROUP_ID));
        }
    }

    if (parser.find(FUNC_MOUNTS) != parser.end()) {
        const auto &funcMounts = parser.at(FUNC_MOUNTS);
        for (auto &m : funcMounts) {
            auto funcMount = funcMountConfig->add_funcmounts();
            if (m.find(FUNC_MOUNT_TYPE) != m.end()) {
                funcMount->set_mounttype(std::string(m.at(FUNC_MOUNT_TYPE)));
            }
            if (m.find(FUNC_MOUNT_RESOURCE) != m.end()) {
                funcMount->set_mountresource(std::string(m.at(FUNC_MOUNT_RESOURCE)));
            }
            if (m.find(FUNC_MOUNT_SHARE_PATH) != m.end()) {
                funcMount->set_mountsharepath(std::string(m.at(FUNC_MOUNT_SHARE_PATH)));
            }
            if (m.find(FUNC_MOUNT_LOCAL_MOUNT_PATH) != m.end()) {
                funcMount->set_localmountpath(std::string(m.at(FUNC_MOUNT_LOCAL_MOUNT_PATH)));
            }
            if (m.find(FUNC_MOUNT_STATUS) != m.end()) {
                funcMount->set_status(std::string(m.at(FUNC_MOUNT_STATUS)));
            }
        }
    }
}

std::shared_ptr<messages::DeployRequest> BuildDeployRequestConfigByLayerInfo(
    const Layer &info, const std::shared_ptr<messages::DeployRequest> &config)
{
    config->mutable_deploymentconfig()->set_objectid(info.objectID);
    config->mutable_deploymentconfig()->set_bucketid(info.bucketID);
    config->mutable_deploymentconfig()->set_hostname(info.hostName);
    config->mutable_deploymentconfig()->set_securitytoken(info.securityToken);
    config->mutable_deploymentconfig()->set_temporaryaccesskey(info.temporaryAccessKey);
    config->mutable_deploymentconfig()->set_temporarysecretkey(info.temporarySecretKey);
    config->mutable_deploymentconfig()->set_storagetype(info.storageType);
    config->mutable_deploymentconfig()->set_sha512(info.sha512);
    config->mutable_deploymentconfig()->set_sha256(info.sha256);
    if (info.storageType == LOCAL_STORAGE_TYPE) {
        config->mutable_deploymentconfig()->set_deploydir(info.codePath);
    } else if (info.storageType == COPY_STORAGE_TYPE) {
        // when func code need to be copied, reuse objectID as the codePath
        config->mutable_deploymentconfig()->set_objectid(info.codePath);
    }
    return config;
}

}  // namespace functionsystem::function_agent
