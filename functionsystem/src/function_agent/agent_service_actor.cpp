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

#include "agent_service_actor.h"

#include <async/async.hpp>
#include <async/asyncafter.hpp>
#include <async/defer.hpp>
#include <chrono>
#include <limits>
#include <memory>
#include <sys/inotify.h>

#include "async/future.hpp"
#include "common/constants/actor_name.h"
#include "common/logs/logging.h"
#include "common/metrics/metrics_adapter.h"
#include "common/resource_view/resource_tool.h"
#include "common/types/instance_state.h"
#include "common/utils/actor_worker.h"
#include "common/utils/exec_utils.h"
#include "common/utils/generate_message.h"
#include "common/utils/struct_transfer.h"
#include "function_agent/common/constants.h"
#include "function_agent/common/utils.h"

namespace functionsystem::function_agent {
using messages::RuleType;

static const int32_t GRACE_SHUTDOWN_DELAY = 3;
static const int32_t GRACE_SHUTDOWN_TIMEOUT_MS = 1000;
static const uint32_t DOWNLOAD_CODE_RETRY_TIMES = 5;
static const uint32_t GRACE_SHUTDOWN_CLEAN_AGENT_TIMEOUT = 3 * UPDATE_AGENT_STATUS_TIMEOUT;  // 1500ms
static const std::string SF_CONFIG_PATH = "SF_CONFIG_PATH";
static const std::string SF_SCHEDULE_TIMEOUT_MS = "SF_SCHEDULE_TIMEOUT_MS";
static const std::string SF_INSTANCE_TYPE_NOTE = "SF_INSTANCE_TYPE_NOTE";
static const std::string SF_DELEGATE_DIRECTORY_INFO = "SF_DELEGATE_DIRECTORY_INFO";
static const std::string SF_INVOKE_LABELS = "SF_INVOKE_LABELS";
static const std::string SF_FUNCTION_SIGNATURE = "SF_FUNCTION_SIGNATURE";
static const std::string RUNTIME_MICROSERVICE_AZ_ENV = "RUNTIME_MICROSERVICE_AZ";
static const std::string INSTANCE_AZ = "az";

DeployResult AgentServiceActor::PrepareSharedDir(std::shared_ptr<messages::DeployInstanceRequest> &req)
{
    auto iter = req->createoptions().find(DELEGATE_SHARED_DIRECTORY);
    if (iter == req->createoptions().end()) {
        return DeployResult{};
    }
    auto deployDir = GetDeployDir();
    auto dest = deployers_[SHARED_DIR_STORAGE_TYPE]->GetDestination(deployDir, "", iter->second);
    auto ttlIter = req->createoptions().find(DELEGATE_SHARED_DIRECTORY_TTL);
    if (ttlIter != req->createoptions().end()) {
        try {
            deployers_[SHARED_DIR_STORAGE_TYPE]->SetTTL(dest, std::stoi(ttlIter->second));
        } catch (const std::invalid_argument &e) {
            YRLOG_WARN("{}|shared dir ttl is invalid, {}", req->instanceid(), ttlIter->second);
            auto deployRes = DeployResult{};
            deployRes.status = Status(StatusCode::FUNC_AGENT_INVALID_DEPLOY_DIRECTORY, "shared dir ttl is invalid");
            return deployRes;
        } catch (const std::out_of_range &e) {
            YRLOG_WARN("{}|shared dir ttl is out of range, {}", req->instanceid(), ttlIter->second);
            deployers_[SHARED_DIR_STORAGE_TYPE]->SetTTL(dest, std::numeric_limits<int>::max());
        }
    }
    auto request = std::make_shared<messages::DeployRequest>();
    request->mutable_deploymentconfig()->set_deploydir(deployDir);
    request->mutable_deploymentconfig()->set_objectid(iter->second);
    auto res = deployers_[SHARED_DIR_STORAGE_TYPE]->Deploy(request);
    if (res.status.IsError()) {
        YRLOG_WARN("failed to create shared dir, {}", res.status.ToString());
        return res;
    }
    (void)req->mutable_createoptions()->insert({ SHARED_DIRECTORY_PATH, dest });
    AddCodeRefer(dest, req->instanceid(), deployers_[SHARED_DIR_STORAGE_TYPE]);
    YRLOG_INFO("success to create shared dir: {}", dest);
    return DeployResult{};
}

messages::DeployInstanceResponse AgentServiceActor::InitDeployInstanceResponse(
    const int32_t code, const std::string &message, const messages::DeployInstanceRequest &source)
{
    messages::DeployInstanceResponse target;
    target.set_instanceid(source.instanceid());
    target.set_requestid(source.requestid());
    target.set_code(code);
    target.set_message(message);
    return target;
}

void AgentServiceActor::InitKillInstanceResponse(messages::KillInstanceResponse *target,
                                                 const messages::KillInstanceRequest &source)
{
    target->set_instanceid(source.instanceid());
    target->set_requestid(source.requestid());
}

void AgentServiceActor::DeployInstance(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    auto deployInstanceRequest = std::make_shared<messages::DeployInstanceRequest>();
    if (!deployInstanceRequest->ParseFromString(msg)) {
        YRLOG_ERROR("{}|{}|failed to parse request for instance({}) deployment.", deployInstanceRequest->traceid(),
                    deployInstanceRequest->requestid(), deployInstanceRequest->instanceid());
        return;
    }
    YRLOG_DEBUG("{}|{}|DeployInstance request for instance({}): {}", deployInstanceRequest->traceid(),
                deployInstanceRequest->requestid(), deployInstanceRequest->instanceid(),
                deployInstanceRequest->ShortDebugString());
    const std::string &requestID = deployInstanceRequest->requestid();
    // if functionAgent registration to localScheduler is not complete, refuse request from localScheduler
    if (!isRegisterCompleted_) {
        YRLOG_ERROR(
            "{}|{}|functionAgent registration to localScheduler is not complete, ignore deploy instance({}) request.",
            deployInstanceRequest->traceid(), requestID, deployInstanceRequest->instanceid());
        return;
    }

    // 1.if instance or request id is illegal, don't deploy and response.
    if (requestID.empty() || deployInstanceRequest->instanceid().empty()) {
        YRLOG_ERROR("{}|request or instance's id is illegal.", deployInstanceRequest->traceid());
        auto resp = InitDeployInstanceResponse(static_cast<int32_t>(StatusCode::FUNC_AGENT_REQUEST_ID_ILLEGAL_ERROR),
                                               "request or instance's id is illegal.", *deployInstanceRequest);
        (void)Send(from, "DeployInstanceResponse", resp.SerializeAsString());
        return;
    }

    // 2.if the deployer not found, don't deploy and response.
    if (auto storageType = deployInstanceRequest->funcdeployspec().storagetype();
        deployers_.find(storageType) == deployers_.end()) {
        YRLOG_ERROR("{}|{}|can't find a deployer for storage type({}), instance({}).", deployInstanceRequest->traceid(),
                    requestID, storageType, deployInstanceRequest->instanceid());

        auto resp = InitDeployInstanceResponse(static_cast<int32_t>(StatusCode::FUNC_AGENT_INVALID_DEPLOYER_ERROR),
                                               "can't found a Deployer for storage type#" + storageType,
                                               *deployInstanceRequest);
        (void)Send(from, "DeployInstanceResponse", resp.SerializeAsString());
        return;
    }

    YRLOG_DEBUG("s3Config credentialType: {}", s3Config_.credentialType);
    std::string storageType = deployInstanceRequest->funcdeployspec().storagetype();
    // 2.2 if proxy credential rotation, but not have AK/SK/token
    if (storageType == S3_STORAGE_TYPE && s3Config_.credentialType == CREDENTIAL_TYPE_ROTATING_CREDENTIALS) {
        if (const auto token = deployInstanceRequest->funcdeployspec().token(); token.empty()) {
            YRLOG_ERROR("{}|{}|can't find token for credential type({}), instance({}).",
                        deployInstanceRequest->traceid(), requestID, s3Config_.credentialType,
                        deployInstanceRequest->instanceid());
            auto resp = InitDeployInstanceResponse(static_cast<int32_t>(StatusCode::FUNC_AGENT_INVALID_TOKEN_ERROR),
                                                   "can't find token for credential type: " + s3Config_.credentialType,
                                                   *deployInstanceRequest);
            (void)Send(from, "DeployInstanceResponse", resp.SerializeAsString());
            return;
        }
        if (const auto accessKey = deployInstanceRequest->funcdeployspec().accesskey(); accessKey.empty()) {
            YRLOG_ERROR("{}|{}|can't find accessKey for credential type({}), instance({}).",
                        deployInstanceRequest->traceid(), requestID, s3Config_.credentialType,
                        deployInstanceRequest->instanceid());
            auto resp = InitDeployInstanceResponse(
                static_cast<int32_t>(StatusCode::FUNC_AGENT_INVALID_ACCESS_KEY_ERROR),
                "can't find accessKey for credential type: " + s3Config_.credentialType, *deployInstanceRequest);
            (void)Send(from, "DeployInstanceResponse", resp.SerializeAsString());
            return;
        }
        if (const auto secretAccessKey = deployInstanceRequest->funcdeployspec().secretaccesskey();
            secretAccessKey.empty()) {
            YRLOG_ERROR("{}|{}|can't find secretAccessKey for credential type({}), instance({}).",
                        deployInstanceRequest->traceid(), requestID, s3Config_.credentialType,
                        deployInstanceRequest->instanceid());
            auto resp = InitDeployInstanceResponse(
                static_cast<int32_t>(StatusCode::FUNC_AGENT_INVALID_SECRET_ACCESS_KEY_ERROR),
                "can't find secretAccessKey for credential type#" + s3Config_.credentialType, *deployInstanceRequest);
            (void)Send(from, "DeployInstanceResponse", resp.SerializeAsString());
            return;
        }
    }

    // 3.if createOption is not null, set network
    auto iter = deployInstanceRequest->createoptions().find(NETWORK_CONFIG);
    if (iter != deployInstanceRequest->createoptions().end()
        && !SetNetwork(NetworkTool::ParseNetworkConfig(iter->second))) {
        YRLOG_ERROR("{}|{}|failed to set network for instance({}), network config: {}.",
                    deployInstanceRequest->traceid(), requestID, deployInstanceRequest->instanceid(), iter->second);
        auto resp = InitDeployInstanceResponse(static_cast<int32_t>(StatusCode::FUNC_AGENT_SET_NETWORK_ERROR),
                                               "set network failed", *deployInstanceRequest);
        (void)Send(from, "DeployInstanceResponse", resp.SerializeAsString());
        return;
    }

    iter = deployInstanceRequest->createoptions().find(PROBER_CONFIG);
    if (iter != deployInstanceRequest->createoptions().end()) {
        auto configs = NetworkTool::ParseProberConfig(iter->second);
        if (configs.empty() || !NetworkTool::Probe(configs)) {
            YRLOG_ERROR("{}|{}|failed to config probe({}) for instance({}).", deployInstanceRequest->traceid(),
                        requestID, iter->second, deployInstanceRequest->instanceid());
            auto resp = InitDeployInstanceResponse(static_cast<int32_t>(StatusCode::FUNC_AGENT_NETWORK_WORK_ERROR),
                                                   "network function work error", *deployInstanceRequest);
            (void)Send(from, "DeployInstanceResponse", resp.SerializeAsString());
            return;
        }
        (void)litebus::AsyncAfter(PING_TIME_OUT_MS, GetAID(), &AgentServiceActor::StartProbers, configs);
    }

    YRLOG_INFO("{}|{}|received a deploy instance({}) request from {}", deployInstanceRequest->traceid(), requestID,
               deployInstanceRequest->instanceid(), std::string(from));
    deployingRequest_[requestID] = { from, deployInstanceRequest };
    gracefulShutdownTime_ = deployInstanceRequest->gracefulshutdowntime() + GRACE_SHUTDOWN_DELAY;

    // 4. add shared dir
    if (auto res = PrepareSharedDir(deployInstanceRequest); res.status.IsError()) {
        auto resp =
            InitDeployInstanceResponse(static_cast<int32_t>(StatusCode::FUNC_AGENT_INVALID_DEPLOYER_ERROR),
                                       "failed to create shared dir, " + res.status.ToString(), *deployInstanceRequest);
        (void)Send(from, "DeployInstanceResponse", resp.SerializeAsString());
        return;
    }
    // 5. deploy code package (including main, layer, and delegate package) and start runtime
    auto parameters = BuildDeployerParameters(deployInstanceRequest);
    DownloadCodeAndStartRuntime(parameters, deployInstanceRequest);
}

void AgentServiceActor::DownloadCodeAndStartRuntime(
    const std::shared_ptr<std::queue<DeployerParameters>> &deployObjects,
    const std::shared_ptr<messages::DeployInstanceRequest> &req)
{
    if (IsDownloadFailed(req)) {
        DeleteCodeReferByDeployInstanceRequest(req);
        return;
    }
    if (deployObjects->empty()) {
        YRLOG_INFO("{}|directly start runtime({}).", req->requestid(), req->instanceid());
        PrepareEnv(req).OnComplete(
            litebus::Defer(GetAID(), &AgentServiceActor::StartRuntime, req, std::placeholders::_1));
        return;
    }

    auto deployObject = deployObjects->front();
    deployObjects->pop();

    // working dir don't need to increase code refer
    if (!IsDelegateWorkingDirPath(deployObject)) {
        // every time before download code, code refer should increase
        AddCodeRefer(deployObject.destination, deployObject.request->instanceid(), deployObject.deployer);
    }

    bool isMonopoly = req->scheduleoption().schedpolicyname() == MONOPOLY_SCHEDULE;
    if (auto iter = deployingObjects_.find(deployObject.destination); iter != deployingObjects_.end()) {
        // code package is downloading
        YRLOG_DEBUG("{}|{}|code package({}) is downloading. instanceID({})", req->traceid(), req->requestid(),
                    deployObject.destination, req->instanceid());
        iter->second.GetFuture().OnComplete(litebus::Defer(GetAID(), &AgentServiceActor::GetDownloadCodeResult,
                                                           deployObjects, req, deployObject.destination,
                                                           std::placeholders::_1));
    } else if (deployObject.deployer->IsDeployed(deployObject.destination, isMonopoly)) {
        // code package had been downloaded
        YRLOG_DEBUG("{}|{}|code package({}) had been downloaded. instanceID({})", req->traceid(), req->requestid(),
                    deployObject.destination, req->instanceid());
        DownloadCodeAndStartRuntime(deployObjects, req);
    } else {
        // start to download code package
        YRLOG_DEBUG("{}|{}|code package({}) start to download code package. instanceID({})", req->traceid(),
                    req->requestid(), deployObject.destination, req->instanceid());
        (void)deployingObjects_.emplace(deployObject.destination, litebus::Promise<DeployResult>{});
        litebus::Async(GetAID(), &AgentServiceActor::AsyncDownloadCode, deployObject.request, deployObject.deployer)
            .Then(litebus::Defer(GetAID(), &AgentServiceActor::UpdateDeployedObjectByDestination, req,
                                 deployObject.destination, std::placeholders::_1))
            .OnComplete(litebus::Defer(GetAID(), &AgentServiceActor::DownloadCodeAndStartRuntime, deployObjects, req));
    }
}

void AgentServiceActor::DownloadCode(const std::shared_ptr<messages::DeployRequest> &request,
                                     const std::shared_ptr<Deployer> &deployer,
                                     const std::shared_ptr<litebus::Promise<DeployResult>> &promise,
                                     const uint32_t retryTimes)
{
    YRLOG_INFO("start to download code for {}, retry times {}", request->instanceid(), retryTimes);
    auto downloadPromise = litebus::Promise<DeployResult>();
    auto handler = [request, deployer, downloadPromise]() { downloadPromise.SetValue(deployer->Deploy(request)); };
    auto actor = std::make_shared<ActorWorker>();
    auto startTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    (void)actor->AsyncWork(handler).OnComplete([actor](const litebus::Future<Status> &) { actor->Terminate(); });
    downloadPromise.GetFuture().Then([aid(GetAID()), request, deployer, promise, retryTimes,
                                      retryDownloadInterval(retryDownloadInterval_),
                                      startTime](const DeployResult &result) {
        if ((result.status.StatusCode() == StatusCode::FUNC_AGENT_OBS_ERROR_NEED_RETRY
             || result.status.StatusCode() == StatusCode::FUNC_AGENT_OBS_CONNECTION_ERROR)) {
            if (retryTimes < DOWNLOAD_CODE_RETRY_TIMES) {
                litebus::AsyncAfter(retryDownloadInterval, aid, &AgentServiceActor::DownloadCode, request, deployer,
                                    promise, retryTimes + 1);
                return Status::OK();
            }
            // retry exceeds threshold, obs connection error results in alarm
            metrics::MetricsAdapter::GetInstance().SendS3Alarm();
        }
        auto endTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
        YRLOG_INFO("{}|download code cost {} ms", request->instanceid(), (endTime - startTime));
        promise->SetValue(result);
        return Status::OK();
    });
}

litebus::Future<DeployResult> AgentServiceActor::AsyncDownloadCode(
    const std::shared_ptr<messages::DeployRequest> &request, const std::shared_ptr<Deployer> &deployer)
{
    auto promise = std::make_shared<litebus::Promise<DeployResult>>();
    DownloadCode(request, deployer, promise, 1);
    return promise->GetFuture();
}

litebus::Future<Status> AgentServiceActor::PrepareEnv(const DeployInstanceRequest &request)
{
    const auto requestID = request->requestid();
    if (prepareEnvRequest_.find(requestID) != prepareEnvRequest_.end()) {
        return prepareEnvRequest_[requestID]->GetFuture();
    }
    if (!pluginClient_) {
        pluginClient_ = std::make_shared<MultiPluginClient>();
    }
    prepareEnvRequest_[requestID] = std::make_shared<litebus::Promise<Status>>();
    auto promise = prepareEnvRequest_[requestID];
    promise->Associate(pluginClient_->PrepareEnv(request));
    return promise->GetFuture();
}

litebus::Future<bool> AgentServiceActor::RecoverPluginCache(const std::string &message)
{
    if (!pluginClient_) {
        pluginClient_ = std::make_shared<MultiPluginClient>();
    }
    return pluginClient_->RecoverPluginCache(message);
}


bool AgentServiceActor::IsDownloadFailed(const std::shared_ptr<messages::DeployInstanceRequest> &req)
{
    if (failedDownloadRequests_.find(req->requestid()) == failedDownloadRequests_.end()) {
        return false;
    }
    auto from = deployingRequest_[req->requestid()].from;
    auto deployResult = failedDownloadRequests_[req->requestid()];
    auto resp = InitDeployInstanceResponse(static_cast<int32_t>(deployResult.status.StatusCode()),
                                           deployResult.status.GetMessage(), *req);
    (void)Send(from, "DeployInstanceResponse", resp.SerializeAsString());

    deployingRequest_.erase(req->requestid());
    failedDownloadRequests_.erase(req->requestid());
    return true;
}

void AgentServiceActor::GetDownloadCodeResult(const std::shared_ptr<std::queue<DeployerParameters>> &deployObjects,
                                              const std::shared_ptr<messages::DeployInstanceRequest> &req,
                                              const std::string &destination,
                                              const litebus::Future<DeployResult> &result)
{
    // the request failed to download package (notified by other request)
    const auto &deployResult = result.Get();
    if (deployResult.status.IsError()) {
        failedDownloadRequests_[req->requestid()] = deployResult;
        YRLOG_WARN("{}|{}|code package({}) download failed. instanceID({}). ErrCode({}), Msg({})", req->traceid(),
                   req->requestid(), destination, req->instanceid(), fmt::underlying(deployResult.status.StatusCode()),
                   deployResult.status.GetMessage());
    }

    DownloadCodeAndStartRuntime(deployObjects, req);
}

bool AgentServiceActor::UpdateDeployedObjectByDestination(const std::shared_ptr<messages::DeployInstanceRequest> &req,
                                                          const std::string &destination, const DeployResult &result)
{
    YRLOG_DEBUG("Update deployed object.");
    auto iter = deployingObjects_.find(destination);
    if (iter == deployingObjects_.end()) {
        return true;
    }
    // notify other request
    iter->second.SetValue(result);

    // the request failed to download package
    if (result.status.IsError()) {
        failedDownloadRequests_[req->requestid()] = result;
        YRLOG_WARN("{}|{}|code package({}) download failed. instanceID({}). ErrCode({}), Msg({})", req->traceid(),
                   req->requestid(), destination, req->instanceid(), fmt::underlying(result.status.StatusCode()),
                   result.status.GetMessage());
    }

    (void)deployingObjects_.erase(destination);
    return true;
}

void AgentServiceActor::AttachTemporaryAccesskey(const std::string &storageType,
                                                 std::shared_ptr<messages::DeployRequest> &deployRequest,
                                                 const std::shared_ptr<messages::DeployInstanceRequest> &req)
{
    if (storageType == S3_STORAGE_TYPE && s3Config_.credentialType == CREDENTIAL_TYPE_ROTATING_CREDENTIALS
        && (deployRequest->deploymentconfig().temporaryaccesskey().empty()
            && deployRequest->deploymentconfig().temporarysecretkey().empty()
            && deployRequest->deploymentconfig().securitytoken().empty())) {
        YRLOG_DEBUG("attach temporary ak/sk/token");
        deployRequest->mutable_deploymentconfig()->set_temporaryaccesskey(req->funcdeployspec().accesskey());
        deployRequest->mutable_deploymentconfig()->set_temporarysecretkey(req->funcdeployspec().secretaccesskey());
        deployRequest->mutable_deploymentconfig()->set_securitytoken(req->funcdeployspec().token());
    }
}

std::shared_ptr<std::queue<DeployerParameters>> AgentServiceActor::BuildDeployerParameters(
    const std::shared_ptr<messages::DeployInstanceRequest> &req)
{
    std::shared_ptr<std::queue<DeployerParameters>> parameters = std::make_shared<std::queue<DeployerParameters>>();
    // 1. build main package DeployRequest
    std::string storageType = req->funcdeployspec().storagetype();
    // 'copy' storage type generate final deploy path by objectID(src code path)
    if (storageType == COPY_STORAGE_TYPE) {
        req->mutable_funcdeployspec()->set_objectid(req->funcdeployspec().deploydir());
    }

    if (deployers_.find(storageType) == deployers_.end()) {
        YRLOG_ERROR("code package storage type({}) not found", storageType);
        return parameters;
    }
    auto dest = deployers_[storageType]->GetDestination(
        req->funcdeployspec().deploydir(), req->funcdeployspec().bucketid(), req->funcdeployspec().objectid());
    if (!dest.empty()) {
        auto deployRequest = SetDeployRequestConfig(req, nullptr);
        // attach temporary ak/sk/securitytoken when rotating credential in 's3' storage code
        AttachTemporaryAccesskey(storageType, deployRequest, req);
        parameters->push(DeployerParameters{ deployers_[storageType], dest, deployRequest });
    }
    std::string s3DeployDir(req->funcdeployspec().deploydir());  // should be s3 deploy dir for delegate.
    if (auto deployDirIterator = req->createoptions().find("S3_DEPLOY_DIR");
        deployDirIterator != req->createoptions().end()) {
        YRLOG_DEBUG("config s3 deploy dir for delegate.");
        s3DeployDir = deployDirIterator->second;
    }
    // 2. build layers DeployRequest
    AddLayer(req);
    for (const auto &layer : req->funcdeployspec().layers()) {
        auto config(SetDeployRequestConfig(req, std::make_shared<messages::Layer>(layer)));
        config->mutable_deploymentconfig()->set_deploydir(s3DeployDir);
        if (req->scheduleoption().schedpolicyname() == MONOPOLY_SCHEDULE) {
            parameters->push(DeployerParameters{ deployers_[S3_STORAGE_TYPE], s3DeployDir, config });
            continue;
        }
        // Currently, local functions cannot depend on the S3 layer.
        std::string layerDir = litebus::os::Join(s3DeployDir, "layer");
        std::string bucketDir = litebus::os::Join(layerDir, layer.bucketid());
        std::string objectFile = litebus::os::Join(bucketDir, layer.objectid());
        parameters->push(DeployerParameters{ deployers_[S3_STORAGE_TYPE], objectFile, config });
    }

    auto bootstrapIter = req->createoptions().find(DELEGATE_BOOTSTRAP);
    if (bootstrapIter != req->createoptions().end()) {
        (void)req->mutable_createoptions()->insert({ ENV_DELEGATE_BOOTSTRAP, bootstrapIter->second });
    }

    // parse download user code
    auto iter = req->createoptions().find(DELEGATE_DOWNLOAD);
    if (iter == req->createoptions().end()) {
        return parameters;
    }

    auto info = ParseDelegateDownloadInfoByStr(iter->second);
    if (info.IsNone()) {
        YRLOG_ERROR("DELEGATE_DOWNLOAD {} can not parse.", iter->second);
        return parameters;
    }

    // 3. build delegate DeployRequest
    auto config = SetDeployRequestConfig(req, nullptr);
    config->mutable_deploymentconfig()->set_deploydir(s3DeployDir);
    config = BuildDeployRequestConfigByLayerInfo(info.Get(), config);
    if (deployers_.find(info.Get().storageType) == deployers_.end()) {
        YRLOG_ERROR("code package storage type({}) not found", info.Get().storageType);
        return parameters;
    }
    if (info.Get().storageType == WORKING_DIR_STORAGE_TYPE) {
        // 'working_dir' storage type generate final deploy path by objectID(src appID = instanceID)
        config->mutable_deploymentconfig()->set_objectid(req->instanceid());
        // pass codePath (src working dir zip file)
        config->mutable_deploymentconfig()->set_bucketid(info.Get().codePath);
    }
    // attach temporary ak/sk/securitytoken when rotating credential in delegate download 's3' storage code
    AttachTemporaryAccesskey(info.Get().storageType, config, req);
    auto destination = deployers_[info.Get().storageType]->GetDestination(config->deploymentconfig().deploydir(),
                                                                          config->deploymentconfig().bucketid(),
                                                                          config->deploymentconfig().objectid());
    // for monopoly(faas function) will deploy to a fix path(/dcache)
    if (info.Get().storageType == S3_STORAGE_TYPE && req->scheduleoption().schedpolicyname() == MONOPOLY_SCHEDULE) {
        destination = config->deploymentconfig().deploydir();
    }
    if (info.Get().storageType == WORKING_DIR_STORAGE_TYPE) {
        // delegate working dir
        req->mutable_funcdeployspec()->set_deploydir(destination);
        req->mutable_funcdeployspec()->set_storagetype(WORKING_DIR_STORAGE_TYPE);
        // pass unzipped working dir to runtime_manager
        (void)req->mutable_createoptions()->insert({ UNZIPPED_WORKING_DIR, destination });
        // pass origin config (src working dir zip file)
        (void)req->mutable_createoptions()->insert({ YR_WORKING_DIR, info.Get().codePath });
        // pass is user start process to app(runtime)
        (void)req->mutable_createoptions()->insert(
            { YR_APP_MODE, (IsAppDriver(req->createoptions())) ? "true" : "false" });
    } else {
        (void)req->mutable_createoptions()->insert({ ENV_DELEGATE_DOWNLOAD, destination });
        (void)req->mutable_createoptions()->insert({ ENV_DELEGATE_DOWNLOAD_STORAGE_TYPE, info.Get().storageType });
    }
    parameters->push(DeployerParameters{ deployers_[info.Get().storageType], destination, config });
    return parameters;
}

bool AgentServiceActor::SetNetwork(const std::vector<NetworkConfig> &configs)
{
    for (auto &config : configs) {
        if (config.firewallConfig.IsSome() && !NetworkTool::SetFirewall(config.firewallConfig.Get())) {
            YRLOG_ERROR("set firewall failed.");
            return false;
        }

        // get local pod ip
        auto localIP = litebus::os::GetEnv("POD_IP");
        if (localIP.IsNone() || localIP.Get().empty()) {
            YRLOG_ERROR("pod ip is invalid.");
            return false;
        }

        auto addrInfo = NetworkTool::GetAddr(localIP.Get());
        if (addrInfo.IsNone()) {
            YRLOG_ERROR("can not get address info by {}.", localIP.Get());
            return false;
        }

        if (config.tunnelConfig.IsSome()
            && !NetworkTool::SetTunnel(TunnelConfig{ config.tunnelConfig.Get().tunnelName,
                                                     config.tunnelConfig.Get().remoteIP, config.tunnelConfig.Get().mode,
                                                     localIP.Get() })) {
            YRLOG_ERROR("set tunnel failed.");
            return false;
        }

        if (config.routeConfig.IsSome()) {
            if (!NetworkTool::SetRoute(RouteConfig{ config.routeConfig.Get().gateway, config.routeConfig.Get().cidr,
                                                    addrInfo.Get().interface })) {
                YRLOG_ERROR("set route failed.");
                return false;
            }

            auto nameserverList = NetworkTool::GetNameServerList();
            for (std::string &nameserver : nameserverList) {
                auto routeConfig = RouteConfig{config.routeConfig.Get().gateway, nameserver,
                                               addrInfo.Get().interface};
                if (!NetworkTool::SetRoute(routeConfig)) {
                    YRLOG_ERROR("set dns server {} route failed.", nameserver);
                    return false;
                }
            }
        }
    }

    return true;
}

void AgentServiceActor::StartProbers(const std::vector<ProberConfig> &configs)
{
    YRLOG_DEBUG("start probe.");
    if (!NetworkTool::Probe(configs)) {
        UpdateAgentStatusToLocal(static_cast<int32_t>(FUNC_AGENT_STATUS_VPC_PROBE_FAILED));
        return;
    }
    litebus::AsyncAfter(PING_TIME_OUT_MS, GetAID(), &AgentServiceActor::StartProbers, configs);
}

litebus::Future<bool> AgentServiceActor::UpdateAgentStatusToLocal(int32_t status, const std::string &msg)
{
    messages::UpdateAgentStatusRequest request;
    auto requestID = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    request.set_requestid(requestID);
    request.set_status(status);
    request.set_message(msg);
    auto promise = std::make_shared<litebus::Promise<bool>>();
    updateAgentStatusInfoPromises_[requestID] = promise;
    (void)Send(localSchedFuncAgentMgrAID_, "UpdateAgentStatus", request.SerializeAsString());

    updateAgentStatusInfos_[requestID] =
        litebus::AsyncAfter(UPDATE_AGENT_STATUS_TIMEOUT, GetAID(), &AgentServiceActor::RetryUpdateAgentStatusToLocal,
                            requestID, request.SerializeAsString());
    return promise->GetFuture();
}

void AgentServiceActor::RetryUpdateAgentStatusToLocal(const std::string &requestID, const std::string &msg)
{
    auto agentStatusInfosIter = updateAgentStatusInfos_.find(requestID);
    if (agentStatusInfosIter == updateAgentStatusInfos_.end()) {
        YRLOG_ERROR("requestID {} is not in UpdateAgentStatusInfos.", requestID);
        return;
    }
    YRLOG_INFO("{}|retry send UpdateAgentStatus to FuncAgentMgr.", requestID);
    Send(localSchedFuncAgentMgrAID_, "UpdateAgentStatus", std::string(msg));
    updateAgentStatusInfos_[requestID] = litebus::AsyncAfter(
        UPDATE_AGENT_STATUS_TIMEOUT, GetAID(), &AgentServiceActor::RetryUpdateAgentStatusToLocal, requestID, msg);
}

void AgentServiceActor::UpdateAgentStatusResponse(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::UpdateAgentStatusResponse response;
    if (msg.empty() || !response.ParseFromString(msg)) {
        YRLOG_ERROR("message {} is invalid!", msg);
        return;
    }
    auto requestID = response.requestid();
    auto agentStatusInfosIter = updateAgentStatusInfos_.find(requestID);
    if (agentStatusInfosIter == updateAgentStatusInfos_.end()) {
        YRLOG_ERROR("requestID {} is not in UpdateAgentStatusInfos.", requestID);
        return;
    }

    if (!isRegisterCompleted_) {
        YRLOG_ERROR("{}|registration is not complete, ignore update agent status response.", requestID);
        return;
    }
    if (auto iter = updateAgentStatusInfoPromises_.find(requestID);
        iter != updateAgentStatusInfoPromises_.end() && iter->second != nullptr) {
        iter->second->SetValue(true);
    }
    (void)litebus::TimerTools::Cancel(agentStatusInfosIter->second);
    (void)updateAgentStatusInfos_.erase(requestID);
    (void)updateAgentStatusInfoPromises_.erase(requestID);
}

void AgentServiceActor::UpdateRuntimeStatus(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::UpdateRuntimeStatusRequest req;
    if (msg.empty() || !req.ParseFromString(msg)) {
        YRLOG_ERROR("update runtime status failed, message {} is invalid!", msg);
        return;
    }
    YRLOG_INFO("{}|receive update runtime status request from {}, status {}", req.requestid(), std::string(from),
               req.status());

    if (!registerRuntimeMgr_.registered || !isRegisterCompleted_) {
        YRLOG_ERROR("{}|registration is not complete, ignore update runtime status request.", req.requestid());
        return;
    }

    UpdateAgentStatusToLocal(req.status(), req.message());

    messages::UpdateRuntimeStatusResponse rsp;
    rsp.set_requestid(req.requestid());
    rsp.set_status(static_cast<int32_t>(StatusCode::SUCCESS));
    rsp.set_message("update runtime status success");
    (void)Send(from, "UpdateRuntimeStatusResponse", rsp.SerializeAsString());
}

void AgentServiceActor::KillInstance(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    auto killInstanceRequest = std::make_shared<messages::KillInstanceRequest>();
    if (!killInstanceRequest->ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse request for kill instance.");
        return;
    }

    const std::string &requestID = killInstanceRequest->requestid();
    // if functionAgent registration to localScheduler is not complete, refuse request from localScheduler
    if (!registerRuntimeMgr_.registered || !isRegisterCompleted_) {
        YRLOG_ERROR("{}|registration is not complete, ignore kill request for instance({}).", requestID,
                    killInstanceRequest->instanceid());
        return;
    }

    // stop instance
    messages::KillInstanceResponse rsp;

    auto deployerIter = deployers_.find(killInstanceRequest->storagetype());
    if (deployerIter == deployers_.end()) {
        InitKillInstanceResponse(&rsp, *killInstanceRequest);
        rsp.set_code(static_cast<int32_t>(StatusCode::FUNC_AGENT_INVALID_STORAGE_TYPE));
        rsp.set_message("invalid function's storage type " + killInstanceRequest->storagetype());
        YRLOG_ERROR("{}|kill request from {} invalid storage type({}) of instance({})",
                    killInstanceRequest->requestid(), std::string(from), killInstanceRequest->storagetype(),
                    killInstanceRequest->instanceid());
        Send(from, "KillInstanceResponse", rsp.SerializeAsString());
        return;
    }

    killingRequest_[requestID] = { from, killInstanceRequest };
    messages::StopInstanceRequest stopInstanceRequest;
    function_agent::SetStopRuntimeInstanceRequest(stopInstanceRequest, killInstanceRequest);
    YRLOG_INFO("{}|received Kill instance({}) request. Send stop runtime({}) request to RuntimeManager({}-{}).",
               killInstanceRequest->requestid(), killInstanceRequest->instanceid(), killInstanceRequest->runtimeid(),
               registerRuntimeMgr_.name, registerRuntimeMgr_.address);
    Send(litebus::AID(registerRuntimeMgr_.name, registerRuntimeMgr_.address), "StopInstance",
         stopInstanceRequest.SerializeAsString());
}

void AgentServiceActor::SnapshotRuntime(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    auto request = std::make_shared<messages::SnapshotRuntimeRequest>();
    if (!request->ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse SnapshotRuntimeRequest");
        return;
    }

    const std::string &instanceID = request->instanceid();
    const std::string &runtimeID = request->runtimeid();
    YRLOG_INFO("{}|received SnapshotRuntime request for instance({}), runtime({})",
               request->requestid(), instanceID, runtimeID);

    // Prepare response
    messages::SnapshotRuntimeResponse response;
    response.set_requestid(request->requestid());

    // Check if agent is registered
    if (!registerRuntimeMgr_.registered || !isRegisterCompleted_) {
        response.set_code(static_cast<int32_t>(StatusCode::FUNC_AGENT_NOT_REGISTERED));
        response.set_message("function agent is not registered");
        YRLOG_ERROR("{}|registration is not complete, ignore snapshot request for instance({})",
                    request->requestid(), instanceID);
        Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
        return;
    }

    // Forward snapshot request to RuntimeManager
    YRLOG_INFO("{}|forward SnapshotRuntime request to RuntimeManager({}-{}) for instance({}), runtime({})",
               request->requestid(), registerRuntimeMgr_.name, registerRuntimeMgr_.address, instanceID, runtimeID);

    // Store the caller's AID to send response back later
    snapshotRequests_[request->requestid()] = from;

    Send(litebus::AID(registerRuntimeMgr_.name, registerRuntimeMgr_.address), "SnapshotRuntime", std::move(msg));
}

void AgentServiceActor::SnapshotRuntimeResponse(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::SnapshotRuntimeResponse response;
    if (!response.ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse SnapshotRuntimeResponse");
        return;
    }

    const std::string &requestID = response.requestid();
    auto iter = snapshotRequests_.find(requestID);
    if (iter == snapshotRequests_.end()) {
        YRLOG_WARN("{}|snapshot request not found in snapshotRequests_", requestID);
        return;
    }

    YRLOG_INFO("{}|received SnapshotRuntimeResponse from RuntimeManager, code: {}",
               requestID, response.code());

    // Forward response back to the original caller (FunctionAgentMgrActor)
    Send(iter->second, "SnapshotRuntimeResponse", std::move(msg));
    snapshotRequests_.erase(iter);
}

litebus::Future<Status> AgentServiceActor::SetDeployers(const std::string &storageType,
                                                        const std::shared_ptr<Deployer> &deployer)
{
    deployers_[storageType] = deployer;
    return Status::OK();
}

void AgentServiceActor::Init()
{
    ActorBase::Receive("DeployInstance", &AgentServiceActor::DeployInstance);
    ActorBase::Receive("KillInstance", &AgentServiceActor::KillInstance);
    ActorBase::Receive("StartInstanceResponse", &AgentServiceActor::StartInstanceResponse);
    ActorBase::Receive("StopInstanceResponse", &AgentServiceActor::StopInstanceResponse);
    ActorBase::Receive("Registered", &AgentServiceActor::Registered);
    ActorBase::Receive("UpdateResources", &AgentServiceActor::UpdateResources);
    ActorBase::Receive("UpdateMetrics", &AgentServiceActor::UpdateMetrics);
    ActorBase::Receive("UpdateRuntimeStatus", &AgentServiceActor::UpdateRuntimeStatus);
    ActorBase::Receive("UpdateInstanceStatus", &AgentServiceActor::UpdateInstanceStatus);
    ActorBase::Receive("UpdateInstanceStatusResponse", &AgentServiceActor::UpdateInstanceStatusResponse);
    ActorBase::Receive("UpdateAgentStatusResponse", &AgentServiceActor::UpdateAgentStatusResponse);
    ActorBase::Receive("QueryInstanceStatusInfo", &AgentServiceActor::QueryInstanceStatusInfo);
    ActorBase::Receive("QueryInstanceStatusInfoResponse", &AgentServiceActor::QueryInstanceStatusInfoResponse);
    ActorBase::Receive("CleanStatus", &AgentServiceActor::CleanStatus);
    ActorBase::Receive("CleanStatusResponse", &AgentServiceActor::CleanStatusResponse);
    ActorBase::Receive("UpdateCred", &AgentServiceActor::UpdateCred);
    ActorBase::Receive("UpdateCredResponse", &AgentServiceActor::UpdateCredResponse);
    ActorBase::Receive("GracefulShutdownFinish", &AgentServiceActor::GracefulShutdownFinish);
    ActorBase::Receive("SetNetworkIsolationRequest", &AgentServiceActor::SetNetworkIsolationRequest);
    ActorBase::Receive("QueryDebugInstanceInfos", &AgentServiceActor::QueryDebugInstanceInfos);
    ActorBase::Receive("QueryDebugInstanceInfosResponse", &AgentServiceActor::QueryDebugInstanceInfosResponse);
    ActorBase::Receive("StaticFunctionScheduleResponse", &AgentServiceActor::StaticFunctionScheduleResponse);
    ActorBase::Receive("NotifyFunctionStatusChange", &AgentServiceActor::NotifyFunctionStatusChange);
    ActorBase::Receive("SnapshotRuntime", &AgentServiceActor::SnapshotRuntime);
    ActorBase::Receive("SnapshotRuntimeResponse", &AgentServiceActor::SnapshotRuntimeResponse);

    litebus::Async(GetAID(), &AgentServiceActor::RemoveCodePackageAsync);

    metrics::MetricsAdapter::GetInstance().RegisterPhysicalMetricsCounter();
    metrics::MetricsAdapter::GetInstance().RegisterInstanceMetrics();
}

void AgentServiceActor::TimeOutEvent()
{
    YRLOG_INFO("heartbeat with local scheduler timeout");
    if (exiting_) {
        YRLOG_INFO("agent is exiting, no need to register.");
        return;
    }
    if (monopolyUsed_) {
        if (enableRestartForReuse_) {
            YRLOG_INFO("agent was monopoly used by an instance and enableRestartForReuse is true, agent will restart");
            GracefulShutdown().OnComplete(
                [isUnitTestSituation(isUnitTestSituation_)](const litebus::Future<bool> &status) {
                    if (!isUnitTestSituation) {
                        YR_EXIT("function agent restart for reuse");
                    }
                });
            return;
        }
        YRLOG_WARN(
            "the pod was monopoly used by an instance, and instance already exits. this pod is not allow to deploy by "
            "others. registration should be stop and wait pod terminated");
        return;
    }
    litebus::Async(GetAID(), &AgentServiceActor::RegisterAgent)
        .Then(litebus::Defer(GetAID(), &AgentServiceActor::StartPingPong, std::placeholders::_1));
}

void AgentServiceActor::Registered(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    messages::Registered registered;
    if (!registered.ParseFromString(msg)) {
        YRLOG_WARN("invalid registered msg from {} msg {}", std::string(from), msg);
        return;
    }

    if (registerInfo_.registeredPromise.GetFuture().IsOK()) {
        YRLOG_WARN("already received local scheduler registered msg, errCode: {}, errMsg: {}, from: {}",
                   registered.code(), registered.message(), std::string(from));
        return;
    }
    registerInfo_.registeredPromise.SetValue(registered);

    // set ready after first Registered, don't reset during re-register
    isReady_ = true;
    (void)litebus::TimerTools::Cancel(registerInfo_.reRegisterTimer);

    if (registered.code() != int32_t(StatusCode::SUCCESS)) {
        if (registered.code() == static_cast<int32_t>(StatusCode::LS_AGENT_EVICTED)) {
            YRLOG_WARN("agent has been evicted, will not reconnect to it");
            return;
        }

        YRLOG_ERROR("failed to register to local scheduler, errCode: {}, errMsg: {}, from: {}", registered.code(),
                    registered.message(), std::string(from));
        litebus::Async(GetAID(), &AgentServiceActor::CleanRuntimeManagerStatus, 0);
        (void)sendCleanStatusPromise_.GetFuture().OnComplete(
            litebus::Defer(GetAID(), &AgentServiceActor::CommitSuicide));
        return;
    }

    isRegisterCompleted_ = true;
    YRLOG_INFO("succeed to register to local scheduler. from: {}", std::string(from));
}

litebus::Future<messages::Registered> AgentServiceActor::StartPingPong(const messages::Registered &registered)
{
    YRLOG_INFO("gonna startup PingPongActor, agent service name: {}", agentID_);
    if (pingPongDriver_ == nullptr) {
        pingPongDriver_ = std::make_shared<HeartbeatClientDriver>(
            litebus::os::Join(componentName_, agentID_, '-'),
            [aid(GetAID()), agentID = agentID_](const litebus::AID &dst) {
                YRLOG_WARN("pingpong {} timeout, from agent to proxy. dst: {}", agentID, std::string(dst));
                litebus::Async(aid, &AgentServiceActor::TimeOutEvent);
            });
    }
    litebus::AID heartbeatAID(HEARTBEAT_OBSERVER_BASENAME + FUNCTION_AGENT_AGENT_MGR_ACTOR_NAME,
                              localSchedFuncAgentMgrAID_.Url());
    (void)pingPongDriver_->Start(heartbeatAID);
    return registered;
}

void AgentServiceActor::StartInstanceResponse(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::StartInstanceResponse startInstanceResponse;
    if (!startInstanceResponse.ParseFromString(msg)) {
        YRLOG_ERROR("invalid StartInstanceResponse msg from {} msg {}", std::string(from), msg);
        return;
    }

    auto request = deployingRequest_.find(startInstanceResponse.requestid());
    if (request == deployingRequest_.end()) {
        YRLOG_ERROR("{}|can't return start response, maybe instance has been killed.",
                    startInstanceResponse.requestid());
        return;
    }

    if (!registerRuntimeMgr_.registered || !isRegisterCompleted_) {
        YRLOG_ERROR("{}|registration is not complete, ignore start instance response.",
                    startInstanceResponse.requestid());
        return;
    }

    // Repeated deploy should not delete code refer
    if (startInstanceResponse.code() == static_cast<int32_t>(RUNTIME_MANAGER_INSTANCE_HAS_BEEN_DEPLOYED)) {
        YRLOG_INFO("{}|instance({}) has been deployed once", startInstanceResponse.requestid(),
                   request->second.request->instanceid());
        startInstanceResponse.set_code(static_cast<int32_t>(SUCCESS));
    }

    if (startInstanceResponse.code() != 0) {
        YRLOG_ERROR("{}|received start instance response from {}, error code: {}", startInstanceResponse.requestid(),
                    std::string(from), startInstanceResponse.code());
        DeleteCodeReferByDeployInstanceRequest(request->second.request);
    } else {
        YRLOG_INFO("{}|received start instance response. instance({}) runtime({}) address({}) pid({}) containerID({})",
                   startInstanceResponse.requestid(), request->second.request->instanceid(),
                   startInstanceResponse.startruntimeinstanceresponse().runtimeid(),
                   startInstanceResponse.startruntimeinstanceresponse().address(),
                   startInstanceResponse.startruntimeinstanceresponse().pid(),
                   startInstanceResponse.startruntimeinstanceresponse().containerid());
        if (!pluginClient_) {
            pluginClient_ = std::make_shared<MultiPluginClient>();
        }
        pluginClient_->IncreaseEnvRef(request->second.request,
                                      startInstanceResponse.startruntimeinstanceresponse().runtimeid());
    }

    // Extract portMappings if present: StartRuntimeInstanceResponse.port carries JSON when it starts with '{' or '['
    // Old format: {"8888":"40001"} (object), New format: ["tcp:40001:8080", ...] (array)
    const auto &portInField = startInstanceResponse.startruntimeinstanceresponse().port();
    const std::string portMappings =
        (!portInField.empty() && (portInField[0] == '{' || portInField[0] == '[')) ? portInField : "";
    auto deployInstanceResponse = BuildDeployInstanceResponse(startInstanceResponse, request->second.request,
                                                              portMappings);
    (void)runtimesDeploymentCache_->runtimes.emplace(deployInstanceResponse->runtimeid(),
                                                     SetRuntimeInstanceInfo(request->second.request));
    if (auto ret =
            Send(localSchedFuncAgentMgrAID_, "DeployInstanceResponse", deployInstanceResponse->SerializeAsString());
        ret != 1) {
        YRLOG_ERROR("{}|failed({}) to send a response message.", deployInstanceResponse->requestid(), ret);
    }

    (void)deployingRequest_.erase(request);
}

void AgentServiceActor::StopInstanceResponse(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::StopInstanceResponse stopInstanceResponse;
    if (!stopInstanceResponse.ParseFromString(msg)) {
        YRLOG_WARN("invalid StopInstanceResponse msg from {} msg {}", std::string(from), msg);
        return;
    }
    auto requestID = stopInstanceResponse.requestid();
    auto runtimeID = stopInstanceResponse.runtimeid();
    YRLOG_INFO("{}|received StopInstance response from {}, runtimeID: {}", requestID, std::string(from), runtimeID);

    auto request = killingRequest_.find(requestID);
    if (request == killingRequest_.end()) {
        YRLOG_ERROR("Request({}) maybe already killed.", requestID);
        return;
    }

    if (!registerRuntimeMgr_.registered || !isRegisterCompleted_) {
        YRLOG_ERROR("{}|registration is not complete, ignore stop instance response.", requestID);
        return;
    }

    auto killInstanceRequest = request->second.request;
    auto killInstanceResponse = BuildKillInstanceResponse(stopInstanceResponse.code(), stopInstanceResponse.message(),
                                                          requestID, killInstanceRequest->instanceid());
    YRLOG_DEBUG("{}|AgentServiceActor send KillInstanceResponse back to {}", requestID,
                std::string(request->second.from));
    Send(request->second.from, "KillInstanceResponse", killInstanceResponse->SerializeAsString());

    // If a pod is exclusively occupied by an instance, the pod cannot be used by other instances after the instance
    // exits.
    if (killInstanceRequest->ismonopoly()) {
        monopolyUsed_ = true;
    }

    if (monopolyUsed_ && enableRestartForReuse_) {
        YRLOG_INFO(
            "{}|kill monopoly instance({}) and enableRestartForReuse is true, stop heart beat, wait for re-start",
            requestID, killInstanceRequest->instanceid());
        pingPongDriver_->Stop();
        TimeOutEvent();
    }

    // clear function's code package
    auto runtimeIter = runtimesDeploymentCache_->runtimes.find(runtimeID);
    if (runtimeIter == runtimesDeploymentCache_->runtimes.end()) {
        YRLOG_ERROR("AgentServiceActor failed to find deployment config of runtime {}", runtimeID);
        return;
    }

    DeleteCodeReferByRuntimeInstanceInfo(runtimeIter->second);
    if (!pluginClient_) {
        pluginClient_ = std::make_shared<MultiPluginClient>();
    }
    pluginClient_->DecreaseEnvRef(runtimeID, runtimeIter->second);
    (void)runtimesDeploymentCache_->runtimes.erase(runtimeID);
    (void)killingRequest_.erase(requestID);

    if (instanceHealthyMap_.find(killInstanceRequest->instanceid()) != instanceHealthyMap_.end()) {
        (void)instanceHealthyMap_.erase(killInstanceRequest->instanceid());
    }
}

void AgentServiceActor::UpdateResources(const litebus::AID &from, std::string &&, std::string &&msg)
{
    if (exiting_) {
        YRLOG_INFO("agent is exiting, ignore update resources requests.");
        return;
    }
    messages::UpdateResourcesRequest req;
    if (!req.ParseFromString(msg)) {
        YRLOG_WARN("invalid update resource request msg from {} msg {}", std::string(from), msg);
        return;
    }
    YRLOG_DEBUG("received UpdateResources request from {}", std::string(from));
    if (!registerRuntimeMgr_.registered) {
        YRLOG_ERROR("functionAgent({}) registration is not complete, ignore update resources request.", agentID_);
        return;
    }

    req.mutable_resourceunit()->set_id(agentID_);
    req.mutable_resourceunit()->set_alias(alias_);
    resources::Value::Counter cnter;
    (void)req.mutable_resourceunit()->mutable_nodelabels()->insert({ agentID_, cnter });
    if (litebus::os::GetEnv(SF_CONFIG_PATH).IsSome()) {
        (void)cnter.mutable_items()->insert({ agentID_, 1 });
        YRLOG_INFO("add resource unit insert node labels: key={}, value={}", RESOURCE_OWNER_KEY, agentID_);
        (*req.mutable_resourceunit()->mutable_nodelabels())[RESOURCE_OWNER_KEY] = cnter;
    }
    registeredResourceUnit_->CopyFrom(req.resourceunit());

    // update instance metrics
    metrics::MetricsAdapter::GetInstance().GetMetricsContext().InitInstanceMetrics(req);

    (void)Send(localSchedFuncAgentMgrAID_, "UpdateResources", req.SerializeAsString());
}

void AgentServiceActor::UpdateMetrics(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::UpdateResourcesRequest req;
    if (!req.ParseFromString(msg)) {
        YRLOG_WARN("invalid update metrics request msg from {} msg {}", std::string(from), msg);
        return;
    }
    YRLOG_DEBUG("received UpdateMetrics request from {}, from agentID({}), from nodeID({})", std::string(from),
                agentID_, nodeID_);
    if (!registerRuntimeMgr_.registered) {
        YRLOG_ERROR("functionAgent({}) registration is not complete, ignore update resources request.", agentID_);
        return;
    }

    metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetPhysicalMetrics(agentID_, nodeID_,
                                                                                  req.resourceunit());
}

void AgentServiceActor::UpdateInstanceStatus(const litebus::AID &, std::string &&, std::string &&msg)
{
    if (!registerRuntimeMgr_.registered || !isRegisterCompleted_) {
        YRLOG_ERROR("agent({}) registration is not complete, ignore update instance status request.", agentID_);
        return;
    }

    (void)Send(localSchedFuncAgentMgrAID_, "UpdateInstanceStatus", std::move(msg));
}

void AgentServiceActor::UpdateInstanceStatusResponse(const litebus::AID &, std::string &&, std::string &&msg)
{
    if (!registerRuntimeMgr_.registered || !isRegisterCompleted_) {
        YRLOG_ERROR("agent({}) registration is not complete, ignore update instance status response.", agentID_);
        return;
    }

    (void)Send(litebus::AID(RUNTIME_MANAGER_HEALTH_CHECK_ACTOR_NAME, registerRuntimeMgr_.address),
               "UpdateInstanceStatusResponse", std::move(msg));
}

litebus::Future<messages::Registered> AgentServiceActor::RegisterAgent()
{
    YRLOG_INFO("AgentServiceActor start to RegisterAgent to {}", std::string(localSchedFuncAgentMgrAID_));
    messages::Registered response;
    if (registeredResourceUnit_ == nullptr) {
        std::string msg =
            "AgentServiceActor nullptr of registeredResourceUnit_! Maybe runtime_manager is not registered.";
        YRLOG_ERROR(msg);
        response.set_code(static_cast<int32_t>(StatusCode::FUNC_AGENT_RESOURCE_UNIT_IS_NULL));
        response.set_message(msg);
        return response;
    }
    messages::Register registerAgentRequest;
    auto resourceUnit = registerAgentRequest.mutable_resource();
    registeredResourceUnit_->set_id(agentID_);
    registeredResourceUnit_->set_alias(alias_);
    if (litebus::os::GetEnv(SF_CONFIG_PATH).IsSome()) {
        resources::Value::Counter cnter;
        (void)cnter.mutable_items()->insert({ agentID_, 1 });
        YRLOG_INFO("resource unit insert node labels: key={}, value={}", RESOURCE_OWNER_KEY, agentID_);
        (*registeredResourceUnit_->mutable_nodelabels())[RESOURCE_OWNER_KEY] = cnter;
    }
    resourceUnit->CopyFrom(*registeredResourceUnit_);

    // Set Registration information
    messages::FuncAgentRegisInfo funcAgentRegisInfo;
    funcAgentRegisInfo.set_agentaidname(std::string(GetAID().Name()));
    funcAgentRegisInfo.set_runtimemgraid(registerRuntimeMgr_.name);
    funcAgentRegisInfo.set_runtimemgrid(registerRuntimeMgr_.id);
    funcAgentRegisInfo.set_agentaddress(GetAID().Url());
    std::string jsonStr;
    auto ret = google::protobuf::util::MessageToJsonString(funcAgentRegisInfo, &jsonStr);
    if (!ret.ok()) {
        std::string msg = "serialize function agent registration information to json format string failed.";
        YRLOG_ERROR(msg);
        response.set_code(static_cast<int32_t>(StatusCode::FUNC_AGENT_REGIS_INFO_SERIALIZED_FAILED));
        response.set_message(msg);
        return response;
    }
    registerAgentRequest.set_message(jsonStr);
    registerAgentRequest.set_name(agentID_);

    registerInfo_.reRegisterTimer =
        litebus::AsyncAfter(retryRegisterInterval_, GetAID(), &AgentServiceActor::RetryRegisterAgent,
                            registerAgentRequest.SerializeAsString());
    registerInfo_.registeredPromise = litebus::Promise<messages::Registered>();

    YRLOG_INFO("AgentServiceActor gonna send Register request to {}", std::string(localSchedFuncAgentMgrAID_));
    Send(localSchedFuncAgentMgrAID_, "Register", registerAgentRequest.SerializeAsString());
    return registerInfo_.registeredPromise.GetFuture();
}

void AgentServiceActor::RetryRegisterAgent(const std::string &msg)
{
    auto registerResponseFuture = registerInfo_.registeredPromise.GetFuture();
    if (registerResponseFuture.IsOK()) {
        return;
    }

    YRLOG_INFO("AgentServiceActor gonna send Register request to {}", std::string(localSchedFuncAgentMgrAID_));
    Send(localSchedFuncAgentMgrAID_, "Register", std::string(msg));
    registerInfo_.reRegisterTimer =
        litebus::AsyncAfter(retryRegisterInterval_, GetAID(), &AgentServiceActor::RetryRegisterAgent, msg);
}

void AgentServiceActor::MarkRuntimeManagerUnavailable(const std::string &id)
{
    registerHelper_->StopHeartbeatObserver();
    if (registerRuntimeMgr_.id != id) {
        YRLOG_ERROR("failed to find RuntimeManager({}) info", id);
        return;
    }

    YRLOG_WARN("gonna mark RuntimeManager {} as unavailable", id);
    runtimeManagerGracefulShutdown_.SetValue(true);
    registerRuntimeMgr_.registered = false;

    UpdateAgentStatusToLocal(static_cast<int32_t>(RUNTIME_MANAGER_REGISTER_FAILED));
}

Status AgentServiceActor::StartRuntime(const DeployInstanceRequest &request,
                                       const litebus::Future<Status> &prepareEnvRes)
{
    if (prepareEnvRes.IsError()) {
        return Status(StatusCode::FUNC_AGENT_START_RUNTIME_FAILED, "prepare env failed");
    }
    const auto &prepareResult = prepareEnvRes.Get();
    if (prepareResult.IsError()) {
        YRLOG_ERROR("{}|{}|failed to prepare runtime env, instance {}", request->traceid(), request->requestid(),
                    request->instanceid());
        std::string errorMsg = prepareResult.GetMessage();
        if (prepareResult.StatusCode() == StatusCode::REQUEST_TIME_OUT) {
            errorMsg = "prepare runtime env timeout, " + errorMsg;
        }
        auto resp = InitDeployInstanceResponse(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR),
                                               errorMsg, *request);
        (void)Send(localSchedFuncAgentMgrAID_, "DeployInstanceResponse", resp.SerializeAsString());
        prepareEnvRequest_.erase(request->requestid());
        return Status(StatusCode::FUNC_AGENT_START_RUNTIME_FAILED, "prepare env failed");
    }
    YRLOG_INFO("{}|{}|prepare runtime env succeed, instance {}", request->traceid(), request->requestid(),
               request->instanceid());
    for (const auto &[fst, snd] : request->createoptions()) {
        YRLOG_DEBUG("{}|{}|request cmds key {}, value {}", request->traceid(), request->requestid(), fst, snd);
    }
    auto startInstanceRequest = std::make_unique<messages::StartInstanceRequest>();
    function_agent::SetStartRuntimeInstanceRequestConfig(startInstanceRequest, request);
    YRLOG_DEBUG("{}|{}|StartRuntime request for instance({}): {}", request->traceid(), request->requestid(),
                request->instanceid(), startInstanceRequest->ShortDebugString());
    if (request->funcdeployspec().storagetype() == COPY_STORAGE_TYPE) {
        startInstanceRequest->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_deploydir(
            deployers_[COPY_STORAGE_TYPE]->GetDestination("", "", request->funcdeployspec().deploydir()));
    }

    if (!registerRuntimeMgr_.registered) {
        YRLOG_ERROR("{}|{}|runtime-manager not registered, failed to StartRuntime. instance {}", request->traceid(),
                    request->requestid(), request->instanceid());
        auto resp = InitDeployInstanceResponse(static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION),
                                               "invalid runtime-manager", *request);
        (void)Send(localSchedFuncAgentMgrAID_, "DeployInstanceResponse", resp.SerializeAsString());
        prepareEnvRequest_.erase(request->requestid());
        return Status(StatusCode::FUNC_AGENT_START_RUNTIME_FAILED, "invalid runtime-manager");
    }
    YRLOG_INFO("{}|{}|send StartInstance request to ({}-{}), instance: {}", request->traceid(), request->requestid(),
               registerRuntimeMgr_.name, registerRuntimeMgr_.address, request->instanceid());
    Send(litebus::AID(registerRuntimeMgr_.name, registerRuntimeMgr_.address), "StartInstance",
         startInstanceRequest->SerializeAsString());
    prepareEnvRequest_.erase(request->requestid());
    return Status::OK();
}

void AgentServiceActor::SetRegisterHelper(const std::shared_ptr<RegisterHelper> &helper)
{
    registerHelper_ = nullptr;
    registerHelper_ = helper;
    auto func = [aid(GetAID())](const std::string &message) {
        litebus::Async(aid, &AgentServiceActor::ReceiveRegister, message);
    };
    registerHelper_->SetRegisterCallback(func);
}

void AgentServiceActor::ReceiveRegister(const std::string &message)
{
    YRLOG_INFO("receive register message");
    auto rsp = std::make_shared<messages::RegisterRuntimeManagerResponse>();
    messages::RegisterRuntimeManagerRequest req;
    if (!req.ParseFromString(message)) {
        YRLOG_ERROR("failed to parse RuntimeManager register message");
        return;
    }

    if (registerRuntimeMgr_.id == req.id()) {
        if (registerRuntimeMgr_.registered) {
            YRLOG_INFO(
                "{}|FunctionAgent has received RuntimeManager(id:{}) register request before, discard this request",
                agentID_, req.id());
            rsp->set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        } else {
            YRLOG_WARN("{}|FunctionAgent receive RuntimeManager(id:{}) pong timeout and retry register failed",
                       agentID_, req.id());
            rsp->set_code(static_cast<int32_t>(StatusCode::REGISTER_ERROR));
        }
        registerHelper_->SendRegistered(req.name(), req.address(), rsp->SerializeAsString());
        return;
    }

    // update agent service actor's cache
    registerRuntimeMgr_ = { req.name(), req.address(), req.id(), true };

    auto timeoutHandler = [aid(GetAID()), id(registerRuntimeMgr_.id)](const litebus::AID &from, HeartbeatConnection) {
        YRLOG_WARN("heartbeat timeout, from runtime to agent. from: {}.", std::string(from));
        litebus::Async(aid, &AgentServiceActor::MarkRuntimeManagerUnavailable, id);
    };
    registerHelper_->SetHeartbeatObserveDriver(
        litebus::os::Join(COMPONENT_NAME_RUNTIME_MANAGER, registerRuntimeMgr_.name, '-'), registerRuntimeMgr_.address,
        pingTimeoutMs_, timeoutHandler, RUNTIME_MANAGER_ACTOR_NAME);

    auto requestInstanceInfos = req.runtimeinstanceinfos();
    for (const auto &it : requestInstanceInfos) {
        (void)runtimesDeploymentCache_->runtimes.emplace(it.first, it.second);
        AddCodeReferByRuntimeInstanceInfo(it.second);
    }
    registeredResourceUnit_->CopyFrom(req.resourceunit());

    // recover plugin cache from runtime-manager request
    litebus::Async(GetAID(), &AgentServiceActor::RecoverPluginCache, message)
        .OnComplete([rsp, this](const litebus::Future<bool> &pluginRegisterResFut) {
            if (pluginRegisterResFut.IsError() || !pluginRegisterResFut.Get()) {
                // we must not proceed to avoid using incorrect cache
                // for example, a user's virtual env is being deleted while it is still in use,
                // causing the cache to hold invalid references.
                YRLOG_ERROR("agent plugin register error, no need send response back");
                return;
            }
            rsp->set_code(static_cast<int32_t>(StatusCode::SUCCESS));
            YRLOG_INFO("gonna send Registered message back to RuntimeManager({}-{})", registerRuntimeMgr_.name,
                       registerRuntimeMgr_.address);
            registerHelper_->SendRegistered(registerRuntimeMgr_.name, registerRuntimeMgr_.address,
                                            rsp->SerializeAsString());

            // start to register function_agent to local_scheduler
            RegisterAgent()
                .Then(litebus::Defer(GetAID(), &AgentServiceActor::StartPingPong, std::placeholders::_1))
                .Then(litebus::Defer(GetAID(), &AgentServiceActor::CreateStaticFunctionInstance));
        });
}

void AgentServiceActor::AddCodeReferByRuntimeInstanceInfo(const messages::RuntimeInstanceInfo &info)
{
    const std::string &instanceID = info.instanceid();
    // add executor function refer
    auto deployerIter = deployers_.find(info.deploymentconfig().storagetype());
    if (deployerIter == deployers_.end()) {
        YRLOG_ERROR("{}|instance add code refer error, do not have this type of deployer, type = {}", info.instanceid(),
                    info.deploymentconfig().storagetype());
        return;
    }
    auto executorDestination = deployerIter->second->GetDestination(
        info.deploymentconfig().deploydir(), info.deploymentconfig().bucketid(), info.deploymentconfig().objectid());
    AddCodeRefer(executorDestination, instanceID, deployerIter->second);

    auto s3DeployerIter = deployers_.find(S3_STORAGE_TYPE);
    if (s3DeployerIter == deployers_.end()) {
        YRLOG_ERROR("{}|instance add code refer error, do not have S3 deployer", info.instanceid());
        return;
    }
    // add layer function refer
    std::string s3DeployDir(info.deploymentconfig().deploydir());  // should be s3 deploy dir for delegate.
    if (auto deployDirIterator = info.runtimeconfig().userenvs().find("S3_DEPLOY_DIR");
        deployDirIterator != info.runtimeconfig().userenvs().end()) {
        s3DeployDir = deployDirIterator->second;
    }
    for (auto &layer : info.deploymentconfig().layers()) {
        auto layerDestination = s3DeployDir + "/layer/" + layer.bucketid() + "/" + layer.objectid();
        AddCodeRefer(layerDestination, instanceID, deployers_[S3_STORAGE_TYPE]);
    }

    if (auto it = info.runtimeconfig().posixenvs().find(UNZIPPED_WORKING_DIR);
        it != info.runtimeconfig().posixenvs().end()) {
        if (auto fileIter = info.runtimeconfig().posixenvs().find(YR_WORKING_DIR);
            fileIter != info.runtimeconfig().posixenvs().end() && it->second == fileIter->second) {
            YRLOG_DEBUG("instance({}) uses delegate working dir, skip add code refer", info.instanceid());
            return;
        }
        AddCodeRefer(it->second, instanceID, deployers_[WORKING_DIR_STORAGE_TYPE]);
    }
    // add delegate user code function refer
    auto userCodeDestinationIter = info.runtimeconfig().posixenvs().find(ENV_DELEGATE_DOWNLOAD);
    if (userCodeDestinationIter == info.runtimeconfig().posixenvs().end()) {
        return;
    }
    auto delegateCodeStorageIter = info.runtimeconfig().posixenvs().find(ENV_DELEGATE_DOWNLOAD_STORAGE_TYPE);
    if (delegateCodeStorageIter == info.runtimeconfig().posixenvs().end()) {
        return;
    }
    AddCodeRefer(userCodeDestinationIter->second, instanceID, deployers_[delegateCodeStorageIter->second]);

    if (auto it = info.runtimeconfig().posixenvs().find(SHARED_DIRECTORY_PATH);
        it != info.runtimeconfig().posixenvs().end()) {
        AddCodeRefer(it->second, instanceID, deployers_[SHARED_DIR_STORAGE_TYPE]);
    }
}

void AgentServiceActor::AddCodeRefer(const std::string &dstDir, const std::string &instanceID,
                                     const std::shared_ptr<Deployer> &deployer)
{
    ASSERT_IF_NULL(codeReferInfos_);
    if (auto iter = codeReferInfos_->find(dstDir); iter == codeReferInfos_->end()) {
        (void)codeReferInfos_->emplace(dstDir, CodeReferInfo{ { instanceID }, deployer });
    } else {
        (void)iter->second.instanceIDs.emplace(instanceID);
    }
}

void AgentServiceActor::DeleteCodeReferByDeployInstanceRequest(
    const std::shared_ptr<messages::DeployInstanceRequest> &req)
{
    const auto &instanceID = req->instanceid();
    // delete executor function refer
    auto executorDestination = deployers_[req->funcdeployspec().storagetype()]->GetDestination(
        req->funcdeployspec().deploydir(), req->funcdeployspec().bucketid(), req->funcdeployspec().objectid());
    DeleteFunction(executorDestination, instanceID);

    // delete layer function refer
    std::string s3DeployDir(req->funcdeployspec().deploydir());  // should be s3 deploy dir for delegate.
    if (auto deployDirIterator = req->createoptions().find("S3_DEPLOY_DIR");
        deployDirIterator != req->createoptions().end()) {
        s3DeployDir = deployDirIterator->second;
    }
    for (auto &layer : req->funcdeployspec().layers()) {
        auto layerDestination = s3DeployDir + "/layer/" + layer.bucketid() + "/" + layer.objectid();
        DeleteFunction(layerDestination, instanceID);
    }

    // delete working_dir code function refer
    if (auto it = req->createoptions().find(UNZIPPED_WORKING_DIR); it != req->createoptions().end()) {
        DeleteFunction(it->second, instanceID);
    }

    // delete delegate user code function refer
    if (auto it = req->createoptions().find(ENV_DELEGATE_DOWNLOAD); it != req->createoptions().end()) {
        DeleteFunction(it->second, instanceID);
    }

    // delete delegate shared directory function refer
    if (auto it = req->createoptions().find(SHARED_DIRECTORY_PATH); it != req->createoptions().end()) {
        DeleteFunction(it->second, instanceID);
    }
}

void AgentServiceActor::DeleteCodeReferByRuntimeInstanceInfo(const messages::RuntimeInstanceInfo &info)
{
    const auto &instanceID = info.instanceid();
    // delete executor function refer
    auto executorDestination = deployers_[info.deploymentconfig().storagetype()]->GetDestination(
        info.deploymentconfig().deploydir(), info.deploymentconfig().bucketid(), info.deploymentconfig().objectid());
    DeleteFunction(executorDestination, instanceID);

    // delete layer function refer
    std::string s3DeployDir(info.deploymentconfig().deploydir());  // should be s3 deploy dir for delegate.
    if (auto deployDirIterator = info.runtimeconfig().userenvs().find("S3_DEPLOY_DIR");
        deployDirIterator != info.runtimeconfig().userenvs().end()) {
        s3DeployDir = deployDirIterator->second;
    }
    for (auto &layer : info.deploymentconfig().layers()) {
        auto layerDestination = s3DeployDir + "/layer/" + layer.bucketid() + "/" + layer.objectid();
        DeleteFunction(layerDestination, instanceID);
    }

    // delete working_dir code function refer
    if (auto it = info.runtimeconfig().posixenvs().find(UNZIPPED_WORKING_DIR);
        it != info.runtimeconfig().posixenvs().end()) {
        DeleteFunction(it->second, instanceID);
    }

    // delete delegate user code function refer
    if (auto it = info.runtimeconfig().posixenvs().find(ENV_DELEGATE_DOWNLOAD);
        it != info.runtimeconfig().posixenvs().end()) {
        DeleteFunction(it->second, instanceID);
    }
    if (auto it = info.runtimeconfig().posixenvs().find(SHARED_DIRECTORY_PATH);
        it != info.runtimeconfig().posixenvs().end()) {
        DeleteFunction(it->second, instanceID);
    }
}

void AgentServiceActor::DeleteFunction(const std::string &functionDestination, const std::string &instanceID)
{
    ASSERT_IF_NULL(codeReferInfos_);
    auto iter = codeReferInfos_->find(functionDestination);
    if (iter == codeReferInfos_->end()) {
        return;
    }
    if (iter->second.instanceIDs.find(instanceID) != iter->second.instanceIDs.end()) {
        iter->second.lastAccessTimestamp = static_cast<uint64_t>(std::time(nullptr));
        (void)iter->second.instanceIDs.erase(instanceID);
    }
}

void AgentServiceActor::QueryInstanceStatusInfo(const litebus::AID &, std::string &&, std::string &&msg)
{
    if (!registerRuntimeMgr_.registered) {
        YRLOG_ERROR("{}|registration is not complete, ignore query instance status info.", agentID_);
        return;
    }
    (void)Send(litebus::AID(registerRuntimeMgr_.name, registerRuntimeMgr_.address), "QueryInstanceStatusInfo",
               std::move(msg));
}

void AgentServiceActor::QueryInstanceStatusInfoResponse(const litebus::AID &, std::string &&, std::string &&msg)
{
    if (!isRegisterCompleted_) {
        YRLOG_ERROR("{}|registration is not complete, ignore query instance status response.", agentID_);
        return;
    }
    (void)Send(localSchedFuncAgentMgrAID_, "QueryInstanceStatusInfoResponse", std::move(msg));
}

void AgentServiceActor::RemoveCodePackageAsync()
{
    if (remainedClearCodePackageRetryTimes_ == 0) {
        YRLOG_WARN("{}|agent failed to clean code package when clean status", agentID_);
        clearCodePackagePromise_.SetValue(StatusCode::FUNC_AGENT_CLEAN_CODE_PACKAGE_TIME_OUT);
        return;
    }

    ASSERT_IF_NULL(codeReferInfos_);
    if (isCleaningStatus_ && codeReferInfos_->empty()) {
        YRLOG_INFO("{}|agent success to clean code package when clean status", agentID_);
        clearCodePackagePromise_.SetValue(StatusCode::SUCCESS);
        return;
    }

    for (auto codeReferInfoIter = codeReferInfos_->begin(); codeReferInfoIter != codeReferInfos_->end();) {
        ASSERT_IF_NULL(codeReferInfoIter->second.deployer);
        const std::string &objectFile = codeReferInfoIter->first;
        int ttl = codeReferInfoIter->second.deployer->GetTTL(objectFile);
        if (ttl < 0) {
            ttl = codePackageThresholds_.codeagingtime();
        }
        auto now = static_cast<uint64_t>(std::time(nullptr));
        if (codeReferInfoIter->second.instanceIDs.empty()
            && (now - codeReferInfoIter->second.lastAccessTimestamp >= static_cast<uint64_t>(ttl))) {
            if (codeReferInfoIter->second.deployer->Clear(objectFile, objectFile)) {
                codeReferInfoIter = codeReferInfos_->erase(codeReferInfoIter);
                continue;
            }
        }
        (void)++codeReferInfoIter;
    }
    if (remainedClearCodePackageRetryTimes_ >= 0) {
        (void)--remainedClearCodePackageRetryTimes_;
    }
    clearCodePackageTimer_ =
        litebus::AsyncAfter(clearCodePackageInterval_, GetAID(), &AgentServiceActor::RemoveCodePackageAsync);
}

void AgentServiceActor::CommitSuicide()
{
    (void)clearCodePackagePromise_.GetFuture().OnComplete(
        [isUnitTestSituation(this->isUnitTestSituation_)](const litebus::Future<StatusCode> &) -> void {
            if (!isUnitTestSituation) {
                YR_EXIT("function agent suicide");
            }
        });
}

void AgentServiceActor::Finalize()
{
    UpdateAgentStatusToLocal(static_cast<int32_t>(FUNC_AGENT_EXITED), "function_agent exited");
    remainedClearCodePackageRetryTimes_ = 0;
    (void)litebus::TimerTools::Cancel(clearCodePackageTimer_);
}

void AgentServiceActor::CleanRuntimeManagerStatus(uint32_t retryTimes)
{
    if (sendCleanStatusPromise_.GetFuture().IsOK()) {
        return;
    }
    (void)++retryTimes;
    if (retryTimes > MAX_RETRY_SEND_CLEAN_STATUS_TIMES) {
        YRLOG_ERROR("{}|Send clean status to runtime manager({}) time out", agentID_, registerRuntimeMgr_.id);
        sendCleanStatusPromise_.SetValue(StatusCode::RUNTIME_MANAGER_CLEAN_STATUS_RESPONSE_TIME_OUT);
        return;
    }
    messages::CleanStatusRequest cleanStatusRequest;
    cleanStatusRequest.set_name(registerRuntimeMgr_.id);
    YRLOG_INFO("{}|Send clean status to runtime manager({})", agentID_, registerRuntimeMgr_.id);
    (void)Send(litebus::AID(registerRuntimeMgr_.name, registerRuntimeMgr_.address), "CleanStatus",
               cleanStatusRequest.SerializeAsString());

    (void)litebus::AsyncAfter(retrySendCleanStatusInterval_, GetAID(), &AgentServiceActor::CleanRuntimeManagerStatus,
                              retryTimes);
}

void AgentServiceActor::CleanStatus(const litebus::AID &from, std::string &&, std::string &&msg)
{
    YRLOG_DEBUG("{}|receive CleanStatus from local-scheduler, function-agent gonna to suicide", agentID_);
    messages::CleanStatusRequest cleanStatusRequest;
    if (!cleanStatusRequest.ParseFromString(msg)) {
        YRLOG_ERROR("{}|failed to parse local-scheduler({}) CleanStatus message", agentID_, from.HashString());
        return;
    }

    if (cleanStatusRequest.name() != agentID_) {
        YRLOG_WARN("{}|receive wrong CleanStatus message from local-scheduler({})", agentID_, from.Name());
        return;
    }

    isCleaningStatus_ = true;
    remainedClearCodePackageRetryTimes_ = DEFAULT_RETRY_CLEAR_CODE_PACKAGER_TIMES;
    ASSERT_IF_NULL(codeReferInfos_);
    for (auto &codeReferInfoIter : (*codeReferInfos_)) {
        codeReferInfoIter.second.instanceIDs.clear();
    }

    messages::CleanStatusResponse cleanStatusResponse;
    (void)Send(from, "CleanStatusResponse", cleanStatusResponse.SerializeAsString());

    litebus::Async(GetAID(), &AgentServiceActor::CleanRuntimeManagerStatus, 0);
    (void)sendCleanStatusPromise_.GetFuture().OnComplete(litebus::Defer(GetAID(), &AgentServiceActor::CommitSuicide));
}

void AgentServiceActor::CleanStatusResponse(const litebus::AID &from, std::string &&, std::string &&)
{
    YRLOG_DEBUG("{}|receive CleanStatusResponse from runtime-manager ({})", agentID_, from.HashString());
    if (isCleaningStatus_) {
        sendCleanStatusPromise_.SetValue(StatusCode::SUCCESS);
    }
}

void AgentServiceActor::UpdateCred(const litebus::AID &, std::string &&, std::string &&msg)
{
    if (!registerRuntimeMgr_.registered) {
        YRLOG_ERROR("{}|registration is not complete, ignore query instance status info.", agentID_);
        return;
    }
    (void)Send(litebus::AID(registerRuntimeMgr_.name, registerRuntimeMgr_.address), "UpdateCred", std::move(msg));
}

void AgentServiceActor::UpdateCredResponse(const litebus::AID &, std::string &&, std::string &&msg)
{
    if (!isRegisterCompleted_) {
        YRLOG_ERROR("{}|registration is not complete, ignore query instance status response.", agentID_);
        return;
    }
    (void)Send(localSchedFuncAgentMgrAID_, "UpdateCredResponse", std::move(msg));
}

void AgentServiceActor::GracefulShutdownFinish(const litebus::AID &, std::string &&, std::string &&msg)
{
    YRLOG_ERROR("receive graceful shutdown finish from runtime manager");
    runtimeManagerGracefulShutdown_.SetValue(true);
}

litebus::Future<bool> AgentServiceActor::GracefulShutdown()
{
    YRLOG_INFO("graceful shutdown agent service, gracefulShutdownTime: {}", gracefulShutdownTime_);
    exiting_ = true;
    CleanRuntimeManagerStatus(0);
    litebus::Async(GetAID(), &AgentServiceActor::UpdateAgentStatusToLocal,
        static_cast<int32_t>(FUNC_AGENT_EXITING), "function_agent exiting");
    (void)litebus::TimerTools::AddTimer(gracefulShutdownTime_ * GRACE_SHUTDOWN_TIMEOUT_MS, GetAID(),
                                        [promise(runtimeManagerGracefulShutdown_)]() { promise.SetValue(true); });
    return runtimeManagerGracefulShutdown_.GetFuture().Then([aid(GetAID())](const bool res) {
        return litebus::Async(aid, &AgentServiceActor::UpdateAgentStatusToLocal,
                              static_cast<int32_t>(FUNC_AGENT_EXITED), "function_agent exited")
            .After(GRACE_SHUTDOWN_CLEAN_AGENT_TIMEOUT, [](const litebus::Future<bool> future) { return true; });
    });
}

litebus::Future<Status> AgentServiceActor::Readiness()
{
    if (litebus::os::GetEnv(SF_CONFIG_PATH).IsSome()) {
        return IsAgentReadiness();
    }

    if (isReady_) {
        // if the initial register is ok, considering the agent will always be ready, even if proxy is re-started
        return Status::OK();
    }

    return registerInfo_.registeredPromise.GetFuture().Then([](const messages::Registered &) { return Status::OK(); });
}

litebus::Future<Status> AgentServiceActor::IsAgentReadiness()
{
    if (instanceHealthyMap_.empty()) {
        YRLOG_DEBUG_COUNT_60("agent not ready, instance not exist.");
        return litebus::Future<Status>(litebus::Status(FAILED));
    }

    for (const auto &[id, status] : instanceHealthyMap_) {
        if (status != static_cast<int32_t>(InstanceState::RUNNING)) {
            YRLOG_DEBUG_COUNT_60("agent not ready, {} not running.", id);
            return litebus::Future<Status>(litebus::Status(FAILED));
        }
    }

    return Status::OK();
}

bool AgentServiceActor::HandleNetworkIsolation(const std::shared_ptr<messages::SetNetworkIsolationRequest> &req)
{
    switch (req->ruletype()) {
        case RuleType::IPSET_ADD:
            if (req->rules().size() == 1) {  // Only support insert one entry once
                return !ipsetIsolation_->AddRule(req->rules()[0]);
            } else {  // batch restore for performance
                std::vector<std::string> rules;
                for (const auto &rule : req->rules()) {
                    rules.push_back(rule);
                }
                return !ipsetIsolation_->AddRules(rules);
            }
            break;
        case RuleType::IPSET_DELETE:
            for (const auto &rule : req->rules()) {
                if (ipsetIsolation_->RemoveRule(rule) != 0) {
                    return false;
                }
            }
            break;
        case RuleType::IPSET_FLUSH:
            return !ipsetIsolation_->RemoveAllRules();
            break;
        case RuleType::IPTABLES_COMMAND:
        default:
            break;
    }
    return true;
}

void AgentServiceActor::QueryDebugInstanceInfos(const litebus::AID &, std::string &&, std::string &&msg)
{
    if (!registerRuntimeMgr_.registered) {
        YRLOG_ERROR("{}|registration is not complete, ignore query debug instatnce infos.", agentID_);
        return;
    }

    (void)Send(litebus::AID(registerRuntimeMgr_.name, registerRuntimeMgr_.address), "QueryDebugInstanceInfos",
               std::move(msg));
}

void AgentServiceActor::QueryDebugInstanceInfosResponse(const litebus::AID &from, std::string &&, std::string &&msg)
{
    if (!registerRuntimeMgr_.registered || !isRegisterCompleted_) {
        YRLOG_ERROR("agent({}) registration is not complete, ignore query debug instatnce infos response.", agentID_);
        return;
    }

    messages::QueryDebugInstanceInfosResponse rsp;
    if (!rsp.ParseFromString(msg)) {
        YRLOG_ERROR("invalid debug instance response from({}), {}", std::string(from), msg);
        return;
    }
    YRLOG_DEBUG("{}|got instance status response from({}), {}", rsp.requestid(), std::string(from),
                rsp.ShortDebugString());
    (void)Send(localSchedFuncAgentMgrAID_, "QueryDebugInstanceInfosResponse", std::move(msg));
}

void AgentServiceActor::SetNetworkIsolationRequest(const litebus::AID &, std::string &&, std::string &&msg)
{
    auto req = std::make_shared<messages::SetNetworkIsolationRequest>();
    messages::SetNetworkIsolationResponse resp;
    req->ParseFromString(msg);
    resp.set_requestid(req->requestid());
    YRLOG_DEBUG("agent receive SetNetworkIsolationRequest({})", req->requestid());

    if (!ipsetIsolation_->IsIpsetExist()) {
        YRLOG_ERROR("ipset({}) not exist", GetIpsetName());
        resp.set_message("ipset not exist");
        resp.set_code(static_cast<int32_t>(StatusCode::FAILED));
        (void)Send(localSchedFuncAgentMgrAID_, "SetNetworkIsolationResponse", resp.SerializeAsString());
        return;
    }

    if (AgentServiceActor::HandleNetworkIsolation(req)) {
        resp.set_message("success");
        resp.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    } else {
        resp.set_message("ipset command failed");
        resp.set_code(static_cast<int32_t>(StatusCode::FAILED));
        YRLOG_ERROR("agent handle SetNetworkIsolationRequest({}) failed", req->requestid());
    }
    (void)Send(localSchedFuncAgentMgrAID_, "SetNetworkIsolationResponse", resp.SerializeAsString());
}

litebus::Option<StaticFunctionConfig> GetFunctionCfgFromEnv()
{
    auto path = litebus::os::GetEnv(SF_CONFIG_PATH);
    if (path.IsNone() || path.Get().empty()) {
        YRLOG_WARN("env: {} not exist", SF_CONFIG_PATH);
        return litebus::None();
    }

    StaticFunctionConfig cfg = {};
    cfg.functionMetaPath = path.Get();

    if (auto timeout = litebus::os::GetEnv(SF_SCHEDULE_TIMEOUT_MS); timeout.IsSome() && !timeout.Get().empty()) {
        try {
            cfg.scheduleTimeoutMs = std::stoi(timeout.Get());
        } catch (const std::exception &e) {
            YRLOG_WARN("failed to parse env {}, exception e.what():{}", SF_SCHEDULE_TIMEOUT_MS, e.what());
        }
    }

    if (auto type = litebus::os::GetEnv(SF_INSTANCE_TYPE_NOTE); type.IsSome()) {
        cfg.instanceTypeNote = type.Get();
    }

    if (auto directoryInfo = litebus::os::GetEnv(SF_DELEGATE_DIRECTORY_INFO); directoryInfo.IsSome()) {
        cfg.delegateDirectoryInfo = directoryInfo.Get();
    }

    if (auto labels = litebus::os::GetEnv(SF_INVOKE_LABELS); labels.IsSome()) {
        cfg.invokeLabels = labels.Get();
    }

    if (auto podName = litebus::os::GetEnv("POD_NAME"); podName.IsSome()) {
        cfg.extensions["podName"] = podName.Get();
    }

    if (auto podNamespace = litebus::os::GetEnv("POD_NAMESPACE"); podNamespace.IsSome()) {
        cfg.extensions["podNamespace"] = podNamespace.Get();
    }

    if (auto deploymentName = litebus::os::GetEnv("POD_DEPLOYMENT_NAME"); deploymentName.IsSome()) {
        cfg.extensions["podDeploymentName"] = deploymentName.Get();
    }

    if (auto instanceAz = litebus::os::GetEnv(RUNTIME_MICROSERVICE_AZ_ENV); instanceAz.IsSome()) {
        cfg.extensions[INSTANCE_AZ] = instanceAz.Get();
    }

    if (auto dataSystemFeatureUsed = litebus::os::GetEnv("DATA_SYSTEM_FEATURE_USED"); dataSystemFeatureUsed.IsSome()) {
        cfg.extensions["dataSystemFeatureUsed"] = dataSystemFeatureUsed.Get();
    }

    if (auto functionSignature = litebus::os::GetEnv(SF_FUNCTION_SIGNATURE); functionSignature.IsSome()) {
        cfg.functionSignature = functionSignature.Get();
    }
    return cfg;
}

litebus::Option<FunctionMeta> LoadFunctionMetas(const std::string &path)
{
    if (!litebus::os::ExistPath(path)) {
        YRLOG_WARN("function meta file({}) not exist", path);
        return litebus::None();
    }

    const auto metaOpt = litebus::os::Read(path);
    if (metaOpt.IsNone() || metaOpt.Get().empty()) {
        YRLOG_WARN("function meta file({}) is empty", path);
        return litebus::None();
    }

    return GetFuncMetaFromJson(metaOpt.Get());
}

std::shared_ptr<messages::ScheduleRequest> AgentServiceActor::CreateScheduleRequest(const StaticFunctionConfig &config,
                                                                                    const FunctionMeta &meta)
{
    const auto createRequest = std::make_shared<CreateRequest>();

    auto uuid = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    uuid.erase(std::remove(uuid.begin(), uuid.end(), '-'), uuid.end());
    const auto requestId = uuid.substr(0, 8) + uuid.substr(uuid.size() - 8, 8) + "00";

    createRequest->set_traceid(requestId);
    createRequest->set_requestid(requestId);

    auto languageInfo = GetLanguageInfo(meta);
    createRequest->set_function(languageInfo.functionId);

    createRequest->mutable_schedulingops()->set_scheduletimeoutms(config.scheduleTimeoutMs);
    (*createRequest->mutable_schedulingops()->mutable_extension())[SCHEDULE_POLICY] = MONOPOLY_SCHEDULE;
    auto scheduleRequest = TransFromCreateReqToScheduleReq(std::move(*createRequest), "");

    BuildScheduleRequest(scheduleRequest, meta, config);

    (*scheduleRequest->mutable_instance()->mutable_createoptions())[RESOURCE_OWNER_KEY] = STATIC_FUNCTION_OWNER_VALUE;
    (*scheduleRequest->mutable_instance()->mutable_scheduleoption()->mutable_resourceselector())[RESOURCE_OWNER_KEY] =
        agentID_;

    for (const auto &[key, value] : config.extensions) {
        (*scheduleRequest->mutable_instance()->mutable_extensions())[key] = value;
    }
    return scheduleRequest;
}

litebus::Future<Status> AgentServiceActor::CreateStaticFunctionInstance()
{
    const auto funcConfig = GetFunctionCfgFromEnv();
    if (funcConfig.IsNone()) {
        YRLOG_WARN("static function env not exist");
        return Status(StatusCode::FAILED, "static function env not exist");
    }

    const auto funcMeta = LoadFunctionMetas(funcConfig.Get().functionMetaPath);
    if (funcMeta.IsNone()) {
        YRLOG_WARN("invalid function metas info");
        return Status(StatusCode::FAILED, "invalid function metas info");
    }

    enableRestartForReuse_ = true;  // 静态函数支持function-agent故障后重调度使用
    const auto request = CreateScheduleRequest(funcConfig.Get(), funcMeta.Get());

    scheduleResponsePromise_ = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    YRLOG_INFO("{}|agent send static function ({}) schedule request to {}", request->instance().requestid(),
               request->instance().function(), std::string(localSchedFuncAgentMgrAID_));
    (void)Send(localSchedFuncAgentMgrAID_, "StaticFunctionScheduleRequest", request->SerializeAsString());
    (void)AsyncAfter(retryScheduleInterval_, GetAID(), &AgentServiceActor::RetryInstanceSchedule,
                     request->SerializeAsString(), request->requestid(), 0);
    return Status::OK();
}

void AgentServiceActor::RetryInstanceSchedule(const std::string &msg, const std::string &requestId, uint32_t retryTime)
{
    if (!scheduleResponsePromise_) {
        return;
    }
    if (const auto registerResponseFuture = scheduleResponsePromise_->GetFuture();
        registerResponseFuture.IsOK() && registerResponseFuture.Get().code() == static_cast<int>(StatusCode::SUCCESS)) {
        YRLOG_INFO("{}|agent send static function schedule request to {} success", requestId,
                   std::string(localSchedFuncAgentMgrAID_));
        return;
    }
    if (retryTime < DEFAULE_SCHEDULE_RETRY_TIMES) {
        YRLOG_INFO("{}|retry({}) agent send static function schedule request to {}", requestId, retryTime + 1,
                   std::string(localSchedFuncAgentMgrAID_));
        scheduleResponsePromise_ = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
        Send(localSchedFuncAgentMgrAID_, "StaticFunctionScheduleRequest", std::string(msg));
        (void)AsyncAfter(retryScheduleInterval_, GetAID(), &AgentServiceActor::RetryInstanceSchedule, msg, requestId,
                         retryTime + 1);
    }
}

void AgentServiceActor::StaticFunctionScheduleResponse(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::ScheduleResponse scheduleResponse;
    if (msg.empty() || !scheduleResponse.ParseFromString(msg)) {
        YRLOG_WARN("static function schedule response body invalid from({}), msg={}", from.HashString(), msg);
        return;
    }
    scheduleResponsePromise_->SetValue(scheduleResponse);
    YRLOG_DEBUG("{}|static function schedule received forward response, from: {}, instanceId: {}",
                scheduleResponse.requestid(), from.HashString(), scheduleResponse.instanceid());
}

void AgentServiceActor::NotifyFunctionStatusChange(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::StaticFunctionChangeRequest request;
    if (msg.empty() || !request.ParseFromString(msg)) {
        YRLOG_WARN("instance's status change is invalid from {}, msg={}", from.HashString(), msg);
        return;
    }

    messages::StaticFunctionChangeResponse response;
    response.set_instanceid(request.instanceid());
    response.set_requestid(request.requestid());  // for retry

    YRLOG_DEBUG("{}|instance({})'s status changes to {}, from: {}", request.requestid(), request.instanceid(),
                request.status(), from.HashString());
    if (request.status() == static_cast<int32_t>(InstanceState::RUNNING)) {
        instanceHealthyMap_[request.instanceid()] = request.status();
    } else {
        if (instanceHealthyMap_.find(request.instanceid()) != instanceHealthyMap_.end()) {
            (void)instanceHealthyMap_.erase(request.instanceid());
        }
    }

    (void)Send(localSchedFuncAgentMgrAID_, "NotifyFunctionStatusChangeResp", response.SerializeAsString());
}

bool AgentServiceActor::IsDelegateWorkingDirPath(const DeployerParameters &deployObject)
{
    // if the bucket id (working dir) is the code destination path, the deployObject is for a delegate working dir code
    return deployObject.request->deploymentconfig().bucketid() == deployObject.destination;
}

litebus::Future<Status> AgentServiceActor::LoadPlugins(const std::string &configs)
{
    if (!pluginClient_) {
        pluginClient_ = std::make_shared<MultiPluginClient>();
    }
    return pluginClient_->RegisterPlugin(configs);
}

void AgentServiceActor::CodePkgThresholdsCfgCallback(const std::string &path,
    const std::string &name, uint32_t mask)
{
    YRLOG_DEBUG("receive code package thresholds config file event. path: {} name: {} mask: {}", path, name, mask);
    if (mask & IN_DELETE) {
        litebus::AsyncAfter(
            WAIT_UPDATE_THRESHOLDS_CONFIGMAP_MS, GetAID(), &AgentServiceActor::LoadCodePkgThresholdsCfg);
    }
}

void AgentServiceActor::LoadCodePkgThresholdsCfg()
{
    auto configPath = litebus::os::Join(codePkgThresholdsCfgPath_, CODE_PKG_THRESHOLDS_CFG_FILE);
    if (!litebus::os::ExistPath(configPath)) {
        YRLOG_INFO("code package thresholds config file not exist.");
        return;
    }
    std::string jsonStr = Read(configPath);

    nlohmann::json thresholds;
    try {
        thresholds = nlohmann::json::parse(jsonStr);
    } catch (const nlohmann::json::parse_error& e) {
        YRLOG_ERROR("code package thresholds config parse failed. error: {}", e.what());
        return;
    }
    auto maxFileCount = GetBoundedJsonValue<int32_t>(thresholds, "file_count_max",
        MIN_FILE_COUNTS, MAX_FILE_COUNTS);
    if (maxFileCount.has_value()) {
        codePackageThresholds_.set_filecountsmax(maxFileCount.value());
    }
    auto maxZipFileSizeMB = GetBoundedJsonValue<int32_t>(thresholds, "zip_file_size_max_MB",
        MIN_FILE_SIZE_MB, MAX_FILE_SIZE_MB);
    if (maxZipFileSizeMB.has_value()) {
        codePackageThresholds_.set_zipfilesizemaxmb(maxZipFileSizeMB.value());
    }
    auto maxUnzipFileSizeMB = GetBoundedJsonValue<int32_t>(thresholds, "unzip_file_size_max_MB",
        MIN_FILE_SIZE_MB, MAX_FILE_SIZE_MB);
    if (maxUnzipFileSizeMB.has_value()) {
        codePackageThresholds_.set_unzipfilesizemaxmb(maxUnzipFileSizeMB.value());
    }
    auto maxDirDepth = GetBoundedJsonValue<int32_t>(thresholds, "dir_depth_max", MIN_DIR_DEPTH, MAX_DIR_DEPTH);
    if (maxDirDepth.has_value()) {
        codePackageThresholds_.set_dirdepthmax(maxDirDepth.value());
    }

    if (deployers_.find(S3_STORAGE_TYPE) != deployers_.end()) {
        if (const auto s3Deployer = std::dynamic_pointer_cast<RemoteDeployer>(deployers_[S3_STORAGE_TYPE]);
                s3Deployer != nullptr) {
            s3Deployer->UpdateCodePackageThresholds(codePackageThresholds_);
        }
    }
    YRLOG_INFO("update code package thresholds finish. filecountsmax({}), zipfilesizemaxmb({}), unzipfilesizemaxmb({}),"
        " dirdepthmax({})", codePackageThresholds_.filecountsmax(), codePackageThresholds_.zipfilesizemaxmb(),
        codePackageThresholds_.unzipfilesizemaxmb(), codePackageThresholds_.dirdepthmax());
}
}  // namespace functionsystem::function_agent
