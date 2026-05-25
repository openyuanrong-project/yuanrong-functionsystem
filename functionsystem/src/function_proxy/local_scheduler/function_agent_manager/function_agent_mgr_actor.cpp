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
#include "function_agent_mgr_actor.h"

#include <async/asyncafter.hpp>
#include <async/collect.hpp>
#include <async/defer.hpp>
#include <iostream>
#include <nlohmann/json.hpp>

#include "common/constants/signal.h"
#include "common/logs/logging.h"
#include "common/metrics/metrics_adapter.h"
#include "common/resource_view/resource_tool.h"
#include "common/resource_view/resource_type.h"
#include "common/types/instance_state.h"
#include "common/utils/generate_message.h"
#include "common/utils/collect_status.h"
#include "local_scheduler/bundle_manager/bundle_mgr.h"
#include "local_scheduler/instance_control/instance_ctrl.h"
#include "local_scheduler/local_scheduler_service/local_sched_srv.h"

namespace functionsystem::local_scheduler {
using std::string;
const std::string AGENT_INFO_PATH = "/yr/agentInfo/";

FunctionAgentMgrActor::FunctionAgentMgrActor(const std::string &name, const Param &param, const std::string &nodeID,
                                             std::shared_ptr<MetaStoreClient> metaStoreClient)
    : BasisActor(name),
      retryTimes_(param.retryTimes),
      retryCycleMs_(param.retryCycleMs),
      pingTimes_(param.pingTimes),
      pingCycleMs_(param.pingCycleMs),
      enableTenantAffinity_(param.enableTenantAffinity),
      tenantPodReuseTimeWindow_(param.tenantPodReuseTimeWindow),
      enableIpv4TenantIsolation_(param.enableIpv4TenantIsolation),
      invalidAgentGCInterval_(param.invalidAgentGCInterval),
      nodeID_(nodeID),
      metaStoreClient_(std::move(metaStoreClient)),
      enableForceDeletePod_(param.enableForceDeletePod)
{
}

FunctionAgentMgrActor::~FunctionAgentMgrActor() noexcept
{
    tenantCacheMap_.clear();
}

void FunctionAgentMgrActor::BindInstanceCtrl(const std::shared_ptr<InstanceCtrl> &instanceCtrl)
{
    instanceCtrl_ = instanceCtrl;
}

void FunctionAgentMgrActor::BindResourceView(const std::shared_ptr<resource_view::ResourceView> &resourceView)
{
    resourceView_ = resourceView;
}

void FunctionAgentMgrActor::BindLocalSchedSrv(const std::shared_ptr<LocalSchedSrv> &localSchedSrv)
{
    localSchedSrv_ = localSchedSrv;
}

void FunctionAgentMgrActor::BindBundleMgr(const std::shared_ptr<BundleMgr> &bundleMgr)
{
    bundleMgr_ = bundleMgr;
}

void FunctionAgentMgrActor::Init()
{
    Receive("Register", &FunctionAgentMgrActor::Register);
    Receive("UpdateResources", &FunctionAgentMgrActor::UpdateResources);
    Receive("UpdateInstanceStatus", &FunctionAgentMgrActor::UpdateInstanceStatus);
    Receive("DeployInstanceResponse", &FunctionAgentMgrActor::DeployInstanceResp);
    Receive("KillInstanceResponse", &FunctionAgentMgrActor::KillInstanceResp);
    Receive("UpdateAgentStatus", &FunctionAgentMgrActor::UpdateAgentStatus);
    Receive("QueryInstanceStatusInfoResponse", &FunctionAgentMgrActor::QueryInstanceStatusInfoResponse);
    Receive("CleanStatusResponse", &FunctionAgentMgrActor::CleanStatusResponse);
    Receive("SetNetworkIsolationResponse", &FunctionAgentMgrActor::SetNetworkIsolationResponse);
    Receive("UpdateLocalStatus", &FunctionAgentMgrActor::UpdateLocalStatus);
    Receive("UpdateCredResponse", &FunctionAgentMgrActor::UpdateCredResponse);
    Receive("SnapshotRuntimeResponse", &FunctionAgentMgrActor::SnapshotRuntimeResponse);
    Receive("QueryDebugInstanceInfosResponse", &FunctionAgentMgrActor::QueryDebugInstanceInfosResponse);
    Receive("StaticFunctionScheduleRequest", &FunctionAgentMgrActor::StaticFunctionScheduleRequest);
    Receive("NotifyFunctionStatusChangeResp", &FunctionAgentMgrActor::NotifyFunctionStatusChangeResp);
}

litebus::Future<Status> FunctionAgentMgrActor::Sync()
{
    ASSERT_IF_NULL(metaStoreClient_);
    YRLOG_INFO("begin retrieve function agent registration information with proxy NODE ID: {}", nodeID_);
    return metaStoreClient_->Get(AGENT_INFO_PATH + nodeID_, {})
        .Then(litebus::Defer(GetAID(), &FunctionAgentMgrActor::OnSyncAgentRegisInfoParser, std::placeholders::_1));
}

litebus::Future<Status> FunctionAgentMgrActor::Recover()
{
    YRLOG_INFO("start recover heartbeat of function proxy.");
    RecoverHeartBeatHelper();
    SyncFailedAgentBundles();
    SyncFailedAgentInstances();
    return Status::OK();
}

litebus::Future<std::shared_ptr<resource_view::ResourceUnit>> FunctionAgentMgrActor::SetFuncAgentInfo(
    const Status &status, const string &funcAgentID, const std::shared_ptr<resource_view::ResourceUnit> &resourceUnit)
{
    litebus::Promise<std::shared_ptr<resource_view::ResourceUnit>> promiseRet;
    if (status != Status(StatusCode::SUCCESS)) {
        YRLOG_ERROR("failed to set agent({}) info, code: {}", funcAgentID, status.ToString());
        promiseRet.SetFailed(static_cast<int32_t>(status.StatusCode()));
        return promiseRet.GetFuture();
    }

    if (funcAgentTable_.find(funcAgentID) == funcAgentTable_.end()) {
        YRLOG_ERROR("failed to set agent({}) info, cannot find agentID in func agent table", funcAgentID);
        promiseRet.SetFailed(static_cast<int32_t>(StatusCode::LS_AGENT_NOT_FOUND));
        return promiseRet.GetFuture();
    }

    aidTable_[funcAgentTable_[funcAgentID].aid] = funcAgentID;

    if (resourceUnit == nullptr) {
        // recover process, cannot get instances IDs, will wait for update information from function agent
        YRLOG_WARN("agent({}) instance IDs are emtpy. wait for updating instance IDs.", funcAgentID);
        return resourceUnit;
    }

    auto &instances = resourceUnit->instances();
    for (auto &inst : instances) {
        (void)funcAgentTable_[funcAgentID].instanceIDs.insert(inst.first);
    }

    YRLOG_INFO("set agent({}) info successfully. instance IDs: {}", funcAgentID,
               nlohmann::json{ funcAgentTable_[funcAgentID].instanceIDs }.dump());

    promiseRet.SetValue(resourceUnit);
    return promiseRet.GetFuture();
}

litebus::Future<Status> FunctionAgentMgrActor::SyncInstances(
    const std::shared_ptr<resource_view::ResourceUnit> &resourceUnit)
{
    litebus::Promise<Status> promiseRet;

    auto instanceCtrl = instanceCtrl_.lock();
    auto bundleMgr = bundleMgr_.lock();
    if (bundleMgr == nullptr || instanceCtrl == nullptr || resourceUnit == nullptr
        || IsEvictedAgent(resourceUnit->id())) {
        YRLOG_ERROR("sync instances fail. instance ctrl or resourceUnit is null or agent is evicted {}.",
                    resourceUnit != nullptr ? resourceUnit->id() : "");
        promiseRet.SetFailed(static_cast<int32_t>(StatusCode::LS_SYNC_INSTANCE_FAIL));
        return promiseRet.GetFuture();
    }
    auto cache = std::make_shared<resource_view::ResourceUnit>(*resourceUnit);
    resourceUnit->clear_instances();
    auto resourceView = resourceView_.lock();
    if (resourceView == nullptr) {
        return Status(StatusCode::LS_SYNC_INSTANCE_FAIL);
    }
    resource_view::GenerateMinimumUnitBucketInfo(*resourceUnit);
    resourceUnit->set_status(static_cast<uint32_t>(UnitStatus::RECOVERING));
    auto funcAgentID = resourceUnit->id();
    return resourceView->AddResourceUnit(*resourceUnit)
        .Then([bundleMgr, funcAgentID]() { return bundleMgr->SyncBundles(funcAgentID); })
        .Then([instanceCtrl, cache](const Status &status) -> litebus::Future<Status> {
            if (status.StatusCode() != StatusCode::SUCCESS) {
                return Status(StatusCode::LS_SYNC_INSTANCE_FAIL);
            }
            YRLOG_INFO("agent({}) begin sync instances.", cache->id());
            return instanceCtrl->SyncInstances(cache);
        })
        .Then([](const Status &status) -> litebus::Future<Status> {
            if (status.StatusCode() != StatusCode::SUCCESS) {
                return Status(StatusCode::LS_SYNC_INSTANCE_FAIL);
            }
            return status;
        });
}

litebus::Future<Status> FunctionAgentMgrActor::EnableFuncAgent(const litebus::Future<Status> &status,
                                                               const std::string &funcAgentID)
{
    if (funcAgentTable_.find(funcAgentID) == funcAgentTable_.end()) {
        YRLOG_ERROR("failed to set agent({}) info, cannot find agentID in func agent table", funcAgentID);
        return Status(StatusCode::FAILED);
    }

    if (status.IsError() || status.Get() == StatusCode::LS_SYNC_INSTANCE_FAIL) {
        if (!IsEvictedAgent(funcAgentID) || !IsEvictingAgent(funcAgentID)) {
            SendCleanStatusToFunctionAgent(funcAgentTable_[funcAgentID].aid, 0);
        }
        // if agent register or recover failure, the instance of evicting agent can be treated as evicted.
        if (IsEvictingAgent(funcAgentID)) {
            auto req = std::make_shared<messages::EvictAgentRequest>();
            req->set_agentid(funcAgentID);
            OnInstanceEvicted(Status::OK(), req);
        }
    }

    litebus::Promise<Status> ret;
    auto recoverPromise = funcAgentTable_[funcAgentID].recoverPromise;
    if (status.IsError()) {
        YRLOG_WARN("enable agent({}) fail. code: {}", funcAgentID, status.GetErrorCode());
        if (recoverPromise != nullptr) {
            recoverPromise->SetValue(false);
        }
        (void)aidTable_.erase(funcAgentTable_[funcAgentID].aid);
        (void)funcAgentTable_.erase(funcAgentID);
        litebus::Async(GetAID(), &FunctionAgentMgrActor::StopHeartbeat, funcAgentID);
        functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().DeletePodResource(funcAgentID);
        if (auto resourceView = resourceView_.lock()) {
            (void)resourceView->DeleteResourceUnit(funcAgentID);
        }
        if (auto bundleMgr = bundleMgr_.lock()) {
            (void)bundleMgr->NotifyFailedAgent(funcAgentID);
        }
        ret.SetFailed(static_cast<int32_t>(status.GetErrorCode()));
        return ret.GetFuture();
    }

    funcAgentTable_[funcAgentID].isEnable = true;
    if (recoverPromise != nullptr) {
        recoverPromise->SetValue(true);
    }
    YRLOG_INFO("agent({}) enabled successfully.", funcAgentID);

    // after enabled successfully, cleanup funcAgentResUpdatedMap_ for this function agent
    // in case funcAgentResUpdatedMap_ won't be set again
    // when every time function agent updates resources
    if (funcAgentResUpdatedMap_.find(funcAgentID) != funcAgentResUpdatedMap_.end()) {
        YRLOG_DEBUG("erase agent({}) from funcAgentResUpdatedMap after enabled successfully.", funcAgentID);
        (void)funcAgentResUpdatedMap_.erase(funcAgentID);
    }
    if (IsEvictingAgent(funcAgentID)) {
        YRLOG_WARN("registered/recovered agent({}) should be evicting", funcAgentID);
        auto req = std::make_shared<messages::EvictAgentRequest>();
        req->set_agentid(funcAgentID);
        // while IsEvictingAgent return true, key of [funcAgentID] must be existed in funcAgentsRegisMap_.
        req->set_timeoutsec(funcAgentsRegisMap_[funcAgentID].evicttimeoutsec());
        EvictInstanceOnAgent(req);
    }
    auto instanceCtrl = instanceCtrl_.lock();
    instanceCtrl->TriggerToWarmUpFunction(funcAgentID);
    return Status(StatusCode::SUCCESS);
}

litebus::Future<Status> FunctionAgentMgrActor::AddFuncAgent(const Status &status, const std::string &funcAgentID,
                                                            const std::shared_ptr<resource_view::ResourceUnit> &view)
{
    litebus::Promise<Status> ret;
    auto resourceView = resourceView_.lock();
    auto bundleMgr = bundleMgr_.lock();
    if (resourceView == nullptr || bundleMgr == nullptr) {
        YRLOG_ERROR("failed to add func agent({}), resource view or bundleMgr is null.", funcAgentID);
        ret.SetFailed(static_cast<int32_t>(StatusCode::LS_RESOURCE_VIEW_IS_NULL));
        return ret.GetFuture();
    }
    YRLOG_INFO("sync instances for agent({}) has been completed. msg: {}.", funcAgentID, status.ToString());
    // Resource consistency
    if (status == StatusCode::SUCCESS && view) {
        YRLOG_INFO("the resource of etcd and agent({}) are the same.", funcAgentID);
        funcAgentTable_[funcAgentID].isInit = true;
        (void)bundleMgr->UpdateBundlesStatus(funcAgentID, UnitStatus::NORMAL);
        return resourceView->UpdateUnitStatus(funcAgentID, UnitStatus::NORMAL);
    }
    return status;
}

void FunctionAgentMgrActor::Register(const litebus::AID &from, string &&name, string &&msg)
{
    YRLOG_INFO("receive register from {}. name {}.", from.HashString(), name);
    if (!IsReady()) {
        YRLOG_WARN("local_scheduler is not recovered, ignore register from {}", from.HashString());
        return;
    }
    if (localStatus_ == static_cast<int32_t>(RegisStatus::EVICTED)) {
        YRLOG_WARN("local_scheduler reject agent register, nodeId is {}", nodeID_);
        return;
    }
    messages::Register req;
    if (msg.empty() || !req.ParseFromString(msg) || req.message().empty()) {
        YRLOG_ERROR("invalid request body of {}. Check register request has function agent registration info.",
                    from.HashString());
        auto resp = GenRegistered(static_cast<int32_t>(StatusCode::PARAMETER_ERROR), "invalid request body");
        Send(from, "Registered", std::move(resp.SerializeAsString()));
        return;
    }
    const auto &funcAgentID = req.name();
    const auto &address = from.Url();

    // make sure to accept registration request after recovering, if this function agent is in recovery,
    // shown here as funcAgentTable_ has FuncAgentInfo with current funcAgentID as key.
    // then drop this registration request
    if (funcAgentTable_.find(funcAgentID) != funcAgentTable_.end()) {
        if (funcAgentTable_[funcAgentID].isEnable) {
            auto resp = GenRegistered(static_cast<int32_t>(StatusCode::SUCCESS), "");
            Send(from, "Registered", std::move(resp.SerializeAsString()));
        } else {
            YRLOG_WARN("function agent ({}) is recovering, drop its registration request.", funcAgentID);
            ResetHeartbeat(funcAgentID, address);
        }
        return;
    }

    if (!CheckFunctionAgentRegisterParam(from, req)) {
        return;
    }

    funcAgentTable_[funcAgentID] = {
        isEnable : false,
        isInit : false,
        recoverPromise : std::make_shared<litebus::Promise<bool>>(),
        aid : from,
        instanceIDs : {}
    };
    auto resourceUnit = std::make_shared<resource_view::ResourceUnit>(req.resource());

    // put function agent registration information to etcd
    litebus::Async(GetAID(), &FunctionAgentMgrActor::PutAgentRegisInfoWithProxyNodeID)
        .OnComplete(
        litebus::Defer(GetAID(), &FunctionAgentMgrActor::LogPutAgentInfo, std::placeholders::_1, funcAgentID));

    // start HeartBeat
    litebus::Async(GetAID(), &FunctionAgentMgrActor::StartHeartbeat, funcAgentID, address)
        .Then(litebus::Defer(GetAID(), &FunctionAgentMgrActor::SetFuncAgentInfo, std::placeholders::_1, funcAgentID,
                             resourceUnit))
        .Then(litebus::Defer(GetAID(), &FunctionAgentMgrActor::SyncInstances, std::placeholders::_1))
        .Then(litebus::Defer(GetAID(), &FunctionAgentMgrActor::AddFuncAgent, std::placeholders::_1, funcAgentID,
                             resourceUnit))
        .OnComplete(
        litebus::Defer(GetAID(), &FunctionAgentMgrActor::EnableFuncAgent, std::placeholders::_1, funcAgentID));
}

bool FunctionAgentMgrActor::CheckFunctionAgentRegisterParam(const litebus::AID &from, const messages::Register &req)
{
    // get function agent registration information
    messages::FuncAgentRegisInfo regisInfo;
    messages::Registered resp;
    auto jsonOpt = google::protobuf::util::JsonParseOptions();
    jsonOpt.ignore_unknown_fields = true;
    if (!google::protobuf::util::JsonStringToMessage(req.message(), &regisInfo, jsonOpt).ok()) {
        YRLOG_ERROR(
            "invalid request body of {}. "
            "Check register request function agent registration info in correct format.",
            from.HashString());
        resp = GenRegistered(static_cast<int32_t>(StatusCode::PARAMETER_ERROR), "invalid request message format");
        Send(from, "Registered", std::move(resp.SerializeAsString()));
        return false;
    }

    const auto &funcAgentID = req.name();
    if (auto regisInfoIte(funcAgentsRegisMap_.find(funcAgentID)); regisInfoIte != funcAgentsRegisMap_.end()) {
        if (regisInfoIte->second.runtimemgrid() == regisInfo.runtimemgrid() &&
            regisInfoIte->second.statuscode() == static_cast<int>(RegisStatus::FAILED)) {
            YRLOG_WARN("function agent({}) with runtime manager({}) retry register failed, need to clean status.",
                       funcAgentID, regisInfo.runtimemgrid());
            resp = GenRegistered(static_cast<int32_t>(StatusCode::FAILED), funcAgentID + " retry register failed");
            Send(from, "Registered", std::move(resp.SerializeAsString()));
            return false;
        }

        if (regisInfoIte->second.runtimemgrid() == regisInfo.runtimemgrid() &&
            regisInfoIte->second.statuscode() == static_cast<int>(RegisStatus::EVICTED)) {
            YRLOG_WARN("function agent({}) with runtime manager({}) retry register failed, agent has been evicted",
                       funcAgentID, regisInfo.runtimemgrid());
            resp = GenRegistered(static_cast<int32_t>(StatusCode::LS_AGENT_EVICTED),
                                 funcAgentID + " failed to register, has been evicted");
            Send(from, "Registered", std::move(resp.SerializeAsString()));
            return false;
        }
    }

    regisInfo.set_statuscode(static_cast<int>(FunctionAgentMgrActor::RegisStatus::SUCCESS));
    funcAgentsRegisMap_[funcAgentID] = regisInfo;

    resp = GenRegistered(static_cast<int32_t>(StatusCode::SUCCESS), funcAgentID + " register successfully");
    Send(from, "Registered", std::move(resp.SerializeAsString()));
    YRLOG_INFO("get register request of agent({}) and response. aid: {}", funcAgentID, from.HashString());
    return true;
}

void FunctionAgentMgrActor::LogPutAgentInfo(const litebus::Future<Status> &status, const std::string &funcAgentID)
{
    if (status.IsOK()) {
        YRLOG_DEBUG("put function agent ({}) registration to etcd success.", funcAgentID);
        return;
    }
    YRLOG_ERROR("put function agent ({}) registration to etcd error {}", funcAgentID, status.GetErrorCode());
}

bool FunctionAgentMgrActor::ValidateUpdateResourcesRequest(
    messages::UpdateResourcesRequest &req, const litebus::AID &from)
{
    if (aidTable_.find(from) == aidTable_.end()) {
        YRLOG_WARN("function agent {} not registered. failed to update resources.", from.HashString());
        return false;
    }

    if (funcAgentTable_.find(aidTable_[from]) == funcAgentTable_.end()) {
        YRLOG_WARN("function agent {} not registered, failed to update resources.", from.HashString());
        return false;
    }

    // set resource unit for updated resources function agent
    if (funcAgentResUpdatedMap_.find(aidTable_[from]) != funcAgentResUpdatedMap_.end()) {
        std::shared_ptr<resource_view::ResourceUnit> resourceUnit =
            std::make_shared<resource_view::ResourceUnit>(req.resourceunit());
        funcAgentResUpdatedMap_[aidTable_[from]].SetValue(resourceUnit);
        YRLOG_DEBUG("function agent ({}) set ResourceUnit successfully in update resource process.", from.HashString());
    }

    if (!funcAgentTable_[aidTable_[from]].isEnable) {
        YRLOG_WARN("function agent {} isn't enabled. failed to update resources.", from.HashString());
        return false;
    }

    // evicted agent don't need to update resource
    if (funcAgentsRegisMap_.find(aidTable_[from]) != funcAgentsRegisMap_.end() &&
        funcAgentsRegisMap_[aidTable_[from]].statuscode() == static_cast<int32_t>(RegisStatus::EVICTED)) {
        return false;
    }

    return true;
}

void FunctionAgentMgrActor::UpdateResources(const litebus::AID &from, string &&, string &&msg)
{
    messages::UpdateResourcesRequest req;
    if (msg.empty() || !req.ParseFromString(msg)) {
        YRLOG_WARN("invalid request body from {}. failed to update resources.", from.HashString());
        return;
    }
    if (!ValidateUpdateResourcesRequest(req, from)) {
        return;
    }

    functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetPodResource(
        aidTable_[from], req.resourceunit());

    // set resource labels into instance info
    auto nodelabels = std::map<std::string, resources::Value::Counter>(req.resourceunit().nodelabels().begin(),
                                                                       req.resourceunit().nodelabels().end());
    if (auto instanceCtrl = instanceCtrl_.lock()) {
        instanceCtrl->SetNodeLabelsToMetricsContext(aidTable_[from], nodelabels);
    }

    // send resource view
    if (auto resourceView = resourceView_.lock()) {
        if (funcAgentTable_[aidTable_[from]].isInit) {
            auto unit = std::make_shared<resource_view::ResourceUnit>(std::move(*req.mutable_resourceunit()));
            // todo: pass flag to indicate whether to update only actual resources
            (void)resourceView->UpdateResourceUnit(unit, resource_view::UpdateType::UPDATE_DYNAMIC);
        } else {
            YRLOG_DEBUG("start to add resource of agent({}) to view.", from.HashString());
            funcAgentTable_[aidTable_[from]].isInit = true;
            resource_view::GenerateMinimumUnitBucketInfo(*req.mutable_resourceunit());
            resourceView->AddResourceUnit(req.resourceunit());
        }
    } else {
        YRLOG_ERROR("resource view object is null. failed to update resources.");
    }
}

void FunctionAgentMgrActor::UpdateInstanceStatus(const litebus::AID &from, string &&, string &&msg)
{
    if (aidTable_.find(from) == aidTable_.end()) {
        YRLOG_WARN("function agent {} not registered, failed to update resources.", from.HashString());
        return;
    }

    if (funcAgentTable_.find(aidTable_[from]) == funcAgentTable_.end() || !funcAgentTable_[aidTable_[from]].isEnable) {
        YRLOG_WARN("function agent {} isn't enabled, failed to update resources.", from.HashString());
        return;
    }

    messages::UpdateInstanceStatusRequest req;
    if (msg.empty() || !req.ParseFromString(msg)) {
        YRLOG_WARN("invalid request body, failed to update resources.");
        return;
    }

    const auto &info = req.instancestatusinfo();

    auto instanceStatusInfo = GenInstanceStatusInfo(info.instanceid(), info.status(), info.instancemsg(), info.type());
    auto requestID(info.requestid());
    if (auto instanceCtrl = instanceCtrl_.lock()) {
        // need reschedule if req.instancestatusinfo().status() is StatusCode::RUNTIME_ERROR_NON_FATAL
        YRLOG_INFO("update instance({}) status({}) for request({}).", info.instanceid(), info.status(),
                   info.requestid());
        (void)instanceCtrl->UpdateInstanceStatus(instanceStatusInfo)
            .Then(litebus::Defer(GetAID(), &FunctionAgentMgrActor::UpdateInstanceStatusResp, std::placeholders::_1,
                                 from, requestID));
    } else {
        YRLOG_ERROR("instance({}) ctrl is null, failed to update resources for request({}).", info.instanceid(),
                    info.requestid());
    }
}

litebus::Future<Status> FunctionAgentMgrActor::UpdateInstanceStatusResp(const Status &status, const litebus::AID &aid,
                                                                        const string &requestID)
{
    messages::UpdateInstanceStatusResponse resp =
        GenUpdateInstanceStatusResponse(status.StatusCode(), status.ToString(), requestID);
    Send(aid, "UpdateInstanceStatusResponse", resp.SerializeAsString());
    return Status(StatusCode::SUCCESS);
}

void FunctionAgentMgrActor::DeployInstanceResp(const litebus::AID &from, string &&, string &&msg)
{
    messages::DeployInstanceResponse resp;
    if (msg.empty() || !resp.ParseFromString(msg)) {
        YRLOG_WARN("invalid request body, failed to get response of deploy instance from {}.", from.HashString());
        return;
    }

    if (aidTable_.find(from) == aidTable_.end()) {
        YRLOG_WARN("no agent matches {}, failed to get response of deploy instance.", std::string(from));
        return;
    }
    auto funcAgentID = aidTable_[from];
    auto agentDeployNotifyPromise = deployNotifyPromise_.find(funcAgentID);
    if (agentDeployNotifyPromise == deployNotifyPromise_.end()) {
        YRLOG_WARN("no funcAgentID {} matches result! failed to get response of deploy instance.", funcAgentID);
        return;
    }

    auto requestID(resp.requestid());
    if (auto iter(agentDeployNotifyPromise->second.find(requestID));
        iter == agentDeployNotifyPromise->second.end() || iter->second.first == nullptr) {
        YRLOG_WARN("no requestID {} matches result! failed to get response of deploy instance.", requestID);
        return;
    }

    agentDeployNotifyPromise->second[requestID].first->SetValue(resp);
    (void)agentDeployNotifyPromise->second.erase(requestID);
    (void)funcAgentTable_[funcAgentID].instanceIDs.insert(resp.instanceid());

    YRLOG_INFO("{}|deploy instance({}) on {}. address:{} pid:{} code:{} message:{}", requestID, resp.instanceid(),
               funcAgentID, resp.address(), resp.pid(), resp.code(), resp.message());
}

void FunctionAgentMgrActor::KillInstanceResp(const litebus::AID &from, string &&, string &&msg)
{
    messages::KillInstanceResponse resp;
    if (msg.empty() || !resp.ParseFromString(msg)) {
        YRLOG_WARN("invalid request body. failed to get response of kill instance from {}.", from.HashString());
        return;
    }

    auto requestID(resp.requestid());
    if (aidTable_.find(from) == aidTable_.end()) {
        YRLOG_WARN("{}|not find aid({}) to notify response for killing instance.", requestID, from.HashString());
        return;
    }
    std::string funcAgentID = aidTable_[from];
    auto agentKillNotifyPromise = killNotifyPromise_.find(funcAgentID);
    if (agentKillNotifyPromise == killNotifyPromise_.end()) {
        YRLOG_WARN("{}|not find agent({}) promise to notify response for killing instance.", requestID, funcAgentID);
        return;
    }

    if (auto iter(agentKillNotifyPromise->second.find(requestID));
        iter == agentKillNotifyPromise->second.end() || iter->second.first == nullptr) {
        YRLOG_WARN("{}|not find promise to notify response for killing instance.", requestID);
        return;
    }

    YRLOG_INFO("{}|success to kill instance({}) from function_agent({}), resp code({}), resp message({})", requestID,
               resp.instanceid(), funcAgentID, resp.code(), resp.message());
    agentKillNotifyPromise->second[requestID].first->SetValue(resp);
    (void)agentKillNotifyPromise->second.erase(requestID);

    if (funcAgentTable_.find(funcAgentID) == funcAgentTable_.end()) {
        YRLOG_WARN("{}|not find functionAgentID({}) to remove corresponding instance.", requestID, funcAgentID);
        return;
    }
    (void)funcAgentTable_[funcAgentID].instanceIDs.erase(resp.instanceid());

    if (auto iter = monopolyAgents_.find(funcAgentID); iter != monopolyAgents_.end()) {
        YRLOG_DEBUG("{}|agent({}) for instance({}) is monopoly, disconnect from agent", requestID, funcAgentID,
                    resp.instanceid());
        TimeoutEvent(funcAgentID);
        (void)funcAgentsRegisMap_.erase(funcAgentID);
        PutAgentRegisInfoWithProxyNodeID();
    }
}

void FunctionAgentMgrActor::RetryDeploy(const string &requestID, const string &funcAgentID,
                                        const std::shared_ptr<messages::DeployInstanceRequest> &request)
{
    auto agentDeployNotifyPromise = deployNotifyPromise_.find(funcAgentID);
    if (agentDeployNotifyPromise == deployNotifyPromise_.end() ||
        agentDeployNotifyPromise->second.find(requestID) == agentDeployNotifyPromise->second.end() ||
        agentDeployNotifyPromise->second[requestID].first->GetFuture().IsOK()) {
        YRLOG_INFO("{}|a response of deploy instance has been received.", requestID);
        return;
    }

    auto iter = funcAgentTable_.find(funcAgentID);
    if (agentDeployNotifyPromise->second[requestID].second++ < retryTimes_ && iter != funcAgentTable_.end()) {
        YRLOG_INFO("{}|retry to send request to deploy instance, times: {}.", requestID,
                   agentDeployNotifyPromise->second[requestID].second);
        Send(funcAgentTable_[funcAgentID].aid, "DeployInstance", request->SerializeAsString());
        litebus::AsyncAfter(retryCycleMs_, GetAID(), &FunctionAgentMgrActor::RetryDeploy, requestID, funcAgentID,
                            request);
        return;
    }

    YRLOG_ERROR("{}|the number of retry to deploy instance is more than {}.", requestID, retryTimes_);
    messages::DeployInstanceResponse resp = GenDeployInstanceResponse(
        StatusCode::ERR_INNER_COMMUNICATION,
        iter == funcAgentTable_.end() ? funcAgentID + " connection timeout" : "deploy retry fail", requestID);
    agentDeployNotifyPromise->second[requestID].first->SetValue(resp);
    (void)agentDeployNotifyPromise->second.erase(requestID);
}

void FunctionAgentMgrActor::RetryKill(const std::string &requestID, const std::string &funcAgentID,
                                      const std::shared_ptr<messages::KillInstanceRequest> &request)
{
    auto agentKillNotifyPromise = killNotifyPromise_.find(funcAgentID);
    if (agentKillNotifyPromise == killNotifyPromise_.end() ||
        agentKillNotifyPromise->second.find(requestID) == agentKillNotifyPromise->second.end() ||
        agentKillNotifyPromise->second[requestID].first == nullptr ||
        agentKillNotifyPromise->second[requestID].first->GetFuture().IsOK()) {
        YRLOG_INFO("{}|received a response of kill instance.", requestID);
        return;
    }

    auto iter = funcAgentTable_.find(funcAgentID);
    if (agentKillNotifyPromise->second[requestID].second++ < retryTimes_ && iter != funcAgentTable_.end()) {
        Send(funcAgentTable_[funcAgentID].aid, "KillInstance", request->SerializeAsString());
        litebus::AsyncAfter(retryCycleMs_, GetAID(), &FunctionAgentMgrActor::RetryKill, requestID, funcAgentID,
                            request);
        YRLOG_INFO("{}|retry {} times request to kill instance.", requestID,
                   agentKillNotifyPromise->second[requestID].second);
        return;
    }
    messages::KillInstanceResponse resp = GenKillInstanceResponse(
        StatusCode::ERR_INNER_COMMUNICATION,
        iter == funcAgentTable_.end() ? funcAgentID + " connection timeout" : "kill retry fail", requestID);
    agentKillNotifyPromise->second[requestID].first->SetValue(resp);
    (void)agentKillNotifyPromise->second.erase(requestID);
    YRLOG_INFO("{}|the times of retry to kill instance is more than {}.", requestID, retryTimes_);
}

litebus::Future<messages::DeployInstanceResponse> FunctionAgentMgrActor::DeployInstance(
    const std::shared_ptr<messages::DeployInstanceRequest> &request, const string &funcAgentID)
{
    ASSERT_IF_NULL(request);
    auto requestID = request->requestid();

    auto fcAgent = funcAgentTable_.find(funcAgentID);
    if (fcAgent == funcAgentTable_.end()) {
        messages::DeployInstanceResponse response =
            GenDeployInstanceResponse(StatusCode::ERR_INNER_COMMUNICATION, "function agent is not register", requestID);
        YRLOG_ERROR("{}|failed to deploy instance, function agent {} is not registered.", requestID, funcAgentID);
        return response;
    }

    auto notifyPromise = std::make_shared<DeployNotifyPromise>();
    auto emplaceResult = deployNotifyPromise_[funcAgentID].emplace(requestID, std::make_pair(notifyPromise, 0));
    if (!emplaceResult.second) {
        YRLOG_INFO("{}|{}|request ID is repeat.", request->traceid(), requestID);
        return deployNotifyPromise_[funcAgentID][requestID].first->GetFuture();
    }

    YRLOG_INFO("{}|send request to agent({}) for deploying instance({}).", requestID, funcAgentID,
               request->instanceid());
    Send(fcAgent->second.aid, "DeployInstance", request->SerializeAsString());

    litebus::AsyncAfter(retryCycleMs_, GetAID(), &FunctionAgentMgrActor::RetryDeploy, requestID, funcAgentID, request);

    return notifyPromise->GetFuture();
}

litebus::Future<messages::StaticFunctionChangeResponse> FunctionAgentMgrActor::NotifyFunctionStatusChange(
    const std::shared_ptr<messages::StaticFunctionChangeRequest> &request, const string &funcAgentID)
{
    ASSERT_IF_NULL(request);
    auto &requestID = request->requestid();
    const auto fcAgent = funcAgentTable_.find(funcAgentID);
    if (fcAgent == funcAgentTable_.end()) {
        auto &instanceID = request->instanceid();
        messages::StaticFunctionChangeResponse response = GenStaticFunctionChangeResponse(
            StatusCode::ERR_INNER_COMMUNICATION, "function agent is not register", requestID, instanceID);
        YRLOG_ERROR("{}|failed to notify instance {} status change, function agent {} is not registered.", requestID,
                    instanceID, funcAgentID);
        return response;
    }

    auto notifyPromise = std::make_shared<instanceStatusNotifyPromise>();
    auto emplaceResult =
        instanceHealthyNotifyPromise_[funcAgentID].emplace(requestID, std::make_pair(notifyPromise, 0));
    if (!emplaceResult.second) {
        YRLOG_INFO("{}|request ID is repeat.", requestID);
        return instanceHealthyNotifyPromise_[funcAgentID][requestID].first->GetFuture();
    }

    YRLOG_INFO("{}|send request to agent({}) to change instance({}) status to ({}).", requestID, funcAgentID,
               request->instanceid(), request->status());
    Send(fcAgent->second.aid, "NotifyFunctionStatusChange", request->SerializeAsString());

    litebus::AsyncAfter(retryCycleMs_, GetAID(), &FunctionAgentMgrActor::RetryNotifyFunctionStatus, requestID,
                        funcAgentID, request);

    return notifyPromise->GetFuture();
}

void FunctionAgentMgrActor::RetryNotifyFunctionStatus(
    const string &requestID, const string &funcAgentID,
    const std::shared_ptr<messages::StaticFunctionChangeRequest> &request)
{
    auto healthyNotifyPromise = instanceHealthyNotifyPromise_.find(funcAgentID);
    if (healthyNotifyPromise == instanceHealthyNotifyPromise_.end()
        || healthyNotifyPromise->second.find(requestID) == healthyNotifyPromise->second.end()
        || healthyNotifyPromise->second[requestID].first->GetFuture().IsOK()) {
        YRLOG_INFO("{}|a response of notify instance status change has been received.", requestID);
        return;
    }

    auto iter = funcAgentTable_.find(funcAgentID);
    if (healthyNotifyPromise->second[requestID].second++ < retryTimes_ && iter != funcAgentTable_.end()) {
        YRLOG_INFO("{}|retry to send notify instance status change, times: {}.", requestID,
                   healthyNotifyPromise->second[requestID].second);
        Send(funcAgentTable_[funcAgentID].aid, "NotifyFunctionStatusChange", request->SerializeAsString());
        litebus::AsyncAfter(retryCycleMs_, GetAID(), &FunctionAgentMgrActor::RetryNotifyFunctionStatus, requestID,
                            funcAgentID, request);
        return;
    }

    YRLOG_ERROR("{}|the number of retry to notify instance healthy change is more than {}.", requestID, retryTimes_);
    messages::StaticFunctionChangeResponse resp = GenStaticFunctionChangeResponse(
        StatusCode::ERR_INNER_COMMUNICATION,
        iter == funcAgentTable_.end() ? funcAgentID + " connection timeout" : "notify retry fail", requestID,
        request->instanceid());
    healthyNotifyPromise->second[requestID].first->SetValue(resp);
    (void)healthyNotifyPromise->second.erase(requestID);
}

void FunctionAgentMgrActor::NotifyFunctionStatusChangeResp(const litebus::AID &from, string &&, string &&msg)
{
    messages::StaticFunctionChangeResponse resp;
    if (msg.empty() || !resp.ParseFromString(msg)) {
        YRLOG_WARN("invalid request body, failed to get response of notify instance status change from {}.",
                   from.HashString());
        return;
    }

    if (aidTable_.find(from) == aidTable_.end()) {
        YRLOG_WARN("no agent matches {}, failed to get response of notify instance status change.", std::string(from));
        return;
    }
    auto funcAgentID = aidTable_[from];
    auto healthyNotifyPromise = instanceHealthyNotifyPromise_.find(funcAgentID);
    if (healthyNotifyPromise == instanceHealthyNotifyPromise_.end()) {
        YRLOG_WARN("no funcAgentID {} matches result! failed to get response of notify instance status change.",
                   funcAgentID);
        return;
    }

    auto requestID(resp.requestid());
    if (auto iter(healthyNotifyPromise->second.find(requestID));
        iter == healthyNotifyPromise->second.end() || iter->second.first == nullptr) {
        YRLOG_WARN("no requestID {} matches result! failed to get response of notify instance.", requestID);
        return;
    }

    healthyNotifyPromise->second[requestID].first->SetValue(resp);
    (void)healthyNotifyPromise->second.erase(requestID);

    YRLOG_INFO("{}|notify instance status change instance({}) successfully ", requestID, resp.instanceid());
}

litebus::Future<messages::KillInstanceResponse> FunctionAgentMgrActor::KillInstance(
    const std::shared_ptr<messages::KillInstanceRequest> &request, const string &funcAgentID, bool isRecovering)
{
    auto requestID = request->requestid();

    auto fcAgent = funcAgentTable_.find(funcAgentID);
    if (fcAgent == funcAgentTable_.end()) {
        messages::KillInstanceResponse response =
            GenKillInstanceResponse(StatusCode::ERR_INNER_COMMUNICATION, "function agent not register", requestID);
        YRLOG_ERROR("{}|failed to kill instance, function agent {} is not register.", requestID, funcAgentID);
        return response;
    }

    if (!fcAgent->second.isEnable && !isRecovering) {
        messages::KillInstanceResponse response =
            GenKillInstanceResponse(StatusCode::SUCCESS, "function agent may already exited", requestID);
        YRLOG_DEBUG("{}|function agent {} may already exited", requestID, funcAgentID);
        funcAgentTable_[funcAgentID].instanceIDs.erase(request->instanceid());
        return response;
    }

    auto notifyPromise = std::make_shared<KillNotifyPromise>();
    auto notifyFuture = notifyPromise->GetFuture();
    auto emplaceResult = killNotifyPromise_[funcAgentID].emplace(requestID, std::make_pair(notifyPromise, 0));
    if (!emplaceResult.second) {
        YRLOG_INFO("{}|{}|request ID is repeat.", request->traceid(), requestID);
        return killNotifyPromise_[funcAgentID][requestID].first->GetFuture();
    }
    YRLOG_DEBUG("{}|send instance({}) kill request, runtimeID({}), storage type({})", request->requestid(),
                request->instanceid(), request->runtimeid(), request->storagetype());
    Send(fcAgent->second.aid, "KillInstance", request->SerializeAsString());

    litebus::AsyncAfter(retryCycleMs_, GetAID(), &FunctionAgentMgrActor::RetryKill, requestID, funcAgentID, request);

    YRLOG_INFO("{}|send request of kill instance({}) successfully on {}.", requestID, request->instanceid(),
               funcAgentID);
    if (request->ismonopoly()) {
        (void)monopolyAgents_.emplace(funcAgentID);
    }
    return notifyFuture;
}

litebus::Future<std::string> FunctionAgentMgrActor::Dump()
{
    string ret;
    for (auto &fcAgentInfo : funcAgentTable_) {
        ret +=
            nlohmann::json{
                { "ID", fcAgentInfo.first },
                { "aid", fcAgentInfo.second.aid },
                { "instanceIDs", fcAgentInfo.second.instanceIDs }
            }.dump() +
            "\n";
    }

    return ret;
}

litebus::Future<bool> FunctionAgentMgrActor::IsRegistered(const std::string &funcAgentID)
{
    if (funcAgentTable_.find(funcAgentID) == funcAgentTable_.end()) {
        return false;
    }
    return funcAgentTable_[funcAgentID].isEnable;
}

void FunctionAgentMgrActor::TimeoutEvent(const string &funcAgentID)
{
    if (funcAgentTable_.find(funcAgentID) == funcAgentTable_.end()) {
        YRLOG_WARN("func agent id({}) doesn't exist", funcAgentID);
        return;
    }

    (void)aidTable_.erase(funcAgentTable_[funcAgentID].aid);
    if (auto instanceCtrl = instanceCtrl_.lock()) {
        instanceCtrl->PutFailedInstanceStatusByAgentId(funcAgentID);
    }
    if (funcAgentTable_[funcAgentID].recoverPromise != nullptr &&
        funcAgentTable_[funcAgentID].recoverPromise->GetFuture().IsInit()) {
        funcAgentTable_[funcAgentID].recoverPromise->SetValue(false);
    }
    (void)funcAgentTable_.erase(funcAgentID);

    // when lost heartbeat with function agent, there is no need to process the rest of the recovering
    // procedure, so set promise to a failure status
    if (funcAgentResUpdatedMap_.find(funcAgentID) != funcAgentResUpdatedMap_.end()) {
        funcAgentResUpdatedMap_[funcAgentID].SetFailed(
            static_cast<int32_t>(StatusCode::LS_AGENT_MGR_START_HEART_BEAT_FAIL));
    } else {
        litebus::Promise<std::shared_ptr<resource_view::ResourceUnit>> promiseRet;
        promiseRet.SetFailed(static_cast<int32_t>(StatusCode::LS_AGENT_MGR_START_HEART_BEAT_FAIL));
        (void)funcAgentResUpdatedMap_.insert_or_assign(funcAgentID, promiseRet);
    }

    if (monopolyAgents_.find(funcAgentID) != monopolyAgents_.end()) {
        (void)monopolyAgents_.erase(funcAgentID);
    }

    functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().DeletePodResource(funcAgentID);
    if (auto resourceView = resourceView_.lock()) {
        (void)resourceView->DeleteResourceUnit(funcAgentID);
    }

    for (const auto &item : queryReqMap_) {
        if (item.second != funcAgentID) {
            continue;
        }
        (void)queryStatusSync_.RequestTimeout(item.first);
    }

    if (auto iter = deployNotifyPromise_.find(funcAgentID); iter != deployNotifyPromise_.end()) {
        for (auto promise : iter->second) {
            messages::DeployInstanceResponse response =
                GenDeployInstanceResponse(StatusCode::ERR_INNER_COMMUNICATION, "function agent exited", promise.first);
            promise.second.first->SetValue(response);
        }
        iter->second.clear();
        deployNotifyPromise_.erase(funcAgentID);
    }

    if (auto iter = instanceHealthyNotifyPromise_.find(funcAgentID); iter != instanceHealthyNotifyPromise_.end()) {
        for (auto promise : iter->second) {
            messages::StaticFunctionChangeResponse response = GenStaticFunctionChangeResponse(
                StatusCode::ERR_INNER_COMMUNICATION, "function agent exited", promise.first, "");
            promise.second.first->SetValue(response);
        }
        iter->second.clear();
        instanceHealthyNotifyPromise_.erase(funcAgentID);
    }

    if (auto iter = killNotifyPromise_.find(funcAgentID); iter != killNotifyPromise_.end()) {
        for (auto promise : iter->second) {
            messages::KillInstanceResponse response =
                GenKillInstanceResponse(StatusCode::SUCCESS, "function agent may already exited", promise.first);
            promise.second.first->SetValue(response);
        }
        iter->second.clear();
        killNotifyPromise_.erase(funcAgentID);
    }

    if (auto bundleMgr = bundleMgr_.lock(); bundleMgr != nullptr) {
        bundleMgr->NotifyFailedAgent(funcAgentID);
    }
    StopHeartbeat(funcAgentID);
}

void FunctionAgentMgrActor::ResetHeartbeat(const string &funcAgentID, const string &address)
{
    if (pingPongAIDMap_.find(funcAgentID) == pingPongAIDMap_.end()) {
        YRLOG_WARN("failed to reset heartbeat timer if function {}, addr {}, heartbeat not started", funcAgentID,
                   address);
        return;
    }
    if (heartbeatObserveDriver_ == nullptr) {
        YRLOG_WARN("failed to reset heartbeat timer if function {}, addr {}, observer not started", funcAgentID,
                   address);
        return;
    }
    auto pingPongAID = pingPongAIDMap_[funcAgentID];
    heartbeatObserveDriver_->ResetHeartbeatNode(
        pingPongAID, DEFAULT_PING_PONG_TIMEOUT,
        [aid(GetAID()), funcAgentID](const litebus::AID &from, HeartbeatConnection) {
            YRLOG_WARN("heartbeat timeout, from agent to proxy. from: {}.", std::string(from));
            litebus::Async(aid, &FunctionAgentMgrActor::TimeoutEvent, funcAgentID);
        });
}

litebus::Future<Status> FunctionAgentMgrActor::StartHeartbeat(const string &funcAgentID, const string &address)
{
    litebus::AID pingPongAID = litebus::AID(
        HEARTBEAT_CLIENT_BASENAME + litebus::os::Join(COMPONENT_NAME_FUNCTION_AGENT, funcAgentID, '-'), address);
    pingPongAID.SetProtocol(litebus::BUS_TCP);
    if (heartbeatObserveDriver_ == nullptr) {
        heartbeatObserveDriver_ = std::make_shared<HeartbeatObserveDriver>(FUNCTION_AGENT_AGENT_MGR_ACTOR_NAME);
    }

    heartbeatObserveDriver_->AddNewHeartbeatNode(
        pingPongAID, pingCycleMs_ * pingTimes_,
        [aid(GetAID()), funcAgentID](const litebus::AID &from, HeartbeatConnection) {
            YRLOG_WARN("heartbeat timeout, from agent to proxy. from: {}.", std::string(from));
            litebus::Async(aid, &FunctionAgentMgrActor::TimeoutEvent, funcAgentID);
        });

    pingPongAIDMap_[funcAgentID] = pingPongAID;

    return Status(StatusCode::SUCCESS);
}

void FunctionAgentMgrActor::StopHeartbeat(const string &funcAgentID)
{
    if (!IsEvictedAgent(funcAgentID)) {
        // have lost heartbeat with function agent, then update function agent register status failed to etcd
        UpdateFuncAgentRegisInfoStatus(funcAgentID, FunctionAgentMgrActor::RegisStatus::FAILED);
    }
    // defer to garbage collection of failed agent
    litebus::AsyncAfter(invalidAgentGCInterval_, GetAID(), &FunctionAgentMgrActor::DeferGCInvalidAgent, funcAgentID);

    if (heartbeatObserveDriver_ != nullptr) {
        heartbeatObserveDriver_->RemoveHeartbeatNode(pingPongAIDMap_[funcAgentID]);
    }
}

void FunctionAgentMgrActor::UpdateFuncAgentRegisInfoStatus(const string &funcAgentID,
                                                           const FunctionAgentMgrActor::RegisStatus &status)
{
    if (funcAgentsRegisMap_.find(funcAgentID) == funcAgentsRegisMap_.end()) {
        YRLOG_DEBUG("function agent {} not registered in function agent regis map, pass update status.", funcAgentID);
        return;
    }
    messages::FuncAgentRegisInfo updatedInfo = funcAgentsRegisMap_[funcAgentID];
    updatedInfo.set_statuscode(static_cast<int32_t>(status));
    funcAgentsRegisMap_[funcAgentID] = updatedInfo;

    // put function agent registration information to etcd, lost heartbeat so update status to failed
    litebus::Async(GetAID(), &FunctionAgentMgrActor::PutAgentRegisInfoWithProxyNodeID)
        .OnComplete(
        litebus::Defer(GetAID(), &FunctionAgentMgrActor::LogPutAgentInfo, std::placeholders::_1, funcAgentID));
}

void FunctionAgentMgrActor::CleanupAgentResources(const std::string &agentID, bool shouldDeletePod,
                                                  const std::string &logMessage,
                                                  const std::shared_ptr<LocalSchedSrv> &localScheSrv,
                                                  const std::shared_ptr<messages::UpdateAgentStatusRequest> &req)
{
    if (funcAgentTable_.find(agentID) == funcAgentTable_.end()) {
        YRLOG_WARN("try to clean up clean up agent resources, but agent id({}) doesn't exist", agentID);
        return;
    }
    // 1. Disable the Agent
    funcAgentTable_[agentID].isEnable = false;

    // 2. Reschedule all associated instances
    auto &agentInfo = funcAgentTable_[agentID];
    for (const auto &instanceID : agentInfo.instanceIDs) {
        if (auto instanceCtrl = instanceCtrl_.lock()) {
            instanceCtrl->RescheduleAfterJudgeRecoverable(instanceID, agentID);
        }
    }

    // 3. Trigger timeout event and clean up registration information
    TimeoutEvent(agentID);
    funcAgentsRegisMap_.erase(agentID);
    PutAgentRegisInfoWithProxyNodeID();

    // 4. Delete Pod as needed (with safety checks)
    if (shouldDeletePod && localScheSrv) {
        YRLOG_INFO(logMessage.c_str(), agentID);
        localScheSrv->DeletePod(agentID, req->requestid(), req->message());
    }
}

void FunctionAgentMgrActor::UpdateAgentStatus(const litebus::AID &from, string &&, string &&msg)
{
    if (aidTable_.find(from) == aidTable_.end() || funcAgentTable_.find(aidTable_[from]) == funcAgentTable_.end()) {
        YRLOG_WARN("function agent {} not registered, failed to update status.", from.HashString());
        return;
    }

    auto req = std::make_shared<messages::UpdateAgentStatusRequest>();
    if (msg.empty() || !req->ParseFromString(msg)) {
        YRLOG_WARN("invalid request body, failed to update resources.");
        return;
    }

    std::string agentID = aidTable_[from];
    const auto &functionAgentInfo = funcAgentTable_[agentID];
    if (!functionAgentInfo.isEnable) {
        YRLOG_WARN("function agent {} isn't enabled, failed to update status.", from.HashString());
        return;
    }
    auto localScheSrv = localSchedSrv_.lock();
    YRLOG_INFO("{}|Update agent status code: {}, agent :{}, msg: {}", req->requestid(), req->status(), agentID,
               req->message());
    Send(from, "UpdateAgentStatusResponse",
         GenUpdateAgentStatusResponse(req->requestid(), StatusCode::SUCCESS, "").SerializeAsString());
    switch (req->status()) {
        case FUNC_AGENT_STATUS_VPC_PROBE_FAILED:
        case RUNTIME_MANAGER_DISK_USAGE_EXCEED_LIMIT:
            if (localScheSrv == nullptr) {
                YRLOG_ERROR("failed to delete agent({}), localSchedSrv is nullptr.", agentID);
                break;
            }
            if (enableForceDeletePod_) {
                CleanupAgentResources(agentID, true,  // force delete Pod
                                      "exited agent({}) is going to be deleted.", localScheSrv, req);
            }
            break;
        case FUNC_AGENT_EXITING:
            if (auto resourceView = resourceView_.lock()) {
                (void)resourceView->DeleteResourceUnit(agentID);
            }
            break;
        case FUNC_AGENT_EXITED:
        case RUNTIME_MANAGER_REGISTER_FAILED:
            CleanupAgentResources(agentID, enableTenantAffinity_,
                                  "exited agent({}) which may be tainted is going to be deleted.", localScheSrv, req);
        default:
            break;
    }
}

litebus::Future<Status> FunctionAgentMgrActor::PutAgentRegisInfoWithProxyNodeID()
{
    if (abnormal_) {
        // info cannot be written to backend storage when abnormal.
        return Status(StatusCode::ERR_LOCAL_SCHEDULER_ABNORMAL);
    }
    if (persistingAgentInfo_ != nullptr && waitToPutAgentInfo_ == nullptr) {
        waitToPutAgentInfo_ = std::make_shared<litebus::Promise<Status>>();
        return waitToPutAgentInfo_->GetFuture();
    }
    // agentInfo is putting to metastore and other update is waiting to update
    // Merge with currently pending updates
    if (waitToPutAgentInfo_ != nullptr) {
        return waitToPutAgentInfo_->GetFuture();
    }
    persistingAgentInfo_ = std::make_shared<litebus::Promise<Status>>();
    auto future = persistingAgentInfo_->GetFuture();
    DoPutAgentRegisInfoWithProxyNodeID();
    return future;
}

void FunctionAgentMgrActor::DoPutAgentRegisInfoWithProxyNodeID()
{
    YRLOG_INFO("begin put function agent registration information with proxy NODE ID: {}", nodeID_);
    std::string regisInfoStrs = FuncAgentRegisToCollectionStr(funcAgentsRegisMap_);
    PutAgentRegisInfo(regisInfoStrs);
}

litebus::Future<Status> FunctionAgentMgrActor::OnSyncAgentRegisInfoParser(const std::shared_ptr<GetResponse> &getResp)
{
    messages::FuncAgentRegisInfoCollection funcAgentRegisInfoCollection;
    if (getResp->status.IsError()) {
        YRLOG_ERROR("failed to get {}'s function agent info from meta storage", nodeID_);
        return getResp->status;
    }

    if (getResp->kvs.empty()) {
        YRLOG_INFO("get {}'s function agent info from meta storage empty.", nodeID_);
        return Status::OK();
    }
    std::string getRespKvs = getResp->kvs.front().value();
    if (!TransToRegisInfoCollectionFromJson(funcAgentRegisInfoCollection, getRespKvs)) {
        YRLOG_WARN("parse function agent info from JSON {} failed", getRespKvs);
        return Status::OK();
    }

    for (auto &funcAgentInfo : funcAgentRegisInfoCollection.funcagentregisinfomap()) {
        (void)funcAgentsRegisMap_.emplace(funcAgentInfo.first, funcAgentInfo.second);
    }
    localStatus_ = funcAgentRegisInfoCollection.localstatus();
    YRLOG_INFO("get function agent registration information successfully funcAgentsRegisMap size {}, localStatus is {}",
               funcAgentsRegisMap_.size(), localStatus_);
    return Status::OK();
}

std::string FunctionAgentMgrActor::FuncAgentRegisToCollectionStr(
    std::unordered_map<std::string, messages::FuncAgentRegisInfo> &funcAgentsRegisMap)
{
    messages::FuncAgentRegisInfoCollection regisInfoStrCollection;
    for (auto &info : funcAgentsRegisMap) {
        auto infos = regisInfoStrCollection.mutable_funcagentregisinfomap();
        (void)infos->insert({ info.first, info.second });
    }
    regisInfoStrCollection.set_localstatus(localStatus_);

    std::string jsonStr;
    if (!TransToJsonFromRegisInfoCollection(jsonStr, regisInfoStrCollection)) {
        YRLOG_ERROR("failed to trans to json string from FuncAgentRegisInfoCollection");
    }
    return jsonStr;
}

void FunctionAgentMgrActor::PutAgentRegisInfo(const std::string &regisInfoStrs)
{
    ASSERT_IF_NULL(metaStoreClient_);
    YRLOG_INFO("function agent registration infos: {}.", regisInfoStrs);
    (void)metaStoreClient_
        ->Put(AGENT_INFO_PATH + nodeID_, regisInfoStrs, { .leaseId = 0, .prevKv = false, .asyncBackup = false })
        .OnComplete(litebus::Defer(GetAID(), &FunctionAgentMgrActor::OnAgentInfoPut, std::placeholders::_1));
}

void FunctionAgentMgrActor::OnAgentInfoPut(const litebus::Future<std::shared_ptr<PutResponse>> &putResponse)
{
    auto status = Status::OK();
    if (putResponse.IsError() || (putResponse.IsOK() && putResponse.Get()->status.IsError())) {
        YRLOG_WARN("failed to persist agentInfo");
        auto code = putResponse.IsError() ? putResponse.GetErrorCode() : putResponse.Get()->status.StatusCode();
        status = Status(StatusCode::BP_META_STORAGE_PUT_ERROR,
                        "errorResponse: " + std::to_string(code));
    }
    if (persistingAgentInfo_ != nullptr) {
        persistingAgentInfo_->SetValue(status);
        persistingAgentInfo_ = nullptr;
    }
    if (waitToPutAgentInfo_ == nullptr) {
        return;
    }
    // ready to update new agentInfo
    persistingAgentInfo_ = waitToPutAgentInfo_;
    waitToPutAgentInfo_ = nullptr;
    DoPutAgentRegisInfoWithProxyNodeID();
}

void FunctionAgentMgrActor::SyncFailedAgentInstances()
{
    auto instanceCtrl = instanceCtrl_.lock();
    if (instanceCtrl == nullptr) {
        YRLOG_ERROR("failed to sync failed agent instances");
        return;
    }
    (void)instanceCtrl->SyncAgent(funcAgentsRegisMap_);
}

void FunctionAgentMgrActor::SyncFailedAgentBundles()
{
    auto bundleMgr = bundleMgr_.lock();
    if (bundleMgr == nullptr) {
        YRLOG_ERROR("failed to sync failed agent bundles");
        return;
    }
    (void)bundleMgr->SyncFailedBundles(funcAgentsRegisMap_);
}

void FunctionAgentMgrActor::RecoverHeartBeatHelper()
{
    for (auto &funcAgentInfo : funcAgentsRegisMap_) {
        if (auto iter = funcAgentTable_.find(funcAgentInfo.first); iter != funcAgentTable_.end()) {
            YRLOG_INFO("function agent({}) is registering, skip recover", std::string(iter->second.aid));
            continue;
        }

        std::string funcAgentID = funcAgentInfo.first;
        if (funcAgentInfo.second.statuscode() == static_cast<int32_t>(FunctionAgentMgrActor::RegisStatus::FAILED)) {
            YRLOG_WARN("function agent id ({}) register status {} is FAILED, defer to gc.", funcAgentID,
                       funcAgentInfo.second.statuscode());
            litebus::AsyncAfter(invalidAgentGCInterval_, GetAID(), &FunctionAgentMgrActor::DeferGCInvalidAgent,
                                funcAgentID);
        }
        // if function agent registration information status is failed, pass heartbeat recover process
        if (funcAgentInfo.second.statuscode() == static_cast<int32_t>(FunctionAgentMgrActor::RegisStatus::FAILED) ||
            funcAgentInfo.second.statuscode() == static_cast<int32_t>(FunctionAgentMgrActor::RegisStatus::EVICTED)) {
            YRLOG_WARN("function agent id ({}) register status {} is FAILED/EVICTED, passing update resource unit.",
                       funcAgentID, funcAgentInfo.second.statuscode());
            // Prevent the evicting result from being lost after the proxy restarts.
            NotifyEvcitResult(funcAgentID, StatusCode::SUCCESS, "agent already exited or evicted");
            continue;
        }

        funcAgentTable_[funcAgentInfo.first] = { .isEnable = false,
                                                 .isInit = true,
                                                 .recoverPromise = std::make_shared<litebus::Promise<bool>>(),
                                                 .aid = litebus::AID(funcAgentInfo.second.agentaidname(),
                                                                     funcAgentInfo.second.agentaddress()),
                                                 .instanceIDs = {} };
        YRLOG_DEBUG("recover heartbeat processing, get function agent aid:{}.",
                    funcAgentTable_[funcAgentInfo.first].aid.HashString());
        aidTable_[funcAgentTable_[funcAgentInfo.first].aid] = funcAgentInfo.first;

        YRLOG_INFO(
            "find corresponding function agent update resource unit, function-agent id: {}."
            " start recover heart beat with function agent",
            funcAgentID);

        // start HeartBeat
        litebus::Async(GetAID(), &FunctionAgentMgrActor::StartHeartbeat, funcAgentID,
                       funcAgentInfo.second.agentaddress())
            .Then(litebus::Defer(GetAID(), &FunctionAgentMgrActor::SetFuncAgentInfo, std::placeholders::_1, funcAgentID,
                                 nullptr))
            .Then(litebus::Defer(GetAID(), &FunctionAgentMgrActor::SetResourceUnitPromise, std::placeholders::_1,
                                 funcAgentID))
            .Then(litebus::Defer(GetAID(), &FunctionAgentMgrActor::SyncInstances, std::placeholders::_1))
            .Then(litebus::Defer(GetAID(), &FunctionAgentMgrActor::DoAddFuncAgent, std::placeholders::_1, funcAgentID))
            .OnComplete(
            litebus::Defer(GetAID(), &FunctionAgentMgrActor::EnableFuncAgent, std::placeholders::_1, funcAgentID));
    }
}

litebus::Future<std::shared_ptr<resource_view::ResourceUnit>> FunctionAgentMgrActor::SetResourceUnitPromise(
    const std::shared_ptr<resource_view::ResourceUnit> &resourceUnit, const std::string &funcAgentID)
{
    if (resourceUnit != nullptr) {
        return resourceUnit;
    }

    if (funcAgentResUpdatedMap_.find(funcAgentID) != funcAgentResUpdatedMap_.end()) {
        return funcAgentResUpdatedMap_.at(funcAgentID).GetFuture();
    }
    litebus::Promise<std::shared_ptr<resource_view::ResourceUnit>> promise;
    (void)funcAgentResUpdatedMap_.emplace(funcAgentID, promise);
    YRLOG_DEBUG("set function agent AID: {} ResourceUnit promise success.", funcAgentID);
    return promise.GetFuture();
}

litebus::Future<Status> FunctionAgentMgrActor::DoAddFuncAgent(const Status &status, const std::string &funcAgentID)
{
    if (funcAgentResUpdatedMap_.find(funcAgentID) == funcAgentResUpdatedMap_.end()) {
        YRLOG_WARN("failed to find func agent({}) in result map when add func agent.", funcAgentID);
        litebus::Promise<Status> ret;
        ret.SetFailed(static_cast<int32_t>(StatusCode::LS_AGENT_NOT_FOUND));
        return ret.GetFuture();
    }
    // try to wait 3s for view
    auto viewFuture = funcAgentResUpdatedMap_[funcAgentID].GetFuture();
    return viewFuture.Then(
        litebus::Defer(GetAID(), &FunctionAgentMgrActor::AddFuncAgent, status, funcAgentID, std::placeholders::_1));
}

litebus::Future<messages::InstanceStatusInfo> FunctionAgentMgrActor::QueryInstanceStatusInfo(const string &funcAgentID,
                                                                                             const string &instanceID,
                                                                                             const string &runtimeID)
{
    messages::QueryInstanceStatusRequest request;
    request.set_instanceid(instanceID);
    request.set_runtimeid(runtimeID);
    auto requestID = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    request.set_requestid(requestID);
    auto future = queryStatusSync_.AddSynchronizer(requestID);
    YRLOG_INFO("{}|query instance({}) status of runtime({}) from({}), ", requestID, instanceID, runtimeID, funcAgentID);
    if (funcAgentTable_.find(funcAgentID) == funcAgentTable_.end()) {
        litebus::Promise<messages::InstanceStatusInfo> ret;
        ret.SetFailed(static_cast<int32_t>(StatusCode::LS_AGENT_NOT_FOUND));
        return ret.GetFuture();
    }
    Send(funcAgentTable_[funcAgentID].aid, "QueryInstanceStatusInfo", request.SerializeAsString());
    queryReqMap_[requestID] = funcAgentID;
    return future;
}
void FunctionAgentMgrActor::QueryInstanceStatusInfoResponse(const litebus::AID &from, string &&name, string &&msg)
{
    messages::QueryInstanceStatusResponse rsp;
    if (!rsp.ParseFromString(msg)) {
        YRLOG_ERROR("invalid instance status response from({})", std::string(from));
        return;
    }
    YRLOG_INFO("{}|got instance status response from({}), {}", rsp.requestid(), std::string(from),
               rsp.ShortDebugString());
    (void)queryStatusSync_.Synchronized(rsp.requestid(), rsp.instancestatusinfo());
    (void)queryReqMap_.erase(rsp.requestid());
}

litebus::Future<Status> FunctionAgentMgrActor::QueryDebugInstanceInfos()
{
    auto readyCnt = std::make_shared<std::atomic_ulong>(0);
    auto collectFutures = std::make_shared<std::list<litebus::Future<messages::QueryDebugInstanceInfosResponse>>>();
    auto flagPromise = std::make_shared<litebus::Promise<Status>>();
    auto resultPromise = std::make_shared<litebus::Promise<Status>>();
    // 遍历所有注册到proxy的agent
    for (const auto &funcAgentPair : funcAgentTable_) {
        if (!funcAgentPair.second.isEnable) {
            continue;
        }
        messages::QueryDebugInstanceInfosRequest request;
        auto requestID = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        request.set_requestid(requestID);
        auto future = queryDebugInstInfoSync_.AddSynchronizer(requestID);
        Send(funcAgentPair.second.aid, "QueryDebugInstanceInfos", request.SerializeAsString());
        collectFutures->emplace_back(future);
    }
    if (collectFutures->size() == 0) {
        YRLOG_WARN("no enable agent in proxy");
        resultPromise->SetValue(Status::OK());
        return resultPromise->GetFuture();
    }

    for (const auto &iter : *collectFutures) {
        iter.OnComplete([readyCnt, flagPromise,
                         collectFutures](const litebus::Future<messages::QueryDebugInstanceInfosResponse> &rspFuture) {
            if (++(*readyCnt) == collectFutures->size()) {
                flagPromise->SetValue(Status(StatusCode::SUCCESS));
            }
        });
    }

    return flagPromise->GetFuture().Then(
        [collectFutures, aid(GetAID()), resultPromise](const Status &input) -> litebus::Future<Status> {
            ulong errFutureCnt = 0;
            std::list<messages::QueryDebugInstanceInfosResponse> rspList;
            // 遍历所有future结果，成功future则获取，失败future则抛弃
            for (const auto &future : *collectFutures) {
                auto &queryDebugInstInfosRsp = future.Get();
                if (future.IsError() || queryDebugInstInfosRsp.code() != SUCCESS) {
                    errFutureCnt++;
                } else {
                    rspList.emplace_back(queryDebugInstInfosRsp);
                }
            }
            // 无有效response
            if (errFutureCnt == collectFutures->size()) {
                resultPromise->SetFailed(StatusCode::FAILED);
                YRLOG_ERROR("no valid QueryDebugInstanceInfosResponse");
                return resultPromise->GetFuture();
            }
            std::list<messages::DebugInstanceInfo> resList;
            for (auto &rsp : rspList) {
                for (auto &info : rsp.debuginstanceinfos()) {
                    resList.push_back(info);
                }
            }
            // 没有要写入到metastore的debug instance info
            if (resList.empty()) {
                YRLOG_DEBUG("no changed debug instance");
                resultPromise->SetValue(Status::OK());
                return resultPromise->GetFuture();
            }
            // 将agent中查询成功数据存入metastore中
            litebus::Async(aid, &FunctionAgentMgrActor::PutDebugInstanceInfos, resList)
                .OnComplete([resultPromise](const litebus::Future<Status> &status) {
                    if (status.IsError()) {
                        resultPromise->SetFailed(status.GetErrorCode());
                    } else {
                        resultPromise->SetValue(status.Get());
                }
            });
            return resultPromise->GetFuture();
        });
}

void FunctionAgentMgrActor::QueryDebugInstanceInfosResponse(const litebus::AID &from, std::string &&name,
                                                            std::string &&msg)
{
    messages::QueryDebugInstanceInfosResponse rsp;
    if (!rsp.ParseFromString(msg)) {
        YRLOG_ERROR("invalid debug instance response from({})", std::string(from));
        return;
    }
    YRLOG_INFO("{}|get debug instance info response from({}), {}", rsp.requestid(), std::string(from),
               rsp.ShortDebugString());
    (void)queryDebugInstInfoSync_.Synchronized(rsp.requestid(), rsp);
}

litebus::Future<Status> FunctionAgentMgrActor::PutDebugInstanceInfos(
    const std::list<messages::DebugInstanceInfo> &debugInstInfos)
{
    ASSERT_IF_NULL(metaStoreClient_);
    auto promise = std::make_shared<litebus::Promise<Status>>();
    auto transaction = metaStoreClient_->BeginTransaction();
    for (const auto &debugInstInfo : debugInstInfos) {
        std::string jsonStr;
        if (!google::protobuf::util::MessageToJsonString(debugInstInfo, &jsonStr).ok()) {
            YRLOG_ERROR("failed to trans to json string from DebugInstanceInfo");
            continue;
        }
        transaction->Then(
            meta_store::TxnOperation::Create(DEBUG_INSTANCE_PREFIX + debugInstInfo.instanceid(), jsonStr, {}));
    }
    transaction->Commit().OnComplete([promise](litebus::Future<std::shared_ptr<TxnResponse>> future) {
        auto txnResponse = future.Get();
        if (txnResponse->status != SUCCESS) {
            promise->SetFailed(txnResponse->status.StatusCode());
            YRLOG_ERROR("debug instance infos put to metastore failed,errMsg:{}", txnResponse->status.GetMessage());
        } else {
            promise->SetValue(txnResponse->status);
            YRLOG_INFO("debug instance infos put to metastore success");
        }
    });
    return promise->GetFuture();
}

void FunctionAgentMgrActor::SendCleanStatusToFunctionAgent(const litebus::AID &funcAgentAID, uint32_t curRetryTimes)
{
    auto ite = sendCleanStatusPromiseMap_.find(funcAgentAID);
    if (ite == sendCleanStatusPromiseMap_.end()) {
        litebus::Promise<StatusCode> promise;
        (void)sendCleanStatusPromiseMap_.emplace(std::make_pair(funcAgentAID, promise));
    }

    auto sendCleanStatusPromise = sendCleanStatusPromiseMap_[funcAgentAID];
    if (sendCleanStatusPromise.GetFuture().IsOK()) {
        (void)sendCleanStatusPromiseMap_.erase(funcAgentAID);
        return;
    }

    auto funcAgentIdIter = aidTable_.find(funcAgentAID);
    if (funcAgentIdIter == aidTable_.end()) {
        YRLOG_WARN("function agent {} not registered, failed to send CleanStatus request.", funcAgentAID.HashString());
        return;
    }
    const auto &agentID = funcAgentIdIter->second;

    (void)++curRetryTimes;
    if (curRetryTimes > MAX_RETRY_SEND_CLEAN_STATUS_TIMES) {
        YRLOG_ERROR("{}|Send clean status to function agent({}) time out", GetAID().HashString(), agentID);
        (void)sendCleanStatusPromiseMap_.erase(funcAgentAID);
        TimeoutEvent(agentID);
        return;
    }
    YRLOG_INFO("send to clean agent({}) status", agentID);
    messages::CleanStatusRequest cleanStatusRequest;
    cleanStatusRequest.set_name(agentID);
    (void)Send(funcAgentAID, "CleanStatus", cleanStatusRequest.SerializeAsString());

    (void)litebus::AsyncAfter(retrySendCleanStatusInterval_, GetAID(),
                              &FunctionAgentMgrActor::SendCleanStatusToFunctionAgent, funcAgentAID, curRetryTimes);
}

void FunctionAgentMgrActor::CleanStatusResponse(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    auto ite = sendCleanStatusPromiseMap_.find(from);
    if (ite != sendCleanStatusPromiseMap_.end()) {
        ite->second.SetValue(StatusCode::SUCCESS);
    }
    auto funcAgentIdIter = aidTable_.find(from);
    if (funcAgentIdIter == aidTable_.end()) {
        YRLOG_WARN("function agent {} not find, failed to set time out.", from.HashString());
        return;
    }
    // copy, TimeoutEvent will clear maps, agentID may be expired
    std::string agentID = funcAgentIdIter->second;
    TimeoutEvent(agentID);
}

litebus::Future<messages::UpdateCredResponse> FunctionAgentMgrActor::UpdateCred(
    const std::string &funcAgentID, const std::shared_ptr<messages::UpdateCredRequest> &request)
{
    auto requestID = request->requestid();
    auto runtimeID = request->runtimeid();

    if (funcAgentTable_.find(funcAgentID) == funcAgentTable_.end()) {
        messages::UpdateCredResponse response;
        response.set_requestid(requestID);
        response.set_code(static_cast<int>(StatusCode::ERR_INNER_COMMUNICATION));
        response.set_message("function agent is not registered");
        YRLOG_ERROR("{}|failed to update cred, function agent {} is not registered.", requestID, funcAgentID);
        return response;
    }

    auto future = updateTokenSync_.AddSynchronizer(requestID);

    YRLOG_INFO("{}|send request to agent({}) to update cred for runtime({}).", requestID, funcAgentID, runtimeID);
    Send(funcAgentTable_[funcAgentID].aid, "UpdateCred", request->SerializeAsString());

    return future;
}

void FunctionAgentMgrActor::UpdateCredResponse(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::UpdateCredResponse response;
    if (msg.empty() || !response.ParseFromString(msg)) {
        YRLOG_WARN("invalid request body, failed to get response of update token from {}.", from.HashString());
        return;
    }

    auto requestID = response.requestid();
    YRLOG_INFO("{}|update token successfully", requestID);
    (void)updateTokenSync_.Synchronized(requestID, response);
}

litebus::Future<messages::SnapshotRuntimeResponse> FunctionAgentMgrActor::SnapshotRuntime(
    const std::string &requestID,
    const resource_view::InstanceInfo &instanceInfo,
    int32_t ttl)
{
    // 1. 从 instanceInfo 获取 funcAgentID
    std::string funcAgentID = instanceInfo.functionagentid();
    std::string instanceID = instanceInfo.instanceid();

    if (funcAgentID.empty()) {
        messages::SnapshotRuntimeResponse result;
        result.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
        result.set_message(fmt::format("agent not found for instance {}", instanceID));
        YRLOG_ERROR("{}|functionagentid is empty for instance", instanceID);
        return result;
    }

    if (funcAgentTable_.find(funcAgentID) == funcAgentTable_.end()) {
        messages::SnapshotRuntimeResponse result;
        result.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION));
        result.set_message("function agent is not registered");
        YRLOG_ERROR("{}|failed to send snapshot runtime request, function agent {} is not registered.",
                    instanceID, funcAgentID);
        return result;
    }

    // 2. 构造 SnapshotRuntime 请求
    auto request = std::make_shared<messages::SnapshotRuntimeRequest>();
    request->set_requestid(requestID);
    request->set_instanceid(instanceID);
    request->set_runtimeid(instanceInfo.runtimeid());
    request->set_containerid(instanceInfo.containerid());  // containerID is same as runtimeID in container mode
    request->set_ttl(ttl);  // Set TTL from parameter
    auto future = snapshotRuntimeSync_.AddSynchronizer(requestID);

    YRLOG_INFO("{}|send SnapshotRuntime request to agent({}) for instance({}), ttl: {}",
               requestID, funcAgentID, instanceID, ttl);
    Send(funcAgentTable_[funcAgentID].aid, "SnapshotRuntime", request->SerializeAsString());

    // 3. 将 SnapshotRuntimeResponse 消息转换为结构体
    return future;
}

void FunctionAgentMgrActor::SnapshotRuntimeResponse(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::SnapshotRuntimeResponse response;
    if (msg.empty() || !response.ParseFromString(msg)) {
        YRLOG_WARN("invalid request body, failed to get SnapshotRuntimeResponse from {}.", from.HashString());
        return;
    }

    std::string requestID = response.requestid();
    std::string checkpointID = response.has_snapshotinfo() ? response.snapshotinfo().checkpointid() : "N/A";
    YRLOG_INFO("{}|received SnapshotRuntimeResponse, code: {}, checkpointID: {}",
               requestID, response.code(), checkpointID);
    (void)snapshotRuntimeSync_.Synchronized(requestID, response);
}

litebus::Future<Status> FunctionAgentMgrActor::EvictAgent(const std::shared_ptr<messages::EvictAgentRequest> &req)
{
    const auto &agentID = req->agentid();
    YRLOG_INFO("received evict agent({})", agentID);
    if (funcAgentsRegisMap_.find(agentID) == funcAgentsRegisMap_.end()) {
        YRLOG_ERROR("failed to evict, agent({}) not found.", agentID);
        return Status(StatusCode::PARAMETER_ERROR, "agentID not found");
    }
    if (funcAgentTable_.find(agentID) == funcAgentTable_.end()) {
        YRLOG_ERROR("failed to evict, agent({}) not found.", agentID);
        return Status::OK();
    }
    auto curStatus = funcAgentsRegisMap_[agentID].statuscode();
    if (curStatus == static_cast<int32_t>(FunctionAgentMgrActor::RegisStatus::EVICTING)) {
        return Status::OK();
    }
    if (curStatus == static_cast<int32_t>(FunctionAgentMgrActor::RegisStatus::EVICTED) ||
        curStatus == static_cast<int32_t>(FunctionAgentMgrActor::RegisStatus::FAILED)) {
        litebus::Async(GetAID(), &FunctionAgentMgrActor::NotifyEvcitResult, req->agentid(), StatusCode::SUCCESS,
                       "success to evict agent");
        return Status::OK();
    }
    auto promise = funcAgentTable_[agentID].recoverPromise;
    if (promise != nullptr && promise->GetFuture().IsInit()) {
        YRLOG_INFO("agent({}) is recovering, wait until the restoration is complete and continue the eviction.",
                   agentID);
        return promise->GetFuture().Then([agentID, req, aid(GetAID())](const bool &isOK) -> litebus::Future<Status> {
            YRLOG_INFO("agent({}) is recovered, isOk({}).", agentID, isOK);
            if (!isOK) {
                return Status::OK();
            }
            return litebus::Async(aid, &FunctionAgentMgrActor::EvictAgent, req);
        });
    }
    auto preStatus = funcAgentsRegisMap_[agentID].statuscode();
    funcAgentsRegisMap_[agentID].set_statuscode(static_cast<int32_t>(FunctionAgentMgrActor::RegisStatus::EVICTING));
    funcAgentsRegisMap_[agentID].set_evicttimeoutsec(req->timeoutsec());
    auto future = PutAgentRegisInfoWithProxyNodeID();
    future.OnComplete([aid(GetAID()), req, preStatus](const litebus::Future<Status> &future) {
        if (future.IsError() || future.Get().IsError()) {
            YRLOG_ERROR("failed to update agent({}) status", req->agentid());
            litebus::Async(aid, &FunctionAgentMgrActor::RollbackEvictingAgent, req->agentid(), preStatus);
            return;
        }
        litebus::Async(aid, &FunctionAgentMgrActor::EvictInstanceOnAgent, req);
    });
    return future;
}

void FunctionAgentMgrActor::RollbackEvictingAgent(const std::string &agentID, const int32_t &preStatus)
{
    funcAgentsRegisMap_[agentID].set_statuscode(preStatus);
    (void)PutAgentRegisInfoWithProxyNodeID();
}

void FunctionAgentMgrActor::UpdateLocalStatus(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    messages::UpdateLocalStatusRequest req;
    if (msg.empty() || !req.ParseFromString(msg)) {
        YRLOG_WARN("invalid request body from {}. failed to update resources.", from.HashString());
        return;
    }
    if (localStatus_ == static_cast<int32_t>(req.status())) {
        return;
    }
    if (!IsReady()) {
        YRLOG_WARN("local isn't ready yet, ignore update local status");
        return;
    }
    localStatus_ = req.status();
    PutAgentRegisInfoWithProxyNodeID().OnComplete([aid(GetAID()), req, from](const litebus::Future<Status> &future) {
        if (future.IsError() || future.Get().IsError()) {
            YRLOG_ERROR("failed to update local status ({})", req.status());
            litebus::Async(aid, &FunctionAgentMgrActor::NotifyUpdateLocalResult, from, req.status(), false);
            return;
        }
        litebus::Async(aid, &FunctionAgentMgrActor::NotifyUpdateLocalResult, from, req.status(), true);
    });;
}

void FunctionAgentMgrActor::NotifyUpdateLocalResult(litebus::AID from, const uint32_t &localStatus, const bool &healthy)
{
    YRLOG_INFO("UpdateLocalStatus complete, localStatus is ({}), healthy is ({})", localStatus, healthy);
    auto result = std::make_shared<messages::UpdateLocalStatusResponse>();
    result->set_healthy(healthy);
    result->set_status(localStatus);
    (void)Send(from, "UpdateLocalStatusResponse", result->SerializeAsString());
}

void FunctionAgentMgrActor::EvictInstanceOnAgent(const std::shared_ptr<messages::EvictAgentRequest> &req)
{
    auto instanceCtrl = instanceCtrl_.lock();
    if (instanceCtrl == nullptr) {
        YRLOG_ERROR("failed to evict agent({}), instance ctrl is null", req->agentid());
        return;
    }
    auto bundleMgr = bundleMgr_.lock();
    auto resourceView = resourceView_.lock();
    if (resourceView != nullptr && bundleMgr != nullptr) {
        (void)bundleMgr->UpdateBundlesStatus(req->agentid(), UnitStatus::EVICTING);
        (void)resourceView->UpdateUnitStatus(req->agentid(), UnitStatus::EVICTING);
    }
    (void)instanceCtrl->EvictInstanceOnAgent(req).OnComplete(
        litebus::Defer(GetAID(), &FunctionAgentMgrActor::OnInstanceEvicted, std::placeholders::_1, req));
}

void FunctionAgentMgrActor::OnInstanceEvicted(const litebus::Future<Status> &future,
                                              const std::shared_ptr<messages::EvictAgentRequest> &req)
{
    // future of instanceCtrl->EvictInstanceOnAgent return by SetFailed should never happen.
    if (future.IsError()) {
        YRLOG_ERROR("failed to evict agent({}), because of update agent status failure.", req->agentid());
        return;
    }
    // After instances are evicted, the agent can be safely removed.
    if (funcAgentsRegisMap_.find(req->agentid()) == funcAgentsRegisMap_.end()) {
        YRLOG_ERROR("evicted agent({}) is already exit", req->agentid());
        NotifyEvcitResult(req->agentid(), StatusCode::SUCCESS, "agent already exited");
        return;
    }
    functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().DeletePodResource(req->agentid());
    if (auto resourceView = resourceView_.lock(); resourceView != nullptr) {
        (void)resourceView->DeleteResourceUnit(req->agentid());
    }
    auto code = StatusCode::SUCCESS;
    std::string message = "success to evict agent";
    if (future.Get().IsError()) {
        // failure over
        funcAgentsRegisMap_[req->agentid()].set_statuscode(
            static_cast<int32_t>(FunctionAgentMgrActor::RegisStatus::SUCCESS));
        code = future.Get().StatusCode();
        message = future.Get().GetMessage();
    } else {
        funcAgentsRegisMap_[req->agentid()].set_statuscode(
            static_cast<int32_t>(FunctionAgentMgrActor::RegisStatus::EVICTED));
    }
    PutAgentRegisInfoWithProxyNodeID().OnComplete(
        [aid(GetAID()), req, code, message](const litebus::Future<Status> &status) {
            if (status.IsError() || status.Get().IsError()) {
                YRLOG_WARN("put evicted agent({}) status failure.", req->agentid());
                litebus::Async(aid, &FunctionAgentMgrActor::NotifyEvcitResult, req->agentid(), code,
                               message + " (warning: agent status changed failure.)");
                return;
            }
            litebus::Async(aid, &FunctionAgentMgrActor::NotifyEvcitResult, req->agentid(), code, message);
        });
}
void FunctionAgentMgrActor::NotifyEvcitResult(const std::string &agentID, StatusCode code, const std::string &msg)
{
    auto localScheSrv = localSchedSrv_.lock();
    if (localScheSrv == nullptr) {
        YRLOG_ERROR("failed to evict agent({}), localSchedSrv is nullptr.", agentID);
        return;
    }
    auto result = std::make_shared<messages::EvictAgentResult>();
    result->set_code(static_cast<int32_t>(code));
    result->set_message(msg);
    result->set_agentid(agentID);
    localScheSrv->NotifyEvictResult(result);
}

bool FunctionAgentMgrActor::IsEvictingAgent(const std::string &agentID)
{
    if (auto iter(funcAgentsRegisMap_.find(agentID)); iter != funcAgentsRegisMap_.end()) {
        if (static_cast<RegisStatus>(iter->second.statuscode()) == RegisStatus::EVICTING) {
            return true;
        }
    }
    return false;
}

bool FunctionAgentMgrActor::IsEvictedAgent(const std::string &agentID)
{
    if (auto iter(funcAgentsRegisMap_.find(agentID)); iter != funcAgentsRegisMap_.end()) {
        if (static_cast<RegisStatus>(iter->second.statuscode()) == RegisStatus::EVICTED) {
            return true;
        }
    }
    return false;
}

void FunctionAgentMgrActor::SetNetworkIsolation(const std::string &agentID, const RuleType &type,
                                                std::vector<std::string> &rules)
{
    if (funcAgentTable_.find(agentID) == funcAgentTable_.end()) {
        YRLOG_DEBUG("agent({}) may not exist, skip SetNetworkIsolation", agentID);
        return;
    }

    messages::SetNetworkIsolationRequest req;
    req.set_ruletype(type);
    req.set_requestid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    YRLOG_DEBUG("Notify local agent({}) SetNetworkIsolation ruleType({})", agentID, fmt::underlying(type));
    for (const std::string &rule : rules) {
        req.add_rules(rule);
        YRLOG_DEBUG("rule: {}", rule);
    }
    (void)Send(funcAgentTable_[agentID].aid, "SetNetworkIsolationRequest", req.SerializeAsString());
}

void FunctionAgentMgrActor::NotifyLocalAgentsInTenant(const std::shared_ptr<TenantCache> &tenantCache,
    const RuleType &type, std::vector<std::string> &rules, bool isClusterWide)
{
    for (const auto &entry : tenantCache->functionAgentCacheMap) {
        const std::string &functionAgentID = entry.first;
        const FunctionAgentCache &cache = entry.second;
        if (cache.isAgentOnThisNode || isClusterWide) {
            SetNetworkIsolation(functionAgentID, type, rules);
        }
    }
}

void FunctionAgentMgrActor::OnTenantFirstInstanceSchedInLocalPod(
    const std::shared_ptr<TenantCache> tenantCache, const TenantEvent &event)
{
    if (enableIpv4TenantIsolation_) {
        std::vector<std::string> rules{event.agentPodIp};
        SetNetworkIsolation(event.functionAgentID, RuleType::IPSET_ADD, rules);
    }
}

bool FunctionAgentMgrActor::OnTenantInstanceSchedInRemotePodOnAnotherNode(
    const std::shared_ptr<TenantCache> tenantCache, const TenantEvent &event)
{
    if (enableIpv4TenantIsolation_) {
        std::vector<std::string> rules{event.agentPodIp};
        // Notify all agents under this tenant that are on this node
        NotifyLocalAgentsInTenant(tenantCache, RuleType::IPSET_ADD, rules, false);
    }
    return true;
}

bool FunctionAgentMgrActor::OnTenantInstanceSchedInNewPodOnCurrentNode(
    const std::shared_ptr<TenantCache> tenantCache, const TenantEvent &event)
{
    if (enableIpv4TenantIsolation_) {
        std::vector<std::string> rules(tenantCache->podIps.begin(), tenantCache->podIps.end());
        NotifyLocalAgentsInTenant(tenantCache, RuleType::IPSET_ADD, rules, false);
    }
    return true;
}

bool FunctionAgentMgrActor::OnTenantInstanceInPodDeleted(
    const std::shared_ptr<TenantCache> tenantCache, const TenantEvent &event)
{
    if (enableIpv4TenantIsolation_) {
        if (tenantCache->podIps.count(event.agentPodIp) == 0) {
            YRLOG_ERROR("tenant({}) agent({}) pod ip({}) not in cache, need to confirm cache consistency on proxy({})",
                event.tenantID,
                event.functionAgentID,
                event.agentPodIp,
                event.functionProxyID);
            return false;
        }
        if (tenantCache->functionAgentCacheMap.find(event.functionAgentID) ==
            tenantCache->functionAgentCacheMap.end()) {
            return false;
        }
        std::string agentPodIp = tenantCache->functionAgentCacheMap[event.functionAgentID].agentPodIp;
        if (agentPodIp != event.agentPodIp) {
            YRLOG_ERROR("tenant({}) agent({}) pod ip({}) not match ip({}) in cache",
                event.tenantID,
                event.functionAgentID,
                event.agentPodIp,
                agentPodIp);
            return false;
        }
    }
    return true;
}

bool FunctionAgentMgrActor::OnTenantInstanceInPodAllDeleted(
    const std::shared_ptr<TenantCache> tenantCache, const TenantEvent& event)
{
    if (enableIpv4TenantIsolation_) {
        tenantCache->podIps.erase(event.agentPodIp);
        std::vector<std::string> rules{event.agentPodIp};
        NotifyLocalAgentsInTenant(tenantCache, RuleType::IPSET_DELETE, rules, true);

        // Flush the whitelist for reusing the agent's POD
        std::vector<std::string> emptyVector;
        SetNetworkIsolation(event.functionAgentID, RuleType::IPSET_FLUSH, emptyVector);
    }

    if (enableTenantAffinity_) {
        if (tenantPodReuseTimeWindow_ == -1 && event.functionProxyID == nodeID_) {
            YRLOG_DEBUG("try to clear tenant({})/proxy({})/agent({}) tenant label",
                event.tenantID, event.functionProxyID, event.functionAgentID);
        } else if (tenantPodReuseTimeWindow_ < -1) {
            YRLOG_ERROR("Invalid tenantPodReuseTimeWindow({})", tenantPodReuseTimeWindow_);
            return false;
        }
    }
    return true;
}

void FunctionAgentMgrActor::OnTenantUpdateInstance(const TenantEvent &event)
{
    // key: /sn/instance/business/yrk/tenant/default/function/0-system-faasscheduler/
    // version/$latest/defaultaz/941e253514a11c24/a1a262a8-ec21-4000-8000-000000581e3f
    if (event.code != static_cast<int32_t>(InstanceState::RUNNING)) {
        // The tenant isolation feature only focuses on potential new pod IP events.
        YRLOG_DEBUG("tenant instance({}) status code is {}, ignore it", event.instanceID, event.code);
        return;
    }

    if (tenantCacheMap_.find(event.tenantID) == tenantCacheMap_.end()) {
        // Case1: The function instance in the first POD of the tenant.
        YRLOG_DEBUG("tenant({}) has no instance on proxy({})", event.tenantID, event.functionProxyID);
        auto tenantCache = std::make_shared<TenantCache>();
        tenantCacheMap_[event.tenantID] = tenantCache;
        tenantCache->podIps.insert(event.agentPodIp);
        tenantCache->functionAgentCacheMap[event.functionAgentID] = {
            .isAgentOnThisNode = false, .agentPodIp = event.agentPodIp, .instanceIDs = {}};
        tenantCache->functionAgentCacheMap[event.functionAgentID].instanceIDs.insert(event.instanceID);
        if (event.functionProxyID == nodeID_) {
            tenantCache->functionAgentCacheMap[event.functionAgentID].isAgentOnThisNode = true;

            (void)OnTenantFirstInstanceSchedInLocalPod(tenantCache, event);
        }
    } else {  // Case2: The function instance that is not the first one for the tenant.
        auto tenantCache = tenantCacheMap_[event.tenantID];
        if (tenantCache->podIps.count(event.agentPodIp) != 0) {
            // Case: Add a function instance to an existing POD on a node.
            YRLOG_DEBUG("agent pod ip({}) already exist({}), ignore it", event.agentPodIp, event.functionProxyID);
        } else {
            tenantCache->podIps.insert(event.agentPodIp);
            if (event.functionProxyID != nodeID_) {
                // Case3: The function instance added to a new POD on another node
                tenantCache->functionAgentCacheMap[event.functionAgentID] = {
                    .isAgentOnThisNode = false, .agentPodIp = event.agentPodIp, .instanceIDs = {}};
                tenantCache->functionAgentCacheMap[event.functionAgentID].instanceIDs.insert(event.instanceID);

                (void)OnTenantInstanceSchedInRemotePodOnAnotherNode(tenantCache, event);
            } else {
                // Case4: The function instance added to a new POD on the same node
                tenantCache->functionAgentCacheMap[event.functionAgentID] = {
                    .isAgentOnThisNode = true, .agentPodIp = event.agentPodIp, .instanceIDs = {}};
                tenantCache->functionAgentCacheMap[event.functionAgentID].instanceIDs.insert(event.instanceID);

                (void)OnTenantInstanceSchedInNewPodOnCurrentNode(tenantCache, event);
            }
        }
    }
}

void FunctionAgentMgrActor::OnTenantDeleteInstance(const TenantEvent &event)
{
    YRLOG_DEBUG("DeleteInstance when instance({}) status code is {}", event.instanceID, event.code);
    if (tenantCacheMap_.find(event.tenantID) == tenantCacheMap_.end()) {
        YRLOG_WARN("tenant({}) not in cache, need to confirm cache consistency on proxy({})",
            event.tenantID, event.functionProxyID);
        return;
    }
    auto tenantCache = tenantCacheMap_[event.tenantID];
    if (tenantCache == nullptr) {
        YRLOG_ERROR("tenant({}) cache is nullptr, need to confirm cache consistency on proxy({})",
            event.tenantID, event.functionProxyID);
        return;
    }

    auto &instanceIDs = tenantCache->functionAgentCacheMap[event.functionAgentID].instanceIDs;
    if (instanceIDs.erase(event.instanceID) == 0) {
        return;
    }
    if (!(OnTenantInstanceInPodDeleted(tenantCache, event))) {
        return;
    }

    // After deleting all instances, the deletion of the POD can be inferred through cache calculation.
    if (instanceIDs.empty()) {
        tenantCache->functionAgentCacheMap.erase(event.functionAgentID);
        YRLOG_DEBUG("Clear cache entry: tenant({}) agent({}) podIp({})",
            event.tenantID, event.functionAgentID, event.agentPodIp);
        if (!OnTenantInstanceInPodAllDeleted(tenantCache, event)) {
            return;
        }

        if (tenantCacheMap_[event.tenantID]->functionAgentCacheMap.empty()) {
            tenantCacheMap_.erase(event.tenantID);
            YRLOG_DEBUG("Clear cache entry: tenant({})", event.tenantID);
        }
    }
}

void FunctionAgentMgrActor::SetNetworkIsolationResponse(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    messages::SetNetworkIsolationResponse rsp;
    rsp.ParseFromString(msg);
    if (rsp.code() != static_cast<int32_t>(StatusCode::SUCCESS)) {
        YRLOG_ERROR(
            "SetNetworkIsolation req({}) failed, code: {}, message: {}", rsp.requestid(), rsp.code(), rsp.message());
    }
}


void FunctionAgentMgrActor::DeferGCInvalidAgent(const string &agentID)
{
    if (auto iter(funcAgentsRegisMap_.find(agentID)); iter != funcAgentsRegisMap_.end()) {
        if (static_cast<RegisStatus>(iter->second.statuscode()) == RegisStatus::FAILED) {
            YRLOG_WARN("agent({}) is already failed ({}), trigger to garbage collection", agentID,
                       invalidAgentGCInterval_);
            funcAgentsRegisMap_.erase(iter);
            PutAgentRegisInfoWithProxyNodeID();
        }
    }
}

void FunctionAgentMgrActor::OnHealthyStatus(const Status &status)
{
    // Do not care about MetaStore exceptions.
    if (status.IsError()) {
        return;
    }
    if (!IsReady()) {
        return;
    }
    YRLOG_INFO("metastore is recovered. sync local agent status to metastore.");
    (void)PutAgentRegisInfoWithProxyNodeID();
}

litebus::Future<bool> FunctionAgentMgrActor::IsFuncAgentRecovering(const string &funcAgentID)
{
    auto iter = funcAgentTable_.find(funcAgentID);
    if (iter == funcAgentTable_.end()) {
        return false;
    }
    if (iter->second.recoverPromise != nullptr) {
        return iter->second.recoverPromise->GetFuture();
    }
    return false;
}

litebus::Future<Status> FunctionAgentMgrActor::GracefulShutdown()
{
    YRLOG_INFO("start to graceful evict agent, agent num ({})", funcAgentsRegisMap_.size());
    std::list<litebus::Future<Status>> futures;
    for (auto [agentID, regInfo] : funcAgentsRegisMap_) {
        if (regInfo.statuscode() == static_cast<int32_t>(RegisStatus::FAILED)
            || regInfo.statuscode() == static_cast<int32_t>(RegisStatus::EVICTED)
            || regInfo.statuscode() == static_cast<int32_t>(RegisStatus::EVICTING)) {
            YRLOG_INFO("function-agent status is {}, ignore it", agentID, regInfo.statuscode());
            continue;
        }
        auto req = std::make_shared<messages::EvictAgentRequest>();
        req->set_agentid(agentID);
        req->set_timeoutsec(UINT32_MAX);
        auto bundleMgr = bundleMgr_.lock();
        auto resourceView = resourceView_.lock();
        if (resourceView != nullptr && bundleMgr != nullptr) {
            (void)bundleMgr->UpdateBundlesStatus(req->agentid(), UnitStatus::TO_BE_DELETED);
            (void)resourceView->UpdateUnitStatus(req->agentid(), UnitStatus::TO_BE_DELETED);
        }
        if (auto instanceCtrl = instanceCtrl_.lock()) {
            futures.push_back(instanceCtrl->EvictInstanceOnAgent(req));
        }
    }
    localStatus_ = static_cast<int32_t>(RegisStatus::EVICTED);
    abnormal_ = true;
    return CollectStatus(futures, "evict all agent")
        .Then(litebus::Defer(GetAID(), &FunctionAgentMgrActor::DeleteRegisteredAgentInfos));
}

litebus::Future<Status> FunctionAgentMgrActor::DeleteRegisteredAgentInfos()
{
    ASSERT_IF_NULL(metaStoreClient_);
    auto key = AGENT_INFO_PATH + nodeID_;
    funcAgentsRegisMap_.clear();
    funcAgentTable_.clear();
    auto deleteFunc = [metaStoreClient(metaStoreClient_), key]() {
        YRLOG_DEBUG("delete function agent registration infos key: {}.", key);
        (void)metaStoreClient->Delete(key, { false, false })
            .Then([key](const litebus::Future<std::shared_ptr<DeleteResponse>> &deleteResponse) {
                if (deleteResponse.IsError() || (deleteResponse.IsOK() && deleteResponse.Get()->status.IsError())) {
                    auto code = deleteResponse.IsError() ? deleteResponse.GetErrorCode()
                                                         : deleteResponse.Get()->status.StatusCode();
                    YRLOG_ERROR("failed to delete key {} using meta client, error: {}", key, code);
                    return Status(StatusCode::BP_META_STORAGE_DELETE_ERROR, "errorResponse: " + std::to_string(code));
                }
                return Status::OK();
            });
        return Status::OK();
    };
    if (persistingAgentInfo_ != nullptr) {
        return persistingAgentInfo_->GetFuture().OnComplete(deleteFunc);
    }
    deleteFunc();
    return Status(StatusCode::SUCCESS);
}

void FunctionAgentMgrActor::SetAbnormal()
{
    abnormal_ = true;
}

void FunctionAgentMgrActor::StaticFunctionScheduleRequest(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::ScheduleRequest request;
    if (msg.empty() || !request.ParseFromString(msg)) {
        YRLOG_WARN("invalid request body from {}.", from.HashString());
        return;
    }
    auto requestId = request.requestid();
    if (staticFunctionScheduleRequestIDs_.find(requestId) != staticFunctionScheduleRequestIDs_.end()) {
        YRLOG_WARN("{}|static function schedule request is being processed.", requestId);
        return;
    }
    const auto instanceCtrl = instanceCtrl_.lock();
    staticFunctionScheduleRequestIDs_.emplace(requestId);
    YRLOG_INFO("{}|received a static function schedule request from {}. ", requestId, from.HashString());
    const auto scheduleRequest = std::make_shared<messages::ScheduleRequest>(std::move(request));
    scheduleRequest->mutable_instance()->set_parentfunctionproxyaid(instanceCtrl->GetActorAID());
    (void)instanceCtrl->Schedule(scheduleRequest, std::make_shared<litebus::Promise<messages::ScheduleResponse>>())
        .Then(litebus::Defer(GetAID(), &FunctionAgentMgrActor::SendStaticFunctionScheduleResponse,
                             std::placeholders::_1, from));
}

litebus::Future<Status> FunctionAgentMgrActor::SendStaticFunctionScheduleResponse(
    const messages::ScheduleResponse &scheduleResponse, const litebus::AID &from)
{
    auto requestId = scheduleResponse.requestid();
    YRLOG_INFO("{}|static function schedule success, send response to aid: {}", requestId, from.HashString());
    Send(from, "StaticFunctionScheduleResponse", scheduleResponse.SerializeAsString());
    (void)staticFunctionScheduleRequestIDs_.erase(requestId);
    return Status::OK();
}

litebus::Future<Status> FunctionAgentMgrActor::RegisterToWarmUp(
    const std::shared_ptr<messages::DeployInstanceRequest> &request,
    const litebus::Option<std::string> &agentID)
{
    std::list<litebus::Future<messages::DeployInstanceResponse>> deployFutures;
    YRLOG_DEBUG("RegisterToWarmUp deploy request: {}", request->ShortDebugString());
    if (agentID.IsSome()) {
        return DeployInstance(request, agentID.Get())
            .Then([](const messages::DeployInstanceResponse &resp) -> litebus::Future<Status> {
                return Status(static_cast<StatusCode>(resp.code()), resp.message());
            });
    }
    for (auto [agentID, agentInfo] : funcAgentTable_) {
        YRLOG_INFO("send warm up deploy request to agent({})({}) for instance({})", agentID, std::string(agentInfo.aid),
                   request->instanceid());
        deployFutures.push_back(DeployInstance(request, agentID));
    }
    return litebus::Collect(deployFutures).Then(
        [aid(GetAID()), request](const std::list<messages::DeployInstanceResponse> &resps) -> litebus::Future<Status> {
            // todo collect deploy responses
            for (auto &resp : resps) {
                if (resp.code() != static_cast<int32_t>(StatusCode::SUCCESS)) {
                    return Status(static_cast<StatusCode>(resp.code()), resp.message());
                }
            }
            return Status::OK();
        });
}

litebus::Future<Status> FunctionAgentMgrActor::UnRegisterWarmUp(
    const std::shared_ptr<messages::KillInstanceRequest> &request)
{
    std::list<litebus::Future<messages::KillInstanceResponse>> killFutures;
    for (auto [agentID, agentInfo] : funcAgentTable_) {
        YRLOG_INFO("send unregister kill request to agent({})({}) for instance({})", agentID,
                   std::string(agentInfo.aid), request->instanceid());
        killFutures.push_back(KillInstance(request, agentID, false));
    }
    return litebus::Collect(killFutures).Then(
        [aid(GetAID()), request](const std::list<messages::KillInstanceResponse> &resps) -> litebus::Future<Status> {
            // todo collect deploy responses
            return Status::OK();
        });
}

}  // namespace functionsystem::local_scheduler