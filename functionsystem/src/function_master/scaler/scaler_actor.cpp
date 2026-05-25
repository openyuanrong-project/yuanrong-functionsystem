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

#include "function_master/scaler/scaler_actor.h"

#include "nlohmann/json.hpp"

#include "async/collect.hpp"
#include "async/defer.hpp"
#include "common/constants/actor_name.h"
#include "common/constants/constants.h"
#include "common/logs/logging.h"
#include "common/metrics/metrics_adapter.h"
#include "common/status/status.h"
#include "common/types/instance_state.h"
#include "common/utils/files.h"
#include "common/utils/meta_store_kv_operation.h"

namespace functionsystem::scaler {

const std::string ORIGINAL_POOL_NAME = "function-agent";
const std::string RUNTIME_MANAGER_CONTAINER_NAME = "runtime-manager";
const std::string FUNCTION_AGENT_CONTAINER_NAME = "function-agent";
const std::string FUNCTION_AGENT_INIT_CONTAINER_NAME = "function-agent-init";
const std::string DS_WORKER_NAME = "ds-worker";
const std::string DELEGATE_CONTAINER_NAME = "runtime";
const std::string LABEL_SCHEDULE_POLICY = "schedule-policy";
const std::string CPU4COMP_ENV = "CPU4COMP";
const std::string MEM4COMP_ENV = "MEM4COMP";
const std::string INIT_LABELS_ENV = "INIT_LABELS";
const std::string TAINT_EFFECT_PREFER_NO_SCHEDULE = "PreferNoSchedule";
const std::string SYSTEM_FUNCTION_SERVICE_ACCOUNT_NAME = "system-function";
const std::string SYSTEM_CLUSTER_CRITICAL_CLASS_NAME = "system-cluster-critical";
const std::string FUNCTION_AGENT_DYNAMIC = "function-agent-dynamic";
const std::string ANNOTATION_KEY_PREFIX = "yr-labels-";
const std::string ANNOTATION_PLACEHOLDER = "yr-default";
const std::string DELEGATE_POD_LABELS = "DELEGATE_POD_LABELS";
const std::string SEMICOLON_SIGN_SPLIT = ";";
const std::string EQUAL_SIGN_SPLIT = "=";
const std::string CPU_BIND_CPU_VALUE = "1000m";
const std::string CPU_BIND_MEM_VALUE = "900Mi";
const std::string EPHEMERAL_STORAGE_RESOURCE = "ephemeral-storage";
const std::string DEFAULT_EPHEMERAL_STORAGE_VALUE = "500Mi";
const int LABEL_ITEM_LENGTH = 2;
const uint32_t WAIT_POD_READY_TIMEOUT = 5 * 60 * 1000;  // ms
const uint32_t DEFAULT_EVICT_TIMEOUT = 60;              // second
const uint32_t DEFAULT_SECERT_MODE = 288;               // second
const uint32_t CHECK_SYSTEM_FUNCTION_INTERVAL = 60000; // ms
const std::string CONTAINER_ID_PREFIX = "docker://";
const std::string EVICT_ANNOTATION_KEY = "cluster-autoscaler.kubernetes.io/safe-to-evict";

using namespace functionsystem::explorer;

ScalerActor::ScalerActor(const std::string &name) : ActorBase(name)
{
}

ScalerActor::ScalerActor(const std::string &name, const std::shared_ptr<KubeClient> &kubeClient,
                         const std::shared_ptr<MetaStorageAccessor> &metaStorageAccessor, const ScalerParams &params)
    : ActorBase(name)
{
    member_ = std::make_shared<Member>();
    member_->kubeClient = kubeClient;
    member_->metaStorageAccessor = metaStorageAccessor;
    member_->k8sNamespace = params.k8sNamespace;
    member_->poolManager = std::make_shared<PoolManager>(metaStorageAccessor);
    member_->poolManager->RegisterScaleUpHandler([aid(GetAID())](const std::string &poolID, bool isReserved) {
        litebus::Async(aid, &ScalerActor::DoScaleUpPodByPoolID, poolID, isReserved, "", litebus::AID());
    });
    member_->poolManager->RegisterPersistHandler(
        [aid(GetAID())](const std::string &poolID) { litebus::Async(aid, &ScalerActor::PersistencePoolInfo, poolID); });
    member_->systemUpgradeParam = params.systemUpgradeParam;
    member_->gracePeriodSeconds = params.gracePeriodSeconds;
    member_->agentTemplatePath = params.agentTemplatePath;
    member_->poolConfigPath = params.poolConfigPath;
    if (params.enableFrontendPool) {
        std::unordered_map<std::string, std::string> labelSelector = { { REUSE_LABEL_KEY, "true" } };
        member_->frontendManager = std::make_shared<SystemFunctionPodManager>(FRONTEND_FUNCTION_NAME, labelSelector);
        member_->frontendManager->RegisterScaleUpHandler(params.systemUpgradeParam.handlers.scaleUpSystemFuncHandler);
        member_->frontendManager->RegisterScaleDownHandler([aid(GetAID())](const std::string &podName) {
            litebus::Async(aid, &ScalerActor::DeletePodByName, podName);
        });
    }
}

Status ScalerActor::Start()
{
    (void)member_->kubeClient->RegisterK8sStatusChangeHandler(
        K8sStatusChangeEvent::ON_RECOVER, "FUNCTION_MASTER_SCALER", [this]() { RegisterKubeWatcher(); });
    RegisterKubeWatcher();
    if (!member_->poolConfigPath.empty() && litebus::os::ExistPath(member_->poolConfigPath)) {
        (void)member_->poolManager->LoadPodPoolsConfig(member_->poolConfigPath);
        ASSERT_IF_NULL(business_);
        (void)business_->CreatePodPools(member_->poolManager->GetLocalPodPools());
    }
    member_->isSynced = true;
    litebus::AsyncAfter(POD_STATUS_CHECK_INTERVAL_MS, GetAID(), &ScalerActor::StartCheckPodStatus);
    if (member_->frontendManager != nullptr) {
        litebus::AsyncAfter(CHECK_SYSTEM_FUNCTION_INTERVAL, GetAID(), &ScalerActor::StartCheckSystemFunctionNeedScale);
    }
    return Status::OK();
}

void ScalerActor::RegisterKubeWatcher()
{
    WatchHandler podEventHandler = [aid(GetAID())](const K8sEventType &type, const std::shared_ptr<ModelBase> &model) {
        litebus::Async(aid, &ScalerActor::HandlePodModifiedEvent, type, model);
    };
    (void)member_->kubeClient->RegisterWatchHandler("V1Pod", K8sEventType::EVENT_TYPE_MODIFIED,
                                                    std::move(podEventHandler));
    (void)member_->kubeClient->RegisterWatchHandler("V1Pod", K8sEventType::EVENT_TYPE_DELETE,
                                                    std::move(podEventHandler));
    (void)member_->kubeClient->ListNamespacedPod(member_->k8sNamespace, true);

    WatchHandler nodeEventHandler = [aid(GetAID())](const K8sEventType &type, const std::shared_ptr<ModelBase> &model) {
        litebus::Async(aid, &ScalerActor::HandleNodeModifiedEvent, type, model);
    };
    (void)member_->kubeClient->RegisterWatchHandler("V1Node", K8sEventType::EVENT_TYPE_MODIFIED,
                                                    std::move(nodeEventHandler));
    (void)member_->kubeClient->RegisterWatchHandler("V1Node", K8sEventType::EVENT_TYPE_DELETE,
                                                    std::move(nodeEventHandler));
    (void)member_->kubeClient->ListNode(true);

    WatchHandler dHandler = [aid(GetAID())](const K8sEventType &type, const std::shared_ptr<ModelBase> &model) {
        litebus::Async(aid, &ScalerActor::OnDeploymentModified, type, model);
    };
    (void)member_->kubeClient->RegisterWatchHandler("V1Deployment", K8sEventType::EVENT_TYPE_MODIFIED, dHandler);
    (void)member_->kubeClient->RegisterWatchHandler("V1Deployment", K8sEventType::EVENT_TYPE_DELETE, dHandler);
    (void)member_->kubeClient->ListNamespacedDeployment(member_->k8sNamespace, true);
    SyncKubeResourceForChange();
}

void ScalerActor::SyncKubeResourceForChange()
{
    (void)member_->kubeClient->ListNamespacedPod(member_->k8sNamespace)
        .OnComplete([aid(GetAID())](const litebus::Future<std::shared_ptr<V1PodList>> &podList) {
            if (podList.IsError()) {
                YRLOG_WARN("failed to list existing pods while init, code({})", podList.GetErrorCode());
                return;
            }

            for (const auto &pod : podList.Get()->GetItems()) {
                litebus::Async(aid, &ScalerActor::HandlePodModifiedEvent, K8sEventType::EVENT_TYPE_MODIFIED, pod);
            }
        });
    (void)member_->kubeClient->ListNode().OnComplete(
        [aid(GetAID())](const litebus::Future<std::shared_ptr<V1NodeList>> &nodeList) {
            if (nodeList.IsError()) {
                YRLOG_WARN("failed to list existing node while init, code({})", nodeList.GetErrorCode());
                return;
            }

            for (const auto &node : nodeList.Get()->GetItems()) {
                litebus::Async(aid, &ScalerActor::HandleNodeModifiedEvent, K8sEventType::EVENT_TYPE_MODIFIED, node);
            }
        });
    (void)member_->kubeClient->ListNamespacedDeployment(member_->k8sNamespace)
        .OnComplete([aid(GetAID())](const litebus::Future<std::shared_ptr<V1DeploymentList>> &deploymentList) {
            if (deploymentList.IsError()) {
                YRLOG_WARN("failed to list existing node while init, code({})", deploymentList.GetErrorCode());
                return;
            }
            for (const auto &deployment : deploymentList.Get()->GetItems()) {
                litebus::Async(aid, &ScalerActor::OnDeploymentModified, K8sEventType::EVENT_TYPE_MODIFIED, deployment);
            }
        });
}

litebus::Future<Status> ScalerActor::SyncNodes()
{
    litebus::Promise<Status> promise;
    (void)member_->kubeClient->ListNode().OnComplete(
        [aid(GetAID()), promise](const litebus::Future<std::shared_ptr<V1NodeList>> &nodeList) {
            if (nodeList.IsError()) {
                YRLOG_WARN("failed to list existing node while init, code({})", nodeList.GetErrorCode());
                promise.SetFailed(static_cast<int32_t>(StatusCode::FAILED));
                return;
            }

            if (nodeList.Get() == nullptr || nodeList.Get()->GetItems().empty()) {
                YRLOG_WARN("failed to list existing node, list empty");
                promise.SetFailed(static_cast<int32_t>(StatusCode::FAILED));
                return;
            }

            litebus::Async(aid, &ScalerActor::DoSyncNode, nodeList.Get()).OnComplete([promise]() {
                promise.SetValue(Status::OK());
            });
        });
    return promise.GetFuture();
}

litebus::Future<std::unordered_map<std::string, std::shared_ptr<V1Node>>> ScalerActor::DoSyncNode(
    const std::shared_ptr<V1NodeList> &nodeList)
{
    if (nodeList == nullptr) {
        YRLOG_WARN("nodeList is nullptr");
        return std::unordered_map<std::string, std::shared_ptr<V1Node>>();
    }
    if (nodeList->GetItems().empty()) {
        YRLOG_WARN("list existing nodes is empty, no need to sync");
        return std::unordered_map<std::string, std::shared_ptr<V1Node>>();
    }

    std::unordered_map<std::string, std::shared_ptr<V1Node>> nodes;
    for (const auto &node : nodeList->GetItems()) {
        if (node == nullptr) {
            continue;
        }
        auto ip = ParseNodeInternalIP(node);
        if (ip.empty()) {
            continue;
        }
        YRLOG_INFO("sync node({}) ip({})", node->GetMetadata()->GetName(), ip);
        member_->nodes[ip] = node;
        nodes[ip] = node;
    }
    return nodes;
}

litebus::Future<std::shared_ptr<V1DeploymentList>> ScalerActor::ListNamespacedDeployment(const std::string &rNamespace)
{
    if (SyncTemplateDeployment().IsError()) {
        YRLOG_WARN("sync template deployment failed");
    }
    return member_->kubeClient->ListNamespacedDeployment(member_->k8sNamespace);
}

litebus::Future<Status> ScalerActor::SyncDeploymentAndPodPool()
{
    litebus::Promise<Status> promise;
    (void)ListNamespacedDeployment(member_->k8sNamespace)
        .OnComplete([aid(GetAID()), promise](const litebus::Future<std::shared_ptr<V1DeploymentList>> &deploymentList) {
            if (deploymentList.IsError() || deploymentList.Get()->GetItems().empty()) {
                promise.SetFailed(static_cast<int32_t>(StatusCode::FAILED));
                return;
            }
            promise.Associate(litebus::Async(aid, &ScalerActor::SyncDeployments, deploymentList.Get()));
        });
    return promise.GetFuture().Then(litebus::Defer(GetAID(), &ScalerActor::SyncPodPoolInfo, std::placeholders::_1));
}

litebus::Future<Status> ScalerActor::SyncPodAndInstance()
{
    litebus::Promise<Status> promise;
    (void)member_->kubeClient->ListNamespacedPod(member_->k8sNamespace)
        .OnComplete([aid(GetAID()), promise](const litebus::Future<std::shared_ptr<V1PodList>> &podList) {
            if (podList.IsError()) {
                YRLOG_WARN("failed to list existing pods while init, code({})", podList.GetErrorCode());
                promise.SetFailed(static_cast<int32_t>(StatusCode::FAILED));
                return;
            }
            if (podList.Get()->GetItems().empty()) {
                YRLOG_DEBUG("list existing pods is empty, no need to sync");
                promise.SetFailed(static_cast<int32_t>(StatusCode::FAILED));
            }
            promise.Associate(litebus::Async(aid, &ScalerActor::SyncPods, podList.Get()));
        });
    return promise.GetFuture().Then(litebus::Defer(GetAID(), &ScalerActor::SyncInstances, std::placeholders::_1));
}

litebus::Future<Status> ScalerActor::SyncPods(const std::shared_ptr<V1PodList> &podList)
{
    if (podList == nullptr) {
        YRLOG_WARN("podList is nullptr");
        return Status::OK();
    }
    std::unordered_map<std::string, std::shared_ptr<V1Pod>> podMap;
    for (const auto &pod : podList->GetItems()) {
        if (pod == nullptr || pod->GetMetadata() == nullptr || pod->GetStatus() == nullptr) {
            continue;
        }
        auto podName = pod->GetMetadata()->GetName();
        if (!litebus::strings::StartsWithPrefix(podName, ORIGINAL_POOL_NAME) &&
            !litebus::strings::StartsWithPrefix(podName, CUSTOM_POD_PREFIX + ORIGINAL_POOL_NAME)) {
            // don't care non-agent pod
            continue;
        }
        // add to pod map, then sync instance can find the pod name by function agent id
        YRLOG_DEBUG("sync pod(uid={})(name={})(ip={})", pod->GetMetadata()->GetUid(), podName,
                    pod->GetStatus()->GetPodIP());
        podMap[podName] = pod;
        member_->poolManager->OnPodUpdate(pod);
        if (member_->frontendManager) {
            member_->frontendManager->OnPodUpdate(pod);
        }
        if (!IsPodReady(pod)) {
            SyncCreatePodPromise(pod);
        }
    }
    if (!podMap.empty()) {
        // sync replace map
        member_->podNameMap = podMap;
    }
    return Status::OK();
}

litebus::Future<Status> ScalerActor::SyncDeployments(const std::shared_ptr<V1DeploymentList> &deploymentList)
{
    if (deploymentList == nullptr) {
        YRLOG_WARN("deploymentList is nullptr");
        return Status::OK();
    }
    for (const auto &deployment : deploymentList->GetItems()) {
        if (deployment == nullptr || deployment->GetMetadata() == nullptr || deployment->GetStatus() == nullptr) {
            continue;
        }
        auto name = deployment->GetMetadata()->GetName();
        if (!litebus::strings::StartsWithPrefix(name, ORIGINAL_POOL_NAME)) {
            continue;
        }
        member_->poolManager->PutDeployment(deployment);
    }
    return Status::OK();
}

void ScalerActor::SyncCreatePodPromise(const std::shared_ptr<V1Pod> &pod)
{
    auto uid = pod->GetMetadata()->GetUid();
    auto name = pod->GetMetadata()->GetName();
    if (waitForReadyPods_.find(uid) != waitForReadyPods_.end()) {
        return;
    }
    litebus::Promise<std::shared_ptr<V1Pod>> readyPod;
    waitForReadyPods_[uid] = readyPod;
    (void)readyPod.GetFuture()
        .After(WAIT_POD_READY_TIMEOUT, [uid, name](const litebus::Future<std::shared_ptr<V1Pod>> &readyPod) {
            YRLOG_ERROR("sync pod, wait pod({}) name({}) ready timeout", uid, name);
            readyPod.SetFailed(static_cast<int32_t>(StatusCode::GS_START_CREATE_POD_FAILED));
            return readyPod;
        })
        .OnComplete([uid, name, aid(GetAID())](const litebus::Future<std::shared_ptr<V1Pod>> &readyPod) {
            if (readyPod.IsError()) {
                litebus::Async(aid, &ScalerActor::DeleteFailedPod, uid, name);
            }
        });
}

litebus::Future<Status> ScalerActor::SyncInstances(const Status &status)
{
    if (status.IsError()) {
        return status;
    }
    YRLOG_DEBUG("begin to sync instance from etcd");
    auto synced = member_->metaStorageAccessor->Sync(INSTANCE_PATH_PREFIX, true);
    UpdateInstanceEvent(synced.first);
    return Status::OK();
}

litebus::Future<Status> ScalerActor::SyncPodPoolInfo(const Status &status)
{
    if (status.IsError()) {
        return status;
    }
    YRLOG_DEBUG("begin to sync pod pool from etcd");
    auto synced = member_->metaStorageAccessor->Sync(POD_POOL_PREFIX, true);
    UpdatePodPoolEvent(synced.first);
    return Status::OK();
}

void ScalerActor::Finalize()
{
    if (member_->kubeClient != nullptr) {
        member_->kubeClient = nullptr;
    }
    if (member_->poolManager != nullptr) {
        member_->poolManager = nullptr;
    }
    member_->frontendManager = nullptr;
}

void ScalerActor::InitLeader()
{
    auto masterBusiness = std::make_shared<MasterBusiness>(shared_from_this(), member_);
    auto slaveBusiness = std::make_shared<SlaveBusiness>(shared_from_this(), member_);

    (void)businesses_.emplace(MASTER_BUSINESS, masterBusiness);
    (void)businesses_.emplace(SLAVE_BUSINESS, slaveBusiness);

    (void)Explorer::GetInstance().AddLeaderChangedCallback(
        "ScalerActor", [aid(GetAID())](const LeaderInfo &leaderInfo) {
            litebus::Async(aid, &ScalerActor::UpdateLeaderInfo, leaderInfo);
        });

    curStatus_ = SLAVE_BUSINESS;
    business_ = slaveBusiness;
}

void ScalerActor::Init()
{
    InitLeader();
    ActorBase::Receive("CreateAgent", &ScalerActor::CreateAgent);
    ActorBase::Receive("UpdateNodeTaints", &ScalerActor::UpdateNodeTaint);
    ActorBase::Receive("DeletePod", &ScalerActor::DeletePod);
    ActorBase::Receive("UpdateLocalStatusResponse", &ScalerActor::UpdateLocalStatusResponse);
}

Status ScalerActor::CreateResourcePools(const std::shared_ptr<std::vector<ResourcePool>> &pools)
{
    ASSERT_IF_NULL(business_);
    return business_->CreateResourcePools(pools);
}
Status ScalerActor::CreateDeployment(const litebus::Future<std::shared_ptr<V1DeploymentList>> &future,
                                     const std::shared_ptr<std::vector<ResourcePool>> &pools)
{
    if (!future.IsOK()) {
        YRLOG_ERROR("failed to list deployments in default namespace!");
        return Status(StatusCode::GS_START_CREATE_DEPLOYMENTS_FAILED, "failed to list deployment");
    }

    YRLOG_INFO("succeed to list deployments in default namespace!");
    if (SyncTemplateDeployment().IsError()) {
        YRLOG_ERROR("function-agent deployment was not found");
        return Status(StatusCode::GS_START_CREATE_DEPLOYMENTS_FAILED, "failed to find template deployment");
    }

    const auto &deployments = future.Get();  // from OnComplete
    // store the deployments in a map, to speed up the search process
    for (auto &deployment : deployments->GetItems()) {
        if (litebus::strings::StartsWithPrefix(deployment.get()->GetMetadata()->GetName(), ORIGINAL_POOL_NAME)) {
            member_->poolManager->PutDeployment(deployment);
        }
    }

    for (auto &pool : *pools) {
        // deployment already exists, check its pool size and resources config. If the config is different, update it
        auto deploymentIter = member_->poolManager->GetDeployment(PoolDeploymentName(pool.name));
        if (deploymentIter != nullptr) {
            YRLOG_WARN("function-agent pool {}'s deployment already exists.", pool.name);
            CheckExistingDeployment(deploymentIter, pool);
            continue;
        }

        pool.initLabels = DEFAULT_INIT_LABELS;
        // deployment not found in default namespace, create a new one
        YRLOG_INFO("function-agent pool {} does not exist, creating...", pool.name);
        auto newDeploy = ScalerActor::GenerateDeploymentByTemplate(pool, member_->templateDeployment);
        (void)member_->kubeClient
            ->CreateNamespacedDeployment(member_->templateDeployment->GetMetadata()->GetRNamespace(), newDeploy)
            .OnComplete(
            litebus::Defer(GetAID(), &ScalerActor::OnCreateDeploymentComplete, std::placeholders::_1, pool.name));
    }

    return Status(StatusCode::SUCCESS);
}

void ScalerActor::CheckExistingDeployment(const std::shared_ptr<V1Deployment> &deployment, const ResourcePool &pool)
{
    if (ScalerActor::IsSameDeploymentConfig(pool, deployment)) {
        YRLOG_INFO("function-agent pool {}'s deployment config is same as loaded pool, skip.", pool.name);
        return;
    }

    auto newDeployment = ScalerActor::GenerateDeploymentByOrigin(pool, deployment);
    auto changedDeploymentJson = newDeployment->ToJson();
    nlohmann::json deploymentDiffPatch = nlohmann::json::diff(deployment->ToJson(), changedDeploymentJson);
    std::shared_ptr<kube_client::model::Object> body = std::make_shared<kube_client::model::Object>();
    bool isSet = body->FromJson(deploymentDiffPatch);
    if (!isSet) {
        YRLOG_WARN("failed to set object from json patch({}) for pool {}, skip", deploymentDiffPatch.dump(), pool.name);
        return;
    }

    YRLOG_WARN("function-agent pool {}'s deployment config is different with loaded pool, will be updated.", pool.name);
    (void)member_->kubeClient
        ->PatchNamespacedDeployment(newDeployment->GetMetadata()->GetName(),
                                    newDeployment->GetMetadata()->GetRNamespace(), body)
        .OnComplete(
        litebus::Defer(GetAID(), &ScalerActor::OnPatchDeploymentComplete, std::placeholders::_1, pool.name));
}

litebus::Future<std::shared_ptr<V1Pod>> ScalerActor::CreatePod(const std::string &name,
                                                               const std::shared_ptr<V1PodTemplateSpec> &spec,
                                                               const std::string &instanceID,
                                                               const std::map<std::string, std::string> &labels,
                                                               const std::map<std::string, std::string> &annotations)
{
    if (member_->templateDeployment == nullptr) {
        YRLOG_WARN("template deployment is null, aborted create pod by pool");
        litebus::Promise<std::shared_ptr<V1Pod>> promise;
        promise.SetFailed(static_cast<int32_t>(StatusCode::FUNC_AGENT_DEPLOYMENT_CONFIG_NOT_FOUND));
        return promise.GetFuture();
    }

    auto pod = std::make_shared<V1Pod>();
    pod->SetMetadata(spec->GetMetadata());
    pod->GetMetadata()->SetName(name);
    pod->GetMetadata()->SetOwnerReferences({});
    auto &podLabels = pod->GetMetadata()->GetLabels();
    for (auto &label : labels) {
        podLabels[label.first] = label.second;
    }
    podLabels[APP_LABEL_KEY] = FUNCTION_AGENT_DYNAMIC;
    if (!pod->GetMetadata()->AnnotationsIsSet()) {
        pod->GetMetadata()->SetAnnotations({});
    }
    auto &podAnnotations = pod->GetMetadata()->GetAnnotations();
    for (auto &annotation : annotations) {
        podAnnotations[annotation.first] = annotation.second;
    }
    // for custom image or system func
    podAnnotations[ANNOTATION_PLACEHOLDER] = ANNOTATION_PLACEHOLDER;
    pod->SetSpec(spec->GetSpec());
    pod->UnsetApiVersion();
    pod->UnsetKind();
    pod->UnsetStatus();

    if (auto iter = member_->instanceId2PodName.find(instanceID); iter != member_->instanceId2PodName.end()) {
        // delete existed pod, make sure every instance id only has one pod
        (void)DoDeletePod(iter->second);
        member_->podName2InstanceId.erase(iter->second);
    }
    member_->instanceId2PodName[instanceID] = pod->GetMetadata()->GetName();
    member_->podName2InstanceId[pod->GetMetadata()->GetName()] = instanceID;
    return member_->kubeClient->CreateNamespacedPod(member_->templateDeployment->GetMetadata()->GetRNamespace(), pod);
}

bool ScalerActor::IsSameDeploymentConfig(const ResourcePool &pool, const std::shared_ptr<V1Deployment> &deployment)
{
    if (deployment->GetSpec()->GetReplicas() != pool.poolSize) {
        return false;
    }
    auto containers = deployment->GetSpec()->GetRTemplate()->GetSpec()->GetContainers();
    for (auto &container : containers) {
        if (container->GetName() == RUNTIME_MANAGER_CONTAINER_NAME) {
            auto requestResources = container->GetResources()->GetRequests();
            if (requestResources.find(CPU_RESOURCE) == requestResources.end() ||
                pool.requestResources.find(CPU_RESOURCE) == pool.requestResources.end() ||
                requestResources[CPU_RESOURCE] != pool.requestResources.at(CPU_RESOURCE)) {
                return false;
            }
            if (requestResources.find(MEMORY_RESOURCE) == requestResources.end() ||
                pool.requestResources.find(MEMORY_RESOURCE) == pool.requestResources.end() ||
                requestResources[MEMORY_RESOURCE] != pool.requestResources.at(MEMORY_RESOURCE)) {
                return false;
            }

            auto limitResources = container->GetResources()->GetLimits();
            if (limitResources.find(MEMORY_RESOURCE) == limitResources.end() ||
                pool.limitResources.find(MEMORY_RESOURCE) == pool.limitResources.end() ||
                limitResources[MEMORY_RESOURCE] != pool.limitResources.at(MEMORY_RESOURCE)) {
                return false;
            }
            if (limitResources.find(MEMORY_RESOURCE) == limitResources.end() ||
                pool.limitResources.find(MEMORY_RESOURCE) == pool.limitResources.end() ||
                limitResources[MEMORY_RESOURCE] != pool.limitResources.at(MEMORY_RESOURCE)) {
                return false;
            }
        }
    }
    return true;
}

void ScalerActor::GetDsWorkerPort(const std::shared_ptr<V1Deployment> &deployment)
{
    for (const auto &container : deployment->GetSpec()->GetRTemplate()->GetSpec()->GetContainers()) {
        if (container->GetName() != RUNTIME_MANAGER_CONTAINER_NAME) {
            continue;
        }

        for (const auto &env : container->GetEnv()) {
            if (env->GetName() == "DS_WORKER_PORT") {
                dsWorkPort_ = env->GetValue();
                break;
            }
        }

        break;
    }
}

void ScalerActor::AddDataSystemIpAndPort(const std::shared_ptr<V1Container> &container)
{
    std::vector<std::shared_ptr<V1EnvVar>> &env = container->GetEnv();

    auto hostFieldRef = std::make_shared<V1ObjectFieldSelector>();
    hostFieldRef->SetApiVersion("v1");
    hostFieldRef->SetFieldPath("status.hostIP");
    auto hostFrom = std::make_shared<V1EnvVarSource>();
    hostFrom->SetFieldRef(hostFieldRef);
    auto datasystemHost = std::make_shared<V1EnvVar>();
    datasystemHost->SetName("DATASYSTEM_HOST");
    datasystemHost->SetValueFrom(hostFrom);
    (void)env.emplace_back(datasystemHost);

    auto datasystemPort = std::make_shared<V1EnvVar>();
    datasystemPort->SetName("DATASYSTEM_PORT");
    datasystemPort->SetValue(dsWorkPort_);
    (void)env.emplace_back(datasystemPort);

    // set back into container, make sure envIsSet is true
    container->SetEnv(env);
}

std::shared_ptr<V1Deployment> ScalerActor::GenerateDeploymentByTemplate(
    const ResourcePool &pool, const std::shared_ptr<V1Deployment> &templateDeployment)
{
    auto deployment = std::make_shared<V1Deployment>();
    // deep copy from template
    if (!deployment->FromJson(templateDeployment->ToJson())) {
        YRLOG_WARN("failed to decode from template deployment.");
    }

    SetMetaData(pool, deployment);
    deployment->GetSpec()->SetReplicas(pool.isScalable ? 0 : pool.poolSize);
    deployment->GetSpec()->GetSelector()->GetMatchLabels()[APP_LABEL_KEY] = PoolDeploymentName(pool.name);

    auto rt = deployment->GetSpec()->GetRTemplate();
    SetAffinityForPool(pool, rt);
    for (auto &container : rt->GetSpec()->GetContainers()) {
        if (container->GetName() == RUNTIME_MANAGER_CONTAINER_NAME) {
            if (pool.delegateRuntimeManager.IsSome()) {
                auto newImage = pool.delegateRuntimeManager.Get()->GetImage();
                if (!newImage.empty()) {
                    container->SetImage(newImage);
                }
                auto extraEnv = pool.delegateRuntimeManager.Get()->GetEnv();
                for (auto &env : extraEnv) {
                    container->GetEnv().push_back(env);
                }
            }
            for (const auto &volMount : pool.delegateVolumeMounts) {
                container->GetVolumeMounts().push_back(volMount);
            }
            // update metrics (cpu/memory/labels) for runtime_manager
            UpdateContainerResource(container, pool);
            SetResourceEnvs(container, pool);
        } else if (container->GetName() == FUNCTION_AGENT_CONTAINER_NAME) {
            for (const auto &volMount : pool.delegateAgentVolumeMounts) {
                container->GetVolumeMounts().push_back(volMount);
            }
        }
    }
    AddDelegateConfig(pool, rt);
    // add label to tell whether we need to re-use pod
    rt->GetMetadata()->GetLabels().emplace(REUSE_LABEL_KEY, pool.isReuse ? "true" : "false");
    for (const auto &label : pool.delegatePoolLabels) {
        if (label.first.empty()) {
            continue;
        }
        rt->GetMetadata()->GetLabels().emplace(label.first, label.second);
    }
    deployment->GetMetadata()->SetOwnerReferences({});
    // add alarm threshold of the duration for a pending pod
    if (pool.podPendingDurationThreshold > 0) {
        rt->GetMetadata()->GetAnnotations().emplace(PENDING_THRESHOLD,
                                                    std::to_string(pool.podPendingDurationThreshold));
    }
    return deployment;
}

std::shared_ptr<V1PodTemplateSpec> ScalerActor::GeneratePodTemplateSpec(
    const ResourcePool &pool, const std::shared_ptr<V1Deployment> &templateDeployment)
{
    auto deployment = std::make_shared<V1Deployment>();
    // deep copy from template
    if (!deployment->FromJson(templateDeployment->ToJson())) {
        YRLOG_WARN("failed to decode from template deployment.");
    }

    auto rt = deployment->GetSpec()->GetRTemplate();
    for (auto &container: rt->GetSpec()->GetContainers()) {
        if (container->GetName() != FUNCTION_AGENT_CONTAINER_NAME) {
            continue;  // only fo function_agent
        }
        // add delegate volume mount to function_agent
        for (const auto &mount : pool.delegateAgentVolumeMounts) {
            container->GetVolumeMounts().push_back(mount);
        }
    }
    for (auto &container : rt->GetSpec()->GetContainers()) {
        if (container->GetName() != RUNTIME_MANAGER_CONTAINER_NAME) {
            continue;  // only fo runtime_manager
        }

        // update metrics (cpu/memory/labels) for runtime_manager
        if (pool.delegateContainer.IsNone()) {
            // add container limit on delegate container, not on runtime_manager
            UpdateContainerResource(container, pool);
        }
        SetResourceEnvs(container, pool);

        if (pool.delegateContainer.IsSome() && pool.isNeedBindCPU) {
            // bind cpu need to set runtime manager request and limit cpu-mem both to 1000m-900Mi
            container->GetResources()->GetLimits()[CPU_RESOURCE] = CPU_BIND_CPU_VALUE;
            container->GetResources()->GetLimits()[MEMORY_RESOURCE] = CPU_BIND_MEM_VALUE;
            container->GetResources()->GetRequests()[CPU_RESOURCE] = CPU_BIND_CPU_VALUE;
            container->GetResources()->GetRequests()[MEMORY_RESOURCE] = CPU_BIND_MEM_VALUE;
        }

        // add delegate volume mount to runtime_manager
        for (const auto &mount : pool.delegateVolumeMounts) {
            // runtime manager has DAC_OVERRIDE capability, has full access to mounted files.
            // but only system function or delegate container function will allow delegate volume mount,
            // so user code can't access these volumes
            container->GetVolumeMounts().push_back(mount);
        }

        // update runtime_manager config
        if (pool.delegateRuntimeManager.IsSome()) {
            auto newImage = pool.delegateRuntimeManager.Get()->GetImage();
            if (!newImage.empty()) {
                container->SetImage(newImage);
            }
            auto currEnv = container->GetEnv();
            auto extraEnv = pool.delegateRuntimeManager.Get()->GetEnv();
            for (auto &env : extraEnv) {
                currEnv.push_back(env);
            }
            container->SetEnv(currEnv);
        }

        break;  // only for runtime_manager
    }

    if (pool.delegateContainer.IsSome()) {
        auto delegateContainer = pool.delegateContainer.Get();
        delegateContainer->SetName(DELEGATE_CONTAINER_NAME);
        AddDataSystemIpAndPort(delegateContainer);
        // only add delegate decrypt envs for delegate container
        // normal function's delegate decrypt envs will be added in runtime_manager
        for (const auto &delegateDecryptEnv : pool.delegateDecryptEnvs) {
            (void)delegateContainer->GetEnv().emplace_back(delegateDecryptEnv);
        }
        UpdateContainerResource(delegateContainer, pool);
        rt->GetSpec()->GetContainers().emplace(rt->GetSpec()->GetContainers().begin(), delegateContainer);
        rt->GetSpec()->SetTerminationGracePeriodSeconds(pool.terminationGracePeriodSeconds);
        if (delegateContainer->ServiceAccountNameIsSet()) {
            rt->GetSpec()->SetServiceAccountName(delegateContainer->GetServiceAccountName());
        }
    }

    AddDelegateConfig(pool, rt);
    AddAffinityForPodSpec(pool.affinity, rt, pool.isAggregationNodeAffinity);
    if (rt->GetSpec()->SecurityContextIsSet() && pool.delegateSecCompProfile != nullptr) {
        rt->GetSpec()->GetSecurityContext()->SetSeccompProfile(pool.delegateSecCompProfile);
    }
    if (pool.isSystemFunc) {
        // only allow system function to access k8s api-server
        rt->GetSpec()->SetAutomountServiceAccountToken(true);
        rt->GetSpec()->SetServiceAccountName(SYSTEM_FUNCTION_SERVICE_ACCOUNT_NAME);
        rt->GetSpec()->SetPriorityClassName(SYSTEM_CLUSTER_CRITICAL_CLASS_NAME);
    }
    return rt;
}
void ScalerActor::AddDelegateConfig(const ResourcePool &pool, const std::shared_ptr<V1PodTemplateSpec> &rt) const
{
    SetHostAliases(rt->GetSpec(), pool.delegateHostAliases);

    // add delegate volume to pod spec
    for (const auto &volume : pool.delegateVolumes) {
        rt->GetSpec()->GetVolumes().push_back(volume);
    }

    for (const auto &sideCar : pool.delegateSidecars) {
        rt->GetSpec()->GetContainers().push_back(sideCar);
    }

    for (const auto &initContainer : rt->GetSpec()->GetInitContainers()) {
        if (initContainer->GetName() != FUNCTION_AGENT_INIT_CONTAINER_NAME) {
            continue;
        }
        if (!pool.initImage.empty()) {
            initContainer->SetImage(pool.initImage);
        }
        for (const auto &volumeMount : pool.delegateInitVolumeMounts) {
            initContainer->GetVolumeMounts().push_back(volumeMount);
        }

        if (!initContainer->EnvIsSet()) {
            initContainer->SetEnv({});
        }
        for (const auto &env : pool.delegateInitEnvs) {
            initContainer->GetEnv().push_back(env);
        }
    }

    for (const auto &initContainer : pool.delegateInitContainers) {
        rt->GetSpec()->GetInitContainers().push_back(initContainer);
    }

    if (!rt->GetSpec()->TolerationsIsSet()) {
        rt->GetSpec()->SetTolerations({});
    }
    for (const auto &toleration : pool.delegateTolerations) {
        rt->GetSpec()->GetTolerations().push_back(toleration);
    }
    if (!rt->GetSpec()->NodeSelectorIsSet()) {
        rt->GetSpec()->SetNodeSelector({});
    }
    for (const auto &nodeSelector: pool.nodeSelector) {
        rt->GetSpec()->GetNodeSelector()[nodeSelector.first] = nodeSelector.second;
    }
    if (!pool.runtimeClassName.empty()) {
        rt->GetSpec()->SetRuntimeClassName(pool.runtimeClassName);
    }
    if (!rt->GetSpec()->TopologySpreadConstraintsIsSet()) {
        rt->GetSpec()->SetTopologySpreadConstraints({});
    }
    for (const auto &topologySpreadConstraint : pool.delegateTopologySpreadConstraints) {
        rt->GetSpec()->GetTopologySpreadConstraints().push_back(topologySpreadConstraint);
    }
}

void ScalerActor::SetAffinityForPool(const ResourcePool &pool, const std::shared_ptr<V1PodTemplateSpec> &spec)
{
    if (!spec->GetSpec()->AffinityIsSet()) {
        spec->GetSpec()->SetAffinity(std::make_shared<V1Affinity>());
    }
    if (!spec->GetSpec()->GetAffinity()->PodAntiAffinityIsSet()) {
        auto label = PoolDeploymentName(pool.name);
        std::map<std::string, std::string> labels;
        (void)labels.emplace(APP_LABEL_KEY, label);
        spec->GetMetadata()->SetLabels(labels);
        std::unordered_map<AffinityType, std::vector<std::string>> affinityTypeLabels;
        (void)affinityTypeLabels[AffinityType::PreferredAntiAffinity].emplace_back(APP_LABEL_KEY + ":" + label);
        auto affinity = ParseAffinity(affinityTypeLabels);
        spec->GetSpec()->GetAffinity()->SetPodAntiAffinity(affinity->GetPodAntiAffinity());
    }
    AddAffinityForPodSpec(pool.affinity, spec, pool.isAggregationNodeAffinity);
}

void ScalerActor::SetMetaData(const ResourcePool &pool, const std::shared_ptr<V1Deployment> &deployment)
{
    deployment->GetMetadata()->SetName(PoolDeploymentName(pool.name));
    std::map<std::string, std::string> labels;
    (void)labels.emplace(APP_LABEL_KEY, PoolDeploymentName(pool.name));
    deployment->GetMetadata()->SetLabels(labels);
    deployment->GetMetadata()->UnsetCreationTimestamp();
    deployment->GetMetadata()->UnsetGeneration();
    deployment->GetMetadata()->UnsetResourceVersion();
    deployment->GetMetadata()->UnSetSelfLink();
    deployment->GetMetadata()->UnsetUid();
}

void ScalerActor::SetResourceEnvs(const std::shared_ptr<V1Container> &container, const ResourcePool &pool)
{
    for (auto &env : container->GetEnv()) {
        if (env->GetName() == CPU4COMP_ENV && pool.limitResources.find(CPU_RESOURCE) != pool.limitResources.end()) {
            auto cpuRes = pool.limitResources.at(CPU_RESOURCE);
            auto cpuVal = ParseCPUEnv(cpuRes);
            if (!cpuVal.empty()) {
                env->SetValue(cpuVal);
            }
        } else if (env->GetName() == MEM4COMP_ENV &&
                   pool.limitResources.find(MEMORY_RESOURCE) != pool.limitResources.end()) {
            auto memRes = pool.limitResources.at(MEMORY_RESOURCE);
            auto memVal = ParseMemoryEnv(memRes);
            if (!memVal.empty()) {
                env->SetValue(memVal);
            }
        } else if (env->GetName() == INIT_LABELS_ENV) {
            env->SetValue(pool.initLabels);
        }
    }
}

void ScalerActor::OnCreateDeploymentComplete(const litebus::Future<std::shared_ptr<V1Deployment>> &retDeployment,
                                             const std::string &poolName)
{
    if (!retDeployment.IsOK()) {
        YRLOG_WARN("failed to create deployment for pool {}, error code: {}", poolName, retDeployment.GetErrorCode());
        return;
    }
    YRLOG_INFO("succeed to create deployment for pool {}", poolName);
    auto createdDeployment = retDeployment.Get();
    if (createdDeployment != nullptr) {
        member_->poolManager->PutDeployment(createdDeployment);
    }
    DoScaleUpPodByPoolID(poolName, true, "", litebus::AID());
}

void ScalerActor::OnCreatePodPoolComplete(const litebus::Future<std::shared_ptr<V1Deployment>> &retDeployment,
                                          const std::shared_ptr<PodPoolInfo> &poolInfo)
{
    if (!retDeployment.IsOK()) {
        YRLOG_WARN("failed to create pod pool {}, error code: {}", poolInfo->id, retDeployment.GetErrorCode());
        member_->kubeClient->DeleteNamespacedHorizontalPodAutoscaler(POOL_NAME_PREFIX + poolInfo->id + "-hpa",
            member_->k8sNamespace, litebus::None(), litebus::None());
        poolInfo->status = static_cast<int32_t>(PoolState::FAILED);
        poolInfo->readyCount = 0;
        poolInfo->msg = "Create Deployment Failed, ErrorCode is " + std::to_string(retDeployment.GetErrorCode());
        DoPersistencePoolInfo(poolInfo->id);
        return;
    }
    DoPersistencePoolInfo(poolInfo->id);
    YRLOG_INFO("succeed to create pod pool {}", poolInfo->id);
    auto createdDeployment = retDeployment.Get();
    if (createdDeployment != nullptr) {
        member_->poolManager->PutDeployment(createdDeployment);
    }
}

void ScalerActor::OnPatchDeploymentComplete(const litebus::Future<std::shared_ptr<V1Deployment>> &retDeployment,
                                            const std::string &poolName)
{
    if (!retDeployment.IsOK()) {
        YRLOG_WARN("failed to patch deployment for pool {}, error code: {}", poolName, retDeployment.GetErrorCode());
        return;
    }
    YRLOG_INFO("succeed to patch deployment for pool {}", poolName);
    auto updatedDeployment = retDeployment.Get();
    member_->poolManager->PutDeployment(updatedDeployment);
}

void ScalerActor::OnUpdatePodPoolComplete(const litebus::Future<std::shared_ptr<V1Deployment>> &retDeployment,
                                          const std::shared_ptr<PodPoolInfo> &podPool)
{
    if (!retDeployment.IsOK()) {
        YRLOG_WARN("failed to update pod pool {}, error code: {}", podPool->id, retDeployment.GetErrorCode());
        podPool->msg = "Update Deployment Failed, ErrorCode is " + retDeployment.GetErrorCode();
    } else {
        YRLOG_INFO("succeed to update pod pool {}", podPool->id);
    }
    DoPersistencePoolInfo(podPool->id);
}

litebus::Future<Status> ScalerActor::DoDeletePod(const std::string &deletePodName, bool isForce)
{
    if (deletePodName.empty()) {
        // don't need to delete pod, just return ok
        return Status::OK();
    }
    auto promise = std::make_shared<litebus::Promise<Status>>();
    YRLOG_INFO("function-agent pool pod({}) is going to be deleted!", deletePodName);
    (void)DoDeleteNamespacedPod(deletePodName, member_->k8sNamespace, GetPodDeleteOptions(deletePodName, isForce),
                                litebus::Option<bool>(false))
        .OnComplete([deletePodName, promise,
                     poolManager(member_->poolManager)](const litebus::Future<std::shared_ptr<V1Pod>> &retDeletedPod) {
            if (!retDeletedPod.IsOK() && retDeletedPod.GetErrorCode() != litebus::http::ResponseCode::NOT_FOUND) {
                promise->SetFailed(retDeletedPod.GetErrorCode());
                YRLOG_ERROR("failed to delete pod {}, error code: {}", deletePodName, retDeletedPod.GetErrorCode());
                return;
            }
            promise->SetValue(Status::OK());
            YRLOG_INFO("succeed to delete pod {}", deletePodName);
            // To prevent pod event is delay to receive, and it will affect pod scaling
            if (poolManager != nullptr && retDeletedPod.IsOK()) {
                // pod is deleting, and k8s will trigger event (pod is running) before terminate it
                poolManager->OnPodDelete(retDeletedPod.Get(), false);
            }
        });
    return promise->GetFuture();
}

litebus::Future<std::shared_ptr<V1Pod>> ScalerActor::DoDeleteNamespacedPod(
    const std::string &podName, const std::string &rNamespace,
    const litebus::Option<std::shared_ptr<V1DeleteOptions>> &body, const litebus::Option<bool> &orphanDependents)
{
    return member_->kubeClient->DeleteNamespacedPod(podName, rNamespace, body, orphanDependents);
}

litebus::Option<std::shared_ptr<V1DeleteOptions>> ScalerActor::GetPodDeleteOptions(const std::string &podName,
                                                                                   bool isForce)
{
    std::shared_ptr<V1DeleteOptions> deleteOptions = std::make_shared<V1DeleteOptions>();
    if (isForce) {
        deleteOptions->SetGracePeriodSeconds(member_->gracePeriodSeconds);
    }
    deleteOptions->SetPropagationPolicy("Foreground");
    return litebus::Option<std::shared_ptr<V1DeleteOptions>>(deleteOptions);
}

void ScalerActor::OnCreatePodComplete(const litebus::Future<std::shared_ptr<V1Pod>> &retPod,
                                      const std::shared_ptr<messages::CreateAgentRequest> &createAgentRequest,
                                      const litebus::AID &from, bool needWaitPodReady)
{
    if (!retPod.IsOK()) {
        YRLOG_WARN("{}|failed to create pod for instance({}), error code: {}",
                   createAgentRequest->instanceinfo().requestid(), createAgentRequest->instanceinfo().instanceid(),
                   retPod.GetErrorCode());
        SendCreateAgentFailedResponse(from, static_cast<int32_t>(StatusCode::GS_START_CREATE_POD_FAILED),
                                      "failed to create pod, error code: " + std::to_string(retPod.GetErrorCode()),
                                      *createAgentRequest);
        return;
    }

    // only need to wait pod ready in delegate container scenario, normal function can just return to save time
    if (!needWaitPodReady) {
        YRLOG_INFO("{}|succeed to create pod({}) for instance({})", createAgentRequest->instanceinfo().requestid(),
                   retPod.Get()->GetMetadata()->GetName(), createAgentRequest->instanceinfo().instanceid());
        (void)member_->creatingRequests.erase(createAgentRequest->instanceinfo().requestid());
        auto createAgentResponse =
            InitCreateAgentResponse(static_cast<int32_t>(StatusCode::SUCCESS), "", *createAgentRequest);
        (void)Send(from, "CreateAgentResponse", createAgentResponse.SerializeAsString());
        return;
    }

    // wait pod ready to get container id for delegate container
    YRLOG_INFO("{}|succeed to create pod for instance({}), wait for pod({}) ready",
               createAgentRequest->instanceinfo().requestid(), createAgentRequest->instanceinfo().instanceid(),
               retPod.Get()->GetMetadata()->GetName());

    litebus::Promise<std::shared_ptr<V1Pod>> readyPod;
    waitForReadyPods_[retPod.Get()->GetMetadata()->GetUid()] = readyPod;
    (void)readyPod.GetFuture()
        .After(WAIT_POD_READY_TIMEOUT,
            [createAgentRequest, uid(retPod.Get()->GetMetadata()->GetUid())](
                const litebus::Future<std::shared_ptr<V1Pod>> &readyPod) {
                    YRLOG_ERROR("{}|wait pod({}) ready failed, instance({}) timeout",
                        createAgentRequest->instanceinfo().requestid(), uid,
                        createAgentRequest->instanceinfo().instanceid());
                    readyPod.SetFailed(static_cast<int32_t>(StatusCode::GS_START_CREATE_POD_FAILED));
                    return readyPod;
                })
        .OnComplete(litebus::Defer(GetAID(), &ScalerActor::HandlerWaitPodReady, std::placeholders::_1,
                                   createAgentRequest, retPod.Get()->GetMetadata()->GetUid(),
                                   retPod.Get()->GetMetadata()->GetName(), from));
}

void ScalerActor::OnScaleUpPodComplete(const litebus::Future<std::shared_ptr<V1Pod>> &retPod,
                                       const std::shared_ptr<V1Pod> &srcPod, const std::string &poolID,
                                       const std::string &requestID, const litebus::AID &from)
{
    if (!retPod.IsOK()) {
        YRLOG_WARN("{}|failed to scale up pod for pool({}), error code: {}", requestID, poolID, retPod.GetErrorCode());
        if (!requestID.empty()) {
            (void)member_->creatingRequests.erase(requestID);
            (void)Send(
                from, "CreateAgentResponse",
                GenCreateAgentResponse(requestID, static_cast<int32_t>(StatusCode::GS_START_CREATE_POD_FAILED),
                                       "failed to create pod, error code: " + std::to_string(retPod.GetErrorCode()))
                    .SerializeAsString());
        }
        member_->poolManager->OnPodDelete(srcPod);
        return;
    }
    YRLOG_INFO("{}|succeed to scale up pod({}) for pool({})", requestID, srcPod->GetMetadata()->GetName(), poolID);
    if (!requestID.empty()) {
        litebus::Promise<std::shared_ptr<V1Pod>> readyPod;
        waitForReadyPods_[retPod.Get()->GetMetadata()->GetUid()] = std::move(readyPod);
        (void)readyPod.GetFuture()
            .After(WAIT_POD_READY_TIMEOUT, [requestID, podName(retPod.Get()->GetMetadata()->GetName())](
                const litebus::Future<std::shared_ptr<V1Pod>> &readyPod) {
                    YRLOG_ERROR("{}|wait pod({}) ready failed,  timeout", requestID, podName);
                    readyPod.SetFailed(static_cast<int32_t>(StatusCode::GS_START_CREATE_POD_FAILED));
                    return readyPod;
            })
            .OnComplete(litebus::Defer(GetAID(), &ScalerActor::HandlerWaitPoolPodReady, std::placeholders::_1,
                                       requestID, retPod.Get()->GetMetadata()->GetUid(),
                                       retPod.Get()->GetMetadata()->GetName(), from));
    }
    member_->poolManager->OnPodUpdate(srcPod);
}

void ScalerActor::HandlerWaitPodReady(const litebus::Future<std::shared_ptr<V1Pod>> &readyPod,
                                      const std::shared_ptr<messages::CreateAgentRequest> &createAgentRequest,
                                      const std::string &uid, const std::string &name, const litebus::AID &from)
{
    if (readyPod.IsError()) {
        YRLOG_ERROR("{}|wait pod({}) ready failed, instance({}), error: {}",
                    createAgentRequest->instanceinfo().requestid(), uid,
                    createAgentRequest->instanceinfo().instanceid(), readyPod.GetErrorCode());
        SendCreateAgentFailedResponse(
            from, static_cast<int32_t>(StatusCode::GS_START_CREATE_POD_FAILED),
            "wait for pod(" + uid + ") ready failed, error: " + std::to_string(readyPod.GetErrorCode()),
            *createAgentRequest);
        DeleteFailedPod(uid, name);
        return;
    }

    auto createAgentResponse =
        InitCreateAgentResponse(static_cast<int32_t>(StatusCode::SUCCESS), "", *createAgentRequest);
    auto containerStatuses = readyPod.Get()->GetStatus()->GetContainerStatuses();
    for (auto containerStatus : containerStatuses) {
        if (containerStatus->NameIsSet() && containerStatus->GetName() == DELEGATE_CONTAINER_NAME) {
            std::string idWithPrefix = containerStatus->GetContainerID();
            auto containerId = idWithPrefix.substr(CONTAINER_ID_PREFIX.size());
            YRLOG_DEBUG("{}|success to get delegate container({}) for instance({})",
                        createAgentRequest->instanceinfo().requestid(), containerId,
                        createAgentRequest->instanceinfo().instanceid());
            (*createAgentResponse.mutable_updatedcreateoptions())[DELEGATE_CONTAINER_ID_KEY] = containerId;
        }
    }
    YRLOG_INFO("{}|succeed to create pod for instance({})", createAgentRequest->instanceinfo().requestid(),
               createAgentRequest->instanceinfo().instanceid());

    (void)member_->creatingRequests.erase(createAgentRequest->instanceinfo().requestid());
    (void)Send(from, "CreateAgentResponse", createAgentResponse.SerializeAsString());
}

void ScalerActor::HandlerWaitPoolPodReady(const litebus::Future<std::shared_ptr<V1Pod>> &readyPod,
                                          const std::string &requestID, const std::string &uid, const std::string &name,
                                          const litebus::AID &from)
{
    auto createResp = GenCreateAgentResponse(requestID, static_cast<int32_t>(StatusCode::SUCCESS), "");
    if (readyPod.IsError()) {
        createResp = GenCreateAgentResponse(requestID, static_cast<int32_t>(StatusCode::GS_START_CREATE_POD_FAILED),
                                            "timeout to scale up pod");
        DeleteFailedPod(uid, name);
    } else {
        YRLOG_INFO("{}|succeed to create pod({}), pod is ready", requestID, name);
    }
    (void)member_->creatingRequests.erase(requestID);
    waitForReadyPods_.erase(uid);
    (void)Send(from, "CreateAgentResponse", createResp.SerializeAsString());
}

void ScalerActor::DeleteFailedPod(const std::string &uid, const std::string &podName)
{
    YRLOG_INFO("{}|failed to waited pod ready, start to delete pod({})", uid, podName);
    (void)DoDeleteNamespacedPod(podName, member_->k8sNamespace, GetPodDeleteOptions(podName))
        .OnComplete([podName](const litebus::Future<std::shared_ptr<V1Pod>> &retDeletedPod) {
            if (!retDeletedPod.IsOK()) {
                YRLOG_ERROR("failed to delete pod({}), error code: {}", podName, retDeletedPod.GetErrorCode());
                return;
            }
            YRLOG_INFO("succeed to delete pod({})", podName);
        });
    (void)waitForReadyPods_.erase(uid);
}

void ScalerActor::HandlePodModifiedEvent(const K8sEventType &type, const std::shared_ptr<ModelBase> &model)
{
    OnPodModified(type, model);
}

void ScalerActor::HandleNodeModifiedEvent(const K8sEventType &type, const std::shared_ptr<ModelBase> &model)
{
    OnNodeModified(type, model);
}

void ScalerActor::UpdateMemberOnPodDeleted(const std::shared_ptr<V1Pod> &pod)
{
    auto podName = pod->GetMetadata()->GetName();
    (void)member_->podName2InstanceId.erase(podName);
    (void)member_->podNameMap.erase(podName);
    (void)member_->pendingPods.erase(podName);
    if (member_->nodeIdPodNameMap.find(pod->GetSpec()->GetNodeName()) != member_->nodeIdPodNameMap.end()) {
        (void)member_->nodeIdPodNameMap[pod->GetSpec()->GetNodeName()].erase(podName);
    }
    if (member_->migratingPodSet.find(podName) != member_->migratingPodSet.end()) {
        (void)member_->migratingPodSet.erase(podName);
    }
}

void ScalerActor::UpdateMemberOnPodModified(const std::shared_ptr<V1Pod> &pod)
{
    auto podName = pod->GetMetadata()->GetName();
    member_->podNameMap[podName] = pod;
    if (member_->nodeIdPodNameMap.find(pod->GetSpec()->GetNodeName()) != member_->nodeIdPodNameMap.end()) {
        (void)member_->nodeIdPodNameMap[pod->GetSpec()->GetNodeName()].emplace(podName);
    } else {
        member_->nodeIdPodNameMap[pod->GetSpec()->GetNodeName()] = std::set<std::string>{ podName };
    }
}

void ScalerActor::HandleReadyPodOnModified(const K8sEventType &type, const std::shared_ptr<V1Pod> &pod)
{
    if (!IsPodReady(pod)) {
        YRLOG_INFO("receive pod({}) modify event({}), and the pod is not ready", pod->GetMetadata()->GetUid(),
                   fmt::underlying(type));
        return;
    }
    if (auto iter = waitForReadyPods_.find(pod->GetMetadata()->GetUid()); iter != waitForReadyPods_.end()) {
        iter->second.SetValue(pod);
        (void)waitForReadyPods_.erase(iter);
    }
}

void ScalerActor::OnPodModified(const K8sEventType &type, const std::shared_ptr<ModelBase> &model)
{
    auto podPtr = dynamic_cast<V1Pod *>(&(*model));
    if (podPtr == nullptr) {
        return;
    }

    auto pod = std::make_shared<V1Pod>(*podPtr);
    auto podName = pod->GetMetadata()->GetName();
    if (podName.find(DS_WORKER_NAME) != std::string::npos) {
        return;
    }
    if (podName.find(ORIGINAL_POOL_NAME) == std::string::npos) {
        // don't care non-agent pod
        return;
    }

    auto uid = pod->GetMetadata()->GetUid();
    YRLOG_INFO("receive pod(uid={})(name={}) modify event({})", uid, pod->GetMetadata()->GetName(),
               fmt::underlying(type));
    if (type == K8sEventType::EVENT_TYPE_DELETE) {
        UpdateMemberOnPodDeleted(pod);
        member_->poolManager->OnPodDelete(pod);
        if (member_->frontendManager) {
            member_->frontendManager->OnPodDelete(pod);
        }
        return;
    }
    ASSERT_IF_NULL(business_);
    business_->HandleAbnormalPod(pod);

    business_->DeletePodNotBindInstance(pod);
    business_->UpdatePodLabelsWithoutInstance(pod);
    // refresh pod spec
    UpdateMemberOnPodModified(pod);
    member_->poolManager->OnPodUpdate(pod);
    if (member_->frontendManager) {
        member_->frontendManager->OnPodUpdate(pod);
    }
    business_->MigratePodInstanceWithTaints(pod->GetStatus()->GetHostIP(), podName);
    HandleReadyPodOnModified(type, pod);
}

void ScalerActor::MigratePodInstanceWithTaints(const std::string &hostIP, const std::string &podName)
{
    if (member_->nodes.find(hostIP) == member_->nodes.end()) {
        return;
    }
    auto type = CheckNodeFault(member_->nodes[hostIP]);
    if (type == MigrateType::NOT_MIGRATE) {
        return;
    }
    DoPodInstanceMigrate(podName, type);
}

void ScalerActor::DeletePodNotBindInstance(const std::shared_ptr<V1Pod> &pod)
{
    // check if the pod is managed by scaler
    if (!CheckPodNeedDelete(pod)) {
        return;
    }
    auto podName = pod->GetMetadata()->GetName();
    (void)DoDeleteNamespacedPod(podName, member_->k8sNamespace, GetPodDeleteOptions(podName))
        .OnComplete([podName](const litebus::Future<std::shared_ptr<V1Pod>> &retDeletedPod) {
            if (!retDeletedPod.IsOK()) {
                YRLOG_ERROR("failed to delete pod {}, error code: {}", podName, retDeletedPod.GetErrorCode());
                return;
            }
            YRLOG_INFO("succeed to delete pod {}", podName);
        });
}

bool ScalerActor::CheckPodNeedDelete(const std::shared_ptr<V1Pod> &pod)
{
    if (auto it = member_->podName2InstanceId.find(pod->GetMetadata()->GetName());
        it != member_->podName2InstanceId.end()) {
        auto instanceID = it->second;
        if (deletingPodSet_.find(instanceID) != deletingPodSet_.end()) {
            YRLOG_WARN("pod for instance({}) is deleting, skipped", instanceID);
            return false;
        }

        auto instanceExistIt = member_->instanceId2PodName.find(instanceID);
        if (instanceExistIt == member_->instanceId2PodName.end() ||
            instanceExistIt->second != pod->GetMetadata()->GetName()) {
            YRLOG_INFO("pod(uid={})(name={}) will be deleted, the bind instance({}) not existing anymore",
                       pod->GetMetadata()->GetUid(), pod->GetMetadata()->GetName(), instanceID);
            return true;
        }
        return false;
    }
    auto labels = pod->GetMetadata()->GetLabels();
    if (auto it = labels.find(APP_LABEL_KEY); it != labels.end()) {
        auto label = it->second;
        if (label == FUNCTION_AGENT_DYNAMIC) {
            YRLOG_INFO("pod(uid={})(name={}) will be deleted, has no bind instance, but bind function-agent({})",
                       pod->GetMetadata()->GetUid(), pod->GetMetadata()->GetName(), label);
            return true;
        }
        if (!pod->StatusIsSet() || !pod->GetStatus()->PodIPIsSet() || pod->GetStatus()->GetPodIP().empty()) {
            return false;
        }
        if (label == TruncateIllegalLabel(pod->GetMetadata()->GetName())) {
            YRLOG_INFO("pod(uid={})(name={}) will be deleted, has no bind instance, but bind function-agent({})",
                       pod->GetMetadata()->GetUid(), pod->GetMetadata()->GetName(), label);
            return true;
        }
    }
    if (auto iter = labels.find(REUSE_LABEL_KEY); iter != labels.end() && iter->second == "true") {
        if (auto it = labels.find(LABEL_SCHEDULE_POLICY); it != labels.end() && it->second == MONOPOLY_SCHEDULE) {
            YRLOG_DEBUG("pod({}) is reused, but already schedule monopoly instance, need to delete",
                        pod->GetMetadata()->GetName());
            return true;
        }
    }
    return false;
}

void ScalerActor::OnNodeModified(const K8sEventType &type, const std::shared_ptr<ModelBase> &model)
{
    auto nodePtr = dynamic_cast<V1Node *>(&(*model));
    if (nodePtr == nullptr) {
        return;
    }

    auto node = std::make_shared<V1Node>(*nodePtr);
    auto ip = ParseNodeInternalIP(node);
    if (ip.empty()) {
        return;
    }
    if (type == K8sEventType::EVENT_TYPE_DELETE) {
        YRLOG_INFO("receive node({}) ip({}) delete event", node->GetMetadata()->GetName(), ip);
        (void)member_->nodes.erase(ip);
        return;
    }
    YRLOG_INFO("receive node({}) ip({}) update event", node->GetMetadata()->GetName(), ip);
    member_->nodes[ip] = node;
    ASSERT_IF_NULL(business_);
    business_->MigrateNodeInstanceWithTaints(node);
}

void ScalerActor::OnDeploymentModified(const K8sEventType &type, const std::shared_ptr<ModelBase> &model)
{
    auto deploymentPtr = dynamic_cast<V1Deployment *>(&(*model));
    if (deploymentPtr == nullptr) {
        return;
    }
    auto deployment = std::make_shared<V1Deployment>(*deploymentPtr);
    auto name = deployment->GetMetadata()->GetName();
    if (!litebus::strings::StartsWithPrefix(name, POOL_NAME_PREFIX)) {
        return;
    }
    YRLOG_INFO("receive deployment(uid={})(name={}) modify event({})", deployment->GetMetadata()->GetUid(),
               deployment->GetMetadata()->GetName(), fmt::underlying(type));
    if (type == K8sEventType::EVENT_TYPE_DELETE) {
        member_->poolManager->DeleteDeployment(deployment->GetMetadata()->GetName());
    } else {
        member_->poolManager->PutDeployment(deployment);
    }
    ASSERT_IF_NULL(business_);
    auto poolID = name.substr(POOL_NAME_PREFIX.size());
    auto poolInfo = member_->poolManager->GetPodPool(poolID);
    auto poolNames = member_->poolManager->GetLoadedResourcePoolNames();
    if (type != K8sEventType::EVENT_TYPE_DELETE
        && poolInfo == nullptr && poolNames->find(poolID) == poolNames->end()) {
        YRLOG_INFO("receive deployment(uid={})(name={}) modify event({}), need to delete deployment",
                   deployment->GetMetadata()->GetUid(), name, fmt::underlying(type));
        auto poolInfoTmp = std::make_shared<PodPoolInfo>();
        poolInfoTmp->id = poolID;
        business_->DeletePodPool(poolInfoTmp);
        return;
    }
    if (poolInfo == nullptr) {
        return;
    }
    auto labels = deployment->GetSpec()->GetRTemplate()->GetMetadata()->GetLabels();
    // compare if deployment is belong to podpool
    if (labels.find(poolInfo->group) == labels.end() || labels[poolInfo->group] != poolInfo->group) {
        return;
    }
    auto status = poolInfo->status;
    auto readyCount = poolInfo->readyCount;
    auto msg = poolInfo->msg;
    if (type == K8sEventType::EVENT_TYPE_DELETE) {
        if (status == static_cast<int32_t>(PoolState::DELETED)) {
            return;
        }
        status = static_cast<int32_t>(PoolState::FAILED);
        msg = "deployment is deleted";
    } else {
        readyCount = deployment->GetStatus()->GetAvailableReplicas();
        if (!poolInfo->scalable && readyCount >= poolInfo->size) {
            status = static_cast<int32_t>(PoolState::RUNNING);
            msg = "Running";
        }
    }
    if (status != poolInfo->status || readyCount != poolInfo->readyCount) {
        YRLOG_INFO("receive deployment update event, pool({}) status from ({}) to ({})", poolInfo->id, poolInfo->status,
                   status);
        poolInfo->status = status;
        poolInfo->readyCount = readyCount;
        poolInfo->msg = msg;
        business_->PersistencePoolInfo(poolInfo->id);
    }
    DoScaleUpPodByPoolID(poolInfo->id, true, "", litebus::AID());
}

void ScalerActor::MigrateNodeInstanceWithTaints(const std::shared_ptr<V1Node> node)
{
    YRLOG_INFO("MigrateNodeInstanceWithTaints node({})", node->GetMetadata()->GetName());
    auto type = CheckNodeFault(node);
    if (type == MigrateType::NOT_MIGRATE || member_->isUpgrading) {
        return;
    }
    ProcessNodeFault(node->GetMetadata()->GetName(), type);
}

void ScalerActor::UpdateLocalStatusResponse(const litebus::AID &from, std::string && /* name */, std::string &&msg)
{
    auto response = std::make_shared<messages::UpdateLocalStatusResponse>();
    if (!response->ParseFromString(msg)) {
        YRLOG_ERROR("received update local status response from {}, invalid msg {}", std::string(from), msg);
        return;
    }
    auto status = response->status();
    if (!response->healthy()) {
        YRLOG_ERROR("update local status ({}) request to ({}) failed", status, std::string(from));
        (void)DoSendUpdateLocalRequest(from, status);
        return;
    }
}

void ScalerActor::DoSendUpdateLocalRequest(const litebus::AID &from, const uint32_t &status)
{
    auto request = std::make_shared<messages::UpdateLocalStatusRequest>();
    request->set_status(status);
    (void)Send(from, "UpdateLocalStatus", request->SerializeAsString());
}

void ScalerActor::SendUpdateLocalStatusRequest(const std::shared_ptr<V1Node> node, const bool &hasEvictedTaint)
{
    member_->hasEvictedTaint = hasEvictedTaint;
    std::string nodeId = node->GetMetadata()->GetName();
    auto localSchedAid = litebus::AID(nodeId + LOCAL_SCHED_FUNC_AGENT_MGR_ACTOR_NAME_POSTFIX);
    auto ip = ParseNodeInternalIP(node);
    if (ip.empty()) {
        YRLOG_ERROR("SendLocal parse node({}) ip error", nodeId);
    }
    if (!member_->localSchedPort.empty()) {
        localSchedAid.SetUrl(ip + ":" + member_->localSchedPort);
    }
    auto status = hasEvictedTaint ? LocalStatusCode::EVICTED : LocalStatusCode::SUCCESS;
    (void)DoSendUpdateLocalRequest(localSchedAid, status);
}

MigrateType ScalerActor::CheckNodeFault(const std::shared_ptr<V1Node> node)
{
    if (!node->GetSpec()->TaintsIsSet() || node->GetSpec()->GetTaints().empty()) {
        // delete taint: true -> false
        litebus::Async(GetAID(), &ScalerActor::SendUpdateLocalStatusRequest, node, false);
        return MigrateType::NOT_MIGRATE;
    }
    auto taints = node->GetSpec()->GetTaints();
    bool notInTolerations = false;
    bool hasWorkerTaint = false;
    bool hasProxyTaint = false;
    bool hasEvictedTaint = false;
    for (auto iter = taints.begin(); iter != taints.end(); ++iter) {
        auto taintKey = (*iter)->GetKey();
        if (taintKey == member_->workerTaintKey) {
            hasWorkerTaint = true;
        } else if (taintKey == member_->proxyTaintKey) {
            hasProxyTaint = true;
        }
        if (!member_->evictedTaintKey.empty() && taintKey == member_->evictedTaintKey) {
            hasEvictedTaint = true;
            // add taint: false -> true
            litebus::Async(GetAID(), &ScalerActor::SendUpdateLocalStatusRequest, node, true);
        }
        if (member_->taintToleranceSet.find(taintKey) == member_->taintToleranceSet.end()) {
            YRLOG_INFO("node({}) has taint({}), not in tolerance list", node->GetMetadata()->GetName(), taintKey);
            notInTolerations = true;
        }
    }
    if (hasEvictedTaint) {
        return MigrateType::MIGRATE_EVICTED;
    }
    if (hasProxyTaint) {
        return MigrateType::MIGRATE_ALL;
    }
    if (hasWorkerTaint) {
        return MigrateType::MIGRATE_GRACEFUL;
    }
    if (!member_->migrateEnable) {
        return MigrateType::NOT_MIGRATE;
    }
    return notInTolerations ? MigrateType::MIGRATE_UNIQUE : MigrateType::NOT_MIGRATE;
}

void ScalerActor::DoEvictedPodInstanceMigrate(const std::string &podName)
{
    auto instanceID = member_->podName2InstanceId[podName];
    YRLOG_INFO("DoEvictedPodInstanceMigrate pod({}) instanceId({})", podName, instanceID);
    DoPodInstanceGracefulMigrate(podName, instanceID);
}

void ScalerActor::DoPodInstanceMigrate(const std::string &podName, const MigrateType type)
{
    if (type == MigrateType::MIGRATE_EVICTED) {
        DoEvictedPodInstanceMigrate(podName);
        return;
    }
    if (member_->podName2InstanceId.find(podName) == member_->podName2InstanceId.end()) {
        return;
    }
    auto instanceID = member_->podName2InstanceId[podName];
    if (type == MigrateType::MIGRATE_UNIQUE &&
        member_->needMigratingInstanceIds.find(instanceID) == member_->needMigratingInstanceIds.end()) {
        return;
    }
    if (member_->migratingPodSet.find(podName) != member_->migratingPodSet.end()) {
        return;
    }
    // if pod was deleted before init, domain scheduler will wait agent until timeout
    if (CheckPodHasSpecialize(podName)) {
        (void)member_->migratingPodSet.emplace(podName);
        if (type == MigrateType::MIGRATE_GRACEFUL &&
            member_->systemUpgradeParam.handlers.evictAgentHandler != nullptr) {
            YRLOG_INFO("process node fault, start to graceful delete pod({}) instance({})", podName, instanceID);
            DoPodInstanceGracefulMigrate(podName, instanceID);
            return;
        }
        YRLOG_INFO("process node fault, start to delete pod({}) instance({})", podName, instanceID);
        // when node is fault, still need to shut down instances gracefully
        (void)DoDeletePod(podName, false);
    }
}

void ScalerActor::DoPodInstanceGracefulMigrate(const std::string &podName, const std::string &instanceID)
{
    auto iter = member_->podNameMap.find(podName);
    if (iter == member_->podNameMap.end()) {
        YRLOG_WARN("failed to migrate pod({}) instance({}), pod not found", podName, instanceID);
        return;
    }
    auto pod = iter->second;
    auto labels = pod->GetMetadata()->GetLabels();
    auto hostIp = pod->GetStatus()->GetHostIP();
    auto proxyID = member_->nodes[hostIp]->GetMetadata()->GetName();
    YRLOG_DEBUG("migrate pod({}) instance({}) proxyId({})", podName, instanceID, proxyID);
    auto evictReq = std::make_shared<messages::EvictAgentRequest>();
    evictReq->set_timeoutsec(UINT32_MAX);
    evictReq->set_agentid(podName);
    member_->systemUpgradeParam.handlers.evictAgentHandler(proxyID, evictReq)
        .OnComplete([aid(GetAID()), pod, instanceID](litebus::Future<Status> f) {
            litebus::Async(aid, &ScalerActor::RemoveEvictAnnotation, pod, instanceID);
        });
}

void ScalerActor::RemoveEvictAnnotation(const std::shared_ptr<V1Pod> pod, const std::string &instanceID)
{
    auto annotations = pod->GetMetadata()->GetAnnotations();
    auto iter = annotations.find(EVICT_ANNOTATION_KEY);
    if (annotations.empty() || iter == annotations.end()) {
        YRLOG_WARN("pod ({}) has no annotation ({})", pod->GetMetadata()->GetName(), EVICT_ANNOTATION_KEY);
        return;
    }
    auto newPod = std::make_shared<V1Pod>();
    (void)newPod->FromJson(pod->ToJson());
    newPod->GetMetadata()->GetAnnotations().erase(EVICT_ANNOTATION_KEY);
    PatchPod(pod->GetMetadata()->GetName(), pod->GetMetadata()->GetRNamespace(), pod, newPod,
             instanceID.empty() ? pod->GetMetadata()->GetName() : instanceID);
}

bool ScalerActor::CheckPodHasSpecialize(const std::string &podName)
{
    auto iter = member_->podNameMap.find(podName);
    if (iter == member_->podNameMap.end()) {
        return false;
    }
    auto pod = iter->second;
    if (!pod->StatusIsSet() || !pod->GetStatus()->PodIPIsSet() || pod->GetStatus()->GetPodIP().empty()) {
        return false;
    }
    auto labels = pod->GetMetadata()->GetLabels();
    auto it = labels.find(APP_LABEL_KEY);
    if (it == labels.end()) {
        return false;
    }
    auto label = it->second;
    return  it->second == TruncateIllegalLabel(pod->GetMetadata()->GetName());
}

void ScalerActor::ProcessNodeFault(const std::string &nodeName, const MigrateType type)
{
    if (member_->nodeIdPodNameMap.find(nodeName) == member_->nodeIdPodNameMap.end()) {
        return;
    }
    if (type == MigrateType::MIGRATE_UNIQUE && member_->needMigratingInstanceIds.empty()) {
        return;
    }
    auto podNames = member_->nodeIdPodNameMap[nodeName];
    for (auto name : podNames) {
        DoPodInstanceMigrate(name, type);
    }
}

std::shared_ptr<V1Deployment> ScalerActor::GenerateDeploymentByOrigin(const ResourcePool &pool,
                                                                      std::shared_ptr<V1Deployment> originalDeployment)
{
    originalDeployment->GetSpec()->SetReplicas(pool.isScalable ? 0 : pool.poolSize);
    auto containers = originalDeployment->GetSpec()->GetRTemplate()->GetSpec()->GetContainers();
    for (auto &container : containers) {
        if (container->GetName() == RUNTIME_MANAGER_CONTAINER_NAME) {
            UpdateContainerResource(container, pool);

            // for runtime manager to update metrics
            SetResourceEnvs(container, pool);
        }
    }

    return originalDeployment;
}

void ScalerActor::UpdateContainerResource(const std::shared_ptr<V1Container> &container, const ResourcePool &pool)
{
    // if user's config is not in a standard format, set the resources to a default value
    auto resources = std::make_shared<V1ResourceRequirements>();

    std::map<std::string, std::string> requests;
    std::map<std::string, std::string> limits;

    for (const auto &it : pool.requestResources) {
        requests[it.first] = it.second;
        // if no limit specified, specify them by request
        if (pool.limitResources.find(it.first) == pool.limitResources.end()) {
            limits[it.first] = it.second;
        }
    }
    for (const auto &it : pool.limitResources) {
        limits[it.first] = it.second;
    }

    resources->SetRequests(requests);
    resources->SetLimits(limits);

    container->SetResources(std::move(resources));
}

messages::CreateAgentResponse ScalerActor::GenCreateAgentResponse(const std::string &requestID, const int32_t code,
                                                                  const std::string &message)
{
    messages::CreateAgentResponse target;
    target.set_requestid(requestID);
    target.set_code(code);
    target.set_message(message);
    return target;
}

messages::CreateAgentResponse ScalerActor::InitCreateAgentResponse(int32_t code, const std::string &message,
                                                                   const messages::CreateAgentRequest &source)
{
    auto target = GenCreateAgentResponse(source.instanceinfo().requestid(), code, message);
    for (auto &iter : source.instanceinfo().createoptions()) {
        (*target.mutable_updatedcreateoptions())[iter.first] = iter.second;
    }
    return target;
}

void ScalerActor::SendCreateAgentFailedResponse(const litebus::AID &from, int32_t code, const std::string &message,
                                                const messages::CreateAgentRequest &source)
{
    (void)member_->creatingRequests.erase(source.instanceinfo().requestid());
    (void)Send(from, "CreateAgentResponse", InitCreateAgentResponse(code, message, source).SerializeAsString());
}

std::map<std::string, std::string> ScalerActor::CheckAndBuildResourcesMap(const ::resources::InstanceInfo &instanceInfo)
{
    // resourcesMap is built for k8s
    std::map<std::string, std::string> resourcesMap;

    // find CPU/Mem and make sure they exist
    const auto &cpuIter = instanceInfo.resources().resources().find(SCHEDULER_CPU_RESOURCE);
    const auto &memoryIter = instanceInfo.resources().resources().find(SCHEDULER_MEMORY_RESOURCE);
    if (cpuIter == instanceInfo.resources().resources().end() ||
        memoryIter == instanceInfo.resources().resources().end()) {
        YRLOG_WARN("{}|cpu or memory is none in instance({})", instanceInfo.requestid(), instanceInfo.instanceid());
        return resourcesMap;
    }

    // there are some differences between the representations of cpu/mem in YuanRong and k8s, including the key and unit
    resourcesMap[CPU_RESOURCE] = std::to_string(static_cast<int32_t>(cpuIter->second.scalar().value())) + "m";
    resourcesMap[MEMORY_RESOURCE] = std::to_string(static_cast<int32_t>(memoryIter->second.scalar().value())) + "Mi";
    const auto &storageIter = instanceInfo.resources().resources().find(EPHEMERAL_STORAGE_RESOURCE);
    if (storageIter != instanceInfo.resources().resources().end()) {
        resourcesMap[EPHEMERAL_STORAGE_RESOURCE] =
            std::to_string(static_cast<int32_t>(storageIter->second.scalar().value())) + "Mi";
    }

    // if other kind of scalar resources exist, add them
    for (const auto &it : instanceInfo.resources().resources()) {
        if (it.first == SCHEDULER_CPU_RESOURCE || it.first == SCHEDULER_MEMORY_RESOURCE ||
            it.first == EPHEMERAL_STORAGE_RESOURCE || it.second.type() != resources::Value::SCALAR) {
            continue;
        }
        resourcesMap[it.first] = std::to_string(int32_t(it.second.scalar().value()));
    }
    return resourcesMap;
}

void ScalerActor::CreateAgent(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    ASSERT_IF_NULL(business_);
    business_->CreateAgent(from, std::move(name), std::move(msg));
}

void ScalerActor::DeletePod(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    ASSERT_IF_NULL(business_);
    business_->DeletePod(from, std::move(name), std::move(msg));
}

void ScalerActor::DeletePodByName(const std::string &podName)
{
    ASSERT_IF_NULL(business_);
    business_->DeletePod(podName);
}

void ScalerActor::UpdateNodeTaint(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    ASSERT_IF_NULL(business_);
    business_->UpdateNodeTaint(from, std::move(name), std::move(msg));
}

ResourcePool ScalerActor::BuildResourcePool(const ::resources::InstanceInfo &instanceInfo,
                                            const std::map<std::string, std::string> &resourcesMap)
{
    ResourcePool pool;
    pool.name = GeneratePoolNameByResources(instanceInfo.instanceid(), resourcesMap);
    pool.poolSize = 1;
    pool.requestResources = resourcesMap;
    pool.limitResources = resourcesMap;
    // avoid not enough ephemeral-storage
    if (pool.requestResources.find(EPHEMERAL_STORAGE_RESOURCE) != pool.requestResources.end()) {
        pool.requestResources[EPHEMERAL_STORAGE_RESOURCE] = DEFAULT_EPHEMERAL_STORAGE_VALUE;
    }
    for (const auto &ns : instanceInfo.scheduleoption().nodeselector()) {
        pool.nodeSelector[ns.first] = ns.second;
    }

    if (instanceInfo.has_scheduleoption() && !instanceInfo.scheduleoption().resourceselector().empty()) {
        nlohmann::json map;
        for (auto &it : instanceInfo.scheduleoption().resourceselector()) {
            map[it.first] = it.second;
        }
        pool.initLabels = map.dump();
    }

    return pool;
}

void ScalerActor::HandleSharedInstancePut(const resource_view::InstanceInfo &instanceInfo)
{
    auto podName = instanceInfo.functionagentid();
    if (podName.empty()) {
        return;
    }
    YRLOG_DEBUG("{}|instance({}) is not monopoly schedule policy, but has delegate pod labels",
                instanceInfo.requestid(), instanceInfo.instanceid());
    if (auto iter = member_->instanceId2PodName.find(instanceInfo.instanceid());
        iter == member_->instanceId2PodName.end()) {
        YRLOG_DEBUG("{}|shared instance({}) is saved, pod name({})", instanceInfo.requestid(),
                    instanceInfo.instanceid(), podName);
        member_->instanceId2PodName[instanceInfo.instanceid()] = podName;
    }
    ASSERT_IF_NULL(business_);
    business_->UpdatePodLabels(instanceInfo);
}

void ScalerActor::HandleInstancePut(const InstanceInfo &instanceInfo)
{
    if (instanceInfo.scheduleoption().schedpolicyname() != MONOPOLY_SCHEDULE) {
        HandleSharedInstancePut(instanceInfo);
        return;
    }

    // recover map(instanceID -> podName) after scalar restart
    if (auto iter = member_->instanceId2PodName.find(instanceInfo.instanceid());
        iter == member_->instanceId2PodName.end()) {
        auto podName = instanceInfo.functionagentid();
        if (podName != "") {
            YRLOG_DEBUG("{}|instance({}) is saved, pod name({})", instanceInfo.requestid(), instanceInfo.instanceid(),
                        podName);
            member_->instanceId2PodName[instanceInfo.instanceid()] = podName;
            member_->podName2InstanceId[podName] = instanceInfo.instanceid();
        }
    }
    CheckInstanceNeedMigrate(instanceInfo);

    if (IsNeedUpdateLabel(instanceInfo.instancestatus().code())) {
        ASSERT_IF_NULL(business_);
        business_->UpdatePodLabels(instanceInfo);
        return;
    } else if (instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::FATAL) ||
               instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::SCHEDULE_FAILED) ||
               instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::EVICTED)) {
        // only status FATAL(non-recoverable) need to delete pod, FAILED(recoverable) don't delete pod
        if (deletingPodSet_.find(instanceInfo.instanceid()) != deletingPodSet_.end()) {
            YRLOG_DEBUG("{}|pod for instance({}) is deleting, skipped", instanceInfo.requestid(),
                        instanceInfo.instanceid());
            return;
        }

        if (instanceInfo.functionproxyid() == INSTANCE_MANAGER_OWNER) {
            // don't delete pod if instance belongs to instance-manager, wait for node fault taint events to delete pod
            YRLOG_INFO("{}|instance({}) belongs to instance-manager, skip delete pod", instanceInfo.requestid(),
                       instanceInfo.instanceid());
            return;
        }

        (void)deletingPodSet_.emplace(instanceInfo.instanceid());

        std::string podName;
        if (auto iter = member_->instanceId2PodName.find(instanceInfo.instanceid());
            iter != member_->instanceId2PodName.end()) {
            podName = iter->second;
        }
        ASSERT_IF_NULL(business_);
        business_->DeletePod(podName);
        return;
    } else {
        YRLOG_DEBUG("{}|instance({}) status({}) don't need to update agent labels or delete pod",
                    instanceInfo.requestid(), instanceInfo.instanceid(), instanceInfo.instancestatus().code());
    }
}

void ScalerActor::CheckInstanceNeedMigrate(const ::resources::InstanceInfo &instanceInfo)
{
    if (!member_->migrateEnable && member_->migrateResourcePrefix.empty()) {
        return;
    }
    if (instanceInfo.instancestatus().code() != static_cast<int32_t>(InstanceState::CREATING) ||
        instanceInfo.instancestatus().code() != static_cast<int32_t>(InstanceState::RUNNING)) {
        return;
    }
    auto resource = instanceInfo.resources().resources();
    for (auto item : resource) {
        if (item.first.find(member_->migrateResourcePrefix) == item.first.npos) {
            continue;
        }
        YRLOG_INFO("instance({}) resource has migrate prefix", instanceInfo.instanceid());
        (void)member_->needMigratingInstanceIds.emplace(instanceInfo.instanceid());
    }
}

bool ScalerActor::IsNeedUpdateLabel(int32_t code)
{
    return code == static_cast<int32_t>(InstanceState::CREATING) ||
           code == static_cast<int32_t>(InstanceState::RUNNING) ||
           code == static_cast<int32_t>(InstanceState::EXITING) ||
           code == static_cast<int32_t>(InstanceState::SUB_HEALTH);
}

void ScalerActor::HandleSharedInstanceDelete(const resource_view::InstanceInfo &instanceInfo)
{
    (void)member_->instanceId2PodName.erase(instanceInfo.instanceid());
    if (instanceInfo.createoptions().find(DELEGATE_POD_LABELS) == instanceInfo.createoptions().end()) {
        return;
    }
    YRLOG_DEBUG("{}|instance({}) is not monopoly schedule policy, but has delegate pod labels",
                instanceInfo.requestid(), instanceInfo.instanceid());
    ASSERT_IF_NULL(business_);
    business_->UpdatePodLabels(instanceInfo, true);
}

void ScalerActor::HandleInstanceDelete(const InstanceInfo &instanceInfo)
{
    if (instanceInfo.scheduleoption().schedpolicyname() != MONOPOLY_SCHEDULE) {
        HandleSharedInstanceDelete(instanceInfo);
        return;
    }

    std::string podName = "";
    if (auto iter = member_->instanceId2PodName.find(instanceInfo.instanceid());
        iter != member_->instanceId2PodName.end()) {
        podName = iter->second;
    }

    (void)member_->instanceId2PodName.erase(instanceInfo.instanceid());
    (void)member_->needMigratingInstanceIds.erase(instanceInfo.instanceid());
    if (deletingPodSet_.find(instanceInfo.instanceid()) != deletingPodSet_.end()) {
        YRLOG_WARN("{}|pod for instance({}) is deleting, skipped", instanceInfo.requestid(), instanceInfo.instanceid());
        (void)deletingPodSet_.erase(instanceInfo.instanceid());
        return;
    }
    ASSERT_IF_NULL(business_);
    business_->DeletePod(podName);
}

void ScalerActor::UpdatePodLabelsWithoutInstance(const std::shared_ptr<V1Pod> &pod)
{
    auto annotations = pod->GetMetadata()->GetAnnotations();
    if (annotations.empty()) {
        return;
    }
    // it is important when patch labeling a single pod concurrently.
    // when execute json.diff, there will only be add/remove single key, not replace the whole map
    pod->GetMetadata()->GetAnnotations()[ANNOTATION_PLACEHOLDER] = ANNOTATION_PLACEHOLDER;
    auto newPod = std::make_shared<V1Pod>();
    auto originalPodJson = pod->ToJson();
    auto podName = pod->GetMetadata()->GetName();
    (void)newPod->FromJson(originalPodJson);
    bool needUpdate = false;
    for (auto &annotation : annotations) {
        auto key = annotation.first;
        if (!litebus::strings::StartsWithPrefix(key, ANNOTATION_KEY_PREFIX)) {
            continue;
        }
        auto instanceID = key.substr(ANNOTATION_KEY_PREFIX.size());
        if (member_->instanceId2PodName.find(instanceID) == member_->instanceId2PodName.end()) {
            YRLOG_INFO("instance({}) is not found, start to remove labels for pod({})", instanceID,
                       pod->GetMetadata()->GetName());
            RemovePodLabelsByAnnotations(instanceID, newPod);
            needUpdate = true;
        }
    }
    if (!needUpdate) {
        return;
    }
    PatchPod(podName, pod->GetMetadata()->GetRNamespace(), pod, newPod, "");
}

void ScalerActor::UpdatePodLabels(const litebus::Future<std::shared_ptr<V1Pod>> &pod, const InstanceInfo &instanceInfo,
                                  bool isInstanceDelete)
{
    std::string podName = instanceInfo.functionagentid();

    if (pod.IsError()) {
        YRLOG_ERROR("{}|failed to find pod for instance({}), failed to update labels", instanceInfo.requestid(),
                    instanceInfo.instanceid());
        TriggerUpdateLabel(instanceInfo.instanceid(), nullptr);
        return;
    }
    auto latestPod = pod.Get();
    // when cannot receive pod event, just save it to cache from query
    if (member_->podNameMap.find(podName) == member_->podNameMap.end()) {
        member_->podNameMap[podName] = latestPod;
    }
    if (!latestPod->GetMetadata()->AnnotationsIsSet()) {
        latestPod->GetMetadata()->SetAnnotations({});
    }
    // it is important when patch labeling a single pod concurrently.
    // when execute json.diff, there will only be add/remove single key, not replace the whole map
    latestPod->GetMetadata()->GetAnnotations()[ANNOTATION_PLACEHOLDER] = ANNOTATION_PLACEHOLDER;
    // deep copy
    auto newPod = std::make_shared<V1Pod>();
    (void)newPod->FromJson(latestPod->ToJson());
    if (!newPod->GetMetadata()->LabelsIsSet()) {
        newPod->GetMetadata()->SetLabels({});
    }
    if (instanceInfo.scheduleoption().schedpolicyname() == MONOPOLY_SCHEDULE) {
        AddLabels(instanceInfo, newPod->GetMetadata()->GetLabels());
    } else {
        HandlePodLabelsWithAnnotation(instanceInfo, newPod, isInstanceDelete);
    }
    newPod->GetMetadata()->GetAnnotations()[EVICT_ANNOTATION_KEY] = "false";
    PatchPod(latestPod->GetMetadata()->GetName(), latestPod->GetMetadata()->GetRNamespace(), latestPod, newPod,
             instanceInfo.instanceid());
}

void ScalerActor::PatchPod(const std::string &podName, const std::string &nameSpace,
                           const std::shared_ptr<V1Pod> &source, const std::shared_ptr<V1Pod> &target,
                           const std::string &instanceID)
{
    PatchPodWithOwnerReference(source, target);
    nlohmann::json podDiffPatch = nlohmann::json::diff(source->ToJson(), target->ToJson());
    std::shared_ptr<kube_client::model::Object> body = std::make_shared<kube_client::model::Object>();
    bool isSet = body->FromJson(podDiffPatch);
    if (!isSet) {
        YRLOG_WARN("failed to set object json from patch({}) for pod({}), skip", podDiffPatch.dump(), podName);
        litebus::Async(GetAID(), &ScalerActor::TriggerUpdateLabel, instanceID, nullptr);
        return;
    }
    YRLOG_INFO("patch labels for pod({})", podName);
    (void)member_->kubeClient->PatchNamespacedPod(podName, nameSpace, body)
        .OnComplete([podName, instanceID, body, aid(GetAID())](const litebus::Future<std::shared_ptr<V1Pod>> &pod) {
            if (pod.IsError()) {
                YRLOG_ERROR("failed to patch labels for pod({}), body({}) error code({})", podName,
                            body->ToJson().dump(), pod.GetErrorCode());
                litebus::Async(aid, &ScalerActor::TriggerUpdateLabel, instanceID, nullptr);
                return;
            }
            // current request is finished, trigger next request with the latest pod, to avoid pod events delay (update
            // labels from an old pod)
            litebus::Async(aid, &ScalerActor::TriggerUpdateLabel, instanceID, pod.Get());
            YRLOG_INFO("success to patch labels for pod({})", podName);
        });
}

void ScalerActor::PatchPodWithOwnerReference(const std::shared_ptr<V1Pod> &source, const std::shared_ptr<V1Pod> &target)
{
    if (member_->kubeClient->GetOwnerReference() == nullptr ||
        member_->kubeClient->GetOwnerReference()->GetUid().empty()) {
        return;
    }
    if (source->GetMetadata()->GetLabels().find(APP_LABEL_KEY) == source->GetMetadata()->GetLabels().end() ||
        target->GetMetadata()->GetLabels().find(APP_LABEL_KEY) == target->GetMetadata()->GetLabels().end()) {
        return;
    }
    std::string sourceApp = source->GetMetadata()->GetLabels()[APP_LABEL_KEY];
    std::string targetApp = target->GetMetadata()->GetLabels()[APP_LABEL_KEY];
    if (sourceApp != targetApp) {
        YRLOG_DEBUG("patch pod change pod({}) owner reference", target->GetMetadata()->GetName());
        target->GetMetadata()->SetOwnerReferences({ member_->kubeClient->GetOwnerReference() });
    }
}

litebus::Future<std::shared_ptr<V1Pod>> ScalerActor::ReadNamespacedPod(const std::string &rNamespace,
                                                                       const std::string &podName)
{
    return member_->kubeClient->ReadNamespacedPod(member_->k8sNamespace, podName);
}

void ScalerActor::UpdatePodLabelsWithLatestPod(const InstanceInfo &instanceInfo, bool isInstanceDelete,
                                               const std::shared_ptr<V1Pod> &pod)
{
    if (pod != nullptr) {
        UpdatePodLabels(pod, instanceInfo, isInstanceDelete);
        return;
    }
    std::string podName = instanceInfo.functionagentid();
    (void)ReadNamespacedPod(member_->k8sNamespace, podName)
        .OnComplete(litebus::Defer(GetAID(), &ScalerActor::UpdatePodLabels, std::placeholders::_1, instanceInfo,
                                   isInstanceDelete));
}

Status ScalerActor::Register()
{
    auto watchOpt = WatchOption{ true, true };
    YRLOG_INFO("register watch {}", POD_POOL_PREFIX);
    auto observer = [aid(GetAID())](const std::vector<WatchEvent> &events, bool) {
        litebus::Async(aid, &ScalerActor::UpdatePodPoolEvent, events);
        return true;
    };
    auto systemUpgradeSyncer =
        [aid(GetAID())](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<SyncResult> {
        return litebus::Async(aid, &ScalerActor::SystemUpgradeSyncer, getResponse);
    };
    auto instanceInfoSyncer =
        [aid(GetAID())](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<SyncResult> {
        return litebus::Async(aid, &ScalerActor::InstanceInfoSyncer, getResponse);
    };
    auto podPoolSyncer =
        [aid(GetAID())](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<SyncResult> {
        return litebus::Async(aid, &ScalerActor::PodPoolSyncer, getResponse);
    };
    (void)member_->metaStorageAccessor->RegisterObserver(POD_POOL_PREFIX, watchOpt, observer, podPoolSyncer);

    if (member_->systemUpgradeParam.isEnabled && member_->systemUpgradeParam.systemUpgradeWatcher != nullptr) {
        YRLOG_INFO("register watch upgrade switch, key:{}", member_->systemUpgradeParam.systemUpgradeKey);
        (void)member_->systemUpgradeParam.systemUpgradeWatcher->RegisterObserver(
            member_->systemUpgradeParam.systemUpgradeKey, watchOpt,
            [aid(GetAID())](const std::vector<WatchEvent> &events, bool) {
                litebus::Async(aid, &ScalerActor::UpdateSystemUpgradeSwitchEvent, events);
                return true;
            }, systemUpgradeSyncer);
    }
    YRLOG_INFO("register watch {} ", INSTANCE_PATH_PREFIX);
    (void)member_->metaStorageAccessor->RegisterObserver(
        INSTANCE_PATH_PREFIX, watchOpt, [aid(GetAID())](const std::vector<WatchEvent> &events, bool) {
            litebus::Async(aid, &ScalerActor::UpdateInstanceEvent, events);
            return true;
        }, instanceInfoSyncer);
    return Status::OK();
}

void ScalerActor::UpdateInstanceEvent(const std::vector<WatchEvent> &events)
{
    for (const auto &event : events) {
        auto eventKey = TrimKeyPrefix(event.kv.key(), member_->metaStorageAccessor->GetMetaClient()->GetTablePrefix());
        auto instanceID = GetInstanceID(eventKey);
        switch (event.eventType) {
            case EVENT_TYPE_PUT: {
                resource_view::InstanceInfo instanceInfo;
                if (!TransToInstanceInfoFromJson(instanceInfo, event.kv.value())) {
                    YRLOG_ERROR("failed to trans to instanceInfo from json string, instanceID: {}", instanceID);
                    break;
                }
                // for low-reliability instance, skip it to improve performance
                if (instanceInfo.lowreliability()) {
                    break;
                }
                YRLOG_INFO(
                    "{}|receive instance put event, instanceID({}), runtime({}), proxy({}), agent({}), parent({}), "
                    "group({}),status({}), reason({})",
                    instanceInfo.requestid(), instanceID, instanceInfo.runtimeid(), instanceInfo.functionproxyid(),
                    instanceInfo.functionagentid(), instanceInfo.parentid(), instanceInfo.groupid(),
                    instanceInfo.instancestatus().code(), instanceInfo.instancestatus().msg());
                HandleInstancePut(instanceInfo);
                break;
            }
            case EVENT_TYPE_DELETE: {
                resource_view::InstanceInfo instanceInfo;
                if (!TransToInstanceInfoFromJson(instanceInfo, event.prevKv.value())) {
                    YRLOG_ERROR("failed to trans to instanceInfo from json string, instanceID: {}", instanceID);
                    break;
                }
                if (instanceInfo.lowreliability()) {
                    break;
                }
                YRLOG_INFO("{}|receive instance delete event, instanceID({})", instanceInfo.requestid(), instanceID);
                HandleInstanceDelete(instanceInfo);

                break;
            }
            default: {
                YRLOG_WARN("unknown event type {}", fmt::underlying(event.eventType));
            }
        }
    }
}

void ScalerActor::UpdatePodPoolEvent(const std::vector<WatchEvent> &events)
{
    for (const auto &event : events) {
        auto key = TrimKeyPrefix(event.kv.key(), member_->metaStorageAccessor->GetMetaClient()->GetTablePrefix());
        auto poolID = GetPodPoolID(key);
        switch (event.eventType) {
            case EVENT_TYPE_PUT: {
                YRLOG_INFO("receive pod pool info put event, poolID({})", poolID);
                auto podPool = member_->poolManager->GetOrNewPool(poolID);
                PoolManager::TransToPoolInfoFromJson(event.kv.value(), podPool);
                auto status = podPool->status;
                ASSERT_IF_NULL(business_);
                if (status == static_cast<int32_t>(PoolState::NEW)) {
                    business_->CreatePodPool(podPool);
                } else if (status == static_cast<int32_t>(PoolState::UPDATE)) {
                    business_->UpdatePodPool(podPool);
                } else if (status == static_cast<int32_t>(PoolState::DELETED)) {
                    business_->DeletePodPool(podPool);
                }
                break;
            }
            case EVENT_TYPE_DELETE: {
                YRLOG_INFO("receive pod pool info delete event, poolID({})", poolID);
                member_->poolManager->DeletePodPoolInfo(poolID);
                break;
            }
            default: {
                YRLOG_WARN("unknown event type {}", fmt::underlying(event.eventType));
            }
        }
    }
}

void ScalerActor::UpdateSystemUpgradeSwitchEvent(const std::vector<WatchEvent> &events)
{
    for (const auto &event : events) {
        auto key = TrimKeyPrefix(event.kv.key(), member_->metaStorageAccessor->GetMetaClient()->GetTablePrefix());
        switch (event.eventType) {
            case EVENT_TYPE_PUT: {
                YRLOG_INFO("receive system upgrade switch put event, key({})", key);
                auto upgradeInfo = TransToUpgradeInfoFromJson(event.kv.value());
                if (upgradeInfo == nullptr) {
                    YRLOG_ERROR("failed to trans system upgrade switch info, key: {}", key);
                    break;
                }
                HandleSystemUpgrade(upgradeInfo);
                if (member_->systemUpgradeParam.handlers.systemUpgradeHandler != nullptr) {
                    member_->systemUpgradeParam.handlers.systemUpgradeHandler(member_->isUpgrading);
                }
                break;
            }
            case EVENT_TYPE_DELETE: {
                YRLOG_INFO("receive system upgrade switch delete event, key({})", key);
                auto upgradeInfo = TransToUpgradeInfoFromJson(event.prevKv.value());
                if (upgradeInfo == nullptr) {
                    YRLOG_ERROR("failed to trans system upgrade switch info, prevKv: {}", key);
                    break;
                }

                if (upgradeInfo->azID != member_->systemUpgradeParam.azID && upgradeInfo->azID != DEFAULT_AZ_ID) {
                    // if az id is default, handle all events
                    YRLOG_DEBUG("ignore az({}) system upgrade switch events, we are in az({})", upgradeInfo->azID,
                                member_->systemUpgradeParam.azID);
                    return;
                }

                YRLOG_INFO("key is deleted, system status change to status(2) updated");
                member_->isUpgrading = false;
                OnSystemUpgradeFinished();

                if (member_->systemUpgradeParam.handlers.systemUpgradeHandler != nullptr) {
                    member_->systemUpgradeParam.handlers.systemUpgradeHandler(member_->isUpgrading);
                }
                break;
            }
            default: {
                YRLOG_WARN("unknown system upgrade switch event type {}", fmt::underlying(event.eventType));
            }
        }
    }
}

void ScalerActor::HandleSystemUpgrade(std::shared_ptr<UpgradeInfo> upgradeInfo)
{
    if (upgradeInfo->azID != member_->systemUpgradeParam.azID && upgradeInfo->azID != DEFAULT_AZ_ID) {
        // if az id is default, handle all events
        YRLOG_DEBUG("ignore az({}) system upgrade switch events, we are in az({})", upgradeInfo->azID,
                    member_->systemUpgradeParam.azID);
        return;
    }

    if (upgradeInfo->status == SYSTEM_STATUS_UPGRADING) {
        YRLOG_INFO("system status change to status({}) updating", upgradeInfo->status);
        member_->isUpgrading = true;
    } else if (upgradeInfo->status == SYSTEM_STATUS_UPGRADED) {
        YRLOG_INFO("system status change to status({}) updated", upgradeInfo->status);
        member_->isUpgrading = false;
        OnSystemUpgradeFinished();
    } else {
        YRLOG_WARN("try to update system upgrade status with unknown status({})", upgradeInfo->status);
    }
}

void ScalerActor::OnSystemUpgradeFinished()
{
    for (const auto &node : member_->nodes) {
        if (!node.second->SpecIsSet() || !node.second->GetSpec()->TaintsIsSet()) {
            YRLOG_DEBUG("node({})'s taint is not set, ignore", node.first);
            continue;
        }
        auto taints = node.second->GetSpec()->GetTaints();
        for (const auto &taint : taints) {
            if (!taint->KeyIsSet()) {
                continue;
            }

            if (taint->GetKey() == member_->proxyTaintKey &&
                member_->systemUpgradeParam.handlers.localSchedFaultHandler != nullptr) {
                if (member_->systemUpgradeParam.handlers.systemUpgradeHandler != nullptr) {
                    member_->systemUpgradeParam.handlers.systemUpgradeHandler(member_->isUpgrading);
                }
                member_->systemUpgradeParam.handlers.localSchedFaultHandler(node.second->GetMetadata()->GetName());
            }

            if (taint->GetKey() == member_->proxyTaintKey || taint->GetKey() == member_->workerTaintKey ||
                (!member_->evictedTaintKey.empty() && taint->GetKey() == member_->evictedTaintKey)) {
                YRLOG_INFO("node({}) still is unhealthy after upgrading, taint({})", node.first, taint->GetKey());
                MigrateNodeInstanceWithTaints(node.second);
            }
        }
    }
}

void ScalerActor::UpdateLeaderInfo(const explorer::LeaderInfo &leaderInfo)
{
    litebus::AID masterAID(SCALER_ACTOR, leaderInfo.address);

    auto newStatus = leader::GetStatus(GetAID(), masterAID, curStatus_);
    if (businesses_.find(newStatus) == businesses_.end()) {
        YRLOG_WARN("new status({}) business don't exist", newStatus);
        return;
    }
    business_ = businesses_[newStatus];
    ASSERT_IF_NULL(business_);
    business_->OnChange();
    curStatus_ = newStatus;
}

void ScalerActor::CreatePodPool(const std::shared_ptr<PodPoolInfo> &podPool)
{
    auto resourcePool = member_->poolManager->GenerateResourcePool(podPool);
    if (!podPool->horizontalPodAutoscalerSpec.empty()) {
        YRLOG_DEBUG("Start to create HPA for pool({})", resourcePool.name);
        auto scaler = GenerateV2HorizontalPodAutoscaler(resourcePool, member_->k8sNamespace);
        if (scaler == nullptr) {
            YRLOG_DEBUG("{}|Generate HPA failed.", podPool->id);
            podPool->status = static_cast<int32_t>(PoolState::FAILED);
            podPool->msg = "invalid HPA spec, create HPA failed";
            litebus::Async(GetAID(), &ScalerActor::DoPersistencePoolInfo, podPool->id);
            return;
        }

        member_->kubeClient->ListNamespacedHorizontalPodAutoscaler(member_->k8sNamespace, false)
        .OnComplete([podPool, resourcePool, aid(GetAID())]
            (const litebus::Future<std::shared_ptr<V2HorizontalPodAutoscalerList>> &list) {
            if (list.IsError()) {
                YRLOG_ERROR("{}|CreatePodPool List HPA failed.", podPool->id);
                podPool->status = static_cast<int32_t>(PoolState::FAILED);
                podPool->msg = "list HPA failed";
                litebus::Async(aid, &ScalerActor::DoPersistencePoolInfo, podPool->id);
                return;
            }
        })
        .Then(litebus::Defer(GetAID(), &ScalerActor::CreateHPA, podPool, resourcePool, std::placeholders::_1))
        .OnComplete([podPool, resourcePool, aid(GetAID())]
            (const litebus::Future<std::shared_ptr<V2HorizontalPodAutoscaler>> &scaler) {
            if (scaler.IsError()) {
                YRLOG_ERROR("{}|Create HPA failed.", podPool->id);
                podPool->status = static_cast<int32_t>(PoolState::FAILED);
                podPool->msg = "create HPA failed";
                litebus::Async(aid, &ScalerActor::DoPersistencePoolInfo, podPool->id);
                return;
            }
            litebus::Async(aid, &ScalerActor::CreatePodPoolWithResourcePool, podPool, resourcePool);
        });
        return;
    }

    return CreatePodPoolWithResourcePool(podPool, resourcePool);
}

litebus::Future<std::shared_ptr<V2HorizontalPodAutoscaler>> ScalerActor::CreateHPA(
    const std::shared_ptr<PodPoolInfo> &podPool,
    const ResourcePool &pool,
    const std::shared_ptr<V2HorizontalPodAutoscalerList> &list)
{
    auto promise = litebus::Promise<std::shared_ptr<V2HorizontalPodAutoscaler>>();
    for (auto item : list->GetItems()) {
        if (item->GetMetadata()->GetName() == pool.horizontalPodAutoscaler->GetMetadata()->GetName()) {
            YRLOG_ERROR("failed to create hpa({}), HPA exists", item->GetMetadata()->GetName());
            promise.SetFailed(static_cast<int32_t>(StatusCode::FAILED));
            return promise.GetFuture();
        }
    }
    YRLOG_INFO("start to create hpa {}", pool.horizontalPodAutoscaler->GetMetadata()->GetName());
    return member_->kubeClient->CreateNamespacedHorizontalPodAutoscaler(
        member_->k8sNamespace, pool.horizontalPodAutoscaler);
}

void ScalerActor::CreatePodPoolWithResourcePool(
    const std::shared_ptr<PodPoolInfo> &podPool, const ResourcePool &resourcePool)
{
    YRLOG_DEBUG("Start to create pod pool({})", podPool->id);
    if (member_->templateDeployment == nullptr) {
        YRLOG_ERROR("failed to create pod pool({}), function-agent template not found", podPool->id);
        podPool->status = static_cast<int32_t>(PoolState::FAILED);
        podPool->msg = "function-agent template not found";
        DoPersistencePoolInfo(podPool->id);
        return;
    }
    auto deployment = GenerateDeploymentByTemplate(resourcePool, member_->templateDeployment);
    auto name = deployment->GetMetadata()->GetName();
    if (member_->poolManager->GetDeployment(name) != nullptr) {
        YRLOG_ERROR("failed to create pod pool({}), deployment exists", podPool->id);
        podPool->status = static_cast<int32_t>(PoolState::FAILED);
        podPool->msg = "pool already exists";
        DoPersistencePoolInfo(podPool->id);
        return;
    }
    podPool->status = static_cast<int32_t>(PoolState::CREATING);
    podPool->msg = "Creating";
    podPool->readyCount = 0;
    (void)member_->kubeClient->CreateNamespacedDeployment(member_->k8sNamespace, deployment)
        .OnComplete(litebus::Defer(GetAID(), &ScalerActor::OnCreatePodPoolComplete, std::placeholders::_1, podPool));
}

void ScalerActor::UpdatePodPool(const std::shared_ptr<PodPoolInfo> &podPool)
{
    auto resourcePool = member_->poolManager->GenerateResourcePool(podPool);
    if (!podPool->horizontalPodAutoscalerSpec.empty()) {
        YRLOG_DEBUG("Start to update HPA for pool({})", resourcePool.name);
        auto scaler = GenerateV2HorizontalPodAutoscaler(resourcePool, member_->k8sNamespace);
        if (scaler == nullptr) {
            YRLOG_ERROR("{}|Generate HPA failed.", podPool->id);
            podPool->status = static_cast<int32_t>(PoolState::FAILED);
            podPool->msg = "invalid HPA spec, update HPA failed";
            litebus::Async(GetAID(), &ScalerActor::DoPersistencePoolInfo, podPool->id);
            return;
        }

        member_->kubeClient->ListNamespacedHorizontalPodAutoscaler(member_->k8sNamespace, false)
        .OnComplete([podPool, resourcePool, aid(GetAID())]
            (const litebus::Future<std::shared_ptr<V2HorizontalPodAutoscalerList>> &list) {
            if (list.IsError()) {
                YRLOG_ERROR("{}|UpdatePodPool List HPA failed.", podPool->id);
                podPool->status = static_cast<int32_t>(PoolState::FAILED);
                podPool->msg = "list HPA failed";
                litebus::Async(aid, &ScalerActor::DoPersistencePoolInfo, podPool->id);
                return;
            }
        })
        .Then(litebus::Defer(GetAID(), &ScalerActor::UpdateHPA, podPool, resourcePool, std::placeholders::_1))
        .OnComplete([aid(GetAID()), resourcePool, podPool]
            (const litebus::Future<std::shared_ptr<V2HorizontalPodAutoscaler>> &scaler) {
            if (scaler.IsError()) {
                YRLOG_ERROR("{}|UpdatePodPool Update HPA failed.", podPool->id);
                podPool->status = static_cast<int32_t>(PoolState::FAILED);
                podPool->msg = "update HPA failed";
                litebus::Async(aid, &ScalerActor::DoPersistencePoolInfo, podPool->id);
                return;
            }
            litebus::Async(aid, &ScalerActor::UpdatePodPoolWithResourcePool, podPool, resourcePool);
        });
        return;
    }
    return UpdatePodPoolWithResourcePool(podPool, resourcePool);
}

litebus::Future<std::shared_ptr<V2HorizontalPodAutoscaler>> ScalerActor::UpdateHPA(
    const std::shared_ptr<PodPoolInfo> &podPool,
    const ResourcePool &pool,
    const std::shared_ptr<V2HorizontalPodAutoscalerList> &list)
{
    for (auto item : list->GetItems()) {
        if (item->GetMetadata()->GetName() == pool.horizontalPodAutoscaler->GetMetadata()->GetName()) {
            YRLOG_INFO("start to modify hpa {}", item->GetMetadata()->GetName());
            auto oldItem = item->ToJson();
            item->SetSpec(pool.horizontalPodAutoscaler->GetSpec());
            auto newItem = item->ToJson();

            nlohmann::json diffPatch = nlohmann::json::diff(oldItem, newItem);
            std::shared_ptr<kube_client::model::Object> body = std::make_shared<kube_client::model::Object>();
            bool isSet = body->FromJson(diffPatch);
            if (!isSet) {
                YRLOG_WARN("failed to set object from json patch({}) for pool {}, skip",
                    diffPatch.dump(), item->GetMetadata()->GetName());
                auto promise = litebus::Promise<std::shared_ptr<V2HorizontalPodAutoscaler>>();
                promise.SetFailed(static_cast<int32_t>(StatusCode::FAILED));
                return promise.GetFuture();
            }
            return member_->kubeClient->PatchNamespacedHorizontalPodAutoscaler(item->GetMetadata()->GetName(),
                member_->k8sNamespace, body);
        }
    }
    YRLOG_WARN("update hpa({}), hpa not found, need to create", pool.horizontalPodAutoscaler->GetMetadata()->GetName());
    return member_->kubeClient->CreateNamespacedHorizontalPodAutoscaler(
        member_->k8sNamespace, pool.horizontalPodAutoscaler);
}

void ScalerActor::UpdatePodPoolWithResourcePool(
    const std::shared_ptr<PodPoolInfo> &podPool,
    const ResourcePool &resourcePool)
{
    YRLOG_DEBUG("Start to update pod pool({})", podPool->id);
    auto deployName = POOL_NAME_PREFIX + podPool->id;
    auto deployment = member_->poolManager->GetDeployment(deployName);
    if (deployment == nullptr) {
        YRLOG_WARN("update pod pool({}), pool not found", podPool->id);
        podPool->status = static_cast<int32_t>(PoolState::FAILED);
        podPool->readyCount = 0;
        podPool->msg = "pool not found";
        DoPersistencePoolInfo(podPool->id);
        return;
    }
    podPool->status = static_cast<int32_t>(PoolState::UPDATING);
    podPool->msg = "Updating";
    if (podPool->scalable) {
        // if pod is scalable, no need to patch deployment
        DoPersistencePoolInfo(podPool->id);
        ASSERT_IF_NULL(business_);
        business_->ScaleUpPodByPoolID(podPool->id, true, "", litebus::AID());
        return;
    }
    DoUpdatePodPool(podPool, deployment, resourcePool);
}

void ScalerActor::DoUpdatePodPool(const std::shared_ptr<PodPoolInfo> &podPool,
                                  const std::shared_ptr<V1Deployment> &deployment,
                                  const ResourcePool &resourcePool)
{
    auto oldDeploymentJson = deployment->ToJson();
    auto newDeployment = ScalerActor::GenerateDeploymentByOrigin(resourcePool, deployment);
    auto changedDeploymentJson = newDeployment->ToJson();
    nlohmann::json deploymentDiffPatch = nlohmann::json::diff(oldDeploymentJson, changedDeploymentJson);
    std::shared_ptr<kube_client::model::Object> body = std::make_shared<kube_client::model::Object>();
    bool isSet = body->FromJson(deploymentDiffPatch);
    if (!isSet) {
        YRLOG_WARN("failed to set object from json patch({}) for pool {}, skip", deploymentDiffPatch.dump(),
                   podPool->id);
        return;
    }
    (void)member_->kubeClient
        ->PatchNamespacedDeployment(newDeployment->GetMetadata()->GetName(),
                                    newDeployment->GetMetadata()->GetRNamespace(), body)
        .OnComplete(litebus::Defer(GetAID(), &ScalerActor::OnUpdatePodPoolComplete, std::placeholders::_1, podPool));
}

void ScalerActor::PersistencePoolInfo(const std::string &poolID)
{
    ASSERT_IF_NULL(business_);
    business_->PersistencePoolInfo(poolID);
}

void ScalerActor::DoPersistencePoolInfo(const std::string &poolID)
{
    member_->poolManager->PersistencePoolInfo(poolID);
}

void ScalerActor::DeletePodPool(const std::shared_ptr<PodPoolInfo> &podPool)
{
    auto deployName = POOL_NAME_PREFIX + podPool->id;
    auto deployment = member_->poolManager->GetDeployment(deployName);
    if (deployment == nullptr) {
        YRLOG_WARN("delete pod pool({}), pool not found", podPool->id);
    }
    (void)member_->kubeClient->DeleteNamespacedDeployment(deployName, member_->k8sNamespace)
        .OnComplete(litebus::Defer(GetAID(), &ScalerActor::OnDeletePodPoolComplete, std::placeholders::_1, deployName,
                                   podPool));
}

void ScalerActor::OnDeletePodPoolComplete(const litebus::Future<std::shared_ptr<V1Deployment>> &retDeployment,
                                          const std::string &name, const std::shared_ptr<PodPoolInfo> &podPool)
{
    if (!retDeployment.IsOK() && retDeployment.GetErrorCode() != litebus::http::ResponseCode::NOT_FOUND) {
        YRLOG_WARN("failed to delete pod pool {}, error code: {}", podPool->id, retDeployment.GetErrorCode());
        podPool->status = static_cast<int32_t>(PoolState::FAILED);
        podPool->msg = "Delete Deployment Failed, ErrorCode is " + std::to_string(retDeployment.GetErrorCode());
        DoPersistencePoolInfo(podPool->id);
        return;
    }
    member_->kubeClient->DeleteNamespacedHorizontalPodAutoscaler(name + "-hpa",
        member_->k8sNamespace, litebus::None(), litebus::None());
    member_->poolManager->DeleteDeployment(name);
    member_->poolManager->DeletePodPoolInfo(podPool->id, true);
}

void ScalerActor::AddVolumesAndMountsForSystemFunc(ResourcePool &pool)
{
    nlohmann::json volmuesJson = nlohmann::json::array();
    nlohmann::json volumeMountsJson = nlohmann::json::array();
    nlohmann::json initVolumeMountsJson = nlohmann::json::array();
    YRLOG_INFO("ETCD auth type is {}", member_->etcdAuthType);
    if (member_->etcdAuthType == "TLS") {
        nlohmann::json etcdVolume;
        etcdVolume["name"] = "etcd-client-certs";
        etcdVolume["secret"]["secretName"] = member_->etcdSecretName;
        etcdVolume["secret"]["defaultMode"] = DEFAULT_SECERT_MODE;
        volmuesJson.push_back(etcdVolume);
        nlohmann::json etcdVolumeMounts;
        etcdVolumeMounts["name"] = "etcd-client-certs";
        etcdVolumeMounts["mountPath"] = "/home/snuser/resource/etcd";
        volumeMountsJson.push_back(etcdVolumeMounts);
    }

    ParseVolumesFromJson(volmuesJson.dump(), pool.delegateVolumes);
    ParseVolumeMountsFromJson(volumeMountsJson.dump(), pool.delegateVolumeMounts);
    ParseVolumeMountsFromJson(initVolumeMountsJson.dump(), pool.delegateInitVolumeMounts);
}

void ScalerActor::ParseInstanceInfoToPool(ResourcePool &pool, const ::resources::InstanceInfo &instanceInfo)
{
    ParseDelegateDecryptEnv(instanceInfo, pool.delegateDecryptEnvs);
    ParseDelegateContainer(instanceInfo, pool.delegateContainer);
    ParseDelegateRuntimeManager(instanceInfo, pool.delegateRuntimeManager);
    ParseDelegateVolumes(instanceInfo, pool.delegateVolumes);
    ParseDelegateVolumeMounts(instanceInfo, "DELEGATE_VOLUME_MOUNTS", pool.delegateVolumeMounts);
    ParseDelegateVolumeMounts(instanceInfo, "DELEGATE_INIT_VOLUME_MOUNTS", pool.delegateInitVolumeMounts);
    ParseDelegateVolumeMounts(instanceInfo, "DELEGATE_AGENT_VOLUME_MOUNTS", pool.delegateAgentVolumeMounts);
    ParseDelegateHostAliases(instanceInfo, pool.delegateHostAliases);
    ParseDelegateSidecars(instanceInfo, pool.delegateSidecars);
    ParseDelegateInitContainers(instanceInfo, pool.delegateInitContainers);
    ParseDelegateTolerations(instanceInfo, pool.delegateTolerations);
    ParseDelegateInitEnv(instanceInfo, pool.delegateInitEnvs);
    pool.isSystemFunc = IsSystemFunction(instanceInfo);
    if (pool.isSystemFunc) {
        AddVolumesAndMountsForSystemFunc(pool);
    }
    pool.isNeedBindCPU = IsNeedBindCPU(pool.limitResources);

    pool.affinity = ParseAffinity(GetAffinityInfo(instanceInfo));
    ParseAffinityFromCreateOpts(instanceInfo, pool.affinity);
    ParseNodeAffinity(instanceInfo, pool.affinity);
    pool.isAggregationNodeAffinity = IsAggregationMergePolicy(instanceInfo);
    pool.delegateSecCompProfile = ParseSeccompProfile(instanceInfo);
    pool.terminationGracePeriodSeconds = instanceInfo.gracefulshutdowntime();
}

void ScalerActor::DoCreateAgent(ResourcePool &pool, const ::resources::InstanceInfo &instanceInfo,
                                const std::shared_ptr<messages::CreateAgentRequest> &createAgentRequest,
                                const litebus::AID &from)
{
    ParseInstanceInfoToPool(pool, instanceInfo);
    YRLOG_INFO("{}|received a create agent pool({}) from {}.", instanceInfo.requestid(), instanceInfo.instanceid(),
               std::string(from));
    auto spec = GeneratePodTemplateSpec(pool, member_->templateDeployment);
    (void)CreatePod(
        GeneratePodName(pool.name, instanceInfo.function(), pool.delegateContainer.IsSome(), pool.isSystemFunc),
        spec,
        instanceInfo.instanceid(), GetPodLabelsForCreate(instanceInfo), ParseDelegateAnnotations(instanceInfo))
        .OnComplete(litebus::Defer(GetAID(), &ScalerActor::OnCreatePodComplete, std::placeholders::_1,
                                   createAgentRequest, from, pool.delegateContainer.IsSome()));
}

void ScalerActor::DoScaleUpPodByPoolID(const std::string &poolID, bool isReserved, const std::string &requestID,
                                       const litebus::AID &from)
{
    ASSERT_IF_NULL(business_);
    business_->ScaleUpPodByPoolID(poolID, isReserved, requestID, from);
}

void ScalerActor::ScaleUpPodByPoolID(const std::string &poolID, bool isReserved, const std::string &requestID,
                                     const litebus::AID &from)
{
    auto pod = member_->poolManager->TryScaleUpPod(poolID, isReserved);
    if (pod.IsNone()) {
        if (!requestID.empty()) {
            (void)member_->creatingRequests.erase(requestID);
            (void)Send(from, "CreateAgentResponse",
                       GenCreateAgentResponse(requestID, static_cast<int32_t>(StatusCode::GS_START_CREATE_POD_FAILED),
                                              "failed to scale up pod")
                           .SerializeAsString());
        }
        return;
    }
    auto targetPod = pod.Get();
    YRLOG_INFO("{}|start to scale up a new pod({})", requestID, targetPod->GetMetadata()->GetName());
    member_->kubeClient->CreateNamespacedPod(member_->templateDeployment->GetMetadata()->GetRNamespace(), targetPod)
        .OnComplete(litebus::Defer(GetAID(), &ScalerActor::OnScaleUpPodComplete, std::placeholders::_1, targetPod,
                                   poolID, requestID, from));
}

litebus::Future<Status> ScalerActor::DoUpdateNodeTaint(const std::shared_ptr<messages::UpdateNodeTaintRequest> &req,
                                                       const litebus::AID &from)
{
    if (auto iter = updateNodeMap_.find(req->ip()); iter != updateNodeMap_.end()) {
        YRLOG_DEBUG("DoUpdateNodeTaint wait: {}", req->ip());
        return iter->second.GetFuture().Then(litebus::Defer(GetAID(), &ScalerActor::DoUpdateNodeTaint, req, from));
    }

    litebus::Promise<bool> promise;
    updateNodeMap_[req->ip()] = promise;
    YRLOG_DEBUG("DoUpdateNodeTaint: {}", req->ip());

    auto taintKey = req->key();
    if (req->key().find(FUNCTION_PROXY_TAINT_KEY) != std::string::npos) {
        taintKey = member_->proxyTaintKey;
    }
    (void)HandleNodeTaint(req->ip(), taintKey, !req->healthy()).OnComplete(
        litebus::Defer(GetAID(), &ScalerActor::OnUpdateNodeTaintsComplete, std::placeholders::_1, req, from));
    return Status::OK();
}

void ScalerActor::OnUpdateNodeTaintsComplete(const litebus::Future<std::shared_ptr<V1Node>> &retNode,
                                             const std::shared_ptr<messages::UpdateNodeTaintRequest> &req,
                                             const litebus::AID &from)
{
    if (auto iter = updateNodeMap_.find(req->ip()); iter != updateNodeMap_.end()) {
        iter->second.SetValue(true);
        updateNodeMap_.erase(req->ip());
    }

    messages::UpdateNodeTaintResponse rsp;
    rsp.set_requestid(req->requestid());
    if (retNode.IsError()) {
        YRLOG_WARN("{}|failed to update node({}) taints({}) healthy({})", req->requestid(), req->ip(), req->key(),
                   req->healthy());
        rsp.set_code(static_cast<int32_t>(StatusCode::FAILED));
        rsp.set_message("error code : " + std::to_string(retNode.GetErrorCode()));
    } else {
        YRLOG_INFO("{}|update node taints successfully", req->requestid());
        rsp.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    }
    (void)Send(from, "UpdateNodeTaintsResponse", rsp.SerializeAsString());
}

litebus::Future<std::shared_ptr<V1Node>> ScalerActor::HandleNodeTaint(const std::string &ip, const std::string &key,
                                                                      const bool isAdd)
{
    return member_->kubeClient->ListNode()
        .Then(litebus::Defer(GetAID(), &ScalerActor::DoSyncNode, std::placeholders::_1))
        .Then(litebus::Defer(GetAID(), &ScalerActor::DoHandleNodeTaint, ip, key, isAdd, std::placeholders::_1));
}

litebus::Future<std::shared_ptr<V1Node>> ScalerActor::DoHandleNodeTaint(
    const std::string &ip, const std::string &key, const bool isAdd,
    const std::unordered_map<std::string, std::shared_ptr<V1Node>> &nodes)
{
    auto iter = nodes.find(ip);
    if (iter == nodes.end()) {
        return std::make_shared<V1Node>();
    }
    YRLOG_INFO("try to update taint key({}) to ip({}) isAdd({})", key, ip, isAdd);
    auto node = iter->second;
    if (key == member_->workerTaintKey && CheckNodeLabels(node)) {
        // if ds-worker-taint key, and node has some labels, then skip taint node
        YRLOG_INFO("skip taint key({}) for node({})", key, ip);
        return node;
    }
    auto newNode = std::make_shared<V1Node>();
    (void)newNode->FromJson(node->ToJson());
    if (!newNode->GetSpec()->TaintsIsSet()) {
        newNode->GetSpec()->SetTaints({});
    }
    auto taints = newNode->GetSpec()->GetTaints();
    bool hasKey = false;
    for (auto nodeIter = taints.begin(); nodeIter != taints.end(); ++nodeIter) {
        if ((*nodeIter)->GetKey() == key) {
            YRLOG_INFO("HandleNodeTaint member node({}) has taint key({})", node->GetMetadata()->GetName(), key);
            (void)taints.erase(nodeIter);
            hasKey = true;
            break;
        }
    }
    if (isAdd && hasKey) {
        // add taint key and already exist
        YRLOG_INFO("HandleNodeTaint add taint key and already exist node({})", node->GetMetadata()->GetName());
        return std::make_shared<V1Node>();
    }
    if (!isAdd && !hasKey) {
        // del taint key and key not exist
        YRLOG_INFO("HandleNodeTaint del taint key and key not exist node({})", node->GetMetadata()->GetName());
        return std::make_shared<V1Node>();
    }
    if (isAdd) {
        YRLOG_INFO("HandleNodeTaint add taint key node({})", node->GetMetadata()->GetName());
        auto nodeTaint = std::make_shared<V1Taint>();
        nodeTaint->SetKey(key);
        nodeTaint->SetValue("true");
        nodeTaint->SetEffect(TAINT_EFFECT_PREFER_NO_SCHEDULE);
        taints.push_back(nodeTaint);
    }
    newNode->GetSpec()->SetTaints(taints);
    nlohmann::json nodeDiffPatch = nlohmann::json::diff(node->ToJson(), newNode->ToJson());
    std::shared_ptr<functionsystem::kube_client::model::Object> body =
        std::make_shared<functionsystem::kube_client::model::Object>();
    (void)body->FromJson(nodeDiffPatch);
    return member_->kubeClient->PatchNode(node->GetMetadata()->GetName(), body);
}

bool ScalerActor::CheckNodeLabels(const std::shared_ptr<V1Node> &node)
{
    if (!node->GetMetadata()->LabelsIsSet() || member_->workerTaintExcludeLabels.size() == 0) {
        return false;
    }
    auto nodeLabels = node->GetMetadata()->GetLabels();
    for (auto it = member_->workerTaintExcludeLabels.begin(); it != member_->workerTaintExcludeLabels.end(); ++it) {
        if (nodeLabels.find(it->first) != nodeLabels.end() && nodeLabels[it->first] == it->second) {
            return true;
        }
    }
    return false;
}

void ScalerActor::ParseParams(const functionsystem::functionmaster::Flags &flags)
{
    ParseFaultRecoveryParams(flags);
    ParseEtcdParams(flags);
    ParseK8sClientParams(flags);
    std::string prefix = flags.GetSelfTaintPrefix();
    prefix = litebus::strings::Trim(prefix);
    if (!prefix.empty()) {
        member_->workerTaintKey = prefix + DS_WORKER_TAINT_KEY;
        member_->proxyTaintKey = prefix + FUNCTION_PROXY_TAINT_KEY;
    }
}

void ScalerActor::ParseK8sClientParams(const functionsystem::functionmaster::Flags &flags)
{
    member_->kubeClient->SetK8sClientRetryTime(flags.GetKubeClientRetryTimes());
    member_->kubeClient->SetK8sClientCycMs(flags.GetKubeClientRetryCycleMs());
}

void ScalerActor::ParseEtcdParams(const functionsystem::functionmaster::Flags &flags)
{
    member_->etcdAuthType = flags.GetETCDAuthType();
    member_->etcdSecretName = flags.GetEtcdSecretName();
}

void ScalerActor::ParseFaultRecoveryParams(const functionsystem::functionmaster::Flags &flags)
{
    member_->evictedTaintKey = flags.GetEvictedTaintKey();
    member_->localSchedPort = flags.GetLocalSchedulerPort();

    auto migrateEnable = flags.GetMigrateEnable();
    member_->migrateEnable = migrateEnable;
    if (migrateEnable) {
        member_->migrateResourcePrefix = flags.GetMigratePrefix();
    }
    auto taintToleranceList = flags.GetTaintToleranceList();
    if (migrateEnable && !taintToleranceList.empty()) {
        auto faults = litebus::strings::Split(taintToleranceList, SEMICOLON_SIGN_SPLIT);
        for (auto item : faults) {
            member_->taintToleranceSet.emplace(item);
        }
    }
    auto nodeLabels = flags.GetWorkerTaintExcludeLabels();
    if (nodeLabels.empty()) {
        return;
    }
    auto labelArr = litebus::strings::Split(nodeLabels, SEMICOLON_SIGN_SPLIT);
    for (auto item : labelArr) {
        if (item.empty()) {
            return;
        }
        auto label = litebus::strings::Split(item, EQUAL_SIGN_SPLIT);
        if (label.size() == LABEL_ITEM_LENGTH) {
            member_->workerTaintExcludeLabels.emplace(label[0], label[1]);
        }
    }
}

void ScalerActor::MasterBusiness::OnChange()
{
    if (!member_->isSynced) {
        YRLOG_WARN("resource synchronization is not complete");
        return;
    }
    (void)CreatePodPools(member_->poolManager->GetLocalPodPools());
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    actor->SyncPodAndInstance().OnComplete(
        [aid(actor->GetAID())]() { litebus::Async(aid, &ScalerActor::SyncKubeResourceForChange); });
}

void ScalerActor::MasterBusiness::UpdatePodLabels(const InstanceInfo &instanceInfo, bool isInstanceDelete)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    YRLOG_INFO("{}|try to update agent labels for instance({})", instanceInfo.requestid(), instanceInfo.instanceid());
    actor->AddUpdateLabelReq(instanceInfo, isInstanceDelete);
}

void ScalerActor::MasterBusiness::DeletePod(const std::string &podName)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    actor->DoDeletePod(podName);
}

Status ScalerActor::MasterBusiness::CreateResourcePools(const std::shared_ptr<std::vector<ResourcePool>> &pools)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    // before creating deployments, check if the deployment already exists.
    (void)actor->ListNamespacedDeployment(member_->k8sNamespace)
        .OnComplete(litebus::Defer(actor->GetAID(), &ScalerActor::CreateDeployment, std::placeholders::_1, pools));

    return Status(StatusCode::SUCCESS);
}

Status ScalerActor::MasterBusiness::CreatePodPools(
    const std::unordered_map<std::string, std::shared_ptr<PodPoolInfo>> &pools)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);

    if (pools.empty()) {
        return Status::OK();
    }
    for (const auto& podPool : pools) {
        actor->CreatePodPool(podPool.second);
    }
    return Status::OK();
}

void ScalerActor::MasterBusiness::DeletePod(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    auto deletePodRequest = std::make_shared<messages::DeletePodRequest>();
    if (!deletePodRequest->ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse request for DeletePod.");
        return;
    }
    YRLOG_INFO("{}|receive delete pod request, agent: {}, msg: {}", deletePodRequest->requestid(),
               deletePodRequest->functionagentid(), deletePodRequest->message());
    actor->DoDeletePod(deletePodRequest->functionagentid())
        .OnComplete(litebus::Defer(actor->GetAID(), &ScalerActor::OnDeletePodComplete, std::placeholders::_1, from,
                                   deletePodRequest->requestid()));
}

void ScalerActor::OnDeletePodComplete(const litebus::Future<Status> &status, const litebus::AID &from,
                                      const std::string &requestID)
{
    auto deletePodResponse = std::make_shared<messages::DeletePodResponse>();
    deletePodResponse->set_requestid(requestID);
    if (status.IsError()) {
        deletePodResponse->set_code(status.GetErrorCode());
    } else {
        deletePodResponse->set_code(0);
    }
    Send(from, "DeletePodResponse", deletePodResponse->SerializeAsString());
}

void ScalerActor::MasterBusiness::CreateAgent(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    auto createAgentRequest = std::make_shared<messages::CreateAgentRequest>();
    if (!createAgentRequest->ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse request for CreateAgent.");
        actor->SendCreateAgentFailedResponse(from, static_cast<int32_t>(StatusCode::GS_START_CREATE_POD_FAILED),
                                             "resources or requestID is illegal.", *createAgentRequest);
        return;
    }

    // 1.if instance or request id is illegal, don't create deployment and response.
    const auto &instanceInfo = createAgentRequest->instanceinfo();
    if (!instanceInfo.has_resources() || instanceInfo.requestid().empty()) {
        YRLOG_ERROR("instance's resources or requestID is illegal.");
        actor->SendCreateAgentFailedResponse(from, static_cast<int32_t>(StatusCode::GS_START_CREATE_POD_FAILED),
                                             "resources or requestID is illegal.", *createAgentRequest);
        return;
    }

    const std::string &requestID = instanceInfo.requestid();
    if (member_->creatingRequests.find(requestID) != member_->creatingRequests.end()) {
        // if request is repeat, only reply a response.
        YRLOG_ERROR("{}|repeated to create agent.", requestID);
        return;
    }

    member_->creatingRequests[requestID] = *createAgentRequest;
    if (actor->SyncTemplateDeployment().IsError()) {
        YRLOG_ERROR("{}|invalid template deployment in instance({}).", requestID, instanceInfo.instanceid());
        actor->SendCreateAgentFailedResponse(from, static_cast<int32_t>(StatusCode::GS_START_CREATE_POD_FAILED),
                                             "invalid template deployment.", *createAgentRequest);
        return;
    }

    if (auto it = instanceInfo.createoptions().find(AFFINITY_POOL_ID);
            it != instanceInfo.createoptions().end() && !it->second.empty()) {
        // auto scale pod by poolID
        actor->ScaleUpPodByPoolID(it->second, false, requestID, from);
        return;
    }

    // find CPU/Mem and make sure they exist
    auto resourcesMap = actor->CheckAndBuildResourcesMap(instanceInfo);
    if (resourcesMap.size() == 0) {
        actor->SendCreateAgentFailedResponse(from, static_cast<int32_t>(StatusCode::GS_START_CREATE_POD_FAILED),
                                             "resources is illegal.", *createAgentRequest);
        return;
    }

    ResourcePool pool = actor->BuildResourcePool(instanceInfo, resourcesMap);

    actor->DoCreateAgent(pool, instanceInfo, createAgentRequest, from);
}

void ScalerActor::MasterBusiness::UpdateNodeTaint(const litebus::AID &from, std::string && /* name */,
                                                  std::string &&msg)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    auto req = std::make_shared<messages::UpdateNodeTaintRequest>();
    if (!req->ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse request for UpdateNodeTaint.");
        return;
    }
    YRLOG_INFO("{}|receive update node({}) taint request, key({}) healthy({})", req->requestid(), req->ip(), req->key(),
               req->healthy());
    (void)actor->DoUpdateNodeTaint(req, from);
}

void ScalerActor::MasterBusiness::CreatePodPool(const std::shared_ptr<PodPoolInfo> &podPool)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    actor->CreatePodPool(podPool);
}

void ScalerActor::MasterBusiness::UpdatePodPool(const std::shared_ptr<PodPoolInfo> &podPool)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    actor->UpdatePodPool(podPool);
}

void ScalerActor::MasterBusiness::PersistencePoolInfo(const std::string &poolID)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    actor->DoPersistencePoolInfo(poolID);
}

void ScalerActor::MasterBusiness::DeletePodPool(const std::shared_ptr<PodPoolInfo> &podPool)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    actor->DeletePodPool(podPool);
}

void ScalerActor::MasterBusiness::DeletePodNotBindInstance(const std::shared_ptr<V1Pod> &pod)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    actor->DeletePodNotBindInstance(pod);
}

void ScalerActor::MasterBusiness::UpdatePodLabelsWithoutInstance(const std::shared_ptr<V1Pod> &pod)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    actor->UpdatePodLabelsWithoutInstance(pod);
}

void ScalerActor::MasterBusiness::MigratePodInstanceWithTaints(const std::string &hostIP, const std::string &podName)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    actor->MigratePodInstanceWithTaints(hostIP, podName);
}

void ScalerActor::MasterBusiness::MigrateNodeInstanceWithTaints(const std::shared_ptr<V1Node> node)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    actor->MigrateNodeInstanceWithTaints(node);
}

void ScalerActor::MasterBusiness::CheckPodStatus()
{
    YRLOG_DEBUG("Start to check pod status");
    for (auto it = member_->pendingPods.begin(); it != member_->pendingPods.end();) {
        // If podNameMap does not contain current pod info, erase it from pendingPods
        if (member_->podNameMap.find(it->first) == member_->podNameMap.end()) {
            it = member_->pendingPods.erase(it);
            continue;
        }
        // If current pod is not pending anymore, erase it from pendingPods
        auto pod = member_->podNameMap.find(it->first)->second;
        if (!IsPendingPod(pod)) {
            it = member_->pendingPods.erase(it);
            continue;
        }
        int threshold = DEFAULT_POD_PENDING_DURATION_ALARM_THRESHOLD;
        auto annotations = pod->GetMetadata()->GetAnnotations();
        try {
            if (auto annoIt = annotations.find(PENDING_THRESHOLD);
                annoIt != annotations.end() && std::stoi(annoIt->second)) {
                threshold = std::stoi(annoIt->second);
            }
        } catch (const std::exception &e) {
            YRLOG_WARN("Failed to parse pod pending duration alarm threshold {}, error: {}",
                       annotations.find(PENDING_THRESHOLD)->second, e.what());
        }
        if (IsTimeoutPendingPod(pod, it->second, threshold)) {
            auto tp = std::chrono::system_clock::to_time_t(it->second);
            std::stringstream ss;
            ss << std::put_time(std::localtime(&tp), "%Y-%m-%dT%H:%M:%S");
            std::string cause = "which has been pending since " + ss.str();
            YRLOG_WARN("Function master sends {} pending alarm, {}", pod->GetMetadata()->GetName(), cause);
            metrics::MetricsAdapter::GetInstance().SendPodAlarm(pod->GetMetadata()->GetName(), cause);
        }
        ++it;
    }
}

void ScalerActor::MasterBusiness::HandleAbnormalPod(const std::shared_ptr<V1Pod> pod)
{
    auto podName = pod->GetMetadata()->GetName();
    if (IsPendingPod(pod)) {
        if (member_->pendingPods.find(podName) == member_->pendingPods.end()) {
            member_->pendingPods.emplace(podName, std::chrono::system_clock::now());
        }
        return;
    } else {
        member_->pendingPods.erase(podName);
    }
    // Failed/Unknown pod will trigger alarm
    if (IsAbnormalPod(pod)) {
        auto cause = "whose status is changed to " + pod->GetStatus()->GetPhase();
        YRLOG_WARN("Function master sends {} pending alarm, {}", podName, cause);
        metrics::MetricsAdapter::GetInstance().SendPodAlarm(podName, cause);
    }
}

void ScalerActor::MasterBusiness::ScaleUpPodByPoolID(const std::string &poolID, bool isReserved,
                                                     const std::string &requestID, const litebus::AID &from)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    actor->ScaleUpPodByPoolID(poolID, isReserved, requestID, from);
}

void ScalerActor::SlaveBusiness::OnChange()
{
}

void ScalerActor::SlaveBusiness::UpdatePodLabels(const InstanceInfo &, bool)
{
}

void ScalerActor::SlaveBusiness::DeletePod(const std::string &)
{
}

Status ScalerActor::SlaveBusiness::CreateResourcePools(const std::shared_ptr<std::vector<ResourcePool>> &)
{
    return Status::OK();
}

Status ScalerActor::SlaveBusiness::CreatePodPools(
    const std::unordered_map<std::string, std::shared_ptr<PodPoolInfo>> &pools)
{
    return Status::OK();
}

void ScalerActor::SlaveBusiness::CreateAgent(const litebus::AID &from, std::string &&name, std::string &&msg)
{
}

void ScalerActor::SlaveBusiness::DeletePod(const litebus::AID &from, std::string &&name, std::string &&msg)
{
}

void ScalerActor::SlaveBusiness::UpdateNodeTaint(const litebus::AID & /* from */, std::string && /* name */,
                                                 std::string && /* msg */)
{
}

void ScalerActor::SlaveBusiness::DeletePodNotBindInstance(const std::shared_ptr<V1Pod> &pod)
{
}

void ScalerActor::SlaveBusiness::UpdatePodLabelsWithoutInstance(const std::shared_ptr<V1Pod> &pod)
{
}

void ScalerActor::SlaveBusiness::MigratePodInstanceWithTaints(const std::string &hostIP, const std::string &podName)
{
}

void ScalerActor::SlaveBusiness::MigrateNodeInstanceWithTaints(const std::shared_ptr<V1Node> node)
{
}

void ScalerActor::SlaveBusiness::CreatePodPool(const std::shared_ptr<PodPoolInfo> &podPool)
{
}

void ScalerActor::SlaveBusiness::UpdatePodPool(const std::shared_ptr<PodPoolInfo> &podPool)
{
}

void ScalerActor::SlaveBusiness::PersistencePoolInfo(const std::string &poolID)
{
}

void ScalerActor::SlaveBusiness::DeletePodPool(const std::shared_ptr<PodPoolInfo> &podPool)
{
}

void ScalerActor::SlaveBusiness::ScaleUpPodByPoolID(const std::string &poolID, bool isReserved,
                                                    const std::string &requestID, const litebus::AID &from)
{
}

void ScalerActor::SlaveBusiness::CheckPodStatus()
{
}

void ScalerActor::SlaveBusiness::HandleAbnormalPod(const std::shared_ptr<V1Pod> pod)
{
}

// only for test
bool ScalerActor::GetIsUpgrading()
{
    YRLOG_DEBUG("system upgrading is {}", member_->isUpgrading);
    return member_->isUpgrading;
}

// only for test
void ScalerActor::SetIsUpgrading(bool isUpgrading)
{
    YRLOG_DEBUG("set system upgrading to {}", isUpgrading);
    member_->isUpgrading = isUpgrading;
}

void ScalerActor::AddUpdateLabelReq(const InstanceInfo &instanceInfo, bool isInstanceDelete)
{
    YRLOG_INFO("add update label request for instance({})", instanceInfo.instanceid());
    updateLabelMap_[instanceInfo.instanceid()].waitingReq =
        std::make_shared<std::pair<resource_view::InstanceInfo, bool>>(instanceInfo, isInstanceDelete);
    if (updateLabelMap_[instanceInfo.instanceid()].patchingReq == nullptr) {
        YRLOG_INFO("there is no running update label request for instance({}), trigger", instanceInfo.instanceid());
        // no request is running, trigger now
        TriggerUpdateLabel(instanceInfo.instanceid(), nullptr);
    }
}

void ScalerActor::TriggerUpdateLabel(std::string instanceID, const std::shared_ptr<V1Pod> &pod)
{
    // parameter instanceID needs to be copied, because instanceInfo will be deleted, after patchingReq = nullptr
    // last request is finished, clear patchingReq
    updateLabelMap_[instanceID].patchingReq = nullptr;

    // if waitingReq == nullptr, all requests are finished, break loop
    if (updateLabelMap_[instanceID].waitingReq == nullptr) {
        (void)updateLabelMap_.erase(instanceID);
        return;
    }

    // waitingReq -> patchingReq, trigger next request
    YRLOG_INFO("trigger update label for instance({})", instanceID);
    updateLabelMap_[instanceID].patchingReq = updateLabelMap_[instanceID].waitingReq;
    updateLabelMap_[instanceID].waitingReq = nullptr;
    UpdatePodLabelsWithLatestPod(updateLabelMap_[instanceID].patchingReq->first,
                                 updateLabelMap_[instanceID].patchingReq->second, pod);
}

std::shared_ptr<V2HorizontalPodAutoscaler> ScalerActor::GenerateV2HorizontalPodAutoscaler(
    const ResourcePool &pool, const std::string &rNamespace)
{
    if (pool.horizontalPodAutoscaler == nullptr) {
        return nullptr;
    }

    pool.horizontalPodAutoscaler->SetApiVersion("autoscaling/v2beta2");
    pool.horizontalPodAutoscaler->SetKind("HorizontalPodAutoscaler");

    auto metaData = std::make_shared<V1ObjectMeta>();
    metaData->SetName(PoolDeploymentName(pool.name) + "-hpa");
    metaData->SetRNamespace(rNamespace);
    pool.horizontalPodAutoscaler->SetMetadata(metaData);

    auto ref = std::make_shared<V2CrossVersionObjectReference>();
    ref->SetApiVersion("apps/v1");
    ref->SetKind("Deployment");
    ref->SetName(PoolDeploymentName(pool.name));
    pool.horizontalPodAutoscaler->GetSpec()->SetScaleTargetRef(ref);

    return pool.horizontalPodAutoscaler;
}

litebus::Future<SyncResult> ScalerActor::SystemUpgradeSyncer(const std::shared_ptr<GetResponse> &getResponse)
{
    auto prefixKey = member_->systemUpgradeParam.systemUpgradeKey;
    if (getResponse->status.IsError()) {
        YRLOG_INFO("failed to get key({}) from meta storage", prefixKey);
        return SyncResult{ getResponse->status };
    }
    if (getResponse->kvs.empty()) {
        YRLOG_INFO("get no result with key({}) from meta storage, revision is {}", prefixKey,
                   getResponse->header.revision);
        return SyncResult{ Status::OK() };
    }
    std::vector<WatchEvent> events;
    for (auto &kv : getResponse->kvs) {
        WatchEvent event{ .eventType = EVENT_TYPE_PUT, .kv = kv, .prevKv = {} };
        (void)events.emplace_back(event);
    }
    litebus::Async(GetAID(), &ScalerActor::UpdateSystemUpgradeSwitchEvent, events);
    return SyncResult{ Status::OK() };
}

litebus::Future<SyncResult> ScalerActor::InstanceInfoSyncer(const std::shared_ptr<GetResponse> &getResponse)
{
    if (getResponse->status.IsError()) {
        YRLOG_INFO("failed to get key({}) from meta storage", INSTANCE_PATH_PREFIX);
        return SyncResult{ getResponse->status };
    }

    if (getResponse->kvs.empty()) {
        YRLOG_INFO("get no result with key({}) from meta storage, revision is {}", INSTANCE_PATH_PREFIX,
                   getResponse->header.revision);
        return SyncResult{ Status::OK() };
    }

    std::set<std::string> etcdKvSet;
    std::vector<WatchEvent> events;
    for (auto &kv : getResponse->kvs) {
        auto eventKey = TrimKeyPrefix(kv.key(), member_->metaStorageAccessor->GetMetaClient()->GetTablePrefix());
        auto instanceID = GetInstanceID(eventKey);
        WatchEvent event{ .eventType = EVENT_TYPE_PUT, .kv = kv, .prevKv = {} };
        (void)events.emplace_back(event);
        etcdKvSet.emplace(instanceID);
    }
    // update cache by meta store;
    UpdateInstanceEvent(events);

    // delete instance in cache but not in meta store;
    // <instanceID, podName>
    std::unordered_map<std::string, std::string> needToRemove;
    for (const auto &elem : member_->instanceId2PodName) {
        if (auto it = etcdKvSet.find(elem.first); it == etcdKvSet.end()) {
            needToRemove[elem.first] = elem.second;
        }
    }
    // 1. clear instance cache; 2. delete pod or update pod label
    for (const auto &elem : needToRemove) {
        member_->instanceId2PodName.erase(elem.first);
        member_->podName2InstanceId.erase(elem.second);
        auto iter = member_->podNameMap.find(elem.second);
        if (iter == member_->podNameMap.end()) {
            YRLOG_WARN("failed to find pod({}) for instance({})", elem.second, elem.first);
            continue;
        }
        ASSERT_IF_NULL(business_);
        business_->DeletePodNotBindInstance(iter->second);
        business_->UpdatePodLabelsWithoutInstance(iter->second);
    }
    return SyncResult{ Status::OK() };
}

litebus::Future<SyncResult> ScalerActor::PodPoolSyncer(const std::shared_ptr<GetResponse> &getResponse)
{
    if (getResponse->status.IsError()) {
        YRLOG_INFO("failed to get key({}) from meta storage", POD_POOL_PREFIX);
        return SyncResult{ getResponse->status };
    }

    if (getResponse->kvs.empty()) {
        YRLOG_INFO("get no result with key({}) from meta storage, revision is {}", POD_POOL_PREFIX,
                   getResponse->header.revision);
        return SyncResult{ Status::OK() };
    }

    std::vector<WatchEvent> events;
    std::set<std::string> etcdKvSet;
    for (auto &kv : getResponse->kvs) {
        auto eventKey = TrimKeyPrefix(kv.key(), member_->metaStorageAccessor->GetMetaClient()->GetTablePrefix());
        auto poolID = GetPodPoolID(eventKey);
        auto poolInfo = member_->poolManager->GetPodPool(poolID);
        etcdKvSet.emplace(poolID);
        // exist in etcd but not in cache, need update
        if (poolInfo == nullptr) {
            WatchEvent event{ .eventType = EVENT_TYPE_PUT, .kv = kv, .prevKv = {} };
            (void)events.emplace_back(event);
            continue;
        }

        auto cacheJson = member_->poolManager->TransToJsonFromPodPoolInfo(poolInfo);
        // 1. exist both in etcd but cache and value isn't consistent, need update
        // 2. exist both in etcd but cache and value is consistent, but state is DELETED, reUpdate
        if (cacheJson != kv.value() || poolInfo->status == static_cast<int32_t>(PoolState::DELETED)) {
            WatchEvent event{ .eventType = EVENT_TYPE_PUT, .kv = kv, .prevKv = {} };
            (void)events.emplace_back(event);
        }
    }
    UpdatePodPoolEvent(events);

    auto poolMap = member_->poolManager->GetPoolMap();
    for (auto it = poolMap.cbegin(); it != poolMap.cend();) {
        if (etcdKvSet.count(it->first) == 0) {  // poolId not in etcd, need to remove from poolMap
            it = poolMap.erase(it);
        } else {
            ++it;
        }
    }
    return SyncResult{ Status::OK() };
}

void ScalerActor::StartCheckPodStatus()
{
    ASSERT_IF_NULL(business_);
    business_->CheckPodStatus();
    litebus::AsyncAfter(POD_STATUS_CHECK_INTERVAL_MS, GetAID(), &ScalerActor::StartCheckPodStatus);
}

void ScalerActor::StartCheckSystemFunctionNeedScale()
{
    if (member_->frontendManager != nullptr) {
        member_->frontendManager->CheckSystemFunctionNeedScale();
    }
    litebus::AsyncAfter(CHECK_SYSTEM_FUNCTION_INTERVAL, GetAID(), &ScalerActor::StartCheckSystemFunctionNeedScale);
}

Status ScalerActor::SyncTemplateDeployment()
{
    if (member_->templateDeployment != nullptr) {
        YRLOG_INFO("function agent template has exist");
        return Status::OK();
    }
    litebus::Promise<std::shared_ptr<V1Deployment>> promise;
    if (member_->agentTemplatePath.empty() || !litebus::os::ExistPath(member_->agentTemplatePath)) {
        YRLOG_INFO("function agent deployment template json file({}) not exist", member_->agentTemplatePath);
        return Status(StatusCode::FILE_NOT_FOUND);
    }
    std::string jsonStr = Read(member_->agentTemplatePath);
    nlohmann::json confJson;
    try {
        confJson = nlohmann::json::parse(jsonStr);
    } catch (nlohmann::detail::parse_error &e) {
        YRLOG_ERROR("parse json failed, {}, error: {}", jsonStr, e.what());
        return Status(StatusCode::JSON_PARSE_ERROR);
    }
    member_->templateDeployment = std::make_shared<V1Deployment>();
    if (!member_->templateDeployment->FromJson(confJson)) {
        YRLOG_WARN("failed to decode from template deployment.");
    }
    member_->poolManager->PutDeployment(member_->templateDeployment);
    GetDsWorkerPort(member_->templateDeployment);
    return Status::OK();
}
}  // namespace functionsystem::scaler

