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

#include "instance_manager_actor.h"

#include <utility>

#include "async/async.hpp"
#include "async/defer.hpp"
#include "common/constants/actor_name.h"
#include "common/constants/signal.h"
#include "common/logs/logging.h"
#include "common/metadata/metadata.h"
#include "common/metrics/metrics_adapter.h"
#include "common/service_json/service_json.h"
#include "common/types/instance_state.h"
#include "common/utils/collect_status.h"
#include "common/utils/generate_message.h"
#include "common/utils/meta_store_kv_operation.h"
#include "common/utils/struct_transfer.h"
#include "instance_manager_util.h"

namespace functionsystem::instance_manager {
using namespace functionsystem::explorer;

const std::string KEY_ABNORMAL_SCHEDULER_PREFIX = "/yr/abnormal/localscheduler/";   // NOLINT
const std::string KEY_AGENT_INFO_PATH = "/yr/agentInfo/";
const std::string KEY_BUSPROXY_PATH_PREFIX = "/yr/busproxy/business/yrk/tenant/0/node/";
const int64_t CANCEL_TIMEOUT = 5000;
const int64_t ABNORMAL_GC_TIMEOUT = 2 * 60 * 60 * 1000; // 2 hour
const int64_t INSTANCE_COUNT_REPORT_INTERVAL = 10 * 1000; // 10 seconds
const int64_t GARBAGE_COLLECT_INTERVAL = 5 * 60 * 1000; // 5 minutes
const uint64_t FATAL_INSTANCE_TIMEOUT = 3600; // 1 hour in seconds

/**
 * Parse tenantID from instance key.
 * Key format: /sn/instance/business/yrk/tenant/{tenantID}/function/{function}/version/{version}/
 *             defaultaz/{requestID}/{instanceID}
 * @param instanceKey The instance key path
 * @return The tenantID, or empty string if parsing fails
 */
static std::string ParseTenantIDFromInstanceKey(const std::string &instanceKey)
{
    static const std::string prefix = INSTANCE_PATH_PREFIX + "/";
    if (instanceKey.empty() || instanceKey.size() <= prefix.size()) {
        return "";
    }

    // Extract the part after the prefix
    std::string remaining = instanceKey.substr(prefix.size());

    // Split by "/" and get the first segment (tenantID)
    auto pos = remaining.find('/');
    if (pos == std::string::npos) {
        return "";
    }

    return remaining.substr(0, pos);
}

static messages::ForwardKillResponse GenerateForwardKillResponse(const messages::ForwardKillRequest &req,
                                                                        int32_t state, const std::string &msg)
{
    messages::ForwardKillResponse rsp;
    rsp.set_requestid(req.requestid());
    rsp.set_instanceid(req.instance().instanceid());
    rsp.set_code(state);
    rsp.set_message(msg);
    return rsp;
}

InstanceManagerActor::InstanceManagerActor(const std::shared_ptr<MetaStoreClient> &metaClient,
                                           const std::shared_ptr<GlobalScheduler> &scheduler,
                                           const std::shared_ptr<GroupManager> &groupManager,
                                           const std::shared_ptr<ResourceGroupManager> &resourceGroupManager,
                                           const InstanceManagerStartParam &param)
    : ActorBase(INSTANCE_MANAGER_ACTOR_NAME),
      cancelTimout_(CANCEL_TIMEOUT)
{
    member_ = std::make_shared<Member>();
    member_->globalScheduler = scheduler;
    member_->runtimeRecoverEnable = param.runtimeRecoverEnable;
    member_->enableAbnormalDoubleCheck_ = param.enableAbnormalDoubleCheck;
    member_->isMetaStoreEnable = param.isMetaStoreEnable;
    member_->servicesPath = param.servicesPath;
    member_->libPath = param.libPath;
    member_->functionMetaPath = param.functionMetaPath;
    member_->client = metaClient;
    member_->instanceOpt = std::make_shared<InstanceOperator>(metaClient);
    member_->abnormalScheduler = std::make_shared<std::unordered_set<std::string>>();
    member_->groupManager = groupManager;
    member_->resourceGroupManager = resourceGroupManager;
    member_->family = std::make_shared<InstanceFamilyCaches>();
    member_->systemTenantID = param.systemTenantID;
}

bool InstanceManagerActor::UpdateLeaderInfo(const LeaderInfo &leaderInfo)
{
    litebus::AID masterAID(INSTANCE_MANAGER_ACTOR_NAME, leaderInfo.address);
    member_->leaderInfo = leaderInfo;

    auto newStatus = leader::GetStatus(GetAID(), masterAID, curStatus_);
    if (businesses_.find(newStatus) == businesses_.end()) {
        YRLOG_WARN("InstanceManagerActor UpdateLeaderInfo new status({}) business don't exist", newStatus);
        return false;
    }
    business_ = businesses_[newStatus];
    ASSERT_IF_NULL(business_);
    business_->OnChange();
    curStatus_ = newStatus;
    return true;
}

void InstanceManagerActor::Init()
{
    ASSERT_IF_NULL(member_);
    ASSERT_IF_NULL(member_->client);
    ASSERT_IF_NULL(member_->globalScheduler);
    ASSERT_IF_NULL(member_->instanceOpt);
    auto masterBusiness = std::make_shared<MasterBusiness>(member_, shared_from_this());
    auto slaveBusiness = std::make_shared<SlaveBusiness>(member_, shared_from_this());

    (void)businesses_.emplace(MASTER_BUSINESS, masterBusiness);
    (void)businesses_.emplace(SLAVE_BUSINESS, slaveBusiness);

    member_->globalScheduler->LocalSchedAbnormalCallback(
        [aid(GetAID())](const std::string &nodeID) -> litebus::Future<Status> {
            // blocked until migration is complete, then global scheduler update topology.
            return litebus::Async(aid, &InstanceManagerActor::OnLocalSchedFault, nodeID);
        });

    member_->globalScheduler->BindCheckLocalAbnormalCallback(
        [aid(GetAID())](const std::string &nodeID) -> litebus::Future<bool> {
            return litebus::Async(aid, &InstanceManagerActor::IsLocalAbnormal, nodeID);
        });

    member_->globalScheduler->BindLocalDeleteCallback(
        [aid(GetAID())](const std::string &nodeID) {
            litebus::Async(aid, &InstanceManagerActor::DelNode, nodeID);
        });

    member_->globalScheduler->BindLocalAddCallback(
        [aid(GetAID())](const std::string &nodeID) {
            litebus::Async(aid, &InstanceManagerActor::AddNode, nodeID);
        });

    YRLOG_INFO("load local function");
    std::unordered_map<std::string, FunctionMeta> funcMetaMap{};
    LoadLocalFuncMeta(funcMetaMap, member_->functionMetaPath);
    service_json::LoadFuncMetaFromServiceYaml(funcMetaMap, member_->servicesPath, member_->libPath);
    for (const auto &item : funcMetaMap) {
        member_->innerFuncMetaKeys.emplace(item.first);
    }

    (void)member_->client
        ->GetAndWatch(
            KEY_BUSPROXY_PATH_PREFIX, { .prefix = true },
            [aid(GetAID())](const std::vector<WatchEvent> &events, bool) -> bool {
                litebus::Async(aid, &InstanceManagerActor::OnLocalScheduleChange, events);
                return true;
            },
            [](const std::shared_ptr<GetResponse> &) -> litebus::Future<SyncResult> {
                return SyncResult{ Status::OK() };
            })
        .Then([aid(GetAID())](const std::shared_ptr<Watcher> &watcher) -> litebus::Future<Status> {
            litebus::Async(aid, &InstanceManagerActor::OnLocalScheduleWatch, watcher);
            return Status::OK();
        });

    GetAndWatchAbnormal();

    GetAndWatchInstance();
    GetAndWatchFunctionMeta();

    // start debug instance watcher
    GetAndWatchDebugInstance();

    (void)Explorer::GetInstance().AddLeaderChangedCallback(
        "InstanceManager", [aid(GetAID())](const LeaderInfo &leaderInfo) {
            litebus::Async(aid, &InstanceManagerActor::UpdateLeaderInfo, leaderInfo);
        });

    curStatus_ = SLAVE_BUSINESS;
    business_ = slaveBusiness;

    Receive("ForwardKill", &InstanceManagerActor::ForwardKill);
    Receive("ForwardCustomSignalResponse", &InstanceManagerActor::ForwardCustomSignalResponse);
    Receive("TryCancelResponse", &InstanceManagerActor::TryCancelResponse);
    Receive("ForwardQueryInstancesInfo", &InstanceManagerActor::ForwardQueryInstancesInfoHandler);
    Receive("ForwardQueryInstancesInfoResponse", &InstanceManagerActor::ForwardQueryInstancesInfoResponseHandler);
    Receive("ForwardQueryDebugInstancesInfo", &InstanceManagerActor::ForwardQueryDebugInstancesInfoHandler);
    Receive("ForwardQueryDebugInstancesInfoResponse",
            &InstanceManagerActor::ForwardQueryDebugInstancesInfoResponseHandler);

    // 启动周期性上报实例数量指标
    litebus::Async(GetAID(), &InstanceManagerActor::ReportInstanceCountPeriodically);

    // 启动周期性垃圾回收任务
    litebus::Async(GetAID(), &InstanceManagerActor::GarbageCollectFatalInstances);
}

void InstanceManagerActor::GetAndWatchInstance()
{
    auto observer = [aid(GetAID())](const std::vector<WatchEvent> &events, bool synced) -> bool {
        litebus::Async(aid, &InstanceManagerActor::OnInstanceWatchEvent, events, synced);
        return true;
    };
    auto syncer = [aid(GetAID())](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<SyncResult> {
        return litebus::Async(aid, &InstanceManagerActor::OnInstanceInfoSyncer, getResponse);
    };
    auto handler = [aid(GetAID())](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<Status> {
        return litebus::Async(aid, &InstanceManagerActor::CheckSyncResponse, getResponse);
    };

    // eg. /sn/instance/business/yrk/tenant/0/function/../version/..
    (void)member_->client
        ->GetAndWatchWithHandler(INSTANCE_PATH_PREFIX, { .prefix = true, .prevKv = true }, observer, syncer, handler)
        .Then([aid(GetAID())](const std::shared_ptr<Watcher> &watcher) -> litebus::Future<Status> {
            litebus::Async(aid, &InstanceManagerActor::OnInstanceWatch, watcher);
            return Status::OK();
        });
}

void InstanceManagerActor::GetAndWatchFunctionMeta()
{
    auto observer = [aid(GetAID())](const std::vector<WatchEvent> &events, bool synced) -> bool {
        litebus::Async(aid, &InstanceManagerActor::OnFuncMetaWatchEvent, events, synced);
        return true;
    };
    auto syncer = [aid(GetAID())](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<SyncResult> {
        return litebus::Async(aid, &InstanceManagerActor::OnFunctionMetaSyncer, getResponse);
    };
    auto handler = [aid(GetAID())](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<Status> {
        return litebus::Async(aid, &InstanceManagerActor::CheckSyncResponse, getResponse);
    };

    // eg. /yr/functions/business/yrk/tenant/...
    (void)member_->client
        ->GetAndWatchWithHandler(FUNC_META_PATH_PREFIX, { .prefix = true, .prevKv = true }, observer, syncer, handler)
        .Then([aid(GetAID())](const std::shared_ptr<Watcher> &watcher) -> litebus::Future<Status> {
            litebus::Async(aid, &InstanceManagerActor::OnInstanceWatch, watcher);
            return Status::OK();
        });
}

void InstanceManagerActor::GetAndWatchDebugInstance()
{
    // watcher callback func
    auto observer = [aid(GetAID())](const std::vector<WatchEvent> &events, bool synced) -> bool {
        litebus::Async(aid, &InstanceManagerActor::OnDebugInstanceWatchEvent, events, synced);
        return true;
    };
    // default Syncer
    auto syncer = [](const std::shared_ptr<GetResponse> &) -> litebus::Future<SyncResult> {
        return SyncResult{ Status::OK() };
    };
    auto handler = [aid(GetAID())](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<Status> {
        return litebus::Async(aid, &InstanceManagerActor::CheckSyncResponse, getResponse);
    };

    // eg. /yr/functions/business/yrk/tenant/...
    (void)member_->client
        ->GetAndWatchWithHandler(DEBUG_INSTANCE_PREFIX, { .prefix = true, .prevKv = true }, observer, syncer, handler)
        .Then([aid(GetAID())](const std::shared_ptr<Watcher> &watcher) -> litebus::Future<Status> {
            litebus::Async(aid, &InstanceManagerActor::OnInstanceWatch, watcher);
            return Status::OK();
        });
}

void InstanceManagerActor::OnInstanceWatch(const std::shared_ptr<Watcher> &watcher)
{
    member_->watchers.push_back(watcher);
}

void InstanceManagerActor::GetAndWatchAbnormal()
{
    auto observer = [aid(GetAID())](const std::vector<WatchEvent> &events, bool synced) -> bool {
        litebus::Async(aid, &InstanceManagerActor::OnAbnormalSchedulerWatchEvent, events, synced);
        return true;
    };

    auto syncer = [aid(GetAID())](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<SyncResult> {
        return litebus::Async(aid, &InstanceManagerActor::ProxyAbnormalSyncer, getResponse);
    };
    auto handler = [aid(GetAID())](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<Status> {
        return litebus::Async(aid, &InstanceManagerActor::CheckSyncResponse, getResponse);
    };

    (void)member_->client
        ->GetAndWatchWithHandler(KEY_ABNORMAL_SCHEDULER_PREFIX, { .prefix = true, .prevKv = true }, observer, syncer,
                                 handler)
        .Then([aid(GetAID())](const std::shared_ptr<Watcher> &watcher) -> litebus::Future<Status> {
            litebus::Async(aid, &InstanceManagerActor::OnAbnormalSchedulerWatch, watcher);
            return Status::OK();
        });
}

void InstanceManagerActor::OnAbnormalSchedulerWatch(const std::shared_ptr<Watcher> &watcher)
{
    member_->abnormalSchedulerWatcher = watcher;
}

void InstanceManagerActor::OnAbnormalSchedulerWatchEvent(const std::vector<WatchEvent> &events, bool synced)
{
    for (const auto &event : events) {
        switch (event.eventType) {
            case EVENT_TYPE_PUT: {
                (void)member_->abnormalScheduler->emplace(event.kv.value());
                if (member_->abnormalDeferTimer.find(event.kv.value()) != member_->abnormalDeferTimer.end()) {
                    litebus::TimerTools::Cancel(member_->abnormalDeferTimer[event.kv.value()]);
                }
                member_->abnormalDeferTimer[event.kv.value()] = litebus::AsyncAfter(
                    ABNORMAL_GC_TIMEOUT, GetAID(), &InstanceManagerActor::ClearAbnormalScheduler, event.kv.value());
                if (synced && !member_->runtimeRecoverEnable) { // sync event logic
                    YRLOG_DEBUG("sync abnormal scheduler: {}", event.kv.value());
                    auto instances = member_->instances.find(event.kv.value());
                    if (instances == member_->instances.end()) {
                        break;
                    }

                    ASSERT_IF_NULL(business_);
                    business_->OnSyncAbnormalScheduler(instances->second);
                    instances->second.clear();

                    (void)member_->instances.erase(instances);
                }
                break;
            }
            case EVENT_TYPE_DELETE: {
                YRLOG_INFO("receive delete event: {}", event.prevKv.value());
                if (member_->abnormalScheduler->find(event.prevKv.value()) != member_->abnormalScheduler->end()) {
                    (void)member_->abnormalScheduler->erase(event.prevKv.value());
                }
                if (member_->abnormalDeferTimer.find(event.prevKv.value()) != member_->abnormalDeferTimer.end()) {
                    litebus::TimerTools::Cancel(member_->abnormalDeferTimer[event.prevKv.value()]);
                    member_->abnormalDeferTimer.erase(event.prevKv.value());
                }
                break;
            }
            default: {
                YRLOG_ERROR("not supported");
                break;
            }
        }
    }
}

void InstanceManagerActor::Finalize()
{
    for (const auto &watcher : member_->watchers) {
        if (watcher != nullptr) {
            watcher->Close();
        }
    }
    member_->watchers.clear();

    if (member_->abnormalSchedulerWatcher != nullptr) {
        member_->abnormalSchedulerWatcher->Close();
        member_->abnormalSchedulerWatcher = nullptr;
    }

    if (member_->proxyRouteWatcher != nullptr) {
        member_->proxyRouteWatcher->Close();
        member_->proxyRouteWatcher = nullptr;
    }
}

InstanceManagerMap *InstanceManagerActor::Get(const std::string &nodeName, InstanceManagerMap *map)
{
    auto instances = member_->instances.find(nodeName);
    if (instances == member_->instances.end() || map == nullptr) {
        return map;
    }
    map->insert(instances->second.begin(), instances->second.end());
    return map;
}

std::unordered_map<std::string, std::unordered_set<std::string>> InstanceManagerActor::GetInstanceJobMap()
{
    return member_->jobID2InstanceIDs;
}

std::unordered_map<std::string, std::unordered_set<std::string>> InstanceManagerActor::GetInstanceFuncMetaMap()
{
    return member_->funcMeta2InstanceIDs;
}

Status InstanceManagerActor::GetAbnormalScheduler(const std::shared_ptr<std::unordered_set<std::string>> &map)
{
    if (map == nullptr) {
        return Status(FAILED, "map is nullptr");
    }
    for (const auto &item : *(member_->abnormalScheduler)) {
        (void)map->emplace(item);
    }
    return Status::OK();
}

bool InstanceManagerActor::IsInstanceManagedByJob(const std::shared_ptr<InstanceInfo> &info)
{
    if (info->jobid().empty() || info->detached()) {
        // detached instance or empty job
        return false;
    }
    std::unordered_set<std::string> jobIns{};
    auto jobIter = member_->jobID2InstanceIDs.find(info->jobid());
    auto parentIns = member_->family->GetInstance(info->parentid());
    while (parentIns != nullptr) {
        if (IsDriver(parentIns) || IsFrontendFunction(parentIns->function())) {
            return true;
        }
        // parent is detached, it's child will be managed by creator
        if (parentIns->detached()) {
            return false;
        }
        // parent instance already managed by job
        if (jobIter != member_->jobID2InstanceIDs.end()
            && jobIter->second.find(parentIns->instanceid()) != jobIter->second.end()) {
            return true;
        }
        parentIns = member_->family->GetInstance(parentIns->parentid());
    }
    return true;
}

void InstanceManagerActor::OnInstancePut(const std::string &key,
                                         const std::shared_ptr<resource_view::InstanceInfo> &instance)
{
    RETURN_IF_NULL(instance);
    ASSERT_IF_NULL(member_->groupManager);
    ASSERT_IF_NULL(business_);
    if (instance->instancestatus().code() == static_cast<int32_t>(InstanceState::FATAL) ||
        instance->instancestatus().code() == static_cast<int32_t>(InstanceState::EVICTED)) {
        member_->groupManager->OnInstanceAbnormal(key, instance);
    } else {
        member_->groupManager->OnInstancePut(key, instance);
    }
    if (!quotaMgrAID_.Name().empty()
        && instance->instancestatus().code() == static_cast<int32_t>(InstanceState::RUNNING)) {
        Send(quotaMgrAID_, "OnInstanceRunning", instance->SerializeAsString());
    }
    business_->OnInstancePutForFamilyManagement(instance);
    member_->instID2Instance[instance->instanceid()] = std::make_pair(key, instance);
    if (IsInstanceManagedByJob(instance)) {
        member_->jobID2InstanceIDs[instance->jobid()].emplace(instance->instanceid());
    }
    const auto funcKey = GetFuncKeyFromInstancePath(key);
    if (!funcKey.empty() && !IsDriver(instance)) {
        member_->funcMeta2InstanceIDs[funcKey].emplace(instance->instanceid());
    }
    // 1. You can determine whether a node is faulty based on the faulty node record and delete the function instances
    // on the faulty node.
    if (member_->abnormalScheduler->find(instance->functionproxyid()) != member_->abnormalScheduler->end()
        && !member_->runtimeRecoverEnable) {
        YRLOG_INFO("change instance({}) state to FATAL, because scheduler({}) is abnormal.", instance->instanceid(),
                   instance->functionproxyid());
        ASSERT_IF_NULL(business_);
        business_->OnFaultLocalInstancePut(key, instance, instance->functionproxyid() + " is abnormal");
        return;
    }
    // 2. If the node does not exist in the faulty node record but does not exist in the resource view, delete all
    // function instances under the node.
    if (!business_->NodeExists(instance->functionproxyid())) {
        YRLOG_INFO("try to take over instance({}), because scheduler({}) is exited.", instance->instanceid(),
                   instance->functionproxyid());
        ASSERT_IF_NULL(business_);
        business_->OnFaultLocalInstancePut(key, instance, instance->functionproxyid() + " is exited");
        return;
    }
    member_->instances[instance->functionproxyid()][key] = instance;
    // The named instance and recovered instance, the owner is transferred from InstanceManager to the real proxy.
    if (auto &instanceManagerOwner = member_->instances[INSTANCE_MANAGER_OWNER];
        instance->functionproxyid() != INSTANCE_MANAGER_OWNER
        && instanceManagerOwner.find(key) != instanceManagerOwner.end()) {
        (void)instanceManagerOwner.erase(key);
    }
}

void InstanceManagerActor::OnInstanceDelete(const std::string &key,
                                            const std::shared_ptr<resource_view::InstanceInfo> &instance)
{
    RETURN_IF_NULL(instance);
    if (!quotaMgrAID_.Name().empty()) {
        Send(quotaMgrAID_, "OnInstanceExited", instance->SerializeAsString());
    }
    member_->instID2Instance.erase(instance->instanceid());

    if (!instance->jobid().empty() &&
        member_->jobID2InstanceIDs.find(instance->jobid()) != member_->jobID2InstanceIDs.end()) {
        member_->jobID2InstanceIDs[instance->jobid()].erase(instance->instanceid());
        if (member_->jobID2InstanceIDs[instance->jobid()].empty()) {
            member_->jobID2InstanceIDs.erase(instance->jobid());
        }
    }

    auto funcKey = GetFuncKeyFromInstancePath(key);
    if (!funcKey.empty() && member_->funcMeta2InstanceIDs.find(funcKey) != member_->funcMeta2InstanceIDs.end()) {
        member_->funcMeta2InstanceIDs[funcKey].erase(instance->instanceid());
        if (member_->funcMeta2InstanceIDs[funcKey].empty()) {
            member_->funcMeta2InstanceIDs.erase(funcKey);
        }
    }

    auto instances = member_->instances.find(instance->functionproxyid());
    if (instances == member_->instances.end()) {
        return;
    }

    auto iterator = instances->second.find(key);
    if (iterator == instances->second.end()) {
        return;
    }

    (void)instances->second.erase(iterator);
    if (instances->second.empty()) {
        (void)member_->instances.erase(instance->functionproxyid());
    }
}

void InstanceManagerActor::OnInstanceWatchEvent(const std::vector<WatchEvent> &events, bool synced)
{
    if (synced) {
        YRLOG_DEBUG("sync instance watch event size:{}", events.size());
        std::unordered_map<std::string, std::shared_ptr<resource_view::InstanceInfo>> allInstances;
        for (const auto &event : events) {
            auto eventKey = TrimKeyPrefix(event.kv.key(), member_->client->GetTablePrefix());
            auto instance = std::make_shared<resource_view::InstanceInfo>();
            if (TransToInstanceInfoFromJson(*instance, event.kv.value())) {
                // Parse tenantID from key and set it in instance
                std::string tenantID = ParseTenantIDFromInstanceKey(eventKey);
                if (!tenantID.empty()) {
                    instance->set_tenantid(tenantID);
                }
                allInstances.emplace(eventKey, instance);
            } else {
                YRLOG_ERROR("failed to transform instance({}) info from String.", eventKey);
            }
        }
        // response.kvs is not sorted, so descendant instance may appear before parent, which will be considered as a
        // parent-missing instance, and be killed; so add all instances as a parent first
        member_->family->SyncInstances(allInstances);
        for (auto [key, instance] : allInstances) {
            OnInstancePut(key, instance);
        }
        SetInstancesReady();
        return;
    }

    for (const auto &event : events) {
        switch (event.eventType) {
            case EVENT_TYPE_PUT: {
                if (!event.prevKv.value().empty()) {
                    auto history = std::make_shared<resource_view::InstanceInfo>();
                    auto eventKey = TrimKeyPrefix(event.prevKv.key(), member_->client->GetTablePrefix());
                    if (TransToInstanceInfoFromJson(*history, event.prevKv.value())) {
                        OnInstanceDelete(eventKey, history);  // ?
                    }
                }

                auto instance = std::make_shared<resource_view::InstanceInfo>();
                auto eventKey = TrimKeyPrefix(event.kv.key(), member_->client->GetTablePrefix());
                if (TransToInstanceInfoFromJson(*instance, event.kv.value())) {
                    // Parse tenantID from key and set it in instance
                    std::string tenantID = ParseTenantIDFromInstanceKey(eventKey);
                    if (instance->tenantid().empty() && !tenantID.empty()) {
                        instance->set_tenantid(tenantID);
                    }
                    OnInstancePut(eventKey, instance);
                } else {
                    YRLOG_ERROR("failed to transform instance({}) info from String.", eventKey);
                }
                break;
            }
            case EVENT_TYPE_DELETE: {
                auto eventKey = TrimKeyPrefix(event.prevKv.key(), member_->client->GetTablePrefix());
                auto history = std::make_shared<resource_view::InstanceInfo>();
                if (!TransToInstanceInfoFromJson(*history, event.prevKv.value())) {
                    YRLOG_ERROR("failed to transform instance({}) info from String.", eventKey);
                    break;
                }
                OnInstanceDelete(eventKey, history);
                if (member_->groupManager) {
                    member_->groupManager->OnInstanceDelete(eventKey, history);
                }
                if (member_->resourceGroupManager) {
                    member_->resourceGroupManager->OnDeleteInstance(history);
                }
                ASSERT_IF_NULL(business_);
                business_->OnInstanceDeleteForFamilyManagement(eventKey, history);
                break;
            }
            default: {
                YRLOG_ERROR("not supported");
                break;
            }
        }
    }
}

void InstanceManagerActor::OnDebugInstanceWatchEvent(const std::vector<WatchEvent> &events, bool synced)
{
    for (const auto &event : events) {
        switch (event.eventType) {
            case EVENT_TYPE_PUT: {
                auto eventKey = TrimKeyPrefix(event.kv.key(), member_->client->GetTablePrefix());
                YRLOG_DEBUG("event.kv.key(): {}", eventKey);
                auto debugInst = std::make_shared<messages::DebugInstanceInfo>();
                if (TransToDebugInstanceInfoFromJson(*debugInst, event.kv.value())) {
                    member_->debugInstInfoMap[eventKey] = debugInst;
                } else {
                    YRLOG_ERROR("failed to transform instance({}) info from String.", eventKey);
                }
                break;
            }
            case EVENT_TYPE_DELETE: {
                auto eventKey = TrimKeyPrefix(event.prevKv.key(), member_->client->GetTablePrefix());
                member_->debugInstInfoMap.erase(eventKey);
                break;
            }
            default: {
                YRLOG_ERROR("not supported");
                break;
            }
        }
    }
}

void InstanceManagerActor::OnFuncMetaWatchEvent(const std::vector<WatchEvent> &events, bool synced)
{
    for (const auto &event : events) {
        auto eventKey = TrimKeyPrefix(event.kv.key(), member_->client->GetTablePrefix());
        auto funcKey = GetFuncKeyFromFuncMetaPath(eventKey);
        if (funcKey.empty()) {
            YRLOG_WARN("function key is empty, path: {}", eventKey);
            continue;
        }
        YRLOG_DEBUG("receive function meta event, type: {}, funKey: {}, path: {}", fmt::underlying(event.eventType),
                    funcKey, eventKey);

        switch (event.eventType) {
            case EVENT_TYPE_PUT: {
                // ignore function meta put event
                break;
            }
            case EVENT_TYPE_DELETE: {
                ASSERT_IF_NULL(business_);
                business_->OnFuncMetaDelete(funcKey);
                break;
            }
            default: {
                YRLOG_ERROR("not supported");
                break;
            }
        }
    }
}

litebus::Future<Status> InstanceManagerActor::OnLocalSchedFault(const std::string &nodeName)
{
    ASSERT_IF_NULL(business_);
    return business_->OnLocalSchedFault(nodeName);
}

bool InstanceManagerActor::IsLocalAbnormal(const std::string &nodeName)
{
    ASSERT_IF_NULL(business_);
    return business_->IsLocalAbnormal(nodeName);
}

litebus::Future<Status> InstanceManagerActor::KillInstanceWithRetry(
    const std::string &instanceID, const std::shared_ptr<internal::ForwardKillRequest> &killReq)
{
    auto promiseIt = member_->killReqPromises.find(killReq->requestid());
    if (promiseIt == member_->killReqPromises.end()) {
        return Status::OK();
    }
    auto promise = promiseIt->second;

    auto [instanceKey, info] = GetInstanceInfoByInstanceID(instanceID);
    if (info == nullptr) {
        // instance is deleted, ok
        promise->SetValue(Status::OK());
        member_->killReqPromises.erase(killReq->requestid());
        return Status::OK();
    }

    if (info->instancestatus().code() == static_cast<int32_t>(InstanceState::FATAL) &&
        (killReq->req().signal() == FAMILY_EXIT_SIGNAL || info->functionproxyid() == INSTANCE_MANAGER_OWNER ||
         info->functionproxyid().empty())) {
        YRLOG_INFO("instance({}) with proxy({}) is killing with signal({}), now in status({}), will kill the instance.",
                   instanceID, info->functionproxyid(), killReq->req().signal(), info->instancestatus().code());
        // instance is fatal
        //   if want fatal, ok
        //   if owner is instance manager now, ok
        promise->SetValue(Status::OK());
        member_->killReqPromises.erase(killReq->requestid());

        if (info->functionproxyid() != INSTANCE_MANAGER_OWNER && !info->functionproxyid().empty()) {  // force delete
            return Status::OK();
        }
        auto routePath = GenInstanceRouteKey(info->instanceid());
        std::shared_ptr<StoreInfo> routePutInfo = std::make_shared<StoreInfo>(routePath, "");
        std::shared_ptr<StoreInfo> instancePutInfo = std::make_shared<StoreInfo>(instanceKey, "");
        std::shared_ptr<StoreInfo> debugInstPutInfo = nullptr;
        if (IsDebugInstance(info->createoptions())) {
            debugInstPutInfo = std::make_shared<StoreInfo>(DEBUG_INSTANCE_PREFIX + info->instanceid(), "");
        }
        return member_->instanceOpt
            ->ForceDelete(instancePutInfo, routePutInfo, debugInstPutInfo, IsLowReliabilityInstance(*info))
            .Then([key(instanceKey), cacher(member_->operateCacher), instance(info)](const OperateResult &result) {
                if (result.status.IsError()) {
                    YRLOG_ERROR("failed to Delete instance({}) from MetaStore, err status is {}.",
                                instance->instanceid(), fmt::underlying(result.status.StatusCode()));
                    if (TransactionFailedForEtcd(result.status.StatusCode())) {
                        cacher->AddDeleteEvent(INSTANCE_PATH_PREFIX, key);
                    }
                }
                return result.status;
            });
    }

    promise->GetFuture()
        .After(member_->retryKillIntervalMs,
               litebus::Defer(GetAID(), &InstanceManagerActor::KillInstanceWithRetry, instanceID, killReq));

    return member_->globalScheduler->GetLocalAddress(info->functionproxyid())
        .Then(litebus::Defer(GetAID(), &InstanceManagerActor::KillInstanceWithLocalAddr, std::placeholders::_1, info,
                             killReq));
}

void InstanceManagerActor::CompleteKillInstance(const litebus::Future<Status> &status, const std::string &requestID,
                                                const std::string &instanceID)
{
    if (status.IsError()) {
        YRLOG_WARN("{}|kill instance failed, code: {}", requestID, status.GetErrorCode());
        return;
    }
    // if instance is not found, try to clear instance info from meta store
    if (status.Get().StatusCode() == StatusCode::ERR_INSTANCE_NOT_FOUND) {
        YRLOG_INFO("{}|instance not found and try to clear instance info from meta store", requestID);
        auto infoIter = member_->instID2Instance.find(instanceID);
        if (infoIter == member_->instID2Instance.end() || infoIter->second.second == nullptr) {
            YRLOG_WARN("{}|can not find instance info and failed to kill, code({}), msg({}), retry", requestID,
                       fmt::underlying(status.Get().StatusCode()), status.Get().GetMessage());
            (void)member_->killReqPromises.erase(requestID);
            return;
        }

        auto [instanceKey, info] = infoIter->second;
        auto routePath = GenInstanceRouteKey(info->instanceid());
        std::shared_ptr<StoreInfo> routePutInfo = std::make_shared<StoreInfo>(routePath, "");
        std::shared_ptr<StoreInfo> instancePutInfo = std::make_shared<StoreInfo>(instanceKey, "");
        std::shared_ptr<StoreInfo> debugInstPutInfo = nullptr;
        if (IsDebugInstance(info->createoptions())) {
            debugInstPutInfo = std::make_shared<StoreInfo>(DEBUG_INSTANCE_PREFIX + info->instanceid(), "");
        }
        (void)member_->instanceOpt
            ->ForceDelete(instancePutInfo, routePutInfo, debugInstPutInfo, IsLowReliabilityInstance(*info))
            .Then([key(instanceKey), cacher(member_->operateCacher), instance(info)](const OperateResult &result) {
                if (result.status.IsError()) {
                    YRLOG_ERROR("failed to Delete instance({}) from MetaStore, err status is {}.",
                                instance->instanceid(), fmt::underlying(result.status.StatusCode()));
                    if (TransactionFailedForEtcd(result.status.StatusCode())) {
                        cacher->AddDeleteEvent(INSTANCE_PATH_PREFIX, key);
                    }
                }
                return result.status;
            });
        (void)member_->killReqPromises.erase(requestID);
    }
}

void InstanceManagerActor::OnLocalScheduleChange(const std::vector<WatchEvent> &events)
{
    for (const auto &event : events) {
        if (event.eventType == EVENT_TYPE_PUT) {
            member_->proxyRouteSet.emplace(event.kv.key());
        } else if (event.eventType == EVENT_TYPE_DELETE) {
            const auto &key = event.kv.key();
            member_->proxyRouteSet.erase(key);
            YRLOG_DEBUG("{} quit or expire, delete node", key);
            if (auto id = key.substr(KEY_BUSPROXY_PATH_PREFIX.length());
                member_->instances.find(id) != member_->instances.end()) {
                business_->DelNode(id, false);
            }
        }
    }
}

void InstanceManagerActor::OnLocalScheduleWatch(const std::shared_ptr<Watcher> &watcher)
{
    member_->proxyRouteWatcher = watcher;
}

void InstanceManagerActor::OnPutAbnormalScheduler(const litebus::Future<std::shared_ptr<PutResponse>> &ret,
                                                  const std::shared_ptr<litebus::Promise<Status>> &promise,
                                                  const std::string &nodeName)
{
    ASSERT_IF_NULL(business_);
    business_->OnPutAbnormalScheduler(ret, promise, nodeName);
}

void InstanceManagerActor::ForwardQueryInstancesInfoHandler(const litebus::AID &from, std::string &&name,
                                                            std::string &&msg)
{
    auto req = std::make_shared<messages::QueryInstancesInfoRequest>();
    if (!req->ParseFromString(msg)) {
        YRLOG_WARN("invalid QueryInstancesInfoRequest {}", msg);
        return;
    }
    ASSERT_IF_NULL(business_);
    business_->QueryInstancesInfo(req).OnComplete(
        litebus::Defer(GetAID(), &InstanceManagerActor::OnQueryInstancesInfoFinished, from, std::placeholders::_1));
}

void InstanceManagerActor::OnQueryInstancesInfoFinished(
    const litebus::AID &from, const litebus::Future<messages::QueryInstancesInfoResponse> &rsp)
{
    std::string result;
    if (rsp.IsOK()) {
        result = rsp.Get().SerializeAsString();
        YRLOG_INFO("OnQueryInstancesInfoFinished is ok {}", result);
    } else {
        messages::QueryInstancesInfoResponse errRsp;
        errRsp.set_code(common::ErrorCode::ERR_INNER_SYSTEM_ERROR);
        result = errRsp.SerializeAsString();
        YRLOG_INFO("OnQueryInstancesInfoFinished is not ok {}", result);
    }
    YRLOG_INFO("OnQueryInstancesInfoFinished send back {}", result);
    Send(from, "ForwardQueryInstancesInfoResponse", std::move(result));
}

void InstanceManagerActor::ForwardQueryInstancesInfoResponseHandler(const litebus::AID &from, std::string &&name,
                                                                    std::string &&msg)
{
    YRLOG_INFO("ForwardQueryInstancesInfoResponseHandler get {}", msg);
    auto rsp = std::make_shared<messages::QueryInstancesInfoResponse>();
    if (!rsp->ParseFromString(msg)) {
        YRLOG_WARN("invalid QueryInstancesInfoResponse {}", msg);
        return;
    }
    if (member_->queryInstancesPromise) {
        member_->queryInstancesPromise->SetValue(*rsp);
        member_->queryInstancesPromise = nullptr;
    } else {
        YRLOG_WARN("unknown ForwardQueryInstancesInfoResponseHandler({}) received", rsp->requestid());
    }
}

litebus::Future<messages::QueryInstancesInfoResponse> InstanceManagerActor::QueryInstancesInfo(
    std::shared_ptr<messages::QueryInstancesInfoRequest> req)
{
    ASSERT_IF_NULL(business_);
    return business_->QueryInstancesInfo(req);
}

litebus::Future<messages::QueryNamedInsResponse> InstanceManagerActor::QueryNamedIns(
    std::shared_ptr<messages::QueryNamedInsRequest> req)
{
    ASSERT_IF_NULL(business_);
    auto insReq = std::make_shared<messages::QueryInstancesInfoRequest>();
    insReq->set_requestid(req->requestid());
    return business_->QueryInstancesInfo(insReq).Then([req](const messages::QueryInstancesInfoResponse &insRsp) ->
        litebus::Future<messages::QueryNamedInsResponse> {
            auto instances = insRsp.instanceinfos();
            messages::QueryNamedInsResponse rsp;
            rsp.set_requestid(req->requestid());
            for (auto ins : instances) {
                if (auto it = ins.extensions().find(NAMED); it != ins.extensions().end() && it->second == "true") {
                    rsp.add_names(ins.instanceid());
                }
            }
            return rsp;
        });
}

litebus::Future<messages::QueryDebugInstanceInfosResponse> InstanceManagerActor::QueryDebugInstancesInfo(
    std::shared_ptr<messages::QueryDebugInstanceInfosRequest> req)
{
    ASSERT_IF_NULL(business_);
    return business_->QueryDebugInstancesInfo(req);
}

void InstanceManagerActor::ForwardQueryDebugInstancesInfoHandler(const litebus::AID &from, std::string &&name,
                                                                 std::string &&msg)
{
    auto req = std::make_shared<messages::QueryDebugInstanceInfosRequest>();
    if (!req->ParseFromString(msg)) {
        YRLOG_WARN("invalid QueryDebugInstanceInfosRequest {}", msg);
        return;
    }
    ASSERT_IF_NULL(business_);
    business_->QueryDebugInstancesInfo(req).OnComplete(litebus::Defer(
        GetAID(), &InstanceManagerActor::OnQueryDebugInstancesInfoFinished, from, std::placeholders::_1));
}

void InstanceManagerActor::OnQueryDebugInstancesInfoFinished(
    const litebus::AID &from, const litebus::Future<messages::QueryDebugInstanceInfosResponse> &rsp)
{
    std::string result;
    if (rsp.IsOK()) {
        result = rsp.Get().SerializeAsString();
        YRLOG_INFO("OnQueryDebugInstancesInfoFinished is ok {}", result);
    } else {
        messages::QueryDebugInstanceInfosResponse errRsp;
        errRsp.set_code(common::ErrorCode::ERR_INNER_SYSTEM_ERROR);
        result = errRsp.SerializeAsString();
        YRLOG_WARN("OnQueryDebugInstancesInfoFinished is not ok {}", result);
    }
    YRLOG_INFO("Send QueryDebugInstancesInfoFinished to slave node | {}", from.Url());
    Send(from, "ForwardQueryDebugInstancesInfoResponse", std::move(result));
}

void InstanceManagerActor::ForwardQueryDebugInstancesInfoResponseHandler(const litebus::AID &from, std::string &&name,
                                                                         std::string &&msg)
{
    auto rsp = std::make_shared<messages::QueryDebugInstanceInfosResponse>();
    if (!rsp->ParseFromString(msg)) {
        YRLOG_WARN("invalid QueryDebugInstanceInfosResponse {}", msg);
        return;
    }
    if (member_->queryDebugInstancesPromise) {
        member_->queryDebugInstancesPromise->SetValue(*rsp);
        member_->queryDebugInstancesPromise = nullptr;
    } else {
        YRLOG_WARN("unknown ForwardQueryInstancesInfoResponseHandler({}) received", rsp->requestid());
    }
}

void InstanceManagerActor::MasterBusiness::OnPutAbnormalScheduler(
    const litebus::Future<std::shared_ptr<PutResponse>> &ret, const std::shared_ptr<litebus::Promise<Status>> &promise,
    const std::string &nodeName)
{
    auto actor = actor_.lock();
    if (!ret.IsOK() || ret.Get()->status.IsError()) {
        YRLOG_ERROR("failed to write {} to etcd.", nodeName);  // NOLINT
        litebus::Async(actor->GetAID(), &InstanceManagerActor::EraseAbnormalScheduler, nodeName);
        promise->SetValue(Status(StatusCode::ERR_ETCD_OPERATION_ERROR, "failed to write to etcd"));
        member_->operateCacher->AddPutEvent(KEY_ABNORMAL_SCHEDULER_PREFIX, KEY_ABNORMAL_SCHEDULER_PREFIX + nodeName,
                                            nodeName);
        return;
    }
    (void)nodes_.erase(nodeName);
    YRLOG_INFO("success to put abnormal scheduler {}", nodeName);
    if (const auto &instances = member_->instances.find(nodeName); instances != member_->instances.end()) {
        ProcessInstanceOnFaultLocal(nodeName, nodeName + " is abnormal");
        promise->SetValue(Status(StatusCode::SUCCESS, "Success to migrate instances."));
        return;
    }

    if (member_->groupManager) {
        member_->groupManager->OnLocalAbnormal(nodeName);
    }
    promise->SetValue(Status(StatusCode::SUCCESS, "No instances need to be migrated."));
}

litebus::Future<Status> InstanceManagerActor::MasterBusiness::OnLocalSchedFault(const std::string &nodeName)
{
    if (member_->isUpgrading) {
        YRLOG_INFO("system is upgrading, don't notify abnormal scheduler");
        return Status(StatusCode::SUCCESS, "system is upgrading");
    }

    if (member_->enableAbnormalDoubleCheck_
        && member_->proxyRouteSet.find(KEY_BUSPROXY_PATH_PREFIX + nodeName) != member_->proxyRouteSet.end()) {
        YRLOG_INFO("{}'s lease is still in effect, don't notify abnormal scheduler", nodeName);
        (void)nodes_.erase(nodeName);
        return Status(StatusCode::SUCCESS, "proxy's lease still in effect.");
    }

    (void)member_->abnormalScheduler->emplace(nodeName);  // for OnInstancePut
    auto actor = actor_.lock();
    RETURN_STATUS_IF_NULL(actor, StatusCode::FAILED, "InstanceManagerActor is nullptr");
    auto promise = std::make_shared<litebus::Promise<Status>>();

    nlohmann::json info;
    info["nodeName"] = nodeName;
    info["instanceManagerActorAid"] = actor->GetAID();

    (void)member_->client->Put(KEY_ABNORMAL_SCHEDULER_PREFIX + nodeName, info.dump(), {})
        .OnComplete(litebus::Defer(actor->GetAID(), &InstanceManagerActor::OnPutAbnormalScheduler,
                                   std::placeholders::_1, promise, nodeName));
    return promise->GetFuture().OnComplete(
        litebus::Defer(actor->GetAID(), &InstanceManagerActor::ClearAbnormalSchedulerMetaInfo, nodeName));
}

void InstanceManagerActor::EraseAbnormalScheduler(const std::string &nodeName)
{
    (void)member_->abnormalScheduler->erase(nodeName);
}

bool InstanceManagerActor::MasterBusiness::IsLocalAbnormal(const std::string &nodeName)
{
    return member_->abnormalScheduler->find(nodeName) != member_->abnormalScheduler->end();
}

void InstanceManagerActor::MasterBusiness::OnFuncMetaDelete(const std::string &funcKey)
{
    if (member_->funcMeta2InstanceIDs.find(funcKey) == member_->funcMeta2InstanceIDs.end()) {
        return;
    }
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    for (auto instanceID : member_->funcMeta2InstanceIDs[funcKey]) {
        if (member_->instID2Instance.find(instanceID) == member_->instID2Instance.end()) {
            YRLOG_ERROR("failed to find instance({}), skip", instanceID);
            continue;
        }
        KillInstance(member_->instID2Instance[instanceID].second, SHUT_DOWN_SIGNAL, "function meta deleted");
    }
    auto reason = fmt::format("function({}) deleted", funcKey);
    (void)actor->TryCancelSchedule(funcKey, messages::CancelType::FUNCTION, reason);
}

void InstanceManagerActor::MasterBusiness::ProcessInstanceOnFaultLocal(const std::string &nodeName,
                                                                       const std::string &reason)
{
    for (const auto &instance : member_->instances.at(nodeName)) {
        if (instance.second == nullptr) {
            continue;
        }
        // take over the driver instance. directly to delete
        if (IsDriver(instance.second) || IsStaticFunctionInstance(*instance.second)) {
            YRLOG_INFO("the driver ({}) should be deleted because of local({}) abnormal", instance.second->instanceid(),
                       nodeName);
            ForceDelete(instance.first, instance.second);
            continue;
        }
        if (member_->isUpgrading) {
            YRLOG_INFO("system is upgrading, don't change instance to FATAL");
            return;
        }

        if (!IsRuntimeRecoverEnable(*instance.second)) {
            ProcessInstanceNotReSchedule(instance, nodeName, reason);
            continue;
        }

        std::shared_ptr<StoreInfo> routePutInfo = std::make_shared<StoreInfo>();
        std::shared_ptr<StoreInfo> instancePutInfo = std::make_shared<StoreInfo>();
        auto version = instance.second->version(); // version will +1 in GeneratePutInfo
        if (!GeneratePutInfo(instance.second, instancePutInfo, routePutInfo, InstanceState::SCHEDULING,
                             reason)) {
            YRLOG_ERROR("{}|failed to generate put info", instance.second->instanceid());
            return;
        }
        // 2.Put to MetaStore.
        auto actor = actor_.lock();
        ASSERT_IF_NULL(actor);
        (void)member_->instanceOpt
            ->Modify(instancePutInfo, routePutInfo, version, IsLowReliabilityInstance(*instance.second))
            .Then([nodeName, globalScheduler(member_->globalScheduler), instancePtr(instance.second),
                   instanceKey(instance.first), aid(actor->GetAID()),
                   cacher(member_->operateCacher)](const OperateResult &result) {
                if (result.status.IsError()) {
                    YRLOG_ERROR("failed to Put instance({}) to MetaStore, err: {}.", instancePtr->instanceid(),
                                result.status.ToString());
                    if (TransactionFailedForEtcd(result.status.StatusCode())) {
                        cacher->AddPutEvent(INSTANCE_PATH_PREFIX, instancePtr->instanceid(), "SCHEDULING");
                    }
                } else {
                    // 3.Re-schedule.
                    YRLOG_INFO("re-schedule instance({}) because scheduler({}) is fault.", instancePtr->instanceid(),
                               nodeName);
                    litebus::Async(aid, &InstanceManagerActor::TryReschedule, instanceKey, instancePtr,
                                   instancePtr->scheduletimes());
                }
                return true;
            });
        member_->instances[INSTANCE_MANAGER_OWNER][instance.first] = instance.second;
    }
}

void InstanceManagerActor::MasterBusiness::ProcessInstanceNotReSchedule(
    const std::pair<const std::string, std::shared_ptr<resource_view::InstanceInfo>> &instance,
    const std::string &nodeName, const std::string &reason)
{
    RETURN_IF_NULL(instance.second);
    YRLOG_INFO("change instance({}) status to FATAL because {}.", instance.second->instanceid(), reason);

    OnFaultLocalInstancePut(instance.first, instance.second, reason);
}

void InstanceManagerActor::ForwardKill(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    YRLOG_DEBUG("receive ForwardKill from {}", std::string(from));
    ASSERT_IF_NULL(business_);
    business_->ForwardKill(from, std::move(name), std::move(msg));
}

void InstanceManagerActor::ForwardCustomSignalResponse(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    YRLOG_DEBUG("receive ForwardCustomSignalResponse from {}", std::string(from));
    ASSERT_IF_NULL(business_);
    business_->ForwardCustomSignalResponse(from, std::move(name), std::move(msg));
}

bool InstanceManagerActor::CheckKillResult(const OperateResult &result, const std::string &instanceID,
                                           const std::string &requestID, const litebus::AID &from)
{
    messages::ForwardKillResponse rsp;
    rsp.set_requestid(requestID);
    if (result.status.IsError()) {
        YRLOG_ERROR("{}|failed to delete instance({})", requestID, instanceID);
        rsp.set_code(static_cast<int32_t>(StatusCode::ERR_ETCD_OPERATION_ERROR));
        rsp.set_message("failed to delete instance");
        (void)Send(from, "ResponseForwardKill", std::move(rsp.SerializeAsString()));
        return false;
    }
    rsp.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    (void)Send(from, "ResponseForwardKill", std::move(rsp.SerializeAsString()));
    return true;
}

void InstanceManagerActor::HandleSystemUpgrade(bool isUpgrading)
{
    YRLOG_INFO("change system upgrade status to {}", isUpgrading);
    member_->isUpgrading = isUpgrading;
}

void InstanceManagerActor::TryReschedule(const std::string &key,
                                         const std::shared_ptr<resource_view::InstanceInfo> &instance,
                                         uint32_t retryTimes)
{
    ASSERT_IF_NULL(business_);
    business_->TryReschedule(key, instance, retryTimes);
}

void InstanceManagerActor::MasterBusiness::HandleShutDownAll(const litebus::AID &from,
                                                             const messages::ForwardKillRequest &forwardKillRequest)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);

    messages::ForwardKillResponse rsp;
    rsp.set_requestid(forwardKillRequest.requestid());
    auto jobID = forwardKillRequest.req().instanceid();
    if (jobID.empty() || member_->jobID2InstanceIDs.find(jobID) == member_->jobID2InstanceIDs.end()) {
        YRLOG_WARN("failed to kill job, failed to find jobID({}) in cache", jobID);
        rsp.set_code(common::ErrorCode::ERR_NONE);
        rsp.set_message("failed to kill job, failed to find jobID in instance-manager");
        (void)actor->Send(from, "ResponseForwardKill", std::move(rsp.SerializeAsString()));
        return;
    }

    for (auto instanceID : member_->jobID2InstanceIDs[jobID]) {
        if (member_->instID2Instance.find(instanceID) == member_->instID2Instance.end()) {
            YRLOG_ERROR("failed to find instance({}), skip", instanceID);
            continue;
        }
        KillInstance(member_->instID2Instance[instanceID].second, SHUT_DOWN_SIGNAL, "job kill");
    }

    rsp.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    (void)actor->Send(from, "ResponseForwardKill", std::move(rsp.SerializeAsString()));
}

void InstanceManagerActor::MasterBusiness::OnChange()
{
    ResetNodes();
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    (void)member_->globalScheduler->QueryNodes().Then(
        litebus::Defer(actor->GetAID(), &InstanceManagerActor::OnSyncNodes, std::placeholders::_1));
    for (const auto &scheduler : *(member_->abnormalScheduler)) {
        auto instances = member_->instances.find(scheduler);
        if (instances == member_->instances.end()) {
            continue;
        }
        OnSyncAbnormalScheduler(instances->second);
        instances->second.clear();

        (void)member_->instances.erase(instances);
    }

    std::unordered_map<std::string, std::tuple<const std::shared_ptr<InstanceInfo>, const int32_t, std::string>>
        allInstancesNeedToBeKilled;
    // use descendant of to get BFS result, so it will always get most recent absent info
    // if new info comes, will update allInstancesNeedToBeKilled info
    for (const auto &info : member_->family->GetAllDescendantsOf("")) {
        bool isAbnormalInstance = (info->instancestatus().code() == static_cast<int32_t>(InstanceState::FATAL));
        bool isNeedKill = IsInstanceShouldBeKilled(info);
        if (!isAbnormalInstance && !isNeedKill) {
            continue;
        }
        if (isNeedKill) {
            // signal 1: delete  istance from meta store
            allInstancesNeedToBeKilled.emplace(
                info->instanceid(), std::make_tuple(info, SHUT_DOWN_SIGNAL,
                                                    fmt::format("ancestor instance is exited", info->instanceid())));
        }
        auto descendants = member_->family->GetAllDescendantsOf(info->instanceid());
        for (const auto &eachDescendant : descendants) {
            // if parent is abnormal, only set children instance to fatal
            allInstancesNeedToBeKilled.emplace(
                eachDescendant->instanceid(),
                std::make_tuple(eachDescendant, isNeedKill ? SHUT_DOWN_SIGNAL : FAMILY_EXIT_SIGNAL,
                                fmt::format("ancestor instance({}) is {}", info->instanceid(),
                                            isNeedKill ? "exited" : "abnormal")));
        }
    }
    for (const auto &toBeKilled : allInstancesNeedToBeKilled) {
        auto [info, signal, msg] = toBeKilled.second;
        KillInstance(info, signal, msg);
    }
    actor->DoFunctionMetaSyncer();
}

void InstanceManagerActor::MasterBusiness::OnSyncAbnormalScheduler(const InstanceManagerMap &instances)
{
    for (const auto &instance : instances) {
        if (IsDriver(instance.second) || IsStaticFunctionInstance(*instance.second)) {
            YRLOG_INFO("instance({}) is driver, delete directly when local fault", instance.first);
            ForceDelete(instance.first, instance.second);
            return;
        }

        OnFaultLocalInstancePut(instance.first, instance.second, "local-scheduler is abnormal");
    }
}

void InstanceManagerActor::MasterBusiness::ForceDelete(const std::string &key,
                                                       const std::shared_ptr<resource_view::InstanceInfo> &instance)
{
    auto routeKey = GenInstanceRouteKey(instance->instanceid());
    std::shared_ptr<StoreInfo> routePutInfo = std::make_shared<StoreInfo>(routeKey, "");
    std::shared_ptr<StoreInfo> instancePutInfo = std::make_shared<StoreInfo>(key, "");
    std::shared_ptr<StoreInfo> debugInstPutInfo = nullptr;
    if (IsDebugInstance(instance->createoptions())) {
        debugInstPutInfo = std::make_shared<StoreInfo>(DEBUG_INSTANCE_PREFIX + instance->instanceid(), "");
    }
    (void)member_->instanceOpt
        ->ForceDelete(instancePutInfo, routePutInfo, debugInstPutInfo, IsLowReliabilityInstance(*instance))
        .Then([key, cacher(member_->operateCacher), instance](const OperateResult &result) {
            if (result.status.IsError()) {
                YRLOG_ERROR("failed to Delete instance({}) from MetaStore.", instance->instanceid());
                if (TransactionFailedForEtcd(result.status.StatusCode())) {
                    cacher->AddDeleteEvent(INSTANCE_PATH_PREFIX, key);
                }
            }
            return result.status;
        });
}

void InstanceManagerActor::MasterBusiness::OnFaultLocalInstancePut(
    const std::string &key, const std::shared_ptr<resource_view::InstanceInfo> &instance, const std::string &reason)
{
    // 1. process(proxy, agent, runtime) fault: No processing is required.
    // 2. container(proxy, agent) fault: No processing is required.
    // 3. pod or node fault: force delete instance
    RETURN_IF_NULL(instance);
    if (instance->instancestatus().code() == static_cast<int32_t>(InstanceState::EXITING) || IsDriver(instance)
        || IsStaticFunctionInstance(*instance)) {
        YRLOG_INFO("instance({}) is driver or exiting, delete directly when {}", key, reason);
        ForceDelete(key, instance);
        return;
    }
    std::shared_ptr<StoreInfo> routePutInfo = std::make_shared<StoreInfo>();
    std::shared_ptr<StoreInfo> instancePutInfo = std::make_shared<StoreInfo>(key, "");
    auto version = instance->version(); // version will +1 in GeneratePutInfo
    if (!GeneratePutInfo(instance, instancePutInfo, routePutInfo, InstanceState::FATAL, reason)) {
        YRLOG_ERROR("{}|failed to generate put info", instance->instanceid());
        return;
    }

    (void)member_->instanceOpt->Modify(instancePutInfo, routePutInfo, version, IsLowReliabilityInstance(*instance))
        .Then([instance, cacher(member_->operateCacher)](const OperateResult &result) {
            if (result.status.IsError()) {
                YRLOG_ERROR("failed to Put instance({}) to MetaStore, errCode is ({}).", instance->instanceid(),
                            fmt::underlying(result.status.StatusCode()));
                if (TransactionFailedForEtcd(result.status.StatusCode())) {
                    cacher->AddPutEvent(INSTANCE_PATH_PREFIX, instance->instanceid(), "FATAL");
                }
            }
            return result.status;
        });
    member_->instances[INSTANCE_MANAGER_OWNER][key] = instance;
}

void InstanceManagerActor::MasterBusiness::ForwardKill(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    messages::ForwardKillRequest req;
    if (!req.ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse ForwardKillRequest");
        return;
    }
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    if (req.req().signal() == SHUT_DOWN_SIGNAL_ALL) {
        YRLOG_INFO("{}|receive kill job({}) request from {}", req.requestid(), req.req().instanceid(),
                   std::string(from));
        HandleShutDownAll(from, req);
        auto jobID = req.req().instanceid();
        if (member_->resourceGroupManager) {
            member_->resourceGroupManager->OnKillJob(jobID);
        }
        auto reason = fmt::format("job({}) finalized", jobID);
        (void)actor->TryCancelSchedule(jobID, messages::CancelType::JOB, reason);
        return;
    }

    auto info = std::make_shared<InstanceInfo>(req.instance());
    KillInstance(info, req.req().signal(), req.req().payload())
        .OnComplete(
        litebus::Defer(actor->GetAID(), &InstanceManagerActor::OnKillInstance, std::placeholders::_1, req, from));
}

void InstanceManagerActor::OnKillInstance(const litebus::Future<Status> &status,
                                          const messages::ForwardKillRequest &req, const litebus::AID &from)
{
    if (status.IsError()) {
        YRLOG_ERROR("failed to kill instance({}), code: {}", req.instance().instanceid(), status.GetErrorCode());
        messages::ForwardKillResponse rsp = GenerateForwardKillResponse(
            req, status.GetErrorCode(), "failed to kill instance(" + req.instance().instanceid() + ")");
        (void)Send(from, "ResponseForwardKill", std::move(rsp.SerializeAsString()));
        return;
    }

    if (status.Get().IsError()) {
        YRLOG_ERROR("failed to kill instance({}), code: {}, msg: {}", req.instance().instanceid(),
                    fmt::underlying(status.Get().StatusCode()), status.Get().ToString());
        messages::ForwardKillResponse rsp =
            GenerateForwardKillResponse(req, status.Get().StatusCode(), status.Get().ToString());
        (void)Send(from, "ResponseForwardKill", std::move(rsp.SerializeAsString()));
        return;
    }

    messages::ForwardKillResponse rsp = GenerateForwardKillResponse(req, 0, "");
    (void)Send(from, "ResponseForwardKill", std::move(rsp.SerializeAsString()));
}

void InstanceManagerActor::MasterBusiness::KillAllInstances(
    const std::list<std::shared_ptr<InstanceInfo>> &allInstances, const int32_t signal, const std::string &msg)
{
    for (const auto &info : allInstances) {
        KillInstance(info, signal, msg);
    }
}

litebus::Future<Status> InstanceManagerActor::MasterBusiness::KillInstance(const std::shared_ptr<InstanceInfo> &info,
                                                                           const int32_t signal, const std::string &msg)
{
    member_->exitingInstances.insert(info->instanceid());
    auto actor = actor_.lock();
    auto killReq = actor->MakeKillReq(info, "", signal, msg);
    auto promise = std::make_shared<litebus::Promise<Status>>();
    member_->killReqPromises.emplace(killReq->requestid(), promise);
    promise->GetFuture().OnComplete(litebus::Defer(actor->GetAID(), &InstanceManagerActor::CompleteKillInstance,
                                                   std::placeholders::_1, info->requestid(), info->instanceid()));
    ASSERT_IF_NULL(actor);
    litebus::Async(actor->GetAID(), &InstanceManagerActor::KillInstanceWithRetry, info->instanceid(), killReq);
    return promise->GetFuture();
}

bool InstanceManagerActor::MasterBusiness::IsInstanceShouldBeKilled(const std::shared_ptr<InstanceInfo> &info)
{
    if (info->lowreliability() && IsNonRecoverableStatus(info->instancestatus().code())) {
        // low reliability instance, and instance cannot be recovered
        YRLOG_INFO("receive instance({}) event, which is low-reliability and not recoverable, will kill it",
                   info->instanceid());
        return true;
    }
    if (info->detached()) {
        // detached instance will be controlled by user
        return false;
    }
    // parent exist or created by frontend or static function
    bool isParentExists = member_->family->IsInstanceExists(info->parentid()) || IsCreateByFrontend(info)
                          || IsStaticFunctionInstance(*info);
    if (!isParentExists) {
        // parent instance is not exist
        YRLOG_INFO("receive instance({}) event, which parent({}) is not existed , will kill it", info->instanceid(),
                   info->parentid());
        return true;
    }
    bool isParentExiting = (member_->exitingInstances.find(info->parentid()) != member_->exitingInstances.end());
    bool isSelfExiting = (info->instancestatus().code() == static_cast<int32_t>(InstanceState::EXITING)
                          || info->instancestatus().code() == static_cast<int32_t>(InstanceState::EXITED));
    if (isParentExiting && !isSelfExiting) {
        YRLOG_INFO("receive instance({}) event, which parent({}) is exiting , will kill it", info->instanceid(),
                   info->parentid());
        return true;
    }
    return false;
}

bool InstanceManagerActor::MasterBusiness::IsAppDriverFinished(const std::shared_ptr<InstanceInfo> &info)
{
    // for app driver instance, finish includes bellowed situations:
    // successful: code:6(FATAL) + type:1(RETURN)
    // stopped by client: code:6(FATAL) + type:6(KILLED_INFO)
    auto createOpts = info->createoptions();
    bool isAppDriver = createOpts.find(APP_ENTRYPOINT) != createOpts.end();
    bool isFinished = info->instancestatus().code() == static_cast<int32_t>(InstanceState::FATAL)
                      && (info->instancestatus().type() == static_cast<int32_t>(EXIT_TYPE::RETURN)
                          || info->instancestatus().type() == static_cast<int32_t>(EXIT_TYPE::KILLED_INFO));
    return isAppDriver && isFinished;
}

void InstanceManagerActor::MasterBusiness::OnInstancePutForFamilyManagement(const std::shared_ptr<InstanceInfo> info)
{
    YRLOG_DEBUG("receive instance(id={}, parent={}, status={}, type={}) put event", info->instanceid(),
                info->parentid(), info->instancestatus().code(), info->instancestatus().type());
    if (IsFrontendFunction(info->function())) {
        member_->family->AddInstance(info);
        return;
    }

    bool isFatalInstance = (info->instancestatus().code() == static_cast<int32_t>(InstanceState::FATAL));
    // if fatal, process all descendants
    if (isFatalInstance) {
        auto descendants = member_->family->GetAllDescendantsOf(info->instanceid());
        YRLOG_INFO("receive instance({}) abnormal event, going to process ({}) descendants", info->instanceid(),
                   descendants.size());
        auto signal = FAMILY_EXIT_SIGNAL;
        auto msg = fmt::format("ancestor instance({}) is abnormal", info->instanceid());
        // if app driver finishes, kill its descendants
        if (IsAppDriverFinished(info)) {
            YRLOG_INFO("App driver({}) code({}) type({}) finishes, try to kill its descendants", info->instanceid(),
                       info->instancestatus().code(), info->instancestatus().type());
            signal = SHUT_DOWN_SIGNAL;
            msg = fmt::format("app({}) finishes", info->instanceid());
        }
        KillAllInstances(descendants, signal, msg);
    }
    if (IsInstanceShouldBeKilled(info)) {
        // parent missing/exiting, kill it
        KillAllInstances({ info }, SHUT_DOWN_SIGNAL, "");
    }
    member_->family->AddInstance(info);
}

void InstanceManagerActor::MasterBusiness::OnInstanceDeleteForFamilyManagement(
    const std::string &instanceKey, const std::shared_ptr<resource_view::InstanceInfo> &info)
{
    member_->exitingInstances.erase(info->instanceid());

    if (IsFrontendFunction(info->function())) {
        YRLOG_INFO("faas frontend({}) is deleted, take no further move", info->instanceid());
        member_->family->RemoveInstance(info->instanceid());
        return;
    }

    auto descendants = member_->family->GetAllDescendantsOf(info->instanceid());
    YRLOG_DEBUG("receive instance({}) delete event, killing ({}) descendants", info->instanceid(), descendants.size());
    member_->family->RemoveInstance(info->instanceid());
    KillAllInstances(descendants, SHUT_DOWN_SIGNAL, fmt::format("ancestor instance({}) exited", info->instanceid()));
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    auto reason = fmt::format("parent({}) exited", info->instanceid());
    (void)actor->TryCancelSchedule(info->instanceid(), messages::CancelType::PARENT, reason);
}

void InstanceManagerActor::MasterBusiness::TryReschedule(const std::string &key,
                                                         const std::shared_ptr<resource_view::InstanceInfo> &instance,
                                                         uint32_t retryTimes)
{
    if (retryTimes <= 0) {
        YRLOG_ERROR("{}|try to recover instance({}) times exceeded, change status to FATAL", instance->requestid(),
                    instance->instanceid());
        OnFaultLocalInstancePut(key, instance,
            "while local is exited/abnormal, recover times of instance exceeded limit");
        return;
    }

    YRLOG_INFO("re-schedule instance({}) because scheduler is fault", instance->instanceid());
    auto req = std::make_shared<messages::ScheduleRequest>();
    req->set_requestid(instance->requestid());
    *req->mutable_instance() = *instance;
    auto actor = actor_.lock();
    member_->globalScheduler->Schedule(req).OnComplete(
        [aid(actor->GetAID()), key, instance, retryTimes](const litebus::Future<Status> &status) {
            if (status.IsError() || status.Get().IsError()) {
                YRLOG_ERROR("re-schedule instance({}) failed, remaining retry times({}), code: {}, msg: {}",
                            instance->instanceid(), retryTimes - 1,
                            status.IsError() ? status.GetErrorCode() : status.Get().StatusCode(),
                            status.IsError() ? "failed to Schedule" : status.Get().GetMessage());
                litebus::Async(aid, &InstanceManagerActor::TryReschedule, key, instance, retryTimes - 1);
            }
        });
}

void InstanceManagerActor::MasterBusiness::ForwardCustomSignalResponse(const litebus::AID &from, std::string &&name,
                                                                       std::string &&msg)
{
    internal::ForwardKillResponse forwardKillResponse;
    if (msg.empty() || !forwardKillResponse.ParseFromString(msg)) {
        YRLOG_WARN("(custom signal)invalid response body from({}).", from.HashString());
        return;
    }

    auto iter(member_->killReqPromises.find(forwardKillResponse.requestid()));
    if (iter == member_->killReqPromises.end()) {
        YRLOG_WARN("{}|(custom signal)failed to get response, no request matches result",
                   forwardKillResponse.requestid());
        return;
    }

    // if instance is not found, try to clear instance info from meta store in CompleteKillInstance
    if (forwardKillResponse.code() == common::ERR_INSTANCE_NOT_FOUND) {
        iter->second->SetValue(Status{ StatusCode::ERR_INSTANCE_NOT_FOUND, forwardKillResponse.message() });
        return;
    }

    if (forwardKillResponse.code() != 0) {
        YRLOG_WARN("{}|(custom signal)failed to kill, code({}), msg({}), retry", forwardKillResponse.requestid(),
                   fmt::underlying(forwardKillResponse.code()), forwardKillResponse.message());
        return;
    }

    YRLOG_DEBUG("{}|(custom signal) get response", forwardKillResponse.requestid());
    iter->second->SetValue(Status::OK());
    (void)member_->killReqPromises.erase(forwardKillResponse.requestid());
}

litebus::Future<messages::QueryInstancesInfoResponse> InstanceManagerActor::MasterBusiness::QueryInstancesInfo(
    std::shared_ptr<messages::QueryInstancesInfoRequest> req)
{
    messages::QueryInstancesInfoResponse rsp;
    rsp.set_requestid(req->requestid());
    rsp.set_code(common::ErrorCode::ERR_NONE);
    for (auto [id, info] : member_->instID2Instance) {
        // copy constructor
        (void)id;
        rsp.mutable_instanceinfos()->Add(resources::InstanceInfo(*info.second));
    }
    return rsp;
}

litebus::Future<messages::QueryDebugInstanceInfosResponse> InstanceManagerActor::MasterBusiness::QueryDebugInstancesInfo
(std::shared_ptr<messages::QueryDebugInstanceInfosRequest> req)
{
    messages::QueryDebugInstanceInfosResponse rsp;
    rsp.set_code(common::ErrorCode::ERR_NONE);
    rsp.set_requestid(req->requestid());
    for (auto &iter : member_->debugInstInfoMap) {
        rsp.mutable_debuginstanceinfos()->Add(messages::DebugInstanceInfo(*iter.second));
    }
    return rsp;
}

void InstanceManagerActor::MasterBusiness::GarbageCollectFatalInstances()
{
    auto nowTimestamp = static_cast<uint64_t>(std::time(nullptr));
    std::vector<std::pair<std::string, std::shared_ptr<resource_view::InstanceInfo>>> instancesToDelete;

    // 查找 INSTANCE_MANAGER_OWNER 中状态为 FATAL 且超过 1 小时的实例
    if (member_->instances.find(INSTANCE_MANAGER_OWNER) != member_->instances.end()) {
        const auto &ownerInstances = member_->instances[INSTANCE_MANAGER_OWNER];

        for (const auto &[key, instance] : ownerInstances) {
            if (!instance) {
                continue;
            }

            // 检查是否为 FATAL 状态
            if (instance->instancestatus().code() != static_cast<int32_t>(InstanceState::FATAL) &&
                instance->instancestatus().code() != static_cast<int32_t>(InstanceState::EVICTED)) {
                continue;
            }

            // 检查是否有 CREATE_TIME_STAMP
            auto extIter = instance->extensions().find(CREATE_TIME_STAMP);
            if (extIter == instance->extensions().end()) {
                YRLOG_WARN("Instance({}) in FATAL state has no CREATE_TIME_STAMP, skip garbage collection",
                           instance->instanceid());
                continue;
            }

            // 解析创建时间戳
            uint64_t createTimestamp = 0;
            try {
                createTimestamp = std::stoull(extIter->second);
            } catch (const std::exception &e) {
                YRLOG_ERROR("Failed to parse CREATE_TIME_STAMP for instance({}): {}",
                            instance->instanceid(), e.what());
                continue;
            }

            // 检查是否超过 1 小时
            if (nowTimestamp > createTimestamp && (nowTimestamp - createTimestamp) > FATAL_INSTANCE_TIMEOUT) {
                YRLOG_INFO("Found FATAL instance({}) exceeding timeout, created at {}, now {}, age {} seconds",
                           instance->instanceid(), createTimestamp, nowTimestamp,
                           nowTimestamp - createTimestamp);
                instancesToDelete.emplace_back(key, instance);
            }
        }
    }

    if (instancesToDelete.empty()) {
        return;
    }
    YRLOG_INFO("Garbage collecting {} FATAL instances that exceeded timeout", instancesToDelete.size());
    for (const auto &[key, instance] : instancesToDelete) {
        YRLOG_INFO("Force deleting FATAL instance({}) key({})", instance->instanceid(), key);
        ForceDelete(key, instance);
    }
}

void InstanceManagerActor::MasterBusiness::DelNode(const std::string &nodeName, const bool force)
{
    if (force) {
        // If the heartbeat between the proxy and master fails, forcibly delete all instances.
        if (nodes_.find(nodeName) == nodes_.end()) {
            return;
        }
        (void)nodes_.erase(nodeName);
    } else {
        // The proxy route expires. If the heartbeat is normal, not delete all instances.
        if (nodes_.find(nodeName) != nodes_.end()) {
            YRLOG_WARN("{} has heartbeat, not delete instances", nodeName);
            return;
        }
    }

    if (member_->instances.find(nodeName) != member_->instances.end()) {
        YRLOG_INFO("{} is exited, trying to take over instance of it", nodeName);
        ProcessInstanceOnFaultLocal(nodeName, nodeName + " is exited.");
    }
}

void InstanceManagerActor::MasterBusiness::AddNode(const std::string &nodeName)
{
    (void)nodes_.insert(nodeName);
}

bool InstanceManagerActor::MasterBusiness::NodeExists(const std::string &nodeName)
{
    if (!nodeSynced_) {
        return true;
    }
    // Instance taken over by the master node.
    if (nodeName == INSTANCE_MANAGER_OWNER) {
        return true;
    }

    if (nodes_.find(nodeName) != nodes_.end()) {
        return true;
    }

    // Route info exists but has not been registered with the master.
    return member_->proxyRouteSet.find(KEY_BUSPROXY_PATH_PREFIX + nodeName) != member_->proxyRouteSet.end();
}

void InstanceManagerActor::MasterBusiness::ResetNodes()
{
    nodeSynced_ = false;
    nodes_.clear();
}

void InstanceManagerActor::MasterBusiness::OnSyncNodes(const std::unordered_set<std::string> &nodes)
{
    nodes_ = nodes;
    std::unordered_set<std::string> tobeTakeOver;
    for (const auto &nodeInstances : member_->instances) {
        if (nodes_.find(nodeInstances.first) != nodes_.end() || nodeInstances.first == INSTANCE_MANAGER_OWNER) {
            continue;
        }

        if (member_->enableAbnormalDoubleCheck_
            && member_->proxyRouteSet.find(KEY_BUSPROXY_PATH_PREFIX + nodeInstances.first)
                   != member_->proxyRouteSet.end()) {
            YRLOG_INFO("{}'s lease is still in effect, don't notify abnormal scheduler", nodeInstances.first);
            continue;
        }
        tobeTakeOver.insert(nodeInstances.first);
    }

    for (auto node : tobeTakeOver) {
        YRLOG_INFO("{} is not existed, try to take over instance on the node", node);
        ProcessInstanceOnFaultLocal(node, node + " is exited");
    }
    nodeSynced_ = true;
}

void InstanceManagerActor::SlaveBusiness::OnChange()
{
}

void InstanceManagerActor::SlaveBusiness::OnSyncAbnormalScheduler(const InstanceManagerMap &)
{
}

void InstanceManagerActor::SlaveBusiness::OnPutAbnormalScheduler(
    const litebus::Future<std::shared_ptr<PutResponse>> &, const std::shared_ptr<litebus::Promise<Status>> &promise,
    const std::string &)
{
    promise->SetValue(Status::OK());
}

void InstanceManagerActor::SlaveBusiness::OnFaultLocalInstancePut(const std::string &,
                                                                  const std::shared_ptr<resource_view::InstanceInfo> &,
                                                                  const std::string &)
{
}

litebus::Future<Status> InstanceManagerActor::SlaveBusiness::OnLocalSchedFault(const std::string &)
{
    return Status::OK();
}

bool InstanceManagerActor::SlaveBusiness::IsLocalAbnormal(const std::string &)
{
    return false;
}

void InstanceManagerActor::SlaveBusiness::ForwardKill(const litebus::AID &, std::string &&, std::string &&)
{
}

void InstanceManagerActor::SlaveBusiness::OnInstancePutForFamilyManagement(const std::shared_ptr<InstanceInfo> info)
{
    YRLOG_DEBUG("slave receive instance(id={}, parent={}, status={}) put event", info->instanceid(), info->parentid(),
                info->instancestatus().code());
    member_->family->AddInstance(info);
}

void InstanceManagerActor::SlaveBusiness::OnInstanceDeleteForFamilyManagement(
    const std::string &instanceKey, const std::shared_ptr<resource_view::InstanceInfo> &info)
{
    YRLOG_DEBUG("slave receive instance({}) delete event", info->instanceid());
    member_->family->RemoveInstance(info->instanceid());
    member_->exitingInstances.erase(info->instanceid());
}

void InstanceManagerActor::SlaveBusiness::TryReschedule(const std::string &,
                                                        const std::shared_ptr<resource_view::InstanceInfo> &, uint32_t)
{
}

void InstanceManagerActor::SlaveBusiness::OnFuncMetaDelete(const std::string &funcKey)
{
}

void InstanceManagerActor::SlaveBusiness::ForwardCustomSignalResponse(const litebus::AID &, std::string &&,
                                                                      std::string &&)
{
}

litebus::Future<messages::QueryInstancesInfoResponse> InstanceManagerActor::SlaveBusiness::QueryInstancesInfo(
    std::shared_ptr<messages::QueryInstancesInfoRequest> req)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);

    if (!member_->queryInstancesPromise) {
        member_->queryInstancesPromise = std::make_shared<litebus::Promise<messages::QueryInstancesInfoResponse>>();
        litebus::AID masterAID(INSTANCE_MANAGER_ACTOR_NAME, member_->leaderInfo.address);
        (void)actor->Send(masterAID, "ForwardQueryInstancesInfo", req->SerializeAsString());
        YRLOG_INFO("Slave Instance Manager send QueryInstancesInfo to Master {}", std::string(masterAID));
    }
    return member_->queryInstancesPromise->GetFuture();
}

litebus::Future<messages::QueryDebugInstanceInfosResponse> InstanceManagerActor::SlaveBusiness::QueryDebugInstancesInfo(
    std::shared_ptr<messages::QueryDebugInstanceInfosRequest> req)
{
    auto actor = actor_.lock();
    ASSERT_IF_NULL(actor);
    if (!member_->queryDebugInstancesPromise) {
        member_->queryDebugInstancesPromise =
            std::make_shared<litebus::Promise<messages::QueryDebugInstanceInfosResponse>>();
        litebus::AID masterAID(INSTANCE_MANAGER_ACTOR_NAME, member_->leaderInfo.address);
        (void)actor->Send(masterAID, "ForwardQueryDebugInstancesInfo", req->SerializeAsString());
        YRLOG_INFO("Slave Instance Manager send QueryDebugInstancesInfo to Master {}", std::string(masterAID));
    }
    return member_->queryDebugInstancesPromise->GetFuture();
}

bool InstanceManagerActor::SlaveBusiness::NodeExists(const std::string &nodeName)
{
    return true;
}

void InstanceManagerActor::DoFunctionMetaSyncer()
{
    isInstancesReady_.GetFuture().OnComplete([aid(GetAID())](const litebus::Future<bool> fut) {
        litebus::Async(aid, &InstanceManagerActor::FunctionMetaSyncer);
    });
}

litebus::Future<SyncResult> InstanceManagerActor::FunctionMetaSyncer()
{
    GetOption opts;
    opts.prefix = true;
    return member_->client->Get(FUNC_META_PATH_PREFIX, opts)
        .Then(litebus::Defer(GetAID(), &InstanceManagerActor::OnFunctionMetaSyncer, std::placeholders::_1));
}

litebus::Future<SyncResult> InstanceManagerActor::OnFunctionMetaSyncer(const std::shared_ptr<GetResponse> &getResponse)
{
    if (getResponse->status.IsError()) {
        YRLOG_INFO("failed to get key({}) from meta storage", FUNC_META_PATH_PREFIX);
        return SyncResult{ getResponse->status };
    }

    if (getResponse->kvs.empty()) {
        YRLOG_INFO("get no result with key({}) from meta storage, revision is {}", FUNC_META_PATH_PREFIX,
                   getResponse->header.revision);
        return SyncResult{ Status::OK() };
    }

    std::set<std::string> etcdKvSet;
    for (auto &kv : getResponse->kvs) {
        auto eventKey = TrimKeyPrefix(kv.key(), member_->client->GetTablePrefix());
        auto funcKey = GetFuncKeyFromFuncMetaPath(eventKey);
        if (!funcKey.empty()) {
            etcdKvSet.emplace(funcKey);
        }
    }
    for (const auto &funcKey : member_->funcMeta2InstanceIDs) {
        // for faas executor function or register by local services.yaml, no need to delete
        if (member_->innerFuncMetaKeys.find(funcKey.first) != member_->innerFuncMetaKeys.end()) {
            continue;
        }
        ASSERT_IF_NULL(business_);
        if (etcdKvSet.count(funcKey.first) == 0) {  // funcKey not in etcd, need to delete
            business_->OnFuncMetaDelete(funcKey.first);
        }
    }
    return SyncResult{ Status::OK() };
}

litebus::Future<SyncResult> InstanceManagerActor::ProxyAbnormalSyncer(const std::shared_ptr<GetResponse> &getResponse)
{
    if (getResponse->status.IsError()) {
        YRLOG_INFO("failed to get key({}) from meta storage", KEY_ABNORMAL_SCHEDULER_PREFIX);
        return SyncResult{ getResponse->status };
    }

    if (getResponse->kvs.empty()) {
        YRLOG_INFO("get no result with key({}) from meta storage, revision is {}", KEY_ABNORMAL_SCHEDULER_PREFIX,
                   getResponse->header.revision);
        return SyncResult{ Status::OK() };
    }
    std::list<litebus::Future<Status>> futures;
    for (auto &kv : getResponse->kvs) {
        WatchEvent event{ .eventType = EVENT_TYPE_PUT, .kv = kv, .prevKv = {} };
        auto promise = std::make_shared<litebus::Promise<Status>>();
        std::shared_ptr<PutResponse> putResponse = std::make_shared<PutResponse>();
        putResponse->status = Status::OK();
        litebus::Async(GetAID(), &InstanceManagerActor::OnPutAbnormalScheduler, putResponse, promise, kv.value());
        futures.emplace_back(promise->GetFuture());
    }
    return CollectStatus(futures, "proxy abnormal syncer").Then([getResponse](const Status &status) {
        return SyncResult{ status };
    });
}

litebus::Future<SyncResult> InstanceManagerActor::InstanceInfoSyncer()
{
    GetOption opts;
    opts.prefix = true;
    return member_->client->Get(INSTANCE_PATH_PREFIX, opts)
        .Then(litebus::Defer(GetAID(), &InstanceManagerActor::OnInstanceInfoSyncer, std::placeholders::_1));
}

litebus::Future<SyncResult> InstanceManagerActor::OnInstanceInfoSyncer(const std::shared_ptr<GetResponse> &getResponse)
{
    if (getResponse->status.IsError()) {
        YRLOG_INFO("failed to get key({}) from meta storage", INSTANCE_PATH_PREFIX);
        SetInstancesReady();
        return SyncResult{ getResponse->status};
    }
    if (getResponse->kvs.empty()) {
        YRLOG_INFO("get no result with key({}) from meta storage, revision is {}", INSTANCE_PATH_PREFIX,
                   getResponse->header.revision);
        SetInstancesReady();
        // try to replay meta store operation
        return ReplayFailedInstanceOperation(getResponse->header.revision + 1);
    }

    std::vector<WatchEvent> watchEvents;
    std::set<std::string> etcdKvMap;
    // update cache by meta store;
    YRLOG_INFO("Start to update instance info from metastore");

    std::unordered_map<std::string, std::shared_ptr<resource_view::InstanceInfo>> allInstances;
    for (const auto &kv : getResponse->kvs) {
        auto eventKey = TrimKeyPrefix(kv.key(), member_->client->GetTablePrefix());
        auto instance = std::make_shared<resource_view::InstanceInfo>();
        if (TransToInstanceInfoFromJson(*instance, kv.value())) {
            allInstances.emplace(eventKey, instance);
            etcdKvMap.emplace(instance->instanceid());
        }
    }
    // response.kvs is not sorted, so descendant instance may appear before parent, which will be considered as a
    // parent-missing instance, and be killed; so add all instances as a parent first
    member_->family->SyncInstances(allInstances);
    for (auto [key, instance] : allInstances) {
        OnInstancePut(key, instance);
    }
    // delete instance in cache but not in meta store;
    std::vector<InstanceKeyInfoPair> needToRemove;
    for (const auto &instance : member_->instID2Instance) {
        if (auto it = etcdKvMap.find(instance.first); it == etcdKvMap.end()) {
            needToRemove.emplace_back(std::make_pair(instance.second.first, instance.second.second));
        }
    }
    for (auto iter = needToRemove.cbegin(); iter != needToRemove.cend(); iter++) {
        YRLOG_INFO("Delete key({}) instance info from cache", iter->first);
        OnInstanceDelete(iter->first, iter->second);
        if (member_->groupManager) {
            member_->groupManager->OnInstanceDelete(iter->first, iter->second);
        }
        ASSERT_IF_NULL(business_);
        business_->OnInstanceDeleteForFamilyManagement(iter->first, iter->second);
    }
    SetInstancesReady();
    // replay meta store operation
    return ReplayFailedInstanceOperation(getResponse->header.revision + 1);
}

void InstanceManagerActor::ReplayFailedDeleteOperation(std::list<litebus::Future<Status>> &futures,
                                                       std::shared_ptr<std::set<std::string>> eraseDelKeys)
{
    auto delEventMap = member_->operateCacher->GetDeleteEventMap();
    auto delEvent = delEventMap.find(INSTANCE_PATH_PREFIX);
    if (delEvent != delEventMap.end()) {
        for (const auto &instanceKey : delEvent->second) {
            std::shared_ptr<StoreInfo> instancePutInfo = std::make_shared<StoreInfo>(instanceKey, "");
            std::shared_ptr<StoreInfo> debugInstPutInfo = nullptr;
            auto pos = instancePutInfo->key.find_last_of('/');
            if (pos == std::string::npos) {
                return;
            }
            auto instanceId = instancePutInfo->key.substr(pos + 1);
            auto pair = GetInstanceInfoByInstanceID(instanceId);
            if (pair.second != nullptr && IsDebugInstance(pair.second->createoptions())) {
                debugInstPutInfo = std::make_shared<StoreInfo>(DEBUG_INSTANCE_PREFIX + instanceId, "");
            }
            std::shared_ptr<StoreInfo> routePutInfo = std::make_shared<StoreInfo>(GenInstanceRouteKey(instanceId), "");
            auto promise = std::make_shared<litebus::Promise<Status>>();
            auto onDelete = [instanceKey, promise, eraseDelKeys](const OperateResult &result) {
                if (result.status.IsOk()) {
                    YRLOG_DEBUG("finish to replay operation for {}", instanceKey);
                    eraseDelKeys->emplace(instanceKey);
                }
                promise->SetValue(result.status);
                return result.status;
            };
            (void)member_->instanceOpt->ForceDelete(instancePutInfo, routePutInfo, debugInstPutInfo, false)
                .Then([onDelete, aid(GetAID())](const OperateResult &result) {
                    auto execute = [onDelete, result]() { return onDelete(result); };
                    return litebus::Async(aid, &InstanceManagerActor::Execute, execute);
                });
            futures.emplace_back(promise->GetFuture());
        }
    }
}

void InstanceManagerActor::ReplayFailedPutOperation(std::list<litebus::Future<Status>> &futures,
                                                    std::shared_ptr<std::set<std::string>> erasePutKeys)
{
    auto putEventMap = member_->operateCacher->GetPutEventMap();
    auto instanceEvent = putEventMap.find(INSTANCE_PATH_PREFIX);
    if (instanceEvent != putEventMap.end()) {
        for (const auto &event : instanceEvent->second) {
            auto iter = member_->instID2Instance.find(event.first);
            if (iter == member_->instID2Instance.end()) {  // instance is not exist
                erasePutKeys->emplace(event.first);
                continue;
            }
            auto &instance = iter->second.second;
            auto promise = std::make_shared<litebus::Promise<Status>>();
            futures.emplace_back(promise->GetFuture());

            auto tranState = event.second == "FATAL" ? InstanceState::FATAL : InstanceState::SCHEDULING;
            std::shared_ptr<StoreInfo> routePutInfo = std::make_shared<StoreInfo>();
            std::shared_ptr<StoreInfo> instancePutInfo = std::make_shared<StoreInfo>();
            auto version = instance->version();  // version will +1 in GeneratePutInfo
            if (!GeneratePutInfo(instance, instancePutInfo, routePutInfo, tranState, "local scheduler is abnormal")) {
                YRLOG_ERROR("{}|failed to generate put info", instance->instanceid());
                promise->SetValue(Status(StatusCode::FAILED, "failed to generate put info"));
                continue;
            }
            auto onModify = [aid(GetAID()), key(event.first), erasePutKeys, promise, tranState, instancePtr(instance),
                             instanceKey(instancePutInfo->key)](const OperateResult &result) {
                if (result.status.IsOk()) {
                    erasePutKeys->emplace(key);
                    YRLOG_DEBUG("finish to replay operation for {} and try to reschedule", instanceKey);
                    if (tranState == InstanceState::SCHEDULING) {  // need to replay reschedule operation
                        litebus::Async(aid, &InstanceManagerActor::TryReschedule, instanceKey, instancePtr,
                                       instancePtr->scheduletimes());
                    }
                }
                promise->SetValue(result.status);
                return result.status;
            };
            (void)member_->instanceOpt
                ->Modify(instancePutInfo, routePutInfo, version, IsLowReliabilityInstance(*instance))
                .Then([onModify, aid(GetAID())](const OperateResult &result) {
                    auto execute = [onModify, result]() { return onModify(result); };
                    return litebus::Async(aid, &InstanceManagerActor::Execute, execute);
                });
        }
    }
}

Status InstanceManagerActor::Execute(std::function<Status()> fn)
{
    return fn();
}

litebus::Future<SyncResult> InstanceManagerActor::ReplayFailedInstanceOperation(int64_t revision)
{
    std::list<litebus::Future<Status>> futures;
    auto eraseDelKeys = std::make_shared<std::set<std::string>>();
    auto erasePutKeys = std::make_shared<std::set<std::string>>();

    ReplayFailedDeleteOperation(futures, eraseDelKeys);
    ReplayFailedPutOperation(futures, erasePutKeys);

    return CollectStatus(futures, "instance info syncer")
        .Then([cacher(member_->operateCacher), eraseDelKeys, erasePutKeys](const Status &status) {
            for (const auto &key : *eraseDelKeys) {
                cacher->EraseDeleteEvent(INSTANCE_PATH_PREFIX, key);
            }
            for (const auto &key : *erasePutKeys) {
                cacher->ErasePutEvent(INSTANCE_PATH_PREFIX, key);
            }
            return SyncResult{ status };
        });
}

void InstanceManagerActor::OnHealthyStatus(const Status &status)
{
    YRLOG_INFO("metastore is recovered. sync abnormal status to metastore.");
    GetOption opts;
    opts.prefix = true;
    member_->client->Get(KEY_ABNORMAL_SCHEDULER_PREFIX, opts)
        .Then(litebus::Defer(GetAID(), &InstanceManagerActor::ProxyAbnormalSyncer, std::placeholders::_1));

    InstanceInfoSyncer().OnComplete([aid(GetAID())](const litebus::Future<SyncResult> &res) {
        litebus::Async(aid, &InstanceManagerActor::FunctionMetaSyncer);
    });
}

litebus::Future<Status> InstanceManagerActor::TryCancelSchedule(const std::string &id, const messages::CancelType &type,
                                                                const std::string &reason)
{
    auto promise = std::make_shared<litebus::Promise<Status>>();
    auto cancelRequest = std::make_shared<messages::CancelSchedule>();
    cancelRequest->set_id(id);
    cancelRequest->set_type(type);
    cancelRequest->set_reason(reason);
    cancelRequest->set_msgid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    cancelPromise_[cancelRequest->msgid()] = promise;
    member_->globalScheduler->GetRootDomainInfo().OnComplete(
        litebus::Defer(GetAID(), &InstanceManagerActor::DoTryCancel, std::placeholders::_1, cancelRequest, promise));
    return promise->GetFuture();
}

void InstanceManagerActor::TryCancelResponse(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    auto resp = messages::CancelScheduleResponse();
    if (!resp.ParseFromString(msg)) {
        YRLOG_WARN("received try cancel response from {}, invalid msg {} ignore", std::string(from), msg);
        return;
    }
    if (cancelPromise_.find(resp.msgid()) == cancelPromise_.end()) {
        YRLOG_WARN("received try cancel response from {}, invalid msgid {} ignore", std::string(from), resp.msgid());
        return;
    }
    cancelPromise_[resp.msgid()]->SetValue(
        Status(static_cast<StatusCode>(resp.status().code()), resp.status().message()));
    (void)cancelPromise_.erase(resp.msgid());
}

void InstanceManagerActor::DoTryCancel(const litebus::Future<litebus::Option<NodeInfo>> &future,
                                       const std::shared_ptr<messages::CancelSchedule> &cancelRequest,
                                       const std::shared_ptr<litebus::Promise<Status>> &promise)
{
    if (future.IsError() || future.Get().IsNone()) {
        YRLOG_ERROR("failed to cancel, get empty root domain info.");
        promise->SetValue(Status(StatusCode::ERR_INNER_SYSTEM_ERROR));
        (void)cancelPromise_.erase(cancelRequest->msgid());
        return;
    }
    auto root = future.Get().Get();
    auto aid = litebus::AID(root.name + DOMAIN_SCHEDULER_SRV_ACTOR_NAME_POSTFIX, root.address);
    YRLOG_WARN("send try cancel schedule request, cancel({}) type({}) reason({}) msgid({})", cancelRequest->id(),
               fmt::underlying(cancelRequest->type()), cancelRequest->reason(), cancelRequest->msgid());
    Send(aid, "TryCancelSchedule", cancelRequest->SerializeAsString());
    (void)promise->GetFuture().After(cancelTimout_, [aid(GetAID()), globalScheduler(member_->globalScheduler),
                                                     cancelRequest, promise](const litebus::Future<Status> &) {
        globalScheduler->GetRootDomainInfo().OnComplete(litebus::Defer(aid, &InstanceManagerActor::DoTryCancel,
                                                                       std::placeholders::_1, cancelRequest, promise));
        return Status::OK();
    });
}

void InstanceManagerActor::AddNode(const std::string &nodeName)
{
    ASSERT_IF_NULL(business_);
    business_->AddNode(nodeName);
}

void InstanceManagerActor::DelNode(const std::string &nodeName)
{
    ASSERT_IF_NULL(business_);
    business_->DelNode(nodeName, true);
}

Status InstanceManagerActor::OnSyncNodes(const std::unordered_set<std::string> &nodes)
{
    ASSERT_IF_NULL(business_);
    business_->OnSyncNodes(nodes);
    return Status::OK();
}

void InstanceManagerActor::ClearAbnormalSchedulerMetaInfo(const std::string &node)
{
    auto agentInfoKey = KEY_AGENT_INFO_PATH + node;
    member_->client->Delete(agentInfoKey, { false, false })
        .OnComplete([agentInfoKey](const litebus::Future<std::shared_ptr<DeleteResponse>> &deleteResponse) {
            auto code =
                deleteResponse.IsError() ? deleteResponse.GetErrorCode() : deleteResponse.Get()->status.StatusCode();
            YRLOG_INFO("delete key {}, code: {}", agentInfoKey, code);
        });
    auto busProxyKey = KEY_BUSPROXY_PATH_PREFIX + node;
    member_->client->Delete(busProxyKey, { false, false })
        .OnComplete([busProxyKey](const litebus::Future<std::shared_ptr<DeleteResponse>> &deleteResponse) {
            auto code =
                deleteResponse.IsError() ? deleteResponse.GetErrorCode() : deleteResponse.Get()->status.StatusCode();
            YRLOG_INFO("delete key {}, code: {}", busProxyKey, code);
        });
}

void InstanceManagerActor::ClearAbnormalScheduler(const std::string &node)
{
    if (member_->abnormalScheduler->find(node) == member_->abnormalScheduler->end()) {
        return;
    }
    auto abnormalKey = KEY_ABNORMAL_SCHEDULER_PREFIX + node;
    member_->client->Delete(abnormalKey, { false, false })
        .OnComplete([abnormalKey](const litebus::Future<std::shared_ptr<DeleteResponse>> &deleteResponse) {
            auto code =
                deleteResponse.IsError() ? deleteResponse.GetErrorCode() : deleteResponse.Get()->status.StatusCode();
            YRLOG_INFO("delete key {}, code: {}", abnormalKey, code);
        });
    ClearAbnormalSchedulerMetaInfo(node);
    member_->abnormalScheduler->erase(node);
    member_->abnormalDeferTimer.erase(node);
}

litebus::Future<Status> InstanceManagerActor::CheckSyncResponse(const std::shared_ptr<GetResponse> &response)
{
    if (!response->status.IsOk()) {
        YRLOG_ERROR("failed to sync data, err is {}, gonna to suicide", response->status.ToString());
        CommitSuicide();
        return Status(StatusCode::FAILED);
    }
    if (response->header.revision > INT64_MAX - 1) {
        YRLOG_ERROR("revision({}) add operation will exceed the maximum value({}) of INT64, gonna to suicide",
                    response->header.revision, INT64_MAX);
        CommitSuicide();
        return Status(StatusCode::FAILED);
    }
    return Status::OK();
}

void InstanceManagerActor::CommitSuicide()
{
    if (!isSuicide_) {
        isSuicide_ = true;
        (void)raise(SIGINT);
    }
}

litebus::Future<std::pair<std::string, std::shared_ptr<InstanceInfo>>> InstanceManagerActor::GetInstanceInfoByID(
    const std::string &instanceID)
{
    if (isInstancesReady_.GetFuture().IsInit()) {
        YRLOG_WARN("instance-manager is not ready, wait");
    }
    return isInstancesReady_.GetFuture().Then(
        litebus::Defer(GetAID(), &InstanceManagerActor::GetInstanceInfoByInstanceID, instanceID));
}

void InstanceManagerActor::SetInstancesReady()
{
    isInstancesReady_.SetValue(true);
}
// todo(lwy_robb): should caculated in instance on etcd event
void InstanceManagerActor::ReportInstanceCountPeriodically()
{
    // 只有 master 节点上报指标
    if (curStatus_ != MASTER_BUSINESS) {
        litebus::AsyncAfter(INSTANCE_COUNT_REPORT_INTERVAL, GetAID(),
                            &InstanceManagerActor::ReportInstanceCountPeriodically);
        return;
    }

    // 统计各个节点的 RUNNING 状态实例数量
    std::unordered_map<std::string, size_t> nodeInstanceCount;
    size_t totalInstanceCount = 0;

    for (const auto &[nodeID, instanceMap] : member_->instances) {
        if (nodeID == INSTANCE_MANAGER_OWNER) {
            continue;  // 跳过 master 自身实例
        }
        size_t count = 0;
        // 只统计 RUNNING 状态的实例
        for (const auto &[key, instance] : instanceMap) {
            if (instance && instance->instancestatus().code() == static_cast<int32_t>(InstanceState::RUNNING)
                && !instance->issystemfunc()) {
                count++;
            }
        }
        nodeInstanceCount[nodeID] = count;
        totalInstanceCount += count;

        // 为每个节点上报 RUNNING 状态实例数量
        functionsystem::metrics::MeterTitle meterTitle{
            "yr_instance_count",
            "Number of running instances on each node",
            "count"
        };
        functionsystem::metrics::MeterData meterData{
            static_cast<double>(count),
            { { "node_id", nodeID } }
        };
        functionsystem::metrics::MetricsAdapter::GetInstance().ReportDoubleGauge(
            meterTitle, meterData, {});
    }

    // 上报集群总 RUNNING 状态实例数量
    functionsystem::metrics::MeterTitle totalMeterTitle{
        "yr_cluster_instance_total",
        "Total number of running instances in the cluster",
        "count"
    };
    functionsystem::metrics::MeterData totalMeterData{
        static_cast<double>(totalInstanceCount),
        {}
    };
    functionsystem::metrics::MetricsAdapter::GetInstance().ReportDoubleGauge(
        totalMeterTitle, totalMeterData, {});

    YRLOG_DEBUG("Report running instance count metrics: total={}, nodes={}",
                totalInstanceCount, nodeInstanceCount.size());

    // 调度下一次上报
    litebus::AsyncAfter(INSTANCE_COUNT_REPORT_INTERVAL, GetAID(),
                        &InstanceManagerActor::ReportInstanceCountPeriodically);
}

void InstanceManagerActor::GarbageCollectFatalInstances()
{
    ASSERT_IF_NULL(business_);
    business_->GarbageCollectFatalInstances();

    // 调度下一次垃圾回收
    litebus::AsyncAfter(GARBAGE_COLLECT_INTERVAL, GetAID(),
                        &InstanceManagerActor::GarbageCollectFatalInstances);
}
}  // namespace functionsystem::instance_manager
