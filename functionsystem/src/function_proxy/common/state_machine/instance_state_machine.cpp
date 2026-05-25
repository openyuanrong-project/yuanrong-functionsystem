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

#include "instance_state_machine.h"

#include <unordered_set>

#include "common/types/instance_state.h"
#include "async/defer.hpp"
#include "async/uuid_generator.hpp"
#include "common/logs/logging.h"
#include "common/metadata/metadata.h"
#include "common/metrics/metrics_adapter.h"
#include "common/utils/meta_store_kv_operation.h"
#include "common/meta_store_adapter/instance_operator.h"
#include "common/utils/struct_transfer.h"

namespace functionsystem {
const int32_t MAX_EXIT_TIMES = 3;

static const std::unordered_map<InstanceState, std::unordered_set<InstanceState>> STATE_TRANSITION_MAP = {
    { InstanceState::NEW, { InstanceState::SCHEDULING } },
    { InstanceState::SCHEDULING,
      { InstanceState::SCHEDULING, InstanceState::CREATING, InstanceState::FAILED, InstanceState::FATAL,
        InstanceState::EXITING, InstanceState::SCHEDULE_FAILED } },
    { InstanceState::CREATING,
      { InstanceState::RUNNING, InstanceState::FAILED, InstanceState::EXITING, InstanceState::FATAL } },
    { InstanceState::RUNNING,
      { InstanceState::FAILED, InstanceState::EXITING, InstanceState::FATAL, InstanceState::EVICTING,
        InstanceState::SUB_HEALTH, InstanceState::SUSPEND } },
    { InstanceState::SUB_HEALTH,
      { InstanceState::FAILED, InstanceState::EXITING, InstanceState::FATAL, InstanceState::EVICTING,
        InstanceState::RUNNING } },
    { InstanceState::FAILED, { InstanceState::SCHEDULING, InstanceState::EXITING, InstanceState::FATAL } },
    { InstanceState::FATAL, { InstanceState::EXITING } },
    { InstanceState::EXITING, { InstanceState::FATAL } },
    { InstanceState::EVICTING, { InstanceState::EVICTED, InstanceState::FATAL } },
    { InstanceState::SCHEDULE_FAILED, { InstanceState::SCHEDULING, InstanceState::EXITING } },
    { InstanceState::EVICTED, { InstanceState::EXITING, InstanceState::FATAL } },
    { InstanceState::SUSPEND,
      { InstanceState::CREATING, InstanceState::SCHEDULING, InstanceState::RUNNING,
        InstanceState::FATAL, InstanceState::EXITING } },
};

/**
 * The status of scheduling and creating does not require persistent routeInfo. Other status requires persistent.
 * The high-reliability instance persistent InstanceInfo in each phase.
 * when meta store is disabled:
 * instance statue         : scheduling | creating   | running    | failed
 * data of high-reliability: meta+route | meta+route | meta+route | meta+route
 * data of low-reliability:  meta+route |            | meta+route | meta+route
 *
 * when meta store is enabled:
 * instance statue         : scheduling | creating   | running    | failed
 * data of high-reliability:    meta    |    meta    | meta+route | meta+route
 * data of low-reliability:             |            | meta+route | meta+route
 * @param instanceInfo  instanceInfo
 * @param isMetaStoreEnable isMetaStoreEnable
 * @return PersistenceType
 */
[[maybe_unused]] static PersistenceType GetPersistenceType(const resources::InstanceInfo &instanceInfo,
                                                           bool isMetaStoreEnable)
{
    auto state = static_cast<InstanceState>(instanceInfo.instancestatus().code());
    bool needPersistentRoute = functionsystem::NeedUpdateRouteState(state, isMetaStoreEnable);
    if (IsLowReliabilityInstance(instanceInfo)) {
        YRLOG_INFO("{}|Instance's reliability is low", instanceInfo.requestid());
        return needPersistentRoute ? PersistenceType::PERSISTENT_ALL : PersistenceType::PERSISTENT_NOT;
    }

    return (needPersistentRoute || !isMetaStoreEnable) ? PersistenceType::PERSISTENT_ALL
                                                       : PersistenceType::PERSISTENT_INSTANCE;
}

InstanceStateMachine::InstanceStateMachine(const std::string &nodeID, const std::shared_ptr<InstanceContext> &context,
                                           bool isMetaStoreEnable)
    : owner_(nodeID),
      instanceID_(context != nullptr ? context->GetInstanceInfo().instanceid() : ""),
      instanceContext_(context),
      savePromise_(std::make_shared<litebus::Promise<bool>>()),
      isMetaStoreEnable_(isMetaStoreEnable)
{
    savePromise_->SetValue(true);
}

TransitionResult InstanceStateMachine::VerifyTransitionState(const TransContext &context, std::string &requestID,
                                                             InstanceState oldState)
{
    if (STATE_TRANSITION_MAP.find(oldState) == STATE_TRANSITION_MAP.end()) {
        YRLOG_ERROR("{}|transition failed, instance({}) state({}) not found", requestID, instanceID_,
                    fmt::underlying(oldState));
        return TransitionResult{ litebus::None(), {}, {}, 0, Status(StatusCode::ERR_STATE_MACHINE_ERROR) };
    }

    if (oldState == InstanceState::EXITING) {
        ExitInstance();
        return TransitionResult{ litebus::None(), {}, {}, 0, Status(StatusCode::ERR_STATE_MACHINE_ERROR) };
    }

    auto nextStateList = STATE_TRANSITION_MAP.at(oldState);
    if (nextStateList.find(context.newState) == nextStateList.end()) {
        YRLOG_ERROR("{}|transition failed, instance({}) with state({}) next state can not be {}", requestID,
                    instanceID_, fmt::underlying(oldState), fmt::underlying(context.newState));
        return TransitionResult{ litebus::None(), {}, {}, 0, Status(StatusCode::ERR_STATE_MACHINE_ERROR) };
    }
    if (isLocalAbnormal_) {
        YRLOG_ERROR("{}|local is abnormal, failed to transition instance({}) from ({}) to ({})", requestID, instanceID_,
                    fmt::underlying(oldState), fmt::underlying(context.newState));
        return TransitionResult{ litebus::None(), {}, {}, 0, Status(StatusCode::ERR_LOCAL_SCHEDULER_ABNORMAL) };
    }
    return TransitionResult{ oldState, {}, {}, 0, Status::OK() };
}

// should be locked by caller
void InstanceStateMachine::PrepareTransitionInfo(const TransContext &context, resources::InstanceInfo &instanceInfo,
                                                 resources::InstanceInfo &previousInfo)
{
    auto errCode =
        context.errCode == 0 ? instanceContext_->GetInstanceInfo().instancestatus().errcode() : context.errCode;

    if (context.scheduleReq != nullptr) {
        YRLOG_DEBUG("{}|set scheduleReq instance({}), state({}), errCode({}), exitCode({}), msg({}), type({})",
                    context.scheduleReq->instance().requestid(), context.scheduleReq->instance().instanceid(),
                    fmt::underlying(context.newState), errCode, context.exitCode, context.msg, context.type);
        context.scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(
            static_cast<int32_t>(context.newState));
        context.scheduleReq->mutable_instance()->mutable_instancestatus()->set_errcode(errCode);
        context.scheduleReq->mutable_instance()->mutable_instancestatus()->set_exitcode(context.exitCode);
        context.scheduleReq->mutable_instance()->mutable_instancestatus()->set_msg(context.msg);
        context.scheduleReq->mutable_instance()->mutable_instancestatus()->set_type(context.type);
        auto stamp = std::to_string(static_cast<uint64_t>(std::time(nullptr)));
        if (IsFirstPersistence(instanceInfo, instanceContext_->GetState(), context.version)) {
            (*context.scheduleReq->mutable_instance()->mutable_extensions())[CREATE_TIME_STAMP] = stamp;
        }
        instanceInfo = context.scheduleReq->instance();
        previousInfo = instanceContext_->GetInstanceInfo();
        return;
    }

    previousInfo.CopyFrom(instanceContext_->GetInstanceInfo());
    instanceContext_->SetInstanceState(context.newState, errCode, context.exitCode, context.msg, context.type);
    instanceInfo = instanceContext_->GetInstanceInfo();
}

// should be locked by caller
void InstanceStateMachine::UpdateInstanceVersion(const TransContext &context, resources::InstanceInfo &instanceInfo)
{
    auto version = context.version + 1;
    if (context.scheduleReq != nullptr) {
        if (version != 0 && version <= context.scheduleReq->instance().version()) {
            YRLOG_WARN("{}|can not set version, because new version({}) is <= version({}) of instance({})",
                       context.scheduleReq->instance().requestid(), version, context.scheduleReq->instance().version(),
                       context.scheduleReq->instance().instanceid());
        }
        context.scheduleReq->mutable_instance()->set_version(version);
        instanceInfo = context.scheduleReq->instance();
        YRLOG_DEBUG("{}|set scheduleReq instance({})'s version({})", context.scheduleReq->instance().requestid(),
                    context.scheduleReq->instance().instanceid(), version);
        return;
    }
    instanceContext_->SetVersion(version);
    instanceInfo = instanceContext_->GetInstanceInfo();
    YRLOG_DEBUG("{}|set instance({})'s version({})", instanceContext_->GetRequestID(),
                instanceContext_->GetInstanceInfo().instanceid(), version);
}

litebus::Future<TransitionResult> InstanceStateMachine::PersistenceInstanceInfo(
    const resources::InstanceInfo &newInstanceInfo, const resources::InstanceInfo &prevInstanceInfo,
    const InstanceState oldState, const TransContext &context)
{
    NewSavingPomise();
    return SaveInstanceInfoToMetaStore(newInstanceInfo, prevInstanceInfo, oldState, context)
        .Then([requestID(newInstanceInfo.requestid()), instanceID(instanceID_), context,
               self(shared_from_this())](const TransitionResult &result) -> litebus::Future<TransitionResult> {
            if (!result.status.IsOk()) {
                YRLOG_ERROR("{}|transition instance({}) state failed.", requestID, instanceID);
                self->TagSaved();
                return result;
            }
            // if save info successfully and context.scheduleReq exist, update stateMachine by scheduleReq
            // scheduleReq in stateMachine is the same as scheduleReq, need to copy to avoid multi-thread modification
            if (context.scheduleReq != nullptr) {
                self->UpdateScheduleReq(std::make_shared<messages::ScheduleRequest>(*context.scheduleReq));
                self->SetModRevision(result.currentModRevision);
            }
            self->TagSaved();
            return result;
        });
}

litebus::Future<TransitionResult> InstanceStateMachine::TransitionTo(const TransContext &context)
{
    resources::InstanceInfo instanceInfo;
    resources::InstanceInfo previousInfo;
    InstanceState oldState;
    std::string requestID;
    {
        std::lock_guard<std::recursive_mutex> guard(lock_);
        if (instanceContext_ == nullptr) {
            YRLOG_ERROR("failed to find instance({}) context", instanceID_);
            return TransitionResult{ litebus::None(), {}, {}, 0, Status(StatusCode::ERR_STATE_MACHINE_ERROR) };
        }

        requestID = instanceContext_->GetRequestID();
        oldState = instanceContext_->GetState();
        // if old state is exiting, will execute exitHandler in VerifyTransitionState
        if (context.newState == oldState && oldState != InstanceState::EXITING) {
            YRLOG_WARN("{}|instance({}) state is same, ignore it", requestID, instanceID_);
            return TransitionResult{ oldState, {}, {}, GetVersion(), Status::OK() };
        }

        auto verifyResult = VerifyTransitionState(context, requestID, oldState);
        if (verifyResult.preState.IsNone()) {
            return verifyResult;
        }
        SetInstanceBillingTerminated(instanceID_, context.newState);

        YRLOG_INFO("{}|transition instance({}) state from ({}) to ({}), compare version({})", requestID, instanceID_,
                   fmt::underlying(oldState), fmt::underlying(context.newState), context.version);

        PrepareTransitionInfo(context, instanceInfo, previousInfo);
        auto persistenceType = GetPersistenceType(instanceInfo, isMetaStoreEnable_);
        if (!context.persistence || persistenceType == PersistenceType::PERSISTENT_NOT) {
            if (context.persistence) {  // If expected, reduce the log printing.
                YRLOG_INFO("{}|Persistence is not required because PERSISTENT_NOT", requestID);
            }
            if (context.scheduleReq != nullptr) {
                instanceContext_->UpdateScheduleReq(std::make_shared<messages::ScheduleRequest>(*context.scheduleReq));
            }
            if (instanceInfo.lowreliability() && context.newState == InstanceState::CREATING) {
                // for low-reliability instance, the invoked event may arrive before the listening event.
                PublishToLocalObserver(instanceInfo, 0);
            }
            return TransitionResult{ oldState, {}, previousInfo, context.version, Status::OK() };
        }

        UpdateInstanceVersion(context, instanceInfo);
        if (auto iter = instanceInfo.createoptions().find(RELIABILITY_TYPE);
            (iter != instanceInfo.createoptions().end() && iter->second == "low")
            || context.newState == InstanceState::FATAL) {
            YRLOG_WARN("{}|the {} is low  or instance state({}) is fatal, rm the init args", instanceInfo.requestid(),
                RELIABILITY_TYPE, static_cast<int32_t>(context.newState));
            instanceInfo.clear_args();
        }
    }
    auto stamp = std::to_string(static_cast<uint64_t>(std::time(nullptr)));
    (*instanceInfo.mutable_extensions())["updateTimestamp"] = std::move(stamp);
    return PersistenceInstanceInfo(instanceInfo, previousInfo, oldState, context);
}

litebus::Future<Status> InstanceStateMachine::DelInstance(const std::string &instanceID)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    // make sure that state machine can be deleted
    RETURN_STATUS_IF_NULL(instanceContext_, StatusCode::FAILED, "failed to delete instance, not found context.");

    auto oldState = instanceContext_->GetState();
    auto instanceInfo = instanceContext_->GetInstanceInfo();
    auto persistenceType = GetPersistenceType(instanceInfo, isMetaStoreEnable_);
    ExecuteStateChangeCallback(instanceInfo.requestid(), InstanceState::EXITED);
    if (instanceInfo.functionproxyid() == owner_) {
        std::shared_ptr<StoreInfo> instancePutInfo;
        std::shared_ptr<StoreInfo> routePutInfo;
        // If YR_DEBUG_CONFIG exists in create_option, debuginstancePutInfo->key is not empty.
        std::shared_ptr<StoreInfo> debugInstPutInfo = nullptr;
        std::string keyPath;
        if (IsDebugInstance(instanceInfo.createoptions())) {
            debugInstPutInfo = std::make_shared<StoreInfo>(DEBUG_INSTANCE_PREFIX + instanceInfo.instanceid(), "");
        }
        if (TransToStoredKeys(instanceInfo, instancePutInfo, routePutInfo, persistenceType, keyPath)
            .IsError()) {
            YRLOG_ERROR("failed to delete instance({}), not get key from InstanceInfo.", instanceID);
            return Status(StatusCode::FAILED);
        }

        YRLOG_INFO("try to delete instance({}), state({}), owner({}), version({})", instanceID,
                   fmt::underlying(oldState), instanceInfo.functionproxyid(), instanceInfo.version());
        YRLOG_DEBUG("delete instance to meta store, instance({}), instance status: {}, functionKey: {}, path: {}",
                    instanceInfo.instanceid(), instanceInfo.instancestatus().code(), instanceInfo.function(), keyPath);
        ASSERT_IF_NULL(instanceOpt_);
        return instanceOpt_
            ->Delete(instancePutInfo, routePutInfo, debugInstPutInfo, instanceInfo.version(),
                     IsLowReliabilityInstance(instanceInfo))
            .Then([key(keyPath), self(shared_from_this())](const OperateResult &result) {
                if (result.status.IsOk()) {
                    return Status::OK();
                }
                YRLOG_ERROR("failed to delete key {} from metastore, errorCode: {}, error: {}", key,
                            fmt::underlying(result.status.StatusCode()), result.status.GetMessage());
                if (TransactionFailedForEtcd(result.status.StatusCode())) {
                    self->SetLastSaveFailedState(static_cast<int32_t>(InstanceState::EXITED),
                                                 INVALID_LAST_SAVE_FAILED_STATE);
                }
                return Status(StatusCode::BP_META_STORAGE_DELETE_ERROR, "failed to delete key: " + key);
            });
    }
    YRLOG_WARN("failed to delete instance({}), instance's owner({}) not match machine's owner({}).", instanceID,
               instanceInfo.functionproxyid(), owner_);
    return Status(StatusCode::FAILED);
}

litebus::Future<Status> InstanceStateMachine::ForceDelInstance()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    auto instance = instanceContext_->GetInstanceInfo();
    std::shared_ptr<StoreInfo> instancePutInfo;
    std::shared_ptr<StoreInfo> routePutInfo;
    // If YR_DEBUG_CONFIG exists in create_option, debuginstancePutInfo->key is not empty.
    std::shared_ptr<StoreInfo> debugInstPutInfo = nullptr;
    if (IsDebugInstance(instance.createoptions())) {
        debugInstPutInfo = std::make_shared<StoreInfo>(DEBUG_INSTANCE_PREFIX + instance.instanceid(), "");
    }
    std::string path;
    if (TransToStoredKeys(instance, instancePutInfo, routePutInfo, PersistenceType::PERSISTENT_ALL, path)
            .IsError()) {
        YRLOG_ERROR("failed to delete instance({}), not get key from InstanceInfo.", instance.instanceid());
        return Status(StatusCode::FAILED);
    }

    YRLOG_INFO("{}|force delete instance from metastore, instance({}), functionKey: {}, path: {}", instance.requestid(),
               instance.instanceid(), instance.function(), path);
    ASSERT_IF_NULL(instanceOpt_);
    return instanceOpt_
        ->ForceDelete(instancePutInfo, routePutInfo, debugInstPutInfo, IsLowReliabilityInstance(instance))
        .Then([path, self(shared_from_this())](const OperateResult &result) {
            if (result.status.IsOk()) {
                return Status::OK();
            }
            YRLOG_ERROR("failed to delete key {} from metastore, errorCode: {}, error: {}", path,
                        fmt::underlying(result.status.StatusCode()), result.status.GetMessage());
            if (TransactionFailedForEtcd(result.status.StatusCode())) {
                self->SetLastSaveFailedState(static_cast<int32_t>(InstanceState::EXITED),
                                             INVALID_LAST_SAVE_FAILED_STATE);
            }
            return Status(StatusCode::BP_META_STORAGE_DELETE_ERROR, "failed to delete key: " + path);
        });
}

void InstanceStateMachine::BindMetaStoreClient(const std::shared_ptr<MetaStoreClient> &client)
{
    instanceOpt_ = nullptr;
    instanceOpt_ = std::make_shared<InstanceOperator>(client);
}

void InstanceStateMachine::PublishToLocalObserver(const resources::InstanceInfo &newInstanceInfo, int64_t modRevision)
{
    // publish to local observer
    auto observer = InstanceStateMachine::GetObserver();
    if (observer != nullptr) {
        YRLOG_DEBUG("{}|success to notify instance:{} state", newInstanceInfo.requestid(),
                    newInstanceInfo.instanceid());
        observer->PutInstanceEvent(newInstanceInfo, false, modRevision);
    } else {
        YRLOG_WARN("{}|failed to notify instance:{} state to observer", newInstanceInfo.requestid(),
                   newInstanceInfo.instanceid());
    }
}

// should be locked by caller
litebus::Future<TransitionResult> InstanceStateMachine::SaveInstanceInfoToMetaStore(
    const resources::InstanceInfo &newInstanceInfo, const resources::InstanceInfo &prevInstanceInfo,
    const InstanceState oldState, const TransContext &context)
{
    auto persistenceType = GetPersistenceType(newInstanceInfo, isMetaStoreEnable_);
    std::shared_ptr<StoreInfo> instancePutInfo;
    std::shared_ptr<StoreInfo> routePutInfo;
    std::string keyPath;
    if (TransToStoredData(newInstanceInfo, instancePutInfo, routePutInfo, persistenceType, keyPath).IsError()) {
        return TransitionResult{
            litebus::None(), {}, prevInstanceInfo, 0, Status(StatusCode::ERR_INSTANCE_INFO_INVALID)
        };
    }
    YRLOG_DEBUG(
        "put instance to meta store, instanceID: {}, function: {}, path: {}, status: {}, compare version({}), "
        "persistenceType: {}",
        newInstanceInfo.instanceid(), newInstanceInfo.function(), keyPath, newInstanceInfo.instancestatus().code(),
        context.version, fmt::underlying(persistenceType));
    ASSERT_IF_NULL(instanceOpt_);
    if (IsFirstPersistence(newInstanceInfo, oldState, context.version)) {
        return instanceOpt_->Create(instancePutInfo, routePutInfo, IsLowReliabilityInstance(newInstanceInfo))
            .Then([prevInstanceInfo, oldState, key(keyPath), newInstanceInfo,
                   instanceID(newInstanceInfo.instanceid()), context,
                   self(shared_from_this())](const OperateResult &result) -> litebus::Future<TransitionResult> {
                if (result.status.IsOk()) {
                    YRLOG_DEBUG("success to create instance for key({}), preKeyVersion is {}", key,
                                result.preKeyVersion);
                    if (context.persistence && oldState != context.newState) {
                        // only update after state changed
                        self->PublishToLocalObserver(newInstanceInfo, result.currentModRevision);
                    }
                    if (self->controlPlaneObserver_ != nullptr && !self->isWatching_.load()) {
                        self->controlPlaneObserver_->WatchInstance(instanceID, result.currentModRevision);
                        self->isWatching_.store(true);
                    }
                    return TransitionResult{ oldState,         {},
                                             prevInstanceInfo, result.preKeyVersion + 1,
                                             Status::OK(),     result.currentModRevision };
                }
                YRLOG_ERROR("fail to create instance for key({}), err: {}", key, result.status.ToString());

                InstanceInfo instanceInfoSaved;
                if (!TransToInstanceInfoFromJson(instanceInfoSaved, result.value)) {
                    YRLOG_ERROR("failed to trans to InstanceInfo from value, key: {}, value: {}", key, result.value);
                    return TransitionResult{ litebus::None(), {}, prevInstanceInfo, 0, result.status };
                }
                return TransitionResult{ litebus::None(), instanceInfoSaved, prevInstanceInfo, 0, result.status };
            });
    }
    return instanceOpt_->Modify(instancePutInfo, routePutInfo, context.version,
                                IsLowReliabilityInstance(newInstanceInfo))
        .Then([prevInstanceInfo, oldState, key(keyPath), self(shared_from_this()), version(context.version),
               instanceID(newInstanceInfo.instanceid()), context, newInstanceInfo,
               newState(newInstanceInfo.instancestatus().code()),
               errCode(newInstanceInfo.instancestatus().errcode())](const OperateResult &result) {
            if (result.status.IsOk()) {
                YRLOG_DEBUG("success to modify instance for key({}), preKeyVersion is {}", key, version);
                if (context.persistence && oldState != context.newState) {
                    // only update after state changed
                    self->PublishToLocalObserver(newInstanceInfo, result.currentModRevision);
                }
                if (self->controlPlaneObserver_ != nullptr && !self->isWatching_.load()) {
                    self->controlPlaneObserver_->WatchInstance(instanceID, result.currentModRevision);
                    self->isWatching_.store(true);
                }
                return TransitionResult{ oldState,    {},           prevInstanceInfo,
                                         version + 1, Status::OK(), result.currentModRevision };
            }
            YRLOG_ERROR("fail to modify instance for key({}), err: {}", key, result.status.ToString());
            auto lastFailedState = self->GetLastSaveFailedState();
            if (lastFailedState != static_cast<int32_t>(InstanceState::EXITED)) {
                YRLOG_DEBUG("key({}) last failed state({}), change to({})", key, lastFailedState, newState);
                self->SetLastSaveFailedState(newState, errCode);
            }
            InstanceInfo instanceInfoSaved;
            if (!TransToInstanceInfoFromJson(instanceInfoSaved, result.value)) {
                YRLOG_ERROR("failed to trans to InstanceInfo from json string, key: {}", key);
                return TransitionResult{ litebus::None(), {}, prevInstanceInfo, 0, result.status };
            }
            return TransitionResult{ litebus::None(), instanceInfoSaved, prevInstanceInfo, 0, result.status };
        });
}

Status InstanceStateMachine::TransToStoredKeys(const resources::InstanceInfo &instanceInfo,
                                               std::shared_ptr<StoreInfo> &instancePutInfo,
                                               std::shared_ptr<StoreInfo> &routePutInfo,
                                               const PersistenceType persistence, std::string &key)
{
    if ((persistence == PersistenceType::PERSISTENT_INSTANCE) || (persistence == PersistenceType::PERSISTENT_ALL)) {
        auto path = GenInstanceKey(instanceInfo.function(), instanceInfo.instanceid(), instanceInfo.requestid());
        if (path.IsNone()) {
            YRLOG_ERROR("failed to get instance key from InstanceInfo. instance({})", instanceInfo.instanceid());
            return Status(StatusCode::FAILED);
        }
        key += "(" + path.Get() + ")";
        instancePutInfo = std::make_shared<StoreInfo>(path.Get(), "");
    }
    if ((persistence == PersistenceType::PERSISTENT_ROUTE) || (persistence == PersistenceType::PERSISTENT_ALL)) {
        auto path = GenInstanceRouteKey(instanceInfo.instanceid());
        key += "(" + path + ")";
        routePutInfo = std::make_shared<StoreInfo>(path, "");
        return Status::OK();
    }
    return Status::OK();
}

Status InstanceStateMachine::TransToStoredData(const resources::InstanceInfo &instanceInfo,
                                               std::shared_ptr<StoreInfo> &instancePutInfo,
                                               std::shared_ptr<StoreInfo> &routePutInfo,
                                               const PersistenceType persistence, std::string &key)
{
    if ((persistence == PersistenceType::PERSISTENT_INSTANCE) || (persistence == PersistenceType::PERSISTENT_ALL)) {
        std::string jsonStr;
        litebus::Option<std::string> path;
        if (TransInstanceInfo(instanceInfo, jsonStr, path).IsError()) {
            return Status(StatusCode::FAILED);
        }
        key += "(" + path.Get() + ")";
        instancePutInfo = std::make_shared<StoreInfo>(path.Get(), jsonStr);
    }
    if ((persistence == PersistenceType::PERSISTENT_ROUTE) || (persistence == PersistenceType::PERSISTENT_ALL)) {
        std::string jsonStr;
        std::string path;
        if (TransRouteInfo(instanceInfo, jsonStr, path).IsError()) {
            return Status(StatusCode::FAILED);
        }
        key += "(" + path + ")";
        routePutInfo = std::make_shared<StoreInfo>(path, jsonStr);
    }
    return Status::OK();
}

Status InstanceStateMachine::TransInstanceInfo(const resources::InstanceInfo &instanceInfo, std::string &output,
                                               litebus::Option<std::string> &path)
{
    path = GenInstanceKey(instanceInfo.function(), instanceInfo.instanceid(), instanceInfo.requestid());
    if (path.IsNone()) {
        YRLOG_ERROR("failed to get instance key from InstanceInfo. instance({})", instanceInfo.instanceid());
        return Status(StatusCode::FAILED);
    }
    if (!TransToJsonFromInstanceInfo(output, instanceInfo)) {
        YRLOG_ERROR("failed to trans to json string from InstanceInfo. instance({})", instanceInfo.instanceid());
        return Status(StatusCode::FAILED);
    }
    return Status::OK();
}

Status InstanceStateMachine::TransRouteInfo(const resources::InstanceInfo &instanceInfo, std::string &output,
                                            std::string &path)
{
    path = GenInstanceRouteKey(instanceInfo.instanceid());
    resources::RouteInfo routeInfo;
    TransToRouteInfoFromInstanceInfo(instanceInfo, routeInfo);
    if (!TransToJsonFromRouteInfo(output, routeInfo)) {
        YRLOG_ERROR("failed to trans to json string from routeInfo. instance({})", instanceInfo.instanceid());
        return Status(StatusCode::FAILED);
    }
    return Status::OK();
}

void InstanceStateMachine::UpdateInstanceContext(const std::shared_ptr<InstanceContext> &context)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    instanceContext_ = context;
}

void InstanceStateMachine::UpdateOwner(const std::string &owner)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->UpdateOwner(owner);
}

void InstanceStateMachine::UpdateInstanceInfo(const InstanceInfo &instanceInfo)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->UpdateInstanceInfo(instanceInfo);
    ExecuteStateChangeCallback(instanceInfo.requestid(),
                               static_cast<InstanceState>(instanceInfo.instancestatus().code()));
}

std::string InstanceStateMachine::GetOwner()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    return instanceContext_->GetOwner();
}

void InstanceStateMachine::ReleaseOwner()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->UpdateOwner("");
}

resources::InstanceInfo InstanceStateMachine::GetInstanceInfo()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    return instanceContext_->GetInstanceInfo();
}

std::string InstanceStateMachine::GetRuntimeID()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    return instanceContext_->GetInstanceInfo().runtimeid();
}

InstanceStateMachine::~InstanceStateMachine()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    instanceContext_ = nullptr;
    instanceOpt_ = nullptr;
    stateChangeCallbacks_.clear();
}

litebus::Future<Status> InstanceStateMachine::TryExitInstance(const std::shared_ptr<litebus::Promise<Status>> &promise,
                                                              const std::shared_ptr<KillContext> &killCtx,
                                                              bool isSynchronized)
{
    if (instanceContext_ == nullptr) {
        YRLOG_ERROR("{}|instance({}) context can not find", killCtx->instanceContext->GetRequestID(),
                    killCtx->instanceContext->GetInstanceInfo().instanceid());
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND, "instance info can not find");
    }
    auto oldState = killCtx->instanceContext->GetState();
    if (oldState != GetInstanceState()) {
        YRLOG_WARN("{}|instance({}) state is inconsistent, origin state is ({}), current state is ({})",
                   killCtx->instanceContext->GetRequestID(), instanceID_, fmt::underlying(oldState),
                   fmt::underlying(GetInstanceState()));
        promise->SetValue(
            Status(StatusCode::ERR_INSTANCE_INFO_INVALID, "failed to exit instance, state is inconsistent"));
        return Status(StatusCode::ERR_INSTANCE_INFO_INVALID, "instance state is inconsistent");
    }

    if (GetInstanceState() == InstanceState::EXITING) {
        ExitInstance();
        YRLOG_INFO("instance({}) is exiting, exit instance directly.", instanceID_);
        if (!isSynchronized) {
            promise->SetValue(Status::OK());
        } else {
            promise->SetValue(Status(StatusCode::FAILED, "instance is exiting"));
        }
        return Status(StatusCode::ERR_INSTANCE_INFO_INVALID, "instance is exiting, not handle.");
    }
    YRLOG_INFO("try to exit instance({}) times({}), instance state({})", instanceID_, exitTimes_,
               fmt::underlying(oldState));
    exitTimes_++;
    auto transContext = TransContext{ InstanceState::EXITING, GetVersion(), "exiting" };
    transContext.scheduleReq = killCtx->instanceContext->GetScheduleRequest();
    return TransitionTo(transContext)
        .Then([owner(owner_), promise, exitHandler(exitHandler_), exitFailedHandler(exitFailedHandler_),
               instanceInfo(GetInstanceInfo()), oldState, isSynchronized,
               exitTimes(exitTimes_)](const TransitionResult &result) {
            if (result.version == 0 && exitTimes <= MAX_EXIT_TIMES) {
                exitFailedHandler(result);
                promise->SetValue(Status(StatusCode::ERR_ETCD_OPERATION_ERROR,
                                         "failed to transition to exiting, err: " + result.status.GetMessage()));
                return Status(StatusCode::ERR_ETCD_OPERATION_ERROR,
                              "failed to transition to exiting, err: " + result.status.GetMessage());
            }

            if (exitHandler != nullptr) {
                (void)exitHandler(instanceInfo).OnComplete([promise, isSynchronized]() {
                    if (isSynchronized) {
                        promise->SetValue(Status::OK());
                    }
                });
            } else {
                YRLOG_WARN("failed to exit instance, exit handler is null");
                promise->SetValue(
                    Status(StatusCode::ERR_STATE_MACHINE_ERROR, "failed to exit instance, exit handler is null"));
                return Status(StatusCode::ERR_STATE_MACHINE_ERROR, "failed to exit instance, exit handler is null");
            }

            if (!isSynchronized) {
                promise->SetValue(Status::OK());
            }
            return Status(StatusCode::SUCCESS);
        });
}

void InstanceStateMachine::ExitInstance()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    if (exitHandler_ != nullptr) {
        auto instanceInfo = instanceContext_->GetInstanceInfo();
        (void)exitHandler_(instanceInfo);
    } else {
        YRLOG_ERROR("failed to exit instance, exit handler is null");
    }
}

void InstanceStateMachine::AddStateChangeCallback(const std::unordered_set<InstanceState> &statesConcerned,
                                                  const std::function<void(const resources::InstanceInfo &)> &callback,
                                                  const std::string &eventKey)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    auto nowState = instanceContext_->GetState();
    if (statesConcerned.find(nowState) != statesConcerned.end()) {
        callback(instanceContext_->GetInstanceInfo());
        return;
    }

    auto key = eventKey;
    if (eventKey.empty()) {
        key = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    }
    // make sure every event key only has one callback
    (void)stateChangeCallbacks_.emplace(key, StateChangeCallback{ statesConcerned, callback });
}

void InstanceStateMachine::DeleteStateChangeCallback(const std::string &eventKey)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    if (eventKey.empty()) {
        return;
    }

    stateChangeCallbacks_.erase(eventKey);
}

bool InstanceStateMachine::HasStateChangeCallback(const std::string &eventKey)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    if (eventKey.empty()) {
        return false;
    }

    if (stateChangeCallbacks_.find(eventKey) == stateChangeCallbacks_.end()) {
        return false;
    }

    return true;
}

InstanceState InstanceStateMachine::GetInstanceState()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    return instanceContext_->GetState();
}

std::shared_ptr<messages::ScheduleRequest> InstanceStateMachine::GetScheduleRequest()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    return instanceContext_->GetScheduleRequestCopy();
}

std::shared_ptr<InstanceContext> InstanceStateMachine::GetInstanceContextCopy()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    std::shared_ptr<messages::ScheduleRequest> scheduleRequest = instanceContext_->GetScheduleRequestCopy();
    return std::make_shared<InstanceContext>(scheduleRequest);
}

void InstanceStateMachine::ExecuteStateChangeCallback(const std::string &requestID, const InstanceState newState)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    auto callbackIter = stateChangeCallbacks_.begin();
    auto instanceInfo = instanceContext_->GetInstanceInfo();
    while (callbackIter != stateChangeCallbacks_.end()) {
        if (callbackIter->second.statesConcerned.find(newState) == callbackIter->second.statesConcerned.end()) {
            (void)callbackIter++;
            continue;
        }
        YRLOG_INFO("{}|transition instance({}) state to ({}), to execute callback", requestID, instanceID_,
                   fmt::underlying(newState));
        callbackIter->second.callback(instanceInfo);
        callbackIter = stateChangeCallbacks_.erase(callbackIter);
    }
}
std::string InstanceStateMachine::GetRequestID()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    return instanceContext_->GetRequestID();
}

void InstanceStateMachine::SetScheduleTimes(const int32_t &scheduleTimes)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->SetScheduleTimes(scheduleTimes);
}

void InstanceStateMachine::SetDeployTimes(const int32_t &deployTimes)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->SetDeployTimes(deployTimes);
}

int32_t InstanceStateMachine::GetScheduleTimes()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    return instanceContext_->GetScheduleTimes();
}

int32_t InstanceStateMachine::GetDeployTimes()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    return instanceContext_->GetDeployTimes();
}

void InstanceStateMachine::SetFunctionAgentIDAndHeteroConfig(const ScheduleResult &result)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->SetFunctionAgentIDAndHeteroConfig(result);
}

void InstanceStateMachine::SetRuntimeID(const std::string &runtimeID)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->SetRuntimeID(runtimeID);
}

void InstanceStateMachine::SetStartTime(const std::string &timeInfo)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->SetStartTime(timeInfo);
}

void InstanceStateMachine::SetRuntimeAddress(const std::string &address)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->SetRuntimeAddress(address);
}

void InstanceStateMachine::IncreaseScheduleRound()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->IncreaseScheduleRound();
}

uint32_t InstanceStateMachine::GetScheduleRound()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    return instanceContext_->GetScheduleRound();
}

void InstanceStateMachine::SetCheckpointed(const bool flag)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->SetCheckpointed(flag);
}

void InstanceStateMachine::SetVersion(const int64_t version)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->SetVersion(version);
}

int64_t InstanceStateMachine::GetVersion()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    return instanceContext_->GetVersion();
}

void InstanceStateMachine::SetLocalAbnormal()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    isLocalAbnormal_ = true;
}

void InstanceStateMachine::SetDataSystemHost(const std::string &ip)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->SetDataSystemHost(ip);
}

std::string InstanceStateMachine::Information()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    if (!instanceContext_) {
        return "";
    }
    auto instance = instanceContext_->GetInstanceInfo();
    std::string info = "Instance(" + instanceID_ + ") ";
    if (!instance.runtimeid().empty()) {
        info += "runtimeID(" + instance.runtimeid() + ") ";
    }
    if (!instance.functionproxyid().empty()) {
        info += "on Node(" + instance.functionproxyid() + ") ";
    }
    if (!instance.functionagentid().empty()) {
        info += "of agent(" + instance.functionagentid() + ") ";
    }
    return info;
}

void InstanceStateMachine::NewSavingPomise()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    // isInit mean the instance is persisting
    savePromise_ = std::make_shared<litebus::Promise<bool>>();
}

void InstanceStateMachine::TagSaved()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    if (savePromise_ != nullptr) {
        savePromise_->SetValue(true);
        savePromise_ = nullptr;
    }
}

litebus::Future<bool> InstanceStateMachine::GetSavingFuture()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    if (savePromise_ == nullptr) {
        return false;
    }
    return savePromise_->GetFuture();
}

bool InstanceStateMachine::IsSaving()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    if (savePromise_ == nullptr) {
        return false;
    }
    return savePromise_->GetFuture().IsInit();
}

int64_t InstanceStateMachine::GetGracefulShutdownTime()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    return instanceContext_->GetGracefulShutdownTime();
}

void InstanceStateMachine::SetGracefulShutdownTime(const int64_t time)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->SetGracefulShutdownTime(time);
}

void InstanceStateMachine::SetTraceID(const std::string &traceID)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->SetTraceID(traceID);
}

int32_t InstanceStateMachine::GetLastSaveFailedState()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    return lastSaveFailedState_.first;
}

int32_t InstanceStateMachine::GetLastSaveFailedErrCode()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    return lastSaveFailedState_.second;
}

void InstanceStateMachine::ResetLastSaveFailedState()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    lastSaveFailedState_ = { INVALID_LAST_SAVE_FAILED_STATE, INVALID_LAST_SAVE_FAILED_STATE };
}

void InstanceStateMachine::SetLastSaveFailedState(int32_t state, int32_t errorCode)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    lastSaveFailedState_ = { state, errorCode };
}

litebus::Future<resources::InstanceInfo> InstanceStateMachine::SyncInstanceFromMetaStore()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    litebus::Promise<resources::InstanceInfo> promise;
    auto instanceInfo = instanceContext_->GetInstanceInfo();
    auto key = GenInstanceKey(instanceInfo.function(), instanceInfo.instanceid(), instanceInfo.requestid());
    if (key.IsNone()) {
        YRLOG_WARN("failed to sync instance({}), failed to get instance key from InstanceInfo",
                   instanceInfo.instanceid());
        promise.SetFailed(static_cast<int32_t>(StatusCode::PARAMETER_ERROR));
        return promise.GetFuture();
    }
    instanceOpt_->GetInstance(key.Get()).OnComplete(
        [promise, instanceID(instanceInfo.instanceid()), key](const litebus::Future<OperateResult> &operateResult) {
            if (operateResult.IsError() || operateResult.Get().status.IsError()) {
                YRLOG_ERROR("failed to sync instance({}), failed to get instance from meta store", instanceID);
                promise.SetFailed(static_cast<int32_t>(StatusCode::ERR_ETCD_OPERATION_ERROR));
                return;
            }

            InstanceInfo instanceInfo;
            if (!TransToInstanceInfoFromJson(instanceInfo, operateResult.Get().value)) {
                YRLOG_ERROR("failed to trans to InstanceInfo from json string, key: {}", key.Get());
                promise.SetFailed(static_cast<int32_t>(StatusCode::ERR_ETCD_OPERATION_ERROR));
                return;
            }
            promise.SetValue(instanceInfo);
        });
    return promise.GetFuture();
}

void InstanceStateMachine::SetUpdateByRouteInfo()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    isUpdateByRouteInfo_ = true;
}

bool InstanceStateMachine::GetUpdateByRouteInfo()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    return isUpdateByRouteInfo_;
}

void InstanceStateMachine::SetInstanceBillingTerminated(const std::string &instanceID, const InstanceState &newState)
{
    if (newState == InstanceState::FATAL || newState == InstanceState::FAILED) {
        YRLOG_DEBUG("Status {} instance {}, set billing terminated", fmt::underlying(newState), instanceID);
        metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetBillingInstanceEndTime(
            instanceID,
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());
    }
}

void InstanceStateMachine::PublishDeleteToLocalObserver(const std::string &instanceID)
{
    // publish to local observer
    auto observer = InstanceStateMachine::GetObserver();
    if (observer != nullptr) {
        YRLOG_DEBUG("success to notify instance({}) delete", instanceID);
        observer->DelInstanceEvent(instanceID, GetModRevision());
    } else {
        YRLOG_WARN("failed to notify instance({}) delete to observer", instanceID);
    }
}

bool InstanceStateMachine::IsFirstPersistence(const InstanceInfo &newInstanceInfo, const InstanceState &oldState,
                                              const int64_t version) const
{
    return oldState == InstanceState::NEW ||
           // for group schedule we only need to persist creating
           (oldState == InstanceState::SCHEDULING && !newInstanceInfo.groupid().empty() && version == 0);
}

void InstanceStateMachine::TagStop()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    instanceContext_->TagStop();
}

bool InstanceStateMachine::IsStopped()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    return instanceContext_->IsStopped();
}

void InstanceStateMachine::SetModRevision(const int64_t modRevision)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    instanceContext_->SetModRevision(modRevision);
}

int64_t InstanceStateMachine::GetModRevision()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    return instanceContext_->GetModRevision();
}

litebus::Future<std::string> InstanceStateMachine::GetCancelFuture()
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    ASSERT_IF_NULL(instanceContext_);
    return instanceContext_->GetCancelFuture();
}

void InstanceStateMachine::SetCancel(const std::string &reason)
{
    std::lock_guard<std::recursive_mutex> guard(lock_);
    RETURN_IF_NULL(instanceContext_);
    instanceContext_->SetCancel(reason);
}
}  // namespace functionsystem
