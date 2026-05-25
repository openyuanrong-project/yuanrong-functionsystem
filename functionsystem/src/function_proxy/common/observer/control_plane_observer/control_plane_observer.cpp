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

#include "control_plane_observer.h"

#include <async/async.hpp>

#include "common/constants/actor_name.h"
#include "common/logs/logging.h"

namespace functionsystem::function_proxy {

ControlPlaneObserver::ControlPlaneObserver(const std::shared_ptr<ObserverActor> &actor) : observerActor_(actor)
{
    if (observerActor_ == nullptr) {
        return;
    }
    YRLOG_DEBUG("construct a observer actor : {}", std::string(observerActor_->GetAID()));
}

ControlPlaneObserver::~ControlPlaneObserver() noexcept
{
    if (observerActor_ == nullptr) {
        return;
    }
    litebus::Terminate(observerActor_->GetAID());
    litebus::Await(observerActor_->GetAID());
}

void ControlPlaneObserver::SetDriverEventCbFunc(const DriverEventCbFunc &driverCbFunc)
{
    ASSERT_IF_NULL(observerActor_);
    litebus::Async(observerActor_->GetAID(), &ObserverActor::SetDriverEventCbFunc, driverCbFunc);
}

void ControlPlaneObserver::SetInstanceInfoSyncerCbFunc(const InstanceInfoSyncerCbFunc &instanceInfoSyncerCbFunc)
{
    ASSERT_IF_NULL(observerActor_);
    litebus::Async(observerActor_->GetAID(), &ObserverActor::SetInstanceInfoSyncerFunc, instanceInfoSyncerCbFunc);
}

void ControlPlaneObserver::SetUpdateFuncMetasFunc(const UpdateFuncMetasFunc &updateFuncMetasFunc)
{
    ASSERT_IF_NULL(observerActor_);
    litebus::Async(observerActor_->GetAID(), &ObserverActor::SetUpdateFuncMetasFunc, updateFuncMetasFunc);
}

void ControlPlaneObserver::SetTrafficReportCbFunc(const TrafficReportCbFunc &trafficReportCbFunc)
{
    ASSERT_IF_NULL(observerActor_);
    litebus::Async(observerActor_->GetAID(), &ObserverActor::SetTrafficReportCbFunc, trafficReportCbFunc);
}

void ControlPlaneObserver::Register() const
{
    ASSERT_IF_NULL(observerActor_);
    (void)litebus::Async(observerActor_->GetAID(), &ObserverActor::Register);
}

litebus::Future<Status> ControlPlaneObserver::PutInstance(const resource_view::InstanceInfo &instanceInfo) const
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::PutInstance, instanceInfo, false);
}

litebus::Future<Status> ControlPlaneObserver::DelInstance(const std::string &instanceID) const
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::DelInstance, instanceID);
}

litebus::Future<litebus::Option<resource_view::InstanceInfo>> ControlPlaneObserver::GetInstanceInfoByID(
    const std::string &instanceID) const
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::GetInstanceInfoByID, instanceID);
}

litebus::Future<litebus::Option<InstanceInfoMap>> ControlPlaneObserver::GetAgentInstanceInfoByID(
    const std::string &funcAgentID) const
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::GetAgentInstanceInfoByID, funcAgentID);
}

litebus::Future<litebus::Option<FunctionMeta>> ControlPlaneObserver::GetFuncMeta(const std::string &funcKey) const
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::GetFuncMeta, funcKey);
}

litebus::Future<litebus::Option<litebus::AID>> ControlPlaneObserver::GetLocalSchedulerAID(
    const std::string &proxyID) const
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::GetLocalSchedulerAID, proxyID);
}

litebus::Future<litebus::Option<InstanceInfoMap>> ControlPlaneObserver::GetLocalInstanceInfo() const
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::GetLocalInstanceInfo);
}

litebus::Future<InstanceInfoMap> ControlPlaneObserver::GetAllInstanceInfos() const
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::GetAllInstanceInfos);
}

litebus::Future<bool> ControlPlaneObserver::IsSystemFunction(const std::string &function) const
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::IsSystemFunction, function);
}

void ControlPlaneObserver::Attach(const std::shared_ptr<InstanceListener> &listener) const
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::Attach, listener);
}

void ControlPlaneObserver::Detach(const std::shared_ptr<InstanceListener> &listener) const
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::Detach, listener);
}

litebus::Future<Status> ControlPlaneObserver::PutInstanceEvent(const resource_view::InstanceInfo &instanceInfo,
                                                               bool synced, int64_t modRevision)
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::PutInstanceEvent, instanceInfo, synced,
                          modRevision);
}

void ControlPlaneObserver::FastPutRemoteInstanceEvent(const resource_view::InstanceInfo &instanceInfo, bool synced,
                                                      int64_t modRevision)
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::FastPutRemoteInstanceEvent, instanceInfo, synced,
                          modRevision);
}

litebus::Future<Status> ControlPlaneObserver::DelInstanceEvent(const std::string &instanceID, int64_t modRevision)
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::DelInstanceEvent, instanceID, modRevision);
}

litebus::Future<std::vector<std::string>> ControlPlaneObserver::GetLocalInstances(
    const std::function<bool(const resource_view::InstanceInfo &)> &filter)
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::GetLocalInstances, filter);
}

litebus::Future<bool> ControlPlaneObserver::InstanceSyncDone()
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::InstanceSyncDone);
}

void ControlPlaneObserver::AttachTenantListener(const std::shared_ptr<TenantListener> &listener)
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::AttachTenantListener, listener);
}

void ControlPlaneObserver::DetachTenantListener(const std::shared_ptr<TenantListener> &listener)
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::DetachTenantListener, listener);
}

void ControlPlaneObserver::NotifyUpdateTenantInstance(const TenantEvent &event)
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::NotifyUpdateTenantInstance, event);
}

void ControlPlaneObserver::NotifyDeleteTenantInstance(const TenantEvent &event)
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::NotifyDeleteTenantInstance, event);
}

void ControlPlaneObserver::WatchInstance(const std::string &instanceID, int64_t revision)
{
    ASSERT_IF_NULL(observerActor_);
    litebus::Async(observerActor_->GetAID(), &ObserverActor::WatchInstance, instanceID, revision);
}

litebus::Future<bool> ControlPlaneObserver::IsInstanceWatched(const std::string &instanceID)
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::IsInstanceWatched, instanceID);
}

litebus::Future<resource_view::InstanceInfo> ControlPlaneObserver::GetAndWatchInstance(const std::string &instanceID)
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::GetAndWatchInstance, instanceID);
}

litebus::Future<resource_view::InstanceInfo> ControlPlaneObserver::GetOrWatchInstance(const std::string &instanceID)
{
    ASSERT_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::GetOrWatchInstance, instanceID);
}

void ControlPlaneObserver::CancelWatchInstance(const std::string &instanceID)
{
    ASSERT_IF_NULL(observerActor_);
    litebus::Async(observerActor_->GetAID(), &ObserverActor::CancelWatchInstance, instanceID);
}

}  // namespace functionsystem::function_proxy
