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

#include "instance_view.h"

#include "async/async.hpp"
#include "async/collect.hpp"
#include "async/defer.hpp"
#include "common/communication/proxy/client.h"
#include "common/constants/constants.h"
#include "common/logs/logging.h"
#include "common/proto/pb/posix/resource.pb.h"
#include "common/state_machine/instance_context.h"
#include "common/utils/struct_transfer.h"

namespace functionsystem::busproxy {
using IsReady = bool;
const std::map<InstanceState, IsReady> STATUS_READY = {
    { InstanceState::NEW, false },
    { InstanceState::SCHEDULING, false },
    { InstanceState::CREATING, false },
    { InstanceState::RUNNING, true },
    { InstanceState::FAILED, false },
    { InstanceState::EXITING, false },
    { InstanceState::FATAL, false },
    // rely on reject tag
    // while instance change suspend to creating, need to keep request in flight
    { InstanceState::SUSPEND, true },
};

const int32_t INT_SIGNAL = 2;
const int32_t KILL_SIGNAL = 9;

bool IsReadyStatus(InstanceState status)
{
    if (STATUS_READY.find(status) == STATUS_READY.end()) {
        return false;
    }
    return STATUS_READY.at(status);
}

std::shared_ptr<InstanceRouterInfo> TransferInstanceInfo(const resources::InstanceInfo &instanceInfo,
                                                         const std::string &currentNode)
{
    auto info = std::make_shared<InstanceRouterInfo>();
    info->isReady = IsReadyStatus((InstanceState)instanceInfo.instancestatus().code());
    info->isLocal = instanceInfo.functionproxyid() == currentNode;
    info->runtimeID = instanceInfo.runtimeid();
    info->proxyID = instanceInfo.functionproxyid();
    info->tenantID = instanceInfo.tenantid();
    info->function = instanceInfo.function();
    if (info->isLocal && info->isReady) {
        info->trafficReportType = instanceInfo.trafficreporttype();
    }
    return info;
}

InstanceView::InstanceView(const std::string &nodeID) : nodeID_(nodeID)
{
    eventHandlers_ = {
        { InstanceState::NEW,
          std::bind(&InstanceView::ReadyStatusChanged, this, std::placeholders::_1, std::placeholders::_2) },
        { InstanceState::SCHEDULING,
          std::bind(&InstanceView::ReadyStatusChanged, this, std::placeholders::_1, std::placeholders::_2) },
        { InstanceState::CREATING,
          std::bind(&InstanceView::Creating, this, std::placeholders::_1, std::placeholders::_2) },
        { InstanceState::RUNNING,
          std::bind(&InstanceView::Running, this, std::placeholders::_1, std::placeholders::_2) },
        { InstanceState::FAILED,
          std::bind(&InstanceView::ReadyStatusChanged, this, std::placeholders::_1, std::placeholders::_2) },
        { InstanceState::EXITING,
          std::bind(&InstanceView::ReadyStatusChanged, this, std::placeholders::_1, std::placeholders::_2) },
        { InstanceState::EVICTING,
          std::bind(&InstanceView::Reject, this, std::placeholders::_1, std::placeholders::_2) },
        { InstanceState::FATAL, std::bind(&InstanceView::Fatal, this, std::placeholders::_1, std::placeholders::_2) },
        { InstanceState::EVICTED, std::bind(&InstanceView::Fatal, this, std::placeholders::_1, std::placeholders::_2) },
        { InstanceState::SUB_HEALTH,
          std::bind(&InstanceView::Reject, this, std::placeholders::_1, std::placeholders::_2) },
        { InstanceState::SUSPEND,
          std::bind(&InstanceView::Reject, this, std::placeholders::_1, std::placeholders::_2) },
    };
}

InstanceView::~InstanceView()
{
    for (auto &instance : localInstances_) {
        litebus::Terminate(instance.second->GetAID());
        litebus::Await(instance.second);
    }
}

void InstanceView::Update(const std::string &instanceID, const resources::InstanceInfo &instanceInfo,
                          bool isForceUpdate)
{
    if (allInstances_.find(instanceID) == allInstances_.end()) {
        allInstances_[instanceID] = instanceInfo;
    }
    // When the instance information is published through the local fast channel, the version of the instance
    // information is later than that of the event received from etcd.
    if (allInstances_[instanceID].version() > instanceInfo.version() && !isForceUpdate) {
        YRLOG_INFO("instance ({}) has already been received an higher version info. local({}) received({})", instanceID,
                   allInstances_[instanceID].version(), instanceInfo.version());
        return;
    }
    // instance should be subscribed by local parent
    const auto &parentID = instanceInfo.parentid();
    if (auto iter(localInstances_.find(parentID)); iter != localInstances_.end()) {
        if (subscribedInstances_.find(instanceID) == subscribedInstances_.end() ||
            subscribedInstances_[instanceID].find(parentID) == subscribedInstances_[instanceID].end()) {
            auto routeInfo = TransferInstanceInfo(instanceInfo, nodeID_);
            (void)litebus::Async(localInstances_[parentID]->GetAID(), &InstanceProxy::NotifyChanged, instanceID,
                                 routeInfo);
        }
        (void)SubscribeInstanceEvent(parentID, instanceID);
    }
    auto status = static_cast<InstanceState>(instanceInfo.instancestatus().code());
    YRLOG_DEBUG("instance view Update instance, instanceID: {}, status: {}, proxyID: {},  nodeID:{}, handler {}",
                instanceID, fmt::underlying(status), instanceInfo.functionproxyid(), nodeID_, eventHandlers_.size());
    if (auto iter(eventHandlers_.find(status)); iter != eventHandlers_.end()) {
        iter->second(instanceID, instanceInfo);
    }
    allInstances_[instanceID] = instanceInfo;
}

void InstanceView::Delete(const std::string &instanceID, int64_t)
{
    YRLOG_DEBUG("instance view delete instance({})", instanceID);
    (void)allInstances_.erase(instanceID);
    // delete local instance proxy
    if (localInstances_.find(instanceID) != localInstances_.end()) {
        auto instanceProxy = localInstances_[instanceID];
        (void)litebus::Async(instanceProxy->GetAID(), &InstanceProxy::Delete).OnComplete([instanceProxy]() {
            litebus::Terminate(instanceProxy->GetAID());
        });
        (void)localInstances_.erase(instanceID);
    }

    // delete subscribed info of instanceID
    if (subscribers_.find(instanceID) != subscribers_.end()) {
        for (const auto &subscribed : subscribers_[instanceID]) {
            (void)subscribedInstances_[subscribed].erase(instanceID);
        }
        (void)subscribers_.erase(instanceID);
    }

    // delete info of who subscribed instanceID
    if (subscribedInstances_.find(instanceID) == subscribedInstances_.end()) {
        return;
    }
    for (const auto &subscriber : subscribedInstances_[instanceID]) {
        if (localInstances_.find(subscriber) != localInstances_.end()) {
            (void)litebus::Async(localInstances_[subscriber]->GetAID(), &InstanceProxy::DeleteRemoteDispatcher,
                                 instanceID);
        }
        (void)subscribers_[subscriber].erase(instanceID);
    }
    (void)subscribedInstances_.erase(instanceID);
}

Status InstanceView::SubscribeInstanceEvent(const std::string &subscriber, const std::string &targetInstance,
                                            bool ignoreNonExist)
{
    if (subscribers_.find(subscriber) != subscribers_.end() &&
        subscribers_[subscriber].find(targetInstance) != subscribers_[subscriber].end()) {
        return Status::OK();
    }
    auto instance = allInstances_.find(targetInstance);
    if (instance == allInstances_.end()) {
        YRLOG_WARN("failed to subscribe target ({}) which is not found.", targetInstance);
        // remote dispatcher may be updated, skip delete when ignoreNonExist == true
        if (auto iter(localInstances_.find(subscriber)); iter != localInstances_.end() && !ignoreNonExist) {
            litebus::Async(iter->second->GetAID(), &InstanceProxy::Fatal, targetInstance, "instance not exist",
                           StatusCode::ERR_INSTANCE_NOT_FOUND);
            litebus::Async(iter->second->GetAID(), &InstanceProxy::DeleteRemoteDispatcher, targetInstance);
        }
        return Status::OK();
    }
    if (allInstances_.find(subscriber) == allInstances_.end()) {
        YRLOG_WARN("subscriber ({}) is already deleted, ignore the subscribe ({})", subscriber, targetInstance);
        return Status(StatusCode::ERR_INSTANCE_EXITED, "subscribe instance is not existed");
    }
    YRLOG_INFO("instance ({}) subscribe target ({})", subscriber, targetInstance);
    (void)subscribedInstances_[targetInstance].insert(subscriber);
    (void)subscribers_[subscriber].insert(targetInstance);
    if (instance->second.instancestatus().code() == static_cast<int32_t>(InstanceState::RUNNING)) {
        NotifySubscriberInstanceReady(targetInstance, instance->second);
    }
    if (instance->second.instancestatus().code() == static_cast<int32_t>(InstanceState::EVICTING)) {
        auto routeInfo = TransferInstanceInfo(instance->second, nodeID_);
        routeInfo->isLocal = false;
        auto instanceProxy = localInstances_[subscriber];
        if (instanceProxy == nullptr) {
            YRLOG_ERROR("instance ({}) subscribe target ({}), but instanceProxy is null", subscriber, targetInstance);
            return Status(StatusCode::POINTER_IS_NULL, "instanceProxy is null for subscriber: " + subscriber);
        }
        NotifyChanged(instanceProxy->GetAID(), targetInstance, instance->second.functionproxyid(), routeInfo);
    }
    // while subscribed an already fatal or evicted instance
    if (instance->second.instancestatus().code() == static_cast<int32_t>(InstanceState::FATAL) ||
        instance->second.instancestatus().code() == static_cast<int32_t>(InstanceState::EVICTED)) {
        YRLOG_WARN("instance ({}) subscribe target ({}) which is already failed with status({})", subscriber,
                   targetInstance, instance->second.instancestatus().code());
        auto errCode = instance->second.instancestatus().errcode();
        auto msg = instance->second.instancestatus().msg();
        auto instanceProxy = localInstances_[subscriber];
        if (instanceProxy == nullptr) {
            YRLOG_ERROR("instance ({}) subscribe target ({}), but instanceProxy is null", subscriber, targetInstance);
            return Status(StatusCode::POINTER_IS_NULL, "instanceProxy is null for subscriber: " + subscriber);
        }
        litebus::Async(instanceProxy->GetAID(), &InstanceProxy::Fatal, targetInstance, msg,
                       static_cast<StatusCode>(errCode));
    }
    return Status::OK();
}

void InstanceView::Creating(const std::string &instanceID, const resources::InstanceInfo &instanceInfo)
{
    SpawnInstanceProxy(instanceID, instanceInfo);
    ReadyStatusChanged(instanceID, instanceInfo);
}

void InstanceView::Running(const std::string &instanceID, const resources::InstanceInfo &instanceInfo)
{
    SpawnInstanceProxy(instanceID, instanceInfo);
    NotifyReady(instanceID, instanceInfo);
}

void InstanceView::Fatal(const std::string &instanceID, const resources::InstanceInfo &instanceInfo)
{
    auto errCode = instanceInfo.instancestatus().errcode();
    auto msg = instanceInfo.instancestatus().msg();
    auto proxyID = instanceInfo.functionproxyid();
    YRLOG_INFO("instance({}) is fatal owned ({}), errcode({}), msg({})", instanceID, proxyID, errCode, msg);
    if (auto iter(localInstances_.find(instanceID)); iter != localInstances_.end()) {
        litebus::Async(iter->second->GetAID(), &InstanceProxy::Fatal, instanceID, msg,
                       static_cast<StatusCode>(errCode));
    }
    // notify subscriber
    for (const auto &subscriber : subscribedInstances_[instanceID]) {
        if (localInstances_.find(subscriber) != localInstances_.end() && localInstances_[subscriber] != nullptr) {
            auto instanceProxy = localInstances_[subscriber];
            ASSERT_IF_NULL(instanceProxy);
            litebus::Async(instanceProxy->GetAID(), &InstanceProxy::Fatal, instanceID, msg,
                           static_cast<StatusCode>(errCode));
        }
    }
}

void InstanceView::SpawnInstanceProxy(const std::string &instanceID, const resources::InstanceInfo &instanceInfo)
{
    const auto &functionProxyID = instanceInfo.functionproxyid();
    if (functionProxyID == nodeID_ && localInstances_.find(instanceID) == localInstances_.end()) {
        auto instanceProxy = std::make_shared<InstanceProxy>(instanceID, instanceInfo.tenantid());
        YRLOG_INFO("instance view add local instance, instanceID: {}", instanceID);
        localInstances_[instanceID] = instanceProxy;
        instanceProxy->InitDispatcher();
        auto shared = true;
        if (IsFrontendFunction(instanceInfo.function())) {
            YRLOG_INFO("faasfrontend instance({}) proxy spawn to occupy single thread", instanceID);
            shared = false;
        }

        (void)litebus::Spawn(instanceProxy, shared);
    }
}

void InstanceView::ReadyStatusChanged(const std::string &instanceID, const resources::InstanceInfo &instanceInfo)
{
    bool previousIsReady = IsReadyStatus((InstanceState)allInstances_[instanceID].instancestatus().code());
    if (!previousIsReady) {
        return;
    }
    auto routeInfo = TransferInstanceInfo(instanceInfo, nodeID_);
    for (const auto &subscriber : subscribedInstances_[instanceID]) {
        auto instanceProxy = localInstances_[subscriber];
        ASSERT_IF_NULL(instanceProxy);
        NotifyChanged(instanceProxy->GetAID(), instanceID, instanceInfo.functionproxyid(), routeInfo);
    }

    if (auto iter(localInstances_.find(instanceID)); iter != localInstances_.end()) {
        NotifyChanged(iter->second->GetAID(), instanceID, instanceInfo.functionproxyid(), routeInfo);
    }
}

void InstanceView::NotifyReady(const std::string &instanceID, const resources::InstanceInfo &instanceInfo)
{
    const auto &functionProxyID = instanceInfo.functionproxyid();
    if (functionProxyID == nodeID_) {
        auto instanceProxy = localInstances_[instanceID];
        const auto &address = instanceInfo.runtimeaddress();
        RETURN_IF_NULL(dataInterfaceClientManager_);
        (void)dataInterfaceClientManager_->NewDataInterfacePosixClient(instanceID, instanceInfo.runtimeid(), address)
            .Then([instanceProxy, nodeID(nodeID_), instanceID, address,
                   instanceInfo](const std::shared_ptr<DataInterfacePosixClient> &dataInterfacePosix) {
                if (dataInterfacePosix == nullptr) {
                    YRLOG_ERROR("failed to create data interface posix client for {}, runtime {}, address {}.",
                                instanceID, instanceInfo.runtimeid(), address);
                    return Status::OK();
                }
                auto routeInfo = TransferInstanceInfo(instanceInfo, nodeID);
                routeInfo->localClient = dataInterfacePosix;
                ASSERT_IF_NULL(instanceProxy);
                YRLOG_DEBUG("update data interface posix client for {}, runtime {}, address {}.", instanceID,
                            instanceInfo.runtimeid(), address);
                litebus::Async(instanceProxy->GetAID(), &InstanceProxy::NotifyChanged, instanceID, routeInfo);
                return Status::OK();
            });
    }
    return NotifySubscriberInstanceReady(instanceID, instanceInfo);
}

void InstanceView::NotifyChanged(const litebus::AID &aid, const std::string &instanceID,
                                 const std::string &functionProxyID,
                                 const std::shared_ptr<InstanceRouterInfo> &routeInfo)
{
    RETURN_IF_NULL(routeInfo);
    auto updateCbFunc = [aid, instanceID, routeInfo](const std::shared_ptr<proxy::Client> &client) -> void {
        ASSERT_IF_NULL(client);
        routeInfo->remote = litebus::AID(instanceID, client->GetDstAddress());
        litebus::Async(aid, &InstanceProxy::NotifyChanged, instanceID, routeInfo);
    };

    if (functionProxyID.empty() || functionProxyID == nodeID_) {
        YRLOG_DEBUG("empty functionProxyID or instance is local({}), notify instance({}) change directly",
                    functionProxyID == nodeID_, instanceID);
        routeInfo->remote = litebus::AID(instanceID, aid.Url());
        litebus::Async(aid, &InstanceProxy::NotifyChanged, instanceID, routeInfo);
        return;
    }

    ASSERT_FS(proxyView_);
    auto proxyRPC = proxyView_->Get(functionProxyID);
    if (proxyRPC == nullptr) {
        YRLOG_ERROR("failed to get proxy RPC of {} for instance({}).", functionProxyID, instanceID);
        proxyView_->SetUpdateCbFunc(functionProxyID, updateCbFunc);
        return;
    }
    updateCbFunc(proxyRPC);
}

void InstanceView::NotifySubscriberInstanceReady(const std::string &instanceID,
                                                 const resources::InstanceInfo &instanceInfo)
{
    const auto &functionProxyID = instanceInfo.functionproxyid();
    // The subscriber considers that the instance of the called instance is on the remote end,
    // preventing the loss of the corresponding request that the subscriber has received.
    auto routeInfo = TransferInstanceInfo(instanceInfo, nodeID_);
    routeInfo->isLocal = false;
    for (const auto &subscriber : subscribedInstances_[instanceID]) {
        if (localInstances_.find(subscriber) == localInstances_.end()) {
            continue;
        }
        auto instanceProxy = localInstances_[subscriber];
        ASSERT_IF_NULL(instanceProxy);
        NotifyChanged(instanceProxy->GetAID(), instanceID, functionProxyID, routeInfo);
    }
    // If the running instance is not on the local node but the corresponding instance proxy exists on the local node,
    // change should be notified to that instance proxy in order to migrating cache request
    if (functionProxyID == nodeID_) {
        return;
    }
    if (auto iter(localInstances_.find(instanceID)); iter != localInstances_.end()) {
        NotifyChanged(iter->second->GetAID(), instanceID, functionProxyID, routeInfo);
    }
}

void InstanceView::NotifyMigratingRequest(const std::string &instanceID)
{
    TerminateMigratedInstanceProxy(instanceID);
    if (subscribers_.find(instanceID) == subscribers_.end()) {
        return;
    }
    for (const auto &subscribed : subscribers_[instanceID]) {
        (void)subscribedInstances_[subscribed].erase(instanceID);
    }
    (void)subscribers_.erase(instanceID);
}

void InstanceView::TerminateMigratedInstanceProxy(const std::string &instanceID)
{
    if (localInstances_.find(instanceID) == localInstances_.end()) {
        return;
    }
    auto instanceProxy = localInstances_[instanceID];
    ASSERT_IF_NULL(instanceProxy);
    // To prevent the caller from receiving the return value of the migration request, we should wait for the response
    // message and then exit.
    auto futures = litebus::Async(instanceProxy->GetAID(), &InstanceProxy::GetOnRespFuture);
    (void)litebus::Collect(futures).OnComplete([instanceProxy]() { litebus::Terminate(instanceProxy->GetAID()); });
    (void)localInstances_.erase(instanceID);
}

void InstanceView::Reject(const std::string &instanceID, const resources::InstanceInfo &instanceInfo)
{
    // while proxy restart, the instance prosy may not be spawned
    SpawnInstanceProxy(instanceID, instanceInfo);
    auto errCode = instanceInfo.instancestatus().errcode();
    auto msg = instanceInfo.instancestatus().msg();
    // only instance in local would reject request
    if (auto iter(localInstances_.find(instanceID)); iter != localInstances_.end()) {
        YRLOG_INFO("instance({}) is set to reject request, errcode({}), msg({})", instanceID, errCode, msg);
        litebus::Async(iter->second->GetAID(), &InstanceProxy::Reject, instanceID, msg,
                       static_cast<StatusCode>(errCode));
    }
    // notify remote subscribers to update route info and set reject state
    const auto &functionProxyID = instanceInfo.functionproxyid();
    auto routeInfo = TransferInstanceInfo(instanceInfo, nodeID_);
    routeInfo->isLocal = false;
    for (const auto &subscriber : subscribedInstances_[instanceID]) {
        if (localInstances_.find(subscriber) == localInstances_.end()) {
            continue;
        }
        auto instanceProxy = localInstances_[subscriber];
        if (instanceProxy == nullptr) {
            YRLOG_ERROR("instance ({}) reject subscriber ({}), but instanceProxy is null", instanceID, subscriber);
            continue;
        }
        NotifyChanged(instanceProxy->GetAID(), instanceID, functionProxyID, routeInfo);
        litebus::Async(instanceProxy->GetAID(), &InstanceProxy::Reject, instanceID, msg,
                       static_cast<StatusCode>(errCode));
    }
}

}  // namespace functionsystem::busproxy
