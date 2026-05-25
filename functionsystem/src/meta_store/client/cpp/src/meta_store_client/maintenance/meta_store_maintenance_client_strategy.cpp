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

#include "meta_store_maintenance_client_strategy.h"

#include "common/proto/pb/message_pb.h"
#include "common/utils/actor_worker.h"
#include "common/utils/random_number.h"

namespace functionsystem::meta_store {

const uint32_t META_STORE_HEARTBEAT_PING_TIMES = 5;
const uint32_t META_STORE_HEARTBEAT_PING_CYCLE_MS = 1000;
MetaStoreMaintenanceClientStrategy::MetaStoreMaintenanceClientStrategy(
    const std::string &name, const std::string &address, const std::shared_ptr<MetaStoreExplorer> &explorer,
    const MetaStoreTimeoutOption &timeoutOption)
    : MaintenanceClientStrategy(name, address, explorer, timeoutOption)
{
    maintenanceServiceAid_ = std::make_shared<litebus::AID>("MaintenanceServiceActor", address_);
    auto backOff = [lower(timeoutOption_.operationRetryIntervalLowerBound),
                    upper(timeoutOption_.operationRetryIntervalUpperBound),
                    base(timeoutOption_.grpcTimeout * 1000)](int64_t attempt) {
        return GenerateRandomNumber(base + lower * attempt, base + upper * attempt);
    };
    healthCheckHelper_.SetBackOffStrategy(backOff, timeoutOption_.operationRetryTimes);
}

void MetaStoreMaintenanceClientStrategy::StartHeartbeat()
{
    Link(*maintenanceServiceAid_);
    litebus::AID heartbeatAID(HEARTBEAT_OBSERVER_BASENAME + "meta-store", maintenanceServiceAid_->UnfixUrl());
    if (pingPongDriver_ == nullptr) {
        pingPongDriver_ = std::make_shared<HeartbeatClientDriver>(
            "meta-store", META_STORE_HEARTBEAT_PING_TIMES, META_STORE_HEARTBEAT_PING_CYCLE_MS,
            [aid(GetAID())](const litebus::AID &) {
                litebus::Async(aid, &MetaStoreMaintenanceClientStrategy::HeartBeatTimeout);
            });
    }
    (void)pingPongDriver_->Start(heartbeatAID);
}

void MetaStoreMaintenanceClientStrategy::Init()
{
    ASSERT_IF_NULL(maintenanceServiceAid_);
    YRLOG_INFO("Init maintenance Client actor({}).", maintenanceServiceAid_->HashString());
    Receive("OnHealthCheck", &MetaStoreMaintenanceClientStrategy::OnHealthCheck);
    StartHeartbeat();
}

void MetaStoreMaintenanceClientStrategy::HeartBeatTimeout()
{
    isDisconnected_ = true;
    if (pingPongDriver_ != nullptr) {
        (void)pingPongDriver_->Stop();
        pingPongDriver_ = nullptr;
    }

    if (explorer_->IsNeedExplore()) {
        // if meta-store address needs to be explored, don't wait for old connection to be back, trigger Reconnected
        // immediately
        ReconnectSuccess();
        return;
    }

    HealthCheck().OnComplete([aid(GetAID())](const litebus::Future<StatusResponse> &status) {
        if (status.IsOK()) {
            litebus::Async(aid, &MetaStoreMaintenanceClientStrategy::ReconnectSuccess);
            return;
        }
        YRLOG_ERROR("meta-store is still disconnected, retry");
        litebus::AsyncAfter(META_STORE_HEARTBEAT_PING_CYCLE_MS, aid,
                            &MetaStoreMaintenanceClientStrategy::HeartBeatTimeout);
    });
}

void MetaStoreMaintenanceClientStrategy::Exited(const litebus::AID &from)
{
    if (maintenanceServiceAid_ == nullptr || maintenanceServiceAid_->UnfixUrl() != from.UnfixUrl()) {
        YRLOG_WARN("disconnected from meta-store({}), different from current aid", from.HashString());
        return;
    }

    YRLOG_INFO("disconnected from meta-store({}), isNeedExplore: {}", from.HashString(), explorer_->IsNeedExplore());
    isDisconnected_ = true;
    if (explorer_->IsNeedExplore()) {
        // if meta-store address needs to be explored, don't wait for old connection to be back, trigger Reconnected
        // immediately
        if (pingPongDriver_ != nullptr) {
            (void)pingPongDriver_->Stop();
            pingPongDriver_ = nullptr;
        }
        ReconnectSuccess();
        return;
    }

    if (reconnectTimer_ != nullptr) {
        litebus::TimerTools::Cancel(*reconnectTimer_);
    }
    // wait for server to be connected
    reconnectTimer_ = std::make_shared<litebus::Timer>(
        litebus::AsyncAfter(reconnectInterval_, GetAID(), &MetaStoreMaintenanceClientStrategy::TryReconnect));
}

void MetaStoreMaintenanceClientStrategy::TryReconnect()
{
    if (reconnectConfirmTimer_ != nullptr) {
        litebus::TimerTools::Cancel(*reconnectConfirmTimer_);
    }

    ASSERT_IF_NULL(maintenanceServiceAid_);
    YRLOG_DEBUG("try reconnect to meta-store({})", maintenanceServiceAid_->HashString());
    // notice: reconnect return value don't mean reconnect success, so start a ConfirmTimer which will set the
    // reconnectConfirmTimer_ to nullptr after 5s.
    Link(*maintenanceServiceAid_);
    reconnectConfirmTimer_ = std::make_shared<litebus::Timer>(litebus::AsyncAfter(
        reconnectConfirmInterval_, GetAID(), &MetaStoreMaintenanceClientStrategy::ReconnectSuccess));
}

void MetaStoreMaintenanceClientStrategy::ReconnectSuccess()
{
    ASSERT_IF_NULL(maintenanceServiceAid_);
    YRLOG_INFO("reconnect to meta-store({}) success", maintenanceServiceAid_->HashString());
    reconnectConfirmTimer_ = nullptr;
    if (isDisconnected_) {
        isDisconnected_ = false;
        Reconnected();
    }
}

litebus::Future<bool> MetaStoreMaintenanceClientStrategy::IsConnected()
{
    return true;
}

litebus::Future<StatusResponse> MetaStoreMaintenanceClientStrategy::HealthCheck()
{
    messages::MetaStoreRequest req;
    req.set_requestid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());

    YRLOG_DEBUG("{}|begin to healthCheck", req.requestid());
    return healthCheckHelper_.Begin(req.requestid(), maintenanceServiceAid_, "HealthCheck", req.SerializeAsString());
}

void MetaStoreMaintenanceClientStrategy::OnHealthCheck(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    messages::MetaStoreResponse res;
    RETURN_IF_TRUE(!res.ParseFromString(msg), "failed to parse healthCheck MetaStoreResponse");

    etcdserverpb::StatusResponse response;
    RETURN_IF_TRUE(!response.ParseFromString(res.responsemsg()),
                   "failed to parse healthCheck StatusResponse: " + res.responseid());

    StatusResponse ret;
    if (!response.errors().empty()) {
        try {
            YRLOG_ERROR("{}|failed to health check, error code: {}", res.responseid(), response.errors(0));
            ret.status = Status(StatusCode(std::stoi(response.errors(0))));
        } catch (std::invalid_argument const &ex) {
            YRLOG_ERROR("{}|failed to get health check error code", res.responseid());
            ret.status = Status(StatusCode::FAILED);
        }
        healthCheckHelper_.End(res.responseid(), std::move(ret));
        return;
    }
    YRLOG_DEBUG("{}|success to healthCheck", res.responseid());
    healthCheckHelper_.End(res.responseid(), std::move(ret));
}

void MetaStoreMaintenanceClientStrategy::CheckChannelAndWaitForReconnect()
{
}

void MetaStoreMaintenanceClientStrategy::OnAddressUpdated(const std::string &address)
{
    ASSERT_IF_NULL(maintenanceServiceAid_);
    YRLOG_INFO("maintenance client will update address from {} to {}", address_, address);
    if (address.empty()) {
        YRLOG_ERROR("maintenance client new address is empty, no need to update");
        return;
    }
    address_ = address;
    maintenanceServiceAid_->SetUrl(address);
    StartHeartbeat();
}
}  // namespace functionsystem::meta_store
