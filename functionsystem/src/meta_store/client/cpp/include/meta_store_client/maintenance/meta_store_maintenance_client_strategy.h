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

#ifndef COMMON_META_STORE_CLIENT_MAINTENANCE_META_STORE_MAINTENANCE_CLIENT_STRATEGY_H
#define COMMON_META_STORE_CLIENT_MAINTENANCE_META_STORE_MAINTENANCE_CLIENT_STRATEGY_H

#include "common/heartbeat/heartbeat_client.h"
#include "common/heartbeat/heartbeat_observer.h"
#include "common/utils/request_sync_helper.h"
#include "maintenance_client_strategy.h"

namespace functionsystem::meta_store {

class MetaStoreMaintenanceClientStrategy : public MaintenanceClientStrategy {
public:
    MetaStoreMaintenanceClientStrategy(const std::string &name, const std::string &address,
                                       const std::shared_ptr<MetaStoreExplorer> &explorer,
                                       const MetaStoreTimeoutOption &timeoutOption);

    ~MetaStoreMaintenanceClientStrategy() override = default;

    litebus::Future<StatusResponse> HealthCheck() override;
    litebus::Future<bool> IsConnected() override;
    void CheckChannelAndWaitForReconnect() override;
    void OnAddressUpdated(const std::string &address) override;

protected:
    void Init() override;
    void Exited(const litebus::AID &from) override;

private:
    void StartHeartbeat();
    void OnHealthCheck(const litebus::AID &from, std::string &&name, std::string &&msg);

    std::shared_ptr<litebus::AID> maintenanceServiceAid_;

    BACK_OFF_RETRY_HELPER(MetaStoreMaintenanceClientStrategy, StatusResponse, healthCheckHelper_)

    void TryReconnect();
    void ReconnectSuccess();
    void HeartBeatTimeout();

    std::shared_ptr<litebus::Timer> reconnectTimer_;
    std::shared_ptr<litebus::Timer> reconnectConfirmTimer_;
    uint64_t reconnectInterval_ = 100;
    uint64_t reconnectConfirmInterval_ = 2000;
    std::shared_ptr<HeartbeatClientDriver> pingPongDriver_;

    bool isDisconnected_ = false;
};
}  // namespace functionsystem::meta_store

#endif  // COMMON_META_STORE_CLIENT_MAINTENANCE_META_STORE_MAINTENANCE_CLIENT_STRATEGY_H
