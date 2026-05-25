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

#ifndef TEST_UNIT_MOCKS_MOCK_OBSERVER_H
#define TEST_UNIT_MOCKS_MOCK_OBSERVER_H

#include "function_proxy/common/observer/control_plane_observer/control_plane_observer.h"
#include "gmock/gmock.h"

namespace functionsystem::test {

class MockObserver : public function_proxy::ControlPlaneObserver {
public:
    MockObserver() : function_proxy::ControlPlaneObserver(nullptr)
    {
    }
    ~MockObserver() override
    {
    }
    MOCK_METHOD(litebus::Future<Status>, PutInstance, (const resource_view::InstanceInfo &instanceInfo),
                (const, override));

    MOCK_METHOD(litebus::Future<Status>, DelInstance, (const std::string &instanceID), (const, override));

    MOCK_METHOD(litebus::Future<litebus::Option<FunctionMeta>>, GetFuncMeta, (const std::string &funcKey),
                (const, override));

    MOCK_METHOD(litebus::Future<litebus::Option<resource_view::InstanceInfo>>, GetInstanceInfoByID,
                (const std::string &instanceID), (const, override));

    MOCK_METHOD(litebus::Future<litebus::Option<function_proxy::InstanceInfoMap>>, GetAgentInstanceInfoByID,
                (const std::string &funcAgentID), (const, override));

    MOCK_METHOD(litebus::Future<litebus::Option<litebus::AID>>, GetLocalSchedulerAID, (const std::string &proxyID),
                (const, override));

    MOCK_METHOD(litebus::Future<bool>, IsSystemFunction, (const std::string &function), (const, override));

    MOCK_METHOD(litebus::Future<function_proxy::InstanceInfoMap>, GetAllInstanceInfos, (), (const, override));

    MOCK_METHOD(litebus::Future<Status>, PutInstanceEvent,
                (const resource_view::InstanceInfo &instanceInfo, bool synced, int64_t modRevision),
                (override));

    MOCK_METHOD(void, FastPutRemoteInstanceEvent,
                (const resource_view::InstanceInfo &instanceInfo, bool synced, int64_t modRevision),
                (override));

    MOCK_METHOD(litebus::Future<Status>, DelInstanceEvent, (const std::string &instanceID, int64_t modRevision),
                (override));

    MOCK_METHOD(litebus::Future<std::vector<std::string>>, GetLocalInstances, 
        (const std::function<bool(const resource_view::InstanceInfo &)> &filter), (override));

    MOCK_METHOD(void, SetDriverEventCbFunc, (const function_proxy::DriverEventCbFunc &driverCbFunc), (override));

    MOCK_METHOD(void, SetInstanceInfoSyncerCbFunc,
                (const function_proxy::InstanceInfoSyncerCbFunc &instanceInfoSyncerCbFunc), (override));

    MOCK_METHOD(void, SetUpdateFuncMetasFunc,
                (const function_proxy::UpdateFuncMetasFunc &updateFuncMetasFunc), (override));

    MOCK_METHOD(void, SetTrafficReportCbFunc,
                (const function_proxy::TrafficReportCbFunc &trafficReportCbFunc), (override));

    MOCK_METHOD(void, Attach, (const std::shared_ptr<InstanceListener> &listener), (const, override));

    MOCK_METHOD(void, Detach, (const std::shared_ptr<InstanceListener> &listener), (const, override));

    MOCK_METHOD(litebus::Future<litebus::Option<function_proxy::InstanceInfoMap>>, GetLocalInstanceInfo, (),
                (const, override));

    MOCK_METHOD(void, WatchInstance, (const std::string &instanceID, int64_t revision), (override));

    MOCK_METHOD(litebus::Future<bool>, IsInstanceWatched, (const std::string &instanceID), (override));

    MOCK_METHOD(litebus::Future<resource_view::InstanceInfo>, GetAndWatchInstance, (const std::string &instanceID),
                (override));

    MOCK_METHOD(litebus::Future<resource_view::InstanceInfo>, GetOrWatchInstance, (const std::string &instanceID),
                (override));

    MOCK_METHOD(void, CancelWatchInstance, (const std::string &instanceID), (override));
};

}  // namespace functionsystem::test

#endif  // TEST_UNIT_MOCKS_MOCK_OBSERVER_H
