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

#include "function_proxy/busproxy/instance_view/instance_view.h"

#include <gtest/gtest.h>

#include <nlohmann/json.hpp>

#include "common/proto/pb/posix/resource.pb.h"
#include "common/resource_view/resource_type.h"
#include "common/types/instance_state.h"
#include "mocks/mock_shared_client.h"
#include "mocks/mock_shared_client_manager_proxy.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {
using namespace busproxy;
using namespace ::testing;
const std::string nodeID = "local";

inline resource_view::InstanceInfo GenInstanceInfo(const std::string &instanceID, const std::string &parent,
                                                   const std::string &node, InstanceState instanceStatus,
                                                   int32_t scheduleRound = 0)
{
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid(instanceID);
    instanceInfo.set_parentid(parent);
    instanceInfo.set_functionproxyid(node);
    instanceInfo.mutable_instancestatus()->set_code(int32_t(instanceStatus));
    instanceInfo.set_scheduletimes(3 - scheduleRound);
    return instanceInfo;
}

class InstanceViewTest : public ::testing::Test {
public:
    void SetUp() override
    {
        instanceView_ = std::make_shared<InstanceView>(nodeID);
        proxyView_ = std::make_shared<ProxyView>();
        mockSharedClientManagerProxy_ = std::make_shared<MockSharedClientManagerProxy>();
        instanceView_->BindProxyView(proxyView_);
        instanceView_->BindDataInterfaceClientManager(mockSharedClientManagerProxy_);
        auto address = litebus::GetLitebusAddress();
        url_ = address.ip + ":" + std::to_string(address.port);
    }

    void TearDown() override
    {
        instanceView_->BindDataInterfaceClientManager(nullptr);
        instanceView_ = nullptr;
        mockSharedClientManagerProxy_ = nullptr;
    }

    void UpdateInstance(const std::string &instanceID, const std::string &parent, const std::string &receiveNode,
                        const std::string &locationNode, int32_t scheduleRound = 0)
    {
        auto instanceInfo = GenInstanceInfo(instanceID, parent, receiveNode, InstanceState::SCHEDULING, scheduleRound);
        instanceInfo.set_version(0);
        instanceView_->Update(instanceID, instanceInfo, false);

        instanceInfo = GenInstanceInfo(instanceID, parent, locationNode, InstanceState::CREATING, scheduleRound);
        instanceInfo.set_version(1);
        instanceView_->Update(instanceID, instanceInfo, false);
        // lower version duplicate update
        instanceInfo.set_version(0);
        instanceView_->Update(instanceID, instanceInfo, false);

        instanceInfo = GenInstanceInfo(instanceID, parent, locationNode, InstanceState::RUNNING, scheduleRound);
        instanceInfo.set_version(3);
        auto mockSharedClient = std::make_shared<MockSharedClient>();
        EXPECT_CALL(*mockSharedClientManagerProxy_, NewDataInterfacePosixClient(_, _, _))
            .WillRepeatedly(Return(mockSharedClient));
        instanceView_->Update(instanceID, instanceInfo, false);
        if (locationNode == nodeID) {
            litebus::AID aid(instanceID, url_);
            EXPECT_NE(litebus::GetActor(aid), nullptr);
        }
    }

protected:
    std::shared_ptr<InstanceView> instanceView_;
    std::shared_ptr<ProxyView> proxyView_;
    std::shared_ptr<MockSharedClientManagerProxy> mockSharedClientManagerProxy_;
    std::string url_;
};

class MockDataPlaneObserver : public function_proxy::DataPlaneObserver {
public:
    MockDataPlaneObserver(const std::shared_ptr<InstanceView> &view)
        : function_proxy::DataPlaneObserver(nullptr), instanceView_(view){};
    ~MockDataPlaneObserver() = default;
    litebus::Future<Status> SubscribeInstanceEvent(const std::string &subscriber, const std::string &targetInstance,
                                                   bool ignoreNonExist = false)
    {
        return instanceView_->SubscribeInstanceEvent(subscriber, targetInstance, ignoreNonExist);
    }
    void NotifyMigratingRequest(const std::string &instanceID)
    {
        instanceView_->NotifyMigratingRequest(instanceID);
    }

private:
    std::shared_ptr<InstanceView> instanceView_;
};

TEST_F(InstanceViewTest, InstanceStateChange)
{
    InstanceProxy::BindObserver(std::make_shared<MockDataPlaneObserver>(instanceView_));
    auto localClient = std::make_shared<proxy::Client>(litebus::AID());
    proxyView_->Update(nodeID, localClient);
    std::string parent = "parent";
    UpdateInstance(parent, "driver", nodeID, nodeID);
    std::string childA = "childA";
    UpdateInstance(childA, parent, nodeID, nodeID);

    // not in local
    std::string childB = "childB";
    UpdateInstance(childB, parent, nodeID, "remote1");
    auto client = std::make_shared<proxy::Client>(litebus::AID());
    proxyView_->Update("remote1", client);

    // instance located in another node would not spawn proxy actor
    litebus::AID aid(childB, url_);
    EXPECT_EQ(litebus::GetActor(aid), nullptr);

    auto instanceInfo = GenInstanceInfo(childB, parent, "remote1", InstanceState::FATAL);
    instanceView_->Update(childB, instanceInfo, false);
    instanceView_->SubscribeInstanceEvent(childA, childB, true);

    instanceInfo = GenInstanceInfo(childA, parent, nodeID, InstanceState::FAILED);
    instanceView_->Update(childA, instanceInfo, false);
    instanceView_->SubscribeInstanceEvent(childA, childB);

    // invalid subscribe
    std::string invalidSubscriber = "invalidSubscriber";
    auto ret = instanceView_->SubscribeInstanceEvent(invalidSubscriber, childB);
    EXPECT_EQ(ret.IsOk(), false);

    // migrating to another node
    auto clientB = std::make_shared<proxy::Client>(litebus::AID());
    proxyView_->Update("remote2", clientB);
    UpdateInstance(childA, parent, nodeID, "remote2", 1);
    aid.SetName(childA);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
    instanceView_->Delete(childA, -1);
    instanceView_->Delete(childB, -1);
    instanceView_->Delete(parent, -1);
    aid.SetName(parent);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
    aid.SetName(childA);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
    aid.SetName(childB);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
}

/**
 * Feature: Subscribe to instance in EVICTING state
 * Description: Verify subscribing to an instance that is in EVICTING state
 * Steps:
 * 1. Create parent instance
 * 2. Create child instance in EVICTING state
 * 3. Subscribe parent to child
 * Expectation: Subscriber receives reject notification
 */
TEST_F(InstanceViewTest, SubscribeToEvictingInstance)
{
    InstanceProxy::BindObserver(std::make_shared<MockDataPlaneObserver>(instanceView_));
    auto localClient = std::make_shared<proxy::Client>(litebus::AID());
    proxyView_->Update(nodeID, localClient);

    std::string parent = "parent-evicting";
    std::string child = "child-evicting";
    
    // Create parent instance
    UpdateInstance(parent, "driver", nodeID, nodeID);
    
    // Create child in EVICTING state
    auto instanceInfo = GenInstanceInfo(child, parent, nodeID, InstanceState::EVICTING);
    instanceInfo.mutable_instancestatus()->set_errcode(common::ERR_INSTANCE_EVICTED);
    instanceInfo.mutable_instancestatus()->set_msg("Instance is being evicted");
    instanceView_->Update(child, instanceInfo, false);

    // Subscribe parent to evicting child
    auto ret = instanceView_->SubscribeInstanceEvent(parent, child, false);
    EXPECT_TRUE(ret.IsOk());

    // Cleanup
    instanceView_->Delete(child, -1);
    instanceView_->Delete(parent, -1);
}
}  // namespace functionsystem::test