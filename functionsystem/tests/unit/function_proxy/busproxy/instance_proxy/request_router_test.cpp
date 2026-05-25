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

#include "busproxy/instance_proxy/request_router.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>

#include "function_proxy/busproxy/invocation_handler/invocation_handler.h"
#include "function_proxy/common/observer/data_plane_observer/data_plane_observer.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {
using namespace ::testing;
using namespace busproxy;
const std::string CUSTOMS_TAG = "CUSTOMS_TAG";

class MockTmpInstanceProxy : public busproxy::InstanceProxy {
public:
    MockTmpInstanceProxy(const std::string &instanceID, const std::string &tenantID)
        : busproxy::InstanceProxy(instanceID, tenantID)
    {
    }

    MOCK_METHOD(litebus::Future<Status>, DoForwardCall,
                (const litebus::AID &, const std::shared_ptr<runtime_rpc::StreamingMessage> &), (override));
    MOCK_METHOD(void, ResponseForwardCall, (const litebus::AID &, std::string &&, std::string &&msg), (override));
};


class RequestRouterTest : public ::testing::Test {
public:
    void SetUp() override
    {
        requestRouter_ = std::make_shared<busproxy::RequestRouter>(REQUEST_ROUTER_NAME);
        litebus::Spawn(requestRouter_);

        toProxy_ = std::make_shared<MockTmpInstanceProxy>(remote_, "");
        litebus::Spawn(toProxy_);

        fromProxy_ = std::make_shared<MockTmpInstanceProxy>(local_, "");
        litebus::Spawn(fromProxy_);
    }

    void TearDown() override
    {
        litebus::Terminate(toProxy_->GetAID());
        litebus::Await(toProxy_->GetAID());
        toProxy_ = nullptr;

        litebus::Terminate(fromProxy_->GetAID());
        litebus::Await(fromProxy_->GetAID());
        fromProxy_ = nullptr;

        litebus::Terminate(requestRouter_->GetAID());
        litebus::Await(requestRouter_->GetAID());
        requestRouter_ = nullptr;
    }

    ::internal::RouteCallRequest GenRouteCallRequest(const std::string& requestID, const std::string& messageID) {
        auto request = std::make_shared<runtime_rpc::StreamingMessage>();
        auto callreq = request->mutable_callreq();
        callreq->set_senderid(local_);
        callreq->set_requestid(requestID);
        request->set_messageid(messageID);

        ::internal::RouteCallRequest routeReq;
        routeReq.mutable_req()->CopyFrom(*request);
        return routeReq;
    }

protected:
    std::shared_ptr<busproxy::RequestRouter> requestRouter_;
    std::shared_ptr<MockTmpInstanceProxy> toProxy_;
    std::shared_ptr<MockTmpInstanceProxy> fromProxy_;
    std::string local_ = "local";
    std::string remote_ = "remote";
};

TEST_F(RequestRouterTest, InstanceNotFound)
{
    auto routeReq = GenRouteCallRequest("requestID", "messageID");
    routeReq.set_instanceid("invalid_instance");

    EXPECT_CALL(*toProxy_, DoForwardCall(_, _)).Times(0);

    std::string testMsg;
    bool flag = false;
    EXPECT_CALL(*fromProxy_, ResponseForwardCall(_, _, _))
        .WillOnce([&](const litebus::AID &, std::string &&, std::string &&msg) {
            testMsg = std::move(msg);
            flag = true;
        });

    requestRouter_->ForwardCall(fromProxy_->GetAID(), std::string(), routeReq.SerializeAsString());

    EXPECT_AWAIT_TRUE([&]() -> bool { return flag; });
    auto response = std::make_shared<runtime_rpc::StreamingMessage>();
    (void)response->ParseFromString(testMsg);
    EXPECT_EQ(response->messageid(), "requestID");
    ASSERT_TRUE(response->has_callrsp());
    EXPECT_EQ(static_cast<int>(response->callrsp().code()), static_cast<int>(ERR_INSTANCE_NOT_FOUND));
}

TEST_F(RequestRouterTest, InstanceFound)
{
    auto routeReq = GenRouteCallRequest("requestID", "messageID");
    routeReq.set_instanceid(remote_);

    std::string testMsg;
    bool flag = false;
    EXPECT_CALL(*toProxy_, DoForwardCall(_, _)).WillOnce([&](const litebus::AID &, const auto &) {
        flag = true;
        return litebus::Future<Status>(Status::OK());
    });
    requestRouter_->ForwardCall(fromProxy_->GetAID(), std::string(), routeReq.SerializeAsString());

    EXPECT_AWAIT_TRUE([&]() -> bool { return flag; });
}

}  // namespace functionsystem::test