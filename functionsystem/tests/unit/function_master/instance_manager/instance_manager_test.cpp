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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <async/collect.hpp>

#define private public
#define protected public
#include "common/constants/signal.h"
#include "common/etcd_service/etcd_service_driver.h"
#include "common/metadata/metadata.h"
#include "common/types/instance_state.h"
#include "common/utils/generate_message.h"
#include "common/utils/meta_store_kv_operation.h"
#include "common/utils/struct_transfer.h"
#include "function_master/instance_manager/group_manager.h"
#include "function_master/instance_manager/instance_manager_actor.h"
#include "function_master/instance_manager/instance_manager_driver.h"
#include "mocks/mock_global_schd.h"
#include "mocks/mock_instance_operator.h"
#include "mocks/mock_local_instance_ctrl_actor.h"
#include "mocks/mock_meta_store_client.h"
#include "utils/future_test_helper.h"
#include "utils/generate_info.h"
#include "utils/port_helper.h"

namespace functionsystem::instance_manager::test {
using namespace functionsystem::meta_store::test;
using namespace functionsystem::test;

const std::string KEY_ABNORMAL_SCHEDULER_PREFIX = "/yr/abnormal/localscheduler/";  // NOLINT

const std::string NODE_ID_1 = "/sn/proxy/001";  // NOLINT
const std::string NODE_ID_2 = "/sn/proxy/002";  // NOLINT
const std::string NODE_ID_3 = "/sn/proxy/003";  // NOLINT
const std::string NODE_ID_4 = "/sn/proxy/004";  // NOLINT

std::unordered_set<std::string> NODES = {NODE_ID_1, NODE_ID_2, NODE_ID_3, NODE_ID_4};

const std::string Inst1 = "/sn/instance/business/yrk/tenant/sn/function/function/version/001/defaultaz//sn/instance/business/yrk/tenant/001//sn/instance/business/yrk/tenant/001";
const std::string Inst2 = "/sn/instance/business/yrk/tenant/sn/function/function/version/001/defaultaz//sn/instance/business/yrk/tenant/001//sn/instance/business/yrk/tenant/002";
const std::string Inst3 = "/sn/instance/business/yrk/tenant/sn/function/function/version/001/defaultaz//sn/instance/business/yrk/tenant/001//sn/instance/business/yrk/tenant/003";
const std::string Inst4 = "/sn/instance/business/yrk/tenant/sn/function/function/version/001/defaultaz//sn/instance/business/yrk/tenant/001//sn/instance/business/yrk/tenant/004";

class MockGroupManagerActor : public functionsystem::instance_manager::GroupManagerActor {
public:
    MockGroupManagerActor(const std::shared_ptr<MetaStoreClient> &metaClient,
                          const std::shared_ptr<functionsystem::global_scheduler::GlobalSched> &scheduler)
        : GroupManagerActor(metaClient, scheduler)
    {
    }

    ~MockGroupManagerActor() override = default;

    void Init() override
    {
        YRLOG_INFO("mock group mgr actor inited");
    }
};

class MockGroupManager : public functionsystem::instance_manager::GroupManager {
public:
    explicit MockGroupManager(const std::shared_ptr<MockGroupManagerActor> &actor) : GroupManager(actor)
    {
    }

    MOCK_METHOD(litebus::Future<Status>, OnInstanceAbnormal,
                (const std::string &, const std::shared_ptr<resource_view::InstanceInfo> &), (override));

    MOCK_METHOD(litebus::Future<Status>, OnLocalAbnormal, (const std::string &), (override));

    MOCK_METHOD(litebus::Future<Status>, OnInstancePut,
                (const std::string &, const std::shared_ptr<resource_view::InstanceInfo> &), (override));

    MOCK_METHOD(litebus::Future<Status>, OnInstanceDelete,
                (const std::string &, const std::shared_ptr<resource_view::InstanceInfo> &), (override));
};

class MockBootstrapStubActor : public litebus::ActorBase {
public:
    explicit MockBootstrapStubActor(const std::string &name) : ActorBase(name){};
    void Init() override
    {
        Receive("ResponseForwardKill", &MockBootstrapStubActor::ReceiveKillResponse);
    }

    litebus::Future<Status> SendForwardKill(const litebus::AID &to, const messages::ForwardKillRequest &request)
    {
        promise_ = std::make_shared<litebus::Promise<Status>>();
        Send(to, "ForwardKill", request.SerializeAsString());
        return promise_->GetFuture();
    }

    void ReceiveKillResponse(const litebus::AID &from, std::string &&name, std::string &&msg)
    {
        messages::ForwardKillResponse rsp;
        if (!rsp.ParseFromString(msg)) {
            YRLOG_ERROR("failed to parse ForwardKillResponse");
            return;
        }
        promise_->SetValue(Status(StatusCode(rsp.code()), rsp.message()));
    }

    std::shared_ptr<litebus::Promise<Status>> promise_;
};

class DISABLED_InstanceManagerTest : public ::testing::Test {
protected:
    inline static std::unique_ptr<meta_store::test::EtcdServiceDriver> etcdSrvDriver_;
    inline static std::string metaStoreServerHost_;
    inline static std::string localAddress_;

    std::shared_ptr<MockInstanceCtrlActor> mockInstCtrlActorNode01_;

    [[maybe_unused]] static void SetUpTestSuite()
    {
        etcdSrvDriver_ = std::make_unique<meta_store::test::EtcdServiceDriver>();
        int metaStoreServerPort = functionsystem::test::FindAvailablePort();
        metaStoreServerHost_ = "127.0.0.1:" + std::to_string(metaStoreServerPort);
        etcdSrvDriver_->StartServer(metaStoreServerHost_);
        uint16_t port = GetPortEnv("LITEBUS_PORT", 8080);
        localAddress_ = "127.0.0.1:" + std::to_string(port);
    }

    [[maybe_unused]] static void TearDownTestSuite()
    {
        etcdSrvDriver_->StopServer();
    }

protected:
    static resource_view::InstanceInfo CreateInstance(const std::string &_id, bool isRecoverEnable = false)
    {
        resource_view::InstanceInfo output;
        output.set_instanceid(_id);
        output.set_requestid(INSTANCE_PATH_PREFIX + "/001");

        output.set_runtimeid("/sn/runtime/001");

        output.set_functionagentid("sn/agent/001");
        output.set_functionproxyid(NODE_ID_1);

        output.set_function("sn/function/001");
        output.mutable_schedulerchain()->Add("chain01");
        output.mutable_schedulerchain()->Add("chain02");

        auto status = output.mutable_instancestatus();
        status->set_code((int32_t)InstanceState::RUNNING);
        status->set_msg("Success");

        if (isRecoverEnable) {
            (*output.mutable_createoptions())[RECOVER_RETRY_TIMES_KEY] = "1";
        }

        output.set_version(1);

        output.set_scheduletimes(1);

        return output;
    }

    static void PutInstances(bool isRecoverEnable = false, bool isGenerateKey = false)
    {
        MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
        client.Init();
        std::string jsonString001;
        resource_view::InstanceInfo instance001 = CreateInstance(INSTANCE_PATH_PREFIX + "/001", isRecoverEnable);
        ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString001, instance001));

        if (isGenerateKey) {
            auto instanceKey = GenInstanceKey(instance001.function(), instance001.instanceid(), instance001.requestid());
            ASSERT_TRUE(client.Put(instanceKey.Get(), jsonString001, {}).Get()->status.IsOk());
        } else {
            ASSERT_TRUE(client.Put(instance001.instanceid(), jsonString001, {}).Get()->status.IsOk());
        }

        std::string jsonString002;
        resource_view::InstanceInfo instance002 = CreateInstance(INSTANCE_PATH_PREFIX + "/002", isRecoverEnable);
        auto status = instance002.mutable_instancestatus();
        status->set_code((int32_t)InstanceState::SCHEDULING);
        status->set_msg("scheduling");
        ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString002, instance002));
        if (isGenerateKey) {
            auto instanceKey = GenInstanceKey(instance002.function(), instance002.instanceid(), instance002.requestid());
            ASSERT_TRUE(client.Put(instanceKey.Get(), jsonString002, {}).Get()->status.IsOk());
        } else {
            ASSERT_TRUE(client.Put(instance002.instanceid(), jsonString002, {}).Get()->status.IsOk());
        }

        ASSERT_AWAIT_TRUE(
            [&]() -> bool { return client.Get(INSTANCE_PATH_PREFIX, { .prefix = true }).Get()->kvs.size() == 2; });
    }

    void SetUp() override
    {
        mockInstCtrlActorNode01_ = std::make_shared<MockInstanceCtrlActor>(
            NODE_ID_1 + LOCAL_SCHED_INSTANCE_CTRL_ACTOR_NAME_POSTFIX, NODE_ID_1, local_scheduler::InstanceCtrlConfig{});
        ASSERT_TRUE(litebus::Spawn(mockInstCtrlActorNode01_).OK());
    }

    void TearDown() override
    {
        MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
        client.Init();
        DeleteOption option = { .prevKv = false, .prefix = true };
        client.Delete(INSTANCE_PATH_PREFIX, option).Get()->status.IsOk();
        client.Delete(KEY_ABNORMAL_SCHEDULER_PREFIX, option).Get()->status.IsOk();

        litebus::Terminate(mockInstCtrlActorNode01_->GetAID());
        litebus::Await(mockInstCtrlActorNode01_->GetAID());
    }

    static std::shared_ptr<resource_view::InstanceInfo> MakeInstanceInfo(const std::string &instanceID,
                                                                         const std::string &groupID,
                                                                         const std::string &parentID,
                                                                         const std::string &nodeID,
                                                                         const InstanceState &state)
    {
        auto info = std::make_shared<resource_view::InstanceInfo>();
        info->set_requestid(INSTANCE_PATH_PREFIX + "/" + instanceID);
        info->set_runtimeid("/sn/runtime/001");
        info->set_functionagentid("/sn/agent/001");
        info->set_function("/sn/function/001");
        info->mutable_schedulerchain()->Add("chain01");
        info->mutable_schedulerchain()->Add("chain02");
        info->set_instanceid(instanceID);
        info->set_groupid(groupID);
        info->set_parentid(parentID);
        info->set_functionproxyid(nodeID);
        info->mutable_instancestatus()->set_code(static_cast<int32_t>(state));
        info->set_version(1);
        return info;
    }

    static std::vector<std::shared_ptr<resource_view::InstanceInfo>> MakeInstanceInfos()
    {
        // ""
        // └─A
        //   ├─B
        //   └─C
        //     ├─E
        //     └─D
        //       ├─F
        //       └─G
        return { MakeInstanceInfo("A", "", "", NODE_ID_1, InstanceState::RUNNING),
                 MakeInstanceInfo("B", "", "A", NODE_ID_1, InstanceState::RUNNING),
                 MakeInstanceInfo("C", "", "A", NODE_ID_1, InstanceState::RUNNING),
                 MakeInstanceInfo("D", "", "C", NODE_ID_1, InstanceState::RUNNING),
                 MakeInstanceInfo("E", "", "C", NODE_ID_1, InstanceState::RUNNING),
                 MakeInstanceInfo("F", "", "D", NODE_ID_1, InstanceState::RUNNING),
                 MakeInstanceInfo("G", "", "D", NODE_ID_1, InstanceState::RUNNING) };
    }

    static std::shared_ptr<InstanceManagerActor::Member> GetMemberFromInstanceMgrActor(
        const std::shared_ptr<InstanceManagerActor> &instanceMgrActor)
    {
        return instanceMgrActor->member_;
    }

    static void WaitSyncFuncMeta(const std::shared_ptr<MockMetaStoreClient> mockMetaStoreClient,
                                 const std::shared_ptr<InstanceManagerActor> instanceMgrActor)
    {
        instanceMgrActor->SetInstancesReady();
        std::shared_ptr<GetResponse> emptyResp = std::make_shared<GetResponse>();
        emptyResp->status = Status::OK();
        bool isMetaSynced = false;
        EXPECT_CALL(*mockMetaStoreClient, Get(FUNC_META_PATH_PREFIX, testing::_))
            .WillOnce(testing::DoAll(testing::Assign(&isMetaSynced, true), testing::Return(emptyResp)));
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                       GetLeaderInfo(instanceMgrActor->GetAID()));
        EXPECT_AWAIT_TRUE([&isMetaSynced]() -> bool { return isMetaSynced; });
    }
};

TEST_F(DISABLED_InstanceManagerTest, SyncInstance)  // NOLINT
{
    PutInstances(true);
    auto scheduler = std::make_shared<MockGlobalSched>();
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, BindCheckLocalAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, BindLocalDeleteCallback).Times(1);
    EXPECT_CALL(*scheduler, BindLocalAddCallback).Times(1);
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    functionsystem::instance_manager::InstanceManagerMap map;
    ASSERT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_1, &map).Get();  // block
        return map.size() == 2;  // two history instance
    });

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, StartSyncFail)
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    auto mockMetaStoreClient = std::make_shared<MockMetaStoreClient>(metaStoreServerHost_);
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(mockMetaStoreClient, scheduler);
    groupMgrActor->isSuicide_ = true;
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);
    std::shared_ptr<GetResponse> rep = std::make_shared<GetResponse>();
    rep->status = Status(StatusCode::FAILED, "");
    std::shared_ptr<GetResponse> rep1 = std::make_shared<GetResponse>();
    rep1->status = Status::OK();
    rep1->header.revision = INT64_MAX;
    EXPECT_CALL(*mockMetaStoreClient,
                GetAndWatchWithHandler(testing::_, testing::_, testing::_, testing::_, testing::_))
        .WillRepeatedly(testing::Return(litebus::Future<std::shared_ptr<Watcher>>()));
    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        mockMetaStoreClient, scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    instanceMgrActor->isSuicide_ = true;
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();
    EXPECT_TRUE(instanceMgrActor->CheckSyncResponse(rep).Get().IsError());
    EXPECT_TRUE(instanceMgrActor->CheckSyncResponse(rep1).Get().IsError());
    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, SchedulerWatchTest)  // NOLINT
{
    auto scheduler = std::make_shared<MockGlobalSched>();

    auto client = MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(client, scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    const auto actor = std::make_shared<InstanceManagerActor>(
        client, scheduler, groupMgr, nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });

    const litebus::AID aid = litebus::Spawn(actor);

    const std::string key = "/yr/busproxy/business/yrk/tenant/0/node/xxx";
    EXPECT_AWAIT_READY(client->Put(key, "{}", {}));
    EXPECT_AWAIT_TRUE([&]() -> bool {
        return actor->member_->proxyRouteSet.size() == 1
               && actor->member_->proxyRouteSet.find(key) != actor->member_->proxyRouteSet.end();
    });

    EXPECT_AWAIT_READY(client->Delete(key, {}));
    EXPECT_AWAIT_TRUE([&]() -> bool { return actor->member_->proxyRouteSet.size() == 0; });

    litebus::Terminate(aid);
    litebus::Await(aid);
}

TEST_F(DISABLED_InstanceManagerTest, ReplayFailedDeleteOperationTest)  // NOLINT
{
    auto scheduler = std::make_shared<MockGlobalSched>();

    auto client = MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(client, scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    const auto actor = std::make_shared<InstanceManagerActor>(
        client, scheduler, groupMgr, nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(actor, groupMgrActor);
    instanceMgrDriver->Start();
    litebus::Async(actor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo, GetLeaderInfo(actor->GetAID()));

    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    actor->member_->instanceOpt = mockInstanceOpt;

    std::list<litebus::Future<Status>> futures;
    auto eraseDelKeys = std::make_shared<std::set<std::string>>();
    actor->member_->operateCacher->AddDeleteEvent(
        INSTANCE_PATH_PREFIX,
        "/sn/instance/business/yrk/tenant/0/function/0-system-faasscheduler/version/$latest/defaultaz/"
        "38bb25d0ccc79faa00/e19cf1ed-2455-43eb-a8a0-6c6083cdee2b");
    litebus::Future<StoreInfo> future;
    EXPECT_CALL(*mockInstanceOpt, ForceDelete)
        .WillOnce(testing::DoAll(
            testing::Invoke([&future](std::shared_ptr<StoreInfo>, std::shared_ptr<StoreInfo> routeInfo,
                                      std::shared_ptr<StoreInfo>, bool) { future.SetValue(*routeInfo); }),
            testing::Return(OperateResult{ Status::OK(), "", 3 })));
    actor->ReplayFailedDeleteOperation(futures, eraseDelKeys);
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.Get().key, "/yr/route/business/yrk/e19cf1ed-2455-43eb-a8a0-6c6083cdee2b");
    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, SyncAbnormalScheduler)  // NOLINT
{
    PutInstances();
    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();
    ASSERT_TRUE(client.Put(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_1, NODE_ID_1, {}).Get()->status.IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto response = client.Get(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_1, {}).Get();
        return response->kvs.size() == 1;
    });

    ASSERT_TRUE(client.Put(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_3, NODE_ID_3, {}).Get()->status.IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto response = client.Get(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_3, {}).Get();
        return response->kvs.size() == 1;
    });

    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));

    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto map = std::make_shared<std::unordered_set<std::string>>();
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetAbnormalScheduler, map).Get();  // block
        return map->size() == 2 && map->find(NODE_ID_1) != map->end() && map->find(NODE_ID_3) != map->end();
    });

    ASSERT_AWAIT_TRUE([&]() -> bool {
        functionsystem::instance_manager::InstanceManagerMap map;
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_1, &map).Get();
        return map.empty();  // be deleted because node1 is abnormal
    });

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, PutAndDeleteInstance)  // NOLINT
{
    PutInstances(true);
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    resource_view::InstanceInfo instance003 = CreateInstance(INSTANCE_PATH_PREFIX + "/003", true);
    instance003.set_functionproxyid(NODE_ID_2);
    std::string jsonString;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString, instance003));

    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();
    // eg. /sn/instance/business/yrk/tenant/0/function/0-test-0/version/..
    ASSERT_TRUE(client.Put(instance003.instanceid(), jsonString, {}).Get()->status.IsOk());
    functionsystem::instance_manager::InstanceManagerMap map;
    ASSERT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_1, &map).Get();
        return map.size() == 2;  // two history instance
    });
    ASSERT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_2, &map).Get();
        return map.size() == 1;  // add a new instance
    });

    DeleteOption deleteOption = { .prevKv = false, .prefix = false };
    EXPECT_TRUE(client.Delete(INSTANCE_PATH_PREFIX + "/001", deleteOption).Get()->status.IsOk());

    ASSERT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_1, &map).Get();
        return map.size() == 1;
    });
    ASSERT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_2, &map).Get();
        return map.size() == 1;
    });

    instanceMgrActor->member_->exitingInstances.insert("123");
    instanceMgrActor->member_->family = std::make_shared<InstanceFamilyCaches>();
    auto info = std::make_shared<resource_view::InstanceInfo>();
    info->set_instanceid("123");
    info->set_function("0/0-system-faasfrontend/$latest");
    instanceMgrActor->business_->OnInstanceDeleteForFamilyManagement("", info);
    EXPECT_EQ(instanceMgrActor->member_->exitingInstances.find("123"),
              instanceMgrActor->member_->exitingInstances.end());

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, PutAndDeleteAbnormalScheduler)  // NOLINT
{
    PutInstances();
    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();
    ASSERT_TRUE(client.Put(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_3, NODE_ID_3, {}).Get()->status.IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto response = client.Get(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_3, {}).Get();
        return response->kvs.size() == 1;
    });

    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    auto map = std::make_shared<std::unordered_set<std::string>>();
    ASSERT_AWAIT_TRUE([&]() -> bool {
        map->clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetAbnormalScheduler, map).Get();  // block
        return map->size() == 1 && map->find(NODE_ID_3) != map->end();  // one history
    });

    ASSERT_TRUE(client.Put(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_4, NODE_ID_4, {}).Get()->status.IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto response = client.Get(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_4, {}).Get();
        return response->kvs.size() == 1;
    });

    ASSERT_AWAIT_TRUE([&]() -> bool {
        map->clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetAbnormalScheduler, map).Get();
        return map->size() == 2 && map->find(NODE_ID_4) != map->end();  // add one
    });

    ASSERT_TRUE(client.Delete(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_4, {}).Get()->status.IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto response = client.Get(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_4, {}).Get();
        return response->kvs.empty();
    });

    ASSERT_AWAIT_TRUE([&]() -> bool {
        map->clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetAbnormalScheduler, map).Get();
        return map->size() == 1 && map->find(NODE_ID_3) != map->end() && map->find(NODE_ID_4) == map->end();
    });

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, OnLocalSchedulerFaultRecover)  // NOLINT
{
    PutInstances(true, true);
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    resource_view::InstanceInfo instance003 = CreateInstance(INSTANCE_PATH_PREFIX + "/003", true);
    instance003.set_functionproxyid(NODE_ID_2);
    std::string jsonString003;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString003, instance003));

    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();
    PutOption putOption = { .leaseId = 0, .prevKv = false };
    // eg. /sn/instance/business/yrk/tenant/0/function/0-test-0/version/..
    auto instanceKey = GenInstanceKey(instance003.function(), instance003.instanceid(), instance003.requestid());
    auto future = client.Put(instanceKey.Get(), jsonString003, putOption);
    ASSERT_AWAIT_READY_FOR(future, 1000);
    EXPECT_TRUE(future.Get()->status.IsOk());

    functionsystem::instance_manager::InstanceManagerMap map;
    ASSERT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_1, &map).Get();
        return map.size() == 2;  // two history instance
    });
    EXPECT_EQ(map.at(Inst1)->instancestatus().code(), (int32_t)InstanceState::RUNNING);
    EXPECT_EQ(map.at(Inst2)->instancestatus().code(), (int32_t)InstanceState::SCHEDULING);
    ASSERT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_2, &map).Get();
        return map.size() == 1;  // add a new instance
    });
    EXPECT_EQ(map.at(Inst3)->instancestatus().code(), (int32_t)InstanceState::RUNNING);

    EXPECT_CALL(*scheduler, Schedule)
        .Times(2)
        .WillOnce(testing::Return(Status::OK()))
        .WillOnce(testing::Return(Status::OK()));

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnLocalSchedFault, NODE_ID_1);
    ASSERT_AWAIT_TRUE([aid(instanceMgrActor->GetAID())]() -> bool {
        auto map = std::make_shared<std::unordered_set<std::string>>();
        litebus::Async(aid, &InstanceManagerActor::GetAbnormalScheduler, map).Get();
        return map->size() == 1 && map->find(NODE_ID_1) != map->end();
    });
    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto response = client.Get(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_1, {}).Get();
        return response->kvs.size() == 1;
    });

    EXPECT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, INSTANCE_MANAGER_OWNER, &map).Get();
        return map.size() == 2;
    });
    for (const auto &iterator : map) {
        EXPECT_TRUE(iterator.second->instancestatus().code() == static_cast<int32_t>(InstanceState::SCHEDULING));
    }

    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto kvs = client.Get(Inst1, {}).Get()->kvs;
        if (kvs.size() != 1) {
            return false;
        }

        resource_view::InstanceInfo instance;
        if (!TransToInstanceInfoFromJson(instance, kvs[0].value())) {
            return false;
        }
        return instance.instancestatus().code() == static_cast<int32_t>(InstanceState::SCHEDULING);
    });

    // recover instance
    resource_view::InstanceInfo instance001 = CreateInstance(INSTANCE_PATH_PREFIX + "/001", true);
    instance001.set_functionproxyid(NODE_ID_2);
    instance001.set_functionagentid("/sn/agent/002");
    std::string jsonString001;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString001, instance001));

    instanceKey = GenInstanceKey(instance001.function(), instance001.instanceid(), instance001.requestid());
    auto future001 = client.Put(instanceKey.Get(), jsonString001, putOption);
    ASSERT_AWAIT_READY_FOR(future001, 1000);
    EXPECT_TRUE(future001.Get()->status.IsOk());

    EXPECT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_1, &map).Get();
        return map.empty();
    });
    EXPECT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, INSTANCE_MANAGER_OWNER, &map).Get();
        return map.size() == 1;
    });
    EXPECT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_2, &map).Get();
        return map.size() == 2;
    });
    for (const auto &iterator : map) {
        EXPECT_TRUE(iterator.second->instancestatus().code() == static_cast<int32_t>(InstanceState::RUNNING));
    }

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, OnLocalSchedulerFaultNotRecover)  // NOLINT
{
    PutInstances(false, true);
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    resource_view::InstanceInfo instance003 = CreateInstance(INSTANCE_PATH_PREFIX + "/003");
    instance003.set_functionproxyid(NODE_ID_2);
    std::string jsonString003;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString003, instance003));

    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();
    PutOption putOption = { .leaseId = 0, .prevKv = false };
    // eg. /sn/instance/business/yrk/tenant/0/function/0-test-0/version/..
    auto instanceKey = GenInstanceKey(instance003.function(), instance003.instanceid(), instance003.requestid());
    auto future = client.Put(instanceKey.Get(), jsonString003, putOption);
    ASSERT_AWAIT_READY_FOR(future, 1000);
    EXPECT_TRUE(future.Get()->status.IsOk());

    resource_view::InstanceInfo instance004 = CreateInstance(INSTANCE_PATH_PREFIX + "/004");
    instance004.set_functionproxyid(NODE_ID_1);
    instance004.mutable_instancestatus()->set_code(5);
    std::string jsonString004;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString004, instance004));
    instanceKey = GenInstanceKey(instance004.function(), instance004.instanceid(), instance004.requestid());
    auto future4 = client.Put(instanceKey.Get(), jsonString004, putOption);
    ASSERT_AWAIT_READY_FOR(future4, 1000);
    EXPECT_TRUE(future4.Get()->status.IsOk());

    resource_view::InstanceInfo instanceDriver = CreateInstance(INSTANCE_PATH_PREFIX + "/driver-004");
    instanceDriver.set_functionproxyid(NODE_ID_1);
    instanceDriver.mutable_instancestatus()->set_code(3);
    std::string jsonStringDriver;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonStringDriver, instanceDriver));
    instanceKey = GenInstanceKey(instanceDriver.function(), instanceDriver.instanceid(), instanceDriver.requestid());
    auto futureDriver = client.Put(instanceKey.Get(), jsonStringDriver, putOption);
    ASSERT_AWAIT_READY_FOR(futureDriver, 1000);
    EXPECT_TRUE(futureDriver.Get()->status.IsOk());

    functionsystem::instance_manager::InstanceManagerMap map;
    ASSERT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_1, &map).Get();
        return map.size() == 4;  // two history instance
    });
    EXPECT_EQ(map.at(Inst1)->instancestatus().code(), (int32_t)InstanceState::RUNNING);
    EXPECT_EQ(map.at(Inst2)->instancestatus().code(), (int32_t)InstanceState::SCHEDULING);
    ASSERT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_2, &map).Get();
        return map.size() == 1;  // add a new instance
    });
    EXPECT_EQ(map.at(Inst3)->instancestatus().code(), (int32_t)InstanceState::RUNNING);

    EXPECT_CALL(*scheduler, Schedule).Times(0);
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnLocalSchedFault, NODE_ID_1);

    ASSERT_AWAIT_TRUE([aid(instanceMgrActor->GetAID())]() -> bool {
        auto map = std::make_shared<std::unordered_set<std::string>>();
        litebus::Async(aid, &InstanceManagerActor::GetAbnormalScheduler, map).Get();
        return map->size() == 1 && map->find(NODE_ID_1) != map->end();
    });
    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto response = client.Get(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_1, {}).Get();
        return response->kvs.size() == 1;
    });
    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto response = client.Get(Inst4, {}).Get();
        return response->kvs.empty();
    });
    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto kvs = client.Get(Inst1, {}).Get()->kvs;
        return kvs.size() == 1;
    });

    ASSERT_AWAIT_TRUE([&]() -> bool {
        map.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_1, &map).Get();
        return map.empty();
    });

    map.clear();  // [notice] clear and then Get
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_2, &map).Get();
    EXPECT_EQ(map.size(), size_t{1});

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, PutInstanceAfterAbnormal)  // NOLINT
{
    PutInstances();
    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();
    ASSERT_TRUE(client.Put(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_3, NODE_ID_3, {}).Get()->status.IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto response = client.Get(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_3, {}).Get();
        return response->kvs.size() == 1;
    });

    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));

    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    auto map = std::make_shared<std::unordered_set<std::string>>();
    ASSERT_AWAIT_TRUE([&]() -> bool {
        map->clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetAbnormalScheduler, map).Get();  // block
        return map->size() == 1 && map->find(NODE_ID_3) != map->end();  // one history
    });

    resource_view::InstanceInfo instance003 = CreateInstance(INSTANCE_PATH_PREFIX + "/003");
    instance003.set_functionproxyid(NODE_ID_3);
    std::string jsonString;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString, instance003));

    instanceMgrActor->HandleSystemUpgrade(false);
    // eg. /sn/instance/business/yrk/tenant/0/function/0-test-0/version/..
    ASSERT_TRUE(client.Put(instance003.instanceid(), jsonString, {}).Get()->status.IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool {
        // node3 is abnormal, instance at node3 will be deleted
        auto response = client.Get(instance003.instanceid(), {}).Get();
        return response->kvs.size() == 1;
    });

    // don't delete instance when system is upgrading
    instanceMgrActor->HandleSystemUpgrade(true);
    ASSERT_TRUE(client.Put(instance003.instanceid(), jsonString, {}).Get()->status.IsOk());

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, OnChange)  // NOLINT
{
    PutInstances(true);
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    EXPECT_CALL(*scheduler, GetLocalAddress)
        .WillRepeatedly(testing::Return(litebus::Option<std::string>(localAddress_)));
    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();

    std::string jsonString001;
    resource_view::InstanceInfo instance001 = CreateInstance(INSTANCE_PATH_PREFIX + "/001", true);
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString001, instance001));
    ASSERT_TRUE(client.Put(instance001.instanceid(), jsonString001, {}).Get()->status.IsOk());

    std::string jsonString002;
    resource_view::InstanceInfo instance002 = CreateInstance(INSTANCE_PATH_PREFIX + "/002", true);
    auto status = instance002.mutable_instancestatus();
    status->set_code((int32_t)InstanceState::SCHEDULING);
    status->set_msg("scheduling");
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString002, instance002));
    ASSERT_TRUE(client.Put(instance002.instanceid(), jsonString002, {}).Get()->status.IsOk());

    std::string jsonString003;
    resource_view::InstanceInfo instance003 = CreateInstance(INSTANCE_PATH_PREFIX + "/003", true);
    instance003.set_functionproxyid(NODE_ID_2);
    instance003.set_parentid("frontendParent");
    (*instance003.mutable_extensions())["source"] = "frontend";
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString003, instance003));
    ASSERT_TRUE(client.Put(instance003.instanceid(), jsonString003, {}).Get()->status.IsOk());

    std::string jsonString004;
    resource_view::InstanceInfo instance004 = CreateInstance(INSTANCE_PATH_PREFIX + "/004", true);
    instance004.set_functionproxyid("fake_node");
    instance004.set_parentid("frontendParent");
    (*instance004.mutable_extensions())["source"] = "frontend";
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString004, instance004));
    ASSERT_TRUE(client.Put(instance004.instanceid(), jsonString004, {}).Get()->status.IsOk());

    ASSERT_AWAIT_TRUE(
        [&]() -> bool { return client.Get(INSTANCE_PATH_PREFIX, { .prefix = true }).Get()->kvs.size() == 4; });

    ASSERT_TRUE(client.Put(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_1, NODE_ID_1, {}).Get()->status.IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool {
        auto response = client.Get(KEY_ABNORMAL_SCHEDULER_PREFIX + NODE_ID_1, {}).Get();
        return response->kvs.size() == 1;
    });

    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    auto map = std::make_shared<std::unordered_set<std::string>>();
    ASSERT_AWAIT_TRUE([&]() -> bool {
        map->clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetAbnormalScheduler, map).Get();  // block
        return map->size() == 1 && map->find(NODE_ID_1) != map->end();  // one history
    });

    functionsystem::instance_manager::InstanceManagerMap instMgrMap;
    ASSERT_AWAIT_TRUE([&]() -> bool {
        instMgrMap.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_1, &instMgrMap).Get();
        return instMgrMap.size() == 2;  // two history instance
    });
    ASSERT_AWAIT_TRUE([&]() -> bool {
        instMgrMap.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_2, &instMgrMap).Get();
        return instMgrMap.size() == 1;  // two history instance
    });

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    ASSERT_AWAIT_TRUE([&]() -> bool {
        instMgrMap.clear();  // [notice] clear and then Get
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Get, NODE_ID_1, &instMgrMap).Get();
        return instMgrMap.size() == 0;  // two history instance
    });

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

// SlaveBusiness test cases
TEST_F(DISABLED_InstanceManagerTest, SlaveBusinessTest)  // NOLINT
{
    PutInstances(true);
    auto member = std::make_shared<InstanceManagerActor::Member>();
    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        nullptr, nullptr, nullptr, nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto slaveBusiness = std::make_shared<InstanceManagerActor::SlaveBusiness>(member, instanceMgrActor);
    slaveBusiness->ForwardKill(litebus::AID(), "", "");
    slaveBusiness->IsLocalAbnormal("");
    slaveBusiness->OnChange();
    slaveBusiness->OnFaultLocalInstancePut("", nullptr, "abnormal");
    slaveBusiness->OnSyncAbnormalScheduler(InstanceManagerMap());
    slaveBusiness->OnLocalSchedFault("");
    auto put = litebus::Future<std::shared_ptr<PutResponse>>();
    auto promise = std::make_shared<litebus::Promise<Status>>();
    slaveBusiness->OnPutAbnormalScheduler(put, promise, "");
    EXPECT_EQ(promise->GetFuture().IsOK(), true);
    EXPECT_EQ(promise->GetFuture().Get().IsOk(), true);

    slaveBusiness->member_->exitingInstances.insert("123");
    slaveBusiness->member_->family = std::make_shared<InstanceFamilyCaches>();
    auto info = std::make_shared<resource_view::InstanceInfo>();
    info->set_instanceid("123");
    slaveBusiness->OnInstanceDeleteForFamilyManagement("", info);
    EXPECT_EQ(slaveBusiness->member_->exitingInstances.find("123"), slaveBusiness->member_->exitingInstances.end());
}

TEST_F(DISABLED_InstanceManagerTest, ForwardKillInstance)
{
    std::string jsonString001;
    resource_view::InstanceInfo instance001 = CreateInstance(INSTANCE_PATH_PREFIX + "/001", true);
    instance001.set_functionproxyid("");
    instance001.set_function("0/0-system-faascontroller/$latest");
    instance001.mutable_instancestatus()->set_code(6);
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString001, instance001));
    auto scheduler = std::make_shared<MockGlobalSched>();
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);
    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();
    auto mockBootstrapActor = std::make_shared<MockBootstrapStubActor>("MockBootstrapStubActor");
    litebus::Spawn(mockBootstrapActor);
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));
    messages::ForwardKillRequest req;
    req.set_requestid("kill-0001");
    req.mutable_instance()->set_functionproxyid("");
    req.mutable_instance()->set_function(instance001.function());
    req.mutable_instance()->set_instanceid(instance001.instanceid());
    req.mutable_instance()->set_version(instance001.version());
    auto future = litebus::Async(mockBootstrapActor->GetAID(), &MockBootstrapStubActor::SendForwardKill,
                                 instanceMgrActor->GetAID(), req);
    ASSERT_AWAIT_READY(future);
    litebus::Terminate(mockBootstrapActor->GetAID());
    litebus::Await(mockBootstrapActor->GetAID());
    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

// FamilyManagement test cases
TEST_F(DISABLED_InstanceManagerTest, FamilyManagement_OnParentMissingInstancePut)  // NOLINT
{
    // make new
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, GetLocalAddress)
        .WillRepeatedly(testing::Return(litebus::Option<std::string>(localAddress_)));

    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    // Given: An running instance manager
    // Mocks: local scheduler
    litebus::Future<std::string> sigArg;
    EXPECT_CALL(*mockInstCtrlActorNode01_, MockForwardCustomSignalRequest)
        .WillOnce(testing::DoAll(FutureArg<2>(&sigArg),
                                 testing::Return(std::make_pair(
                                     true, GenForwardKillResponse("requestID0", common::ErrorCode::ERR_NONE, "ok")))));

    litebus::Future<std::string> putGroupArg;
    EXPECT_CALL(*groupMgr, OnInstancePut)
        .WillRepeatedly(testing::DoAll(FutureArg<0>(&putGroupArg), testing::Return(Status::OK())));

    // When : put an instance with an non-existing instance
    auto instA = MakeInstanceInfo("A", "", "X", NODE_ID_1, InstanceState::RUNNING);
    instA->set_detached(true);
    auto instB = MakeInstanceInfo("B", "", "X", NODE_ID_1, InstanceState::RUNNING);
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut,
                   INSTANCE_PATH_PREFIX + "/" + instA->instanceid(), instA);
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut,
                   INSTANCE_PATH_PREFIX + "/" + instB->instanceid(), instB);

    // Then : expect instance C is killed
    ASSERT_AWAIT_READY(sigArg);
    internal::ForwardKillRequest killReq;
    ASSERT_TRUE(killReq.ParseFromString(sigArg.Get()));
    ASSERT_EQ(killReq.req().signal(), SHUT_DOWN_SIGNAL);
    ASSERT_EQ(killReq.req().instanceid(), instB->instanceid());

    // Then : expect group manager get the message
    ASSERT_AWAIT_READY(putGroupArg);

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, FamilyManagement_OnAbnormalInstancePut)  // NOLINT
{
    // make new
    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();

    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, GetLocalAddress)
        .WillRepeatedly(testing::Return(litebus::Option<std::string>(localAddress_)));

    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    // Given: put a lot of instances, on same node (node1)
    auto infos = MakeInstanceInfos();
    for (auto info : infos) {
        if (info->instanceid() == "F") {
            info->set_detached(true);
        }
        litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut,
                       INSTANCE_PATH_PREFIX + "/" + info->instanceid(), info);
    }

    // Mocks:
    std::unordered_map<std::string, litebus::Future<internal::ForwardKillRequest>> sigArgs{
        { "D", litebus::Future<internal::ForwardKillRequest>() },
        { "E", litebus::Future<internal::ForwardKillRequest>() },
        { "G", litebus::Future<internal::ForwardKillRequest>() },
    };
    EXPECT_CALL(*mockInstCtrlActorNode01_, MockForwardCustomSignalRequest)
        .Times(3)
        .WillRepeatedly(testing::DoAll(
            testing::Invoke([&sigArgs](const litebus::AID &, const std::string &,
                                       const std::string &msg) -> std::pair<bool, internal::ForwardKillResponse> {
                internal::ForwardKillRequest req;
                EXPECT_TRUE(req.ParseFromString(msg));
                EXPECT_TRUE(sigArgs.find(req.req().instanceid()) != sigArgs.end());
                sigArgs[req.req().instanceid()].SetValue(req);
                return std::make_pair(true, GenForwardKillResponse(req.requestid(), common::ErrorCode::ERR_NONE, "ok"));
            })));

    litebus::Future<std::string> abnormalInstancePutArg;
    EXPECT_CALL(*groupMgr, OnInstanceAbnormal).Times(1)
        .WillOnce(testing::DoAll(FutureArg<0>(&abnormalInstancePutArg), testing::Return(Status::OK())));

    // When : one of instance is fatal, let's say, instance C
    auto instC = MakeInstanceInfo("C", "", "A", NODE_ID_1, InstanceState::FATAL);
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut,
                   INSTANCE_PATH_PREFIX + "/" + instC->instanceid(), instC);

    // Then : DEG will be set to FATAL, while F not since F is detached
    for (auto [k, future] : sigArgs) {
        YRLOG_INFO("asserting instance {} to be set", k);
        ASSERT_AWAIT_READY(future);
        EXPECT_EQ(future.Get().req().instanceid(), k);
        EXPECT_EQ(future.Get().req().signal(), FAMILY_EXIT_SIGNAL);
        EXPECT_EQ(future.Get().srcinstanceid(), "");
    }

    // Then : and will notify group manager
    ASSERT_AWAIT_READY(abnormalInstancePutArg);

    // ---------------------- app driver ----------------------
    // Mocks:
    std::unordered_map<std::string, litebus::Future<internal::ForwardKillRequest>> sigArgs1{
        { "D", litebus::Future<internal::ForwardKillRequest>() },
        { "E", litebus::Future<internal::ForwardKillRequest>() },
        { "G", litebus::Future<internal::ForwardKillRequest>() },
    };
    EXPECT_CALL(*mockInstCtrlActorNode01_, MockForwardCustomSignalRequest)
        .Times(3)
        .WillRepeatedly(testing::DoAll(
            testing::Invoke([&sigArgs1](const litebus::AID &, const std::string &,
                                       const std::string &msg) -> std::pair<bool, internal::ForwardKillResponse> {
                internal::ForwardKillRequest req;
                EXPECT_TRUE(req.ParseFromString(msg));
                EXPECT_TRUE(sigArgs1.find(req.req().instanceid()) != sigArgs1.end());
                sigArgs1[req.req().instanceid()].SetValue(req);
                return std::make_pair(true, GenForwardKillResponse(req.requestid(), common::ErrorCode::ERR_NONE, "ok"));
            })));
    EXPECT_CALL(*groupMgr, OnInstanceAbnormal)
        .WillOnce(testing::DoAll(FutureArg<0>(&abnormalInstancePutArg), testing::Return(Status::OK())));
    // When: app driver is succeeded,(code:6(FATAL), type:1)
    auto succeededInstAppDriver = MakeInstanceInfo("C", "", "A", NODE_ID_1, InstanceState::FATAL);
    succeededInstAppDriver->mutable_instancestatus()->set_type(1);
    succeededInstAppDriver->mutable_createoptions()->insert({ APP_ENTRYPOINT, "python script.py"});
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut,
           INSTANCE_PATH_PREFIX + "/" + succeededInstAppDriver->instanceid(), succeededInstAppDriver);

    // Then : DEG will be killed, while F not since F is detached
    for (auto [k, future] : sigArgs1) {
        YRLOG_INFO("asserting instance {} to be set", k);
        ASSERT_AWAIT_READY(future);
        EXPECT_EQ(future.Get().req().instanceid(), k);
        EXPECT_EQ(future.Get().req().signal(), SHUT_DOWN_SIGNAL);
        EXPECT_EQ(future.Get().srcinstanceid(), "");
    }

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, FamilyManagement_RetryKill)  // NOLINT
{
    const int retryIntervalMsInThisTest = 300;
    // make new
    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();

    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, GetLocalAddress)
        .WillOnce(testing::Return(litebus::None()))
        .WillOnce(testing::Return(litebus::None()))
        .WillOnce(testing::Return(litebus::Option<std::string>(localAddress_)));

    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    instanceMgrActor->SetKillRetryInterval(retryIntervalMsInThisTest);
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    // Given: put a lot of instances, on same node (node1)
    auto infoA = MakeInstanceInfo("A", "", "", NODE_ID_1, InstanceState::RUNNING);
    auto infoB = MakeInstanceInfo("B", "", "A", NODE_ID_1, InstanceState::RUNNING);

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut,
                   INSTANCE_PATH_PREFIX + "/" + infoA->instanceid(), infoA);
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut,
                   INSTANCE_PATH_PREFIX + "/" + infoB->instanceid(), infoB);

    // Mocks:
    auto promiseB = litebus::Future<internal::ForwardKillRequest>();
    EXPECT_CALL(*mockInstCtrlActorNode01_, MockForwardCustomSignalRequest)
        .WillOnce(testing::DoAll(
            testing::Invoke([&promiseB](const litebus::AID &, const std::string &,
                                        const std::string &msg) -> std::pair<bool, internal::ForwardKillResponse> {
                internal::ForwardKillRequest req;
                EXPECT_TRUE(req.ParseFromString(msg));
                promiseB.SetValue(req);
                return std::make_pair(true, GenForwardKillResponse(req.requestid(), common::ErrorCode::ERR_NONE, "ok"));
            })));

    // When : one of instance is fatal, let's say, instance A
    auto infoAFatal = MakeInstanceInfo("A", "", "", NODE_ID_1, InstanceState::FATAL);
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut,
                   INSTANCE_PATH_PREFIX + "/" + infoAFatal->instanceid(), infoAFatal);

    // Then : B will be set to FATAL
    ASSERT_AWAIT_READY(promiseB);
    EXPECT_EQ(promiseB.Get().req().instanceid(), "B");
    EXPECT_EQ(promiseB.Get().req().signal(), FAMILY_EXIT_SIGNAL);
    EXPECT_EQ(promiseB.Get().srcinstanceid(), "");

    // When : put B fatal event,
    auto infoBFatal = MakeInstanceInfo("B", "", "A", NODE_ID_1, InstanceState::FATAL);
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut,
                   INSTANCE_PATH_PREFIX + "/" + infoBFatal->instanceid(), infoBFatal);

    // Wait another around to make sure no more signal will be forwarded
    std::this_thread::sleep_for(std::chrono::milliseconds(retryIntervalMsInThisTest));
    auto killReqPromiseB = instanceMgrActor->member_->killReqPromises.find(promiseB.Get().requestid());
    if (killReqPromiseB != instanceMgrActor->member_->killReqPromises.end()) {
        // not found means already removed, which is ok
        // found, means this is still async processing in background, should wait the result
        ASSERT_AWAIT_READY(killReqPromiseB->second->GetFuture());
        EXPECT_TRUE(killReqPromiseB->second->GetFuture().Get().IsOk());
    }

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, WatchInstanceMetaJobTest)  // NOLINT
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    std::string funcAgentID = "funcAgent";
    std::string functionA = "123/helloworldA/$latest";
    std::string functionB = "123/helloworldB/$latest";

    std::string instanceIDA = "instanceA";
    InstanceState instanceStatusA = InstanceState::RUNNING;
    auto instanceInfoA = GenInstanceInfo(instanceIDA, funcAgentID, functionA, instanceStatusA);
    instanceInfoA.set_jobid("job1");

    std::string jsonStringA;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonStringA, instanceInfoA));
    auto keyA = GenInstanceKey(functionA, instanceIDA, instanceIDA).Get();

    std::string instanceIDB = "instanceB";
    InstanceState instanceStatusB = InstanceState::SCHEDULING;
    auto instanceInfoB = GenInstanceInfo(instanceIDB, funcAgentID, functionB, instanceStatusB);
    instanceInfoB.set_jobid("job1");

    std::string jsonStringB;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonStringB, instanceInfoB));
    auto keyB = GenInstanceKey(functionB, instanceIDB, instanceIDB).Get();

    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();
    ASSERT_TRUE(client.Put(keyA, jsonStringA, {}).Get()->status.IsOk());
    ASSERT_TRUE(client.Put(keyB, jsonStringB, {}).Get()->status.IsOk());

    ASSERT_AWAIT_TRUE([&]() {
        return litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetInstanceFuncMetaMap).Get().size() ==
               2;
    });
    auto funcMap = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetInstanceFuncMetaMap);
    ASSERT_AWAIT_READY(funcMap);
    EXPECT_TRUE(funcMap.Get().find(functionA) != funcMap.Get().end());
    EXPECT_TRUE(funcMap.Get().at(functionA).find("instanceA") != funcMap.Get().at(functionA).end());

    EXPECT_TRUE(funcMap.Get().find(functionB) != funcMap.Get().end());
    EXPECT_TRUE(funcMap.Get().at(functionB).find("instanceB") != funcMap.Get().at(functionB).end());

    auto jobMap = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetInstanceJobMap);
    ASSERT_AWAIT_READY(jobMap);
    EXPECT_EQ(jobMap.Get().size(), static_cast<long unsigned int>(1));
    EXPECT_TRUE(jobMap.Get().find("job1") != jobMap.Get().end());
    EXPECT_TRUE(jobMap.Get().at("job1").find(instanceIDA) != jobMap.Get().at("job1").end());
    EXPECT_TRUE(jobMap.Get().at("job1").find(instanceIDB) != jobMap.Get().at("job1").end());

    ASSERT_TRUE(client.Delete(keyA, {}).Get()->status.IsOk());
    ASSERT_TRUE(client.Delete(keyB, {}).Get()->status.IsOk());

    ASSERT_AWAIT_TRUE([&]() {
        return litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetInstanceFuncMetaMap).Get().empty();
    });

    funcMap = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetInstanceFuncMetaMap);
    ASSERT_AWAIT_READY(funcMap);
    EXPECT_TRUE(funcMap.Get().empty());

    jobMap = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetInstanceJobMap);
    ASSERT_AWAIT_READY(jobMap);
    EXPECT_TRUE(jobMap.Get().empty());

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, FuncMetaKillTest)  // NOLINT
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    std::string funcAgentID = "funcAgent";
    std::string functionA = "123/helloworldA/$latest";
    std::string functionB = "123/helloworldB/$latest";
    std::string funcPathA = "/yr/functions/business/yrk/tenant/123/function/helloworldA/version/$latest";

    std::string instanceIDA = "instanceA";
    InstanceState instanceStatusA = InstanceState::RUNNING;
    auto instanceInfoA = GenInstanceInfo(instanceIDA, funcAgentID, functionA, instanceStatusA);
    instanceInfoA.set_jobid("job1");
    instanceInfoA.set_functionproxyid(NODE_ID_1);

    std::string jsonStringA;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonStringA, instanceInfoA));
    auto keyA = GenInstanceKey(functionA, instanceIDA, instanceIDA).Get();

    std::string instanceIDB = "instanceB";
    InstanceState instanceStatusB = InstanceState::SCHEDULING;
    auto instanceInfoB = GenInstanceInfo(instanceIDB, funcAgentID, functionB, instanceStatusB);
    instanceInfoB.set_jobid("job1");
    instanceInfoB.set_functionproxyid(NODE_ID_2);

    std::string jsonStringB;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonStringB, instanceInfoB));
    auto keyB = GenInstanceKey(functionB, instanceIDB, instanceIDB).Get();

    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();
    ASSERT_TRUE(client.Put(keyA, jsonStringA, {}).Get()->status.IsOk());
    ASSERT_TRUE(client.Put(keyB, jsonStringB, {}).Get()->status.IsOk());
    ASSERT_TRUE(client.Put(funcPathA, "", {}).Get()->status.IsOk());

    ASSERT_AWAIT_TRUE([&]() {
        return litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetInstanceFuncMetaMap).Get().size() ==
               2;
    });

    litebus::Future<std::string> sigArg;
    EXPECT_CALL(*mockInstCtrlActorNode01_, MockForwardCustomSignalRequest)
        .WillOnce(testing::DoAll(FutureArg<2>(&sigArg),
                                 testing::Return(std::make_pair(
                                     true, GenForwardKillResponse("requestID0", common::ErrorCode::ERR_NONE, "ok")))));
    EXPECT_CALL(*scheduler, GetRootDomainInfo()).WillOnce(testing::Return(litebus::None()));
    EXPECT_CALL(*scheduler, GetLocalAddress(NODE_ID_1))
        .WillOnce(testing::Return(litebus::Option<std::string>(localAddress_)));
    ASSERT_TRUE(client.Delete(funcPathA, {}).Get()->status.IsOk());

    ASSERT_AWAIT_READY(sigArg);
    internal::ForwardKillRequest killReq;
    ASSERT_TRUE(killReq.ParseFromString(sigArg.Get()));
    ASSERT_EQ(killReq.req().signal(), SHUT_DOWN_SIGNAL);
    ASSERT_EQ(killReq.req().instanceid(), instanceIDA);

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, JobKillTest)  // NOLINT
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    std::string funcAgentID = "funcAgent";
    std::string functionA = "123/helloworldA/$latest";
    std::string functionB = "123/helloworldB/$latest";
    std::string funcPathA = "/yr/functions/business/yrk/tenant/123/function/helloworldA/version/$latest";

    std::string instanceIDA = "instanceA";
    InstanceState instanceStatusA = InstanceState::RUNNING;
    auto instanceInfoA = GenInstanceInfo(instanceIDA, funcAgentID, functionA, instanceStatusA);
    instanceInfoA.set_jobid("job1");
    instanceInfoA.set_functionproxyid(NODE_ID_1);

    std::string jsonStringA;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonStringA, instanceInfoA));
    auto keyA = GenInstanceKey(functionA, instanceIDA, instanceIDA).Get();

    std::string instanceIDB = "instanceB";
    InstanceState instanceStatusB = InstanceState::SCHEDULING;
    auto instanceInfoB = GenInstanceInfo(instanceIDB, funcAgentID, functionB, instanceStatusB);
    instanceInfoB.set_jobid("job1");
    instanceInfoB.set_functionproxyid(NODE_ID_1);
    instanceInfoB.set_detached(true);

    std::string jsonStringB;
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonStringB, instanceInfoB));
    auto keyB = GenInstanceKey(functionB, instanceIDB, instanceIDB).Get();

    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();
    ASSERT_TRUE(client.Put(keyA, jsonStringA, {}).Get()->status.IsOk());
    ASSERT_TRUE(client.Put(keyB, jsonStringB, {}).Get()->status.IsOk());

    ASSERT_AWAIT_TRUE([&]() {
        return litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::GetInstanceFuncMetaMap).Get().size() ==
               2;
    });

    litebus::Future<std::string> sigArg1;
    EXPECT_CALL(*mockInstCtrlActorNode01_, MockForwardCustomSignalRequest)
        .WillOnce(testing::DoAll(FutureArg<2>(&sigArg1),
                                 testing::Return(std::make_pair(
                                     true, GenForwardKillResponse("requestID0", common::ErrorCode::ERR_NONE, "ok")))));
    EXPECT_CALL(*scheduler, GetLocalAddress(NODE_ID_1))
        .WillOnce(testing::Return(litebus::Option<std::string>(localAddress_)));

    auto mockBootstrapActor = std::make_shared<MockBootstrapStubActor>("MockBootstrapStubActor");
    litebus::Spawn(mockBootstrapActor);
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));
    messages::ForwardKillRequest req;
    req.set_requestid("request-job1");
    req.mutable_req()->set_signal(SHUT_DOWN_SIGNAL_ALL);
    req.mutable_req()->set_instanceid("job1");
    litebus::Async(mockBootstrapActor->GetAID(), &MockBootstrapStubActor::SendForwardKill,
                   instanceMgrActor->GetAID(), req);

    ASSERT_AWAIT_READY(sigArg1);
    internal::ForwardKillRequest killReq;
    ASSERT_TRUE(killReq.ParseFromString(sigArg1.Get()));
    ASSERT_EQ(killReq.req().signal(), SHUT_DOWN_SIGNAL);
    ASSERT_EQ(killReq.req().instanceid(), instanceIDA);

    // kill invalid job, return ok
    messages::ForwardKillRequest req2;
    req2.set_requestid("request-job2");
    req2.mutable_req()->set_signal(SHUT_DOWN_SIGNAL_ALL);
    req2.mutable_req()->set_instanceid("job2");
    EXPECT_CALL(*scheduler, GetRootDomainInfo()).WillRepeatedly(testing::Return(litebus::None()));
    auto resp = litebus::Async(mockBootstrapActor->GetAID(), &MockBootstrapStubActor::SendForwardKill,
                               instanceMgrActor->GetAID(), req2);
    ASSERT_AWAIT_READY(resp);
    ASSERT_TRUE(resp.Get().IsOk());

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, PutProxyAbnormalFailed)  // NOLINT
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);
    auto mockMetaStoreClient = std::make_shared<MockMetaStoreClient>(metaStoreServerHost_);
    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        mockMetaStoreClient, scheduler, groupMgr, nullptr,
        InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    std::shared_ptr<PutResponse> rep = std::make_shared<PutResponse>();
    rep->status = Status(StatusCode::FAILED, "");
    EXPECT_CALL(*mockMetaStoreClient, Put)
        .WillRepeatedly(testing::Return(litebus::Future<std::shared_ptr<PutResponse>>(rep)));

    auto future = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnLocalSchedFault, NODE_ID_1);
    ASSERT_AWAIT_READY(future);
    ASSERT_FALSE(future.Get().IsOk());

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, ProxyAbnormalSyncerTest)  // NOLINT
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);
    auto mockMetaStoreClient = std::make_shared<MockMetaStoreClient>(metaStoreServerHost_);
    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        mockMetaStoreClient, scheduler, groupMgr, nullptr,
        InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    {   // for get failed
        std::shared_ptr<GetResponse> rep = std::make_shared<GetResponse>();
        rep->status = Status(StatusCode::FAILED, "");

        auto future = instanceMgrActor->ProxyAbnormalSyncer(rep);
        ASSERT_AWAIT_READY(future);
        ASSERT_FALSE(future.Get().status.IsOk());
    }

    {   // for get response is empty
        std::shared_ptr<GetResponse> rep = std::make_shared<GetResponse>();
        rep->status = Status::OK();

        auto future = instanceMgrActor->ProxyAbnormalSyncer(rep);
        ASSERT_AWAIT_READY(future);
        ASSERT_TRUE(future.Get().status.IsOk());
    }

    {   // for get response is not empty
        KeyValue getKeyValue;
        getKeyValue.set_key(KEY_ABNORMAL_SCHEDULER_PREFIX+"Node1") ;
        getKeyValue.set_value("Node1");

        std::shared_ptr<GetResponse> rep = std::make_shared<GetResponse>();
        rep->status = Status::OK();
        rep->kvs.emplace_back(getKeyValue);

        auto future = instanceMgrActor->ProxyAbnormalSyncer(rep);
        ASSERT_AWAIT_READY(future);
        ASSERT_TRUE(future.Get().status.IsOk());
    }

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}


TEST_F(DISABLED_InstanceManagerTest, FunctionMetaSyncerTest)  // NOLINT
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);
    auto mockMetaStoreClient = std::make_shared<MockMetaStoreClient>(metaStoreServerHost_);
    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        mockMetaStoreClient, scheduler, groupMgr, nullptr,
        InstanceManagerStartParam{ .runtimeRecoverEnable = false, .isMetaStoreEnable= false, .servicesPath="/tmp/services.yaml", .libPath="/tmp/", .functionMetaPath="/tmp/executor-meta/" });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    WaitSyncFuncMeta(mockMetaStoreClient, instanceMgrActor);

    {   // for get failed
        litebus::Future<std::shared_ptr<GetResponse>> getResponseFuture;
        std::shared_ptr<GetResponse> rep = std::make_shared<GetResponse>();
        rep->status = Status(StatusCode::FAILED, "");
        getResponseFuture.SetValue(rep);
        EXPECT_CALL(*mockMetaStoreClient, Get).WillOnce(testing::Return(getResponseFuture));

        auto future = instanceMgrActor->FunctionMetaSyncer();
        ASSERT_AWAIT_READY(future);
        ASSERT_FALSE(future.Get().status.IsOk());
    }

    {   // for get response is empty
        litebus::Future<std::shared_ptr<GetResponse>> getResponseFuture;
        std::shared_ptr<GetResponse> rep = std::make_shared<GetResponse>();
        rep->status = Status::OK();
        getResponseFuture.SetValue(rep);
        EXPECT_CALL(*mockMetaStoreClient, Get).WillOnce(testing::Return(getResponseFuture));

        auto future = instanceMgrActor->FunctionMetaSyncer();
        ASSERT_AWAIT_READY(future);
        ASSERT_TRUE(future.Get().status.IsOk());
    }

    {   // for get response is not empty
        auto meta = R"({"funcMetaData":{"layers":[],"name":"0@faaspy@hello","description":"empty function","functionUrn":"sn:cn:yrk:default:function:0@faaspy@hello","reversedConcurrency":0})";
        auto funcKey = R"(/yr/functions/business/yrk/tenant/default/function/0@faaspy@hello/version/latest)";
        KeyValue getKeyValue;
        getKeyValue.set_key(funcKey);
        getKeyValue.set_value(meta);

        litebus::Future<std::shared_ptr<GetResponse>> getResponseFuture;
        std::shared_ptr<GetResponse> rep = std::make_shared<GetResponse>();
        rep->status = Status::OK();
        rep->kvs.emplace_back(getKeyValue);
        getResponseFuture.SetValue(rep);
        EXPECT_CALL(*mockMetaStoreClient, Get).WillOnce(testing::Return(getResponseFuture));

        auto future = instanceMgrActor->FunctionMetaSyncer();
        ASSERT_AWAIT_READY(future);
        ASSERT_TRUE(future.Get().status.IsOk());

        // for delete key in cache but not in etcd
        std::string instanceIDA = "instanceA";
        InstanceState instanceStatusA = InstanceState::RUNNING;
        auto instanceInfoA = GenInstanceInfo(instanceIDA, "funcAgent", "123/helloworldA/$latest", instanceStatusA);
        instanceInfoA.set_functionproxyid(NODE_ID_1);
        instanceInfoA.set_jobid("job-1");
        auto keyA = GenInstanceKey("123/helloworldA/$latest", instanceIDA, instanceIDA).Get();
        std::string instanceIDB = "instanceB";
        auto instanceInfoB =
            GenInstanceInfo(instanceIDB, "funcAgent",
                            "default/0-defaultservice-default/$latest", instanceStatusA);
        instanceInfoB.set_functionproxyid(NODE_ID_1);
        instanceInfoB.set_jobid("job-1");
        auto keyB = GenInstanceKey("default/0-defaultservice-default/$latest", instanceIDA,
                                   instanceIDA)
                        .Get();
        instanceMgrActor->OnInstancePut(keyA, std::make_shared<resource_view::InstanceInfo>(instanceInfoA));
        instanceMgrActor->OnInstancePut(keyB, std::make_shared<resource_view::InstanceInfo>(instanceInfoB));

        ASSERT_AWAIT_TRUE([&]() { return instanceMgrActor->member_->jobID2InstanceIDs["job-1"].size() == 2; });
        ASSERT_AWAIT_TRUE(
            [&]() { return instanceMgrActor->member_->funcMeta2InstanceIDs["123/helloworldA/$latest"].size() == 1; });

        litebus::Future<std::string> sigArg1;
        EXPECT_CALL(*mockInstCtrlActorNode01_, MockForwardCustomSignalRequest)
            .WillOnce(
                testing::DoAll(FutureArg<2>(&sigArg1),
                               testing::Return(std::make_pair(
                                   true, GenForwardKillResponse("requestID0", common::ErrorCode::ERR_NONE, "ok")))));
        EXPECT_CALL(*scheduler, GetLocalAddress(NODE_ID_1))
            .WillOnce(testing::Return(litebus::Option<std::string>(localAddress_)));

        EXPECT_CALL(*mockMetaStoreClient, Get).WillOnce(testing::Return(getResponseFuture));

        future = instanceMgrActor->FunctionMetaSyncer();
        ASSERT_AWAIT_READY(future);
        ASSERT_TRUE(future.Get().status.IsOk());
        ASSERT_AWAIT_READY(sigArg1);
        internal::ForwardKillRequest killReq;
        ASSERT_TRUE(killReq.ParseFromString(sigArg1.Get()));
        ASSERT_EQ(killReq.req().signal(), SHUT_DOWN_SIGNAL);
        ASSERT_EQ(killReq.req().instanceid(), instanceIDA);
    }

    {
        // driver instance will not be deleted when metadata not found
        instanceMgrActor->member_->funcMeta2InstanceIDs.clear();
        std::string instanceID = "instanceDriver";
        auto instanceInfo =
            GenInstanceInfo(instanceID, "funcAgent",
                            "456/0-driver-default/$latest", InstanceState::RUNNING);
        instanceInfo.set_functionproxyid(NODE_ID_1);
        instanceInfo.set_jobid("job-1");
        instanceInfo.mutable_extensions()->emplace("source", "driver");
        auto key = GenInstanceKey("456/0-driver-default/$latest", instanceID,
                                   instanceID).Get();
 
        instanceMgrActor->OnInstancePut(key, std::make_shared<resource_view::InstanceInfo>(instanceInfo));
        // driver instance will not be put into funcMeta2InstanceIDs
        ASSERT_AWAIT_TRUE(
            [&]() { return instanceMgrActor->member_->funcMeta2InstanceIDs["456/0-driver-default/$latest"].size() == 0; });
 
        auto meta = R"({"funcMetaData":{"layers":[],"name":"0@faaspy@hello","description":"empty function","functionUrn":"sn:cn:yrk:12345678901234561234567890123456:function:0@faaspy@hello","reversedConcurrency":0})";
        auto funcKey = R"(/yr/functions/business/yrk/tenant/12345678901234561234567890123456/function/0@faaspy@hello/version/latest)";
        KeyValue getKeyValue;
        getKeyValue.set_key(funcKey);
        getKeyValue.set_value(meta);
 
        litebus::Future<std::shared_ptr<GetResponse>> getResponseFuture;
        std::shared_ptr<GetResponse> rep = std::make_shared<GetResponse>();
        rep->status = Status::OK();
        rep->kvs.emplace_back(getKeyValue);
        getResponseFuture.SetValue(rep);
        EXPECT_CALL(*mockMetaStoreClient, Get).WillOnce(testing::Return(getResponseFuture));
        // will not call KillInstanceWithRetry
        EXPECT_CALL(*scheduler, GetLocalAddress).Times(0);
        auto future = instanceMgrActor->FunctionMetaSyncer();
        ASSERT_AWAIT_READY(future);
        ASSERT_TRUE(future.Get().status.IsOk());
    }
    
    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, InstanceInfoSyncerTest)  // NOLINT
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    std::unordered_set<std::string> nodes = {"siaphisprg00912"};
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(nodes));
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);
    auto mockMetaStoreClient = std::make_shared<MockMetaStoreClient>(metaStoreServerHost_);
    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        mockMetaStoreClient, scheduler, groupMgr, nullptr,
        InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    WaitSyncFuncMeta(mockMetaStoreClient, instanceMgrActor);

    {   // for get failed
        litebus::Future<std::shared_ptr<GetResponse>> getResponseFuture;
        std::shared_ptr<GetResponse> rep = std::make_shared<GetResponse>();
        rep->status = Status(StatusCode::FAILED, "");
        getResponseFuture.SetValue(rep);
        EXPECT_CALL(*mockMetaStoreClient, Get).WillOnce(testing::Return(getResponseFuture));

        auto future = instanceMgrActor->InstanceInfoSyncer();
        ASSERT_AWAIT_READY(future);
        ASSERT_FALSE(future.Get().status.IsOk());
    }

    {   // for get response is empty
        litebus::Future<std::shared_ptr<GetResponse>> getResponseFuture;
        std::shared_ptr<GetResponse> rep = std::make_shared<GetResponse>();
        rep->status = Status::OK();
        getResponseFuture.SetValue(rep);
        EXPECT_CALL(*mockMetaStoreClient, Get).WillOnce(testing::Return(getResponseFuture));

        auto future = instanceMgrActor->InstanceInfoSyncer();
        ASSERT_AWAIT_READY(future);
        ASSERT_TRUE(future.Get().status.IsOk());
    }

    {
        auto instanceKey1 = R"(/sn/instance/business/yrk/tenant/default/function/0-system-faasExecutorPython3.9/version/$latest/defaultaz/d4f050f90ee2b90b00/609d910b-f65d-4efc-8000-000000000046)";
        auto instanceInfoJson1 = R"({"instanceID":"609d910b-f65d-4efc-8000-000000000046","requestID":"d4f050f90ee2b90b00","runtimeID":"runtime-6de59705-0000-4000-8000-00abf61502f6","runtimeAddress":"10.42.2.100:22771","functionAgentID":"functionagent-pool1-776c6db574-nnmrn","functionProxyID":"siaphisprg00912","function":"default/0-system-faasExecutorPython3.9/$latest","scheduleTimes":1,"instanceStatus":{"code":1,"msg":"scheduling"},"jobID":"job-12345678","parentID":"4e7cd507-8645-4600-b33c-f045f13e4beb","deployTimes":1,"version":"1"})";
        auto instance1 = std::make_shared<resource_view::InstanceInfo>();
        ASSERT_TRUE(TransToInstanceInfoFromJson(*instance1, instanceInfoJson1));

        auto instanceKey2 = R"(/sn/instance/business/yrk/tenant/default/function/0-system-faasExecutorPython3.9/version/$latest/defaultaz/xxxxxxxx999/aaaaa88888)";
        auto instanceInfoJson2 = R"({"instanceID":"aaaaa88888","requestID":"xxxxxxxx999","runtimeID":"runtime-6de59705-0000-4000-8000-00abf61502f6","runtimeAddress":"10.42.2.100:22771","functionAgentID":"functionagent-pool1-776c6db574-nnmrn","functionProxyID":"siaphisprg00912","function":"default/0-system-faasExecutorPython3.9/$latest","instanceStatus":{"code":1,"msg":"scheduling"},"jobID":"job-12345678","parentID":"4e7cd507-8645-4600-b33c-f045f13e4beb","deployTimes":1,"version":"1"})";
        InstanceInfo instance2;
        ASSERT_TRUE(TransToInstanceInfoFromJson(instance2, instanceInfoJson2));

        auto instanceInfoJson2forRunning = R"({"instanceID":"aaaaa88888","requestID":"xxxxxxxx999","runtimeID":"runtime-6de59705-0000-4000-8000-00abf61502f6","runtimeAddress":"10.42.2.100:22771","functionAgentID":"functionagent-pool1-776c6db574-nnmrn","functionProxyID":"siaphisprg00912","function":"default/0-system-faasExecutorPython3.9/$latest","instanceStatus":{"code":3,"msg":"running"},"jobID":"job-12345678","parentID":"4e7cd507-8645-4600-b33c-f045f13e4beb","deployTimes":1,"version":"3"})";
        auto instanceRunning = std::make_shared<resource_view::InstanceInfo>();
        ASSERT_TRUE(TransToInstanceInfoFromJson(*instanceRunning, instanceInfoJson2forRunning));

        // mock instanceinfo in cache, but not in etcd
        instanceMgrActor->member_->instID2Instance[instance1->instanceid()] = std::make_pair(instanceKey1, instance1);
        ASSERT_TRUE(instanceMgrActor->member_->instID2Instance.count(instance1->instanceid()) == 1);
        auto checkKey = instance1->instanceid();

        // mock instanceinfo in cache
        instanceMgrActor->member_->instID2Instance[instanceRunning->instanceid()] = std::make_pair(instanceKey2, instanceRunning);

        KeyValue getKeyValue;
        getKeyValue.set_key(instanceKey2);
        getKeyValue.set_value(instanceInfoJson2);
        litebus::Future<std::shared_ptr<GetResponse>> getResponseFuture;
        std::shared_ptr<GetResponse> rep = std::make_shared<GetResponse>();
        rep->status = Status::OK();
        rep->kvs.emplace_back(getKeyValue);

        getResponseFuture.SetValue(rep);
        EXPECT_CALL(*mockMetaStoreClient, Get).WillOnce(testing::Return(getResponseFuture));
        EXPECT_CALL(*groupMgr, OnInstanceDelete(::testing::_, ::testing::_)).WillOnce(testing::Return(litebus::Future<Status>(Status::OK())));


        auto future = instanceMgrActor->InstanceInfoSyncer();
        ASSERT_AWAIT_READY(future);
        ASSERT_TRUE(future.Get().status.IsOk());

        // instanceinfo in cache, but not in etcd, need to be killed
        ASSERT_TRUE(instanceMgrActor->member_->instID2Instance.find(checkKey) == instanceMgrActor->member_->instID2Instance.end());

        // instanceinfo in cache and in etcd, need to update by etcd
        auto cacheInstanceInfo2 = instanceMgrActor->member_->instID2Instance[instanceRunning->instanceid()].second;
        ASSERT_TRUE(cacheInstanceInfo2->version() == 1);
    }

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, InstanceInfoSyncerOperationReplayTest)  // NOLINT
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    std::unordered_set<std::string> nodes = { "siaphisprg00912" };
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(nodes));
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);
    auto mockMetaStoreClient = std::make_shared<MockMetaStoreClient>(metaStoreServerHost_);
    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        mockMetaStoreClient, scheduler, groupMgr, nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    WaitSyncFuncMeta(mockMetaStoreClient, instanceMgrActor);

    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceMgrActor->member_->instanceOpt = mockInstanceOpt;

    {  // for replay
        litebus::Future<std::shared_ptr<GetResponse>> getResponseFuture;
        std::shared_ptr<GetResponse> rep = std::make_shared<GetResponse>();
        rep->status = Status::OK();
        getResponseFuture.SetValue(rep);
        EXPECT_CALL(*mockMetaStoreClient, Get).WillRepeatedly(testing::Return(getResponseFuture));

        EXPECT_CALL(*mockInstanceOpt, ForceDelete)
            .WillOnce(testing::Return(OperateResult{ Status(StatusCode::ERR_ETCD_OPERATION_ERROR), "", 3 }))
            .WillOnce(testing::Return(OperateResult{ Status::OK(), "", 3 }));

        EXPECT_CALL(*mockInstanceOpt, Modify)
            .WillOnce(testing::Return(OperateResult{ Status(StatusCode::ERR_ETCD_OPERATION_ERROR), "", 3 }))
            .WillOnce(testing::Return(OperateResult{ Status::OK(), "", 3 }));

        auto instanceKey1 =
            R"(/sn/instance/business/yrk/tenant/default/function/0-system-faasExecutorPython3.9/version/$latest/defaultaz/d4f050f90ee2b90b00/609d910b-f65d-4efc-8000-000000000046)";
        auto instanceInfoJson1 =
            R"({"instanceID":"609d910b-f65d-4efc-8000-000000000046","requestID":"d4f050f90ee2b90b00","runtimeID":"runtime-6de59705-0000-4000-8000-00abf61502f6","runtimeAddress":"10.42.2.100:22771","functionAgentID":"functionagent-pool1-776c6db574-nnmrn","functionProxyID":"siaphisprg00912","function":"default/0-system-faasExecutorPython3.9/$latest","scheduleTimes":1,"instanceStatus":{"code":1,"msg":"scheduling"},"jobID":"job-12345678","parentID":"4e7cd507-8645-4600-b33c-f045f13e4beb","deployTimes":1,"version":"1"})";
        std::shared_ptr<resource_view::InstanceInfo> instance1 = std::make_shared<resource_view::InstanceInfo>();
        ASSERT_TRUE(TransToInstanceInfoFromJson(*instance1, instanceInfoJson1));

        instanceMgrActor->member_->operateCacher->AddDeleteEvent(INSTANCE_PATH_PREFIX, instanceKey1);

        auto future = instanceMgrActor->InstanceInfoSyncer();  // delete failed
        ASSERT_AWAIT_READY(future);
        ASSERT_TRUE(future.Get().status.IsError());
        ASSERT_FALSE(instanceMgrActor->member_->operateCacher->IsCacheClear(INSTANCE_PATH_PREFIX));

        future = instanceMgrActor->InstanceInfoSyncer();  // delete success
        ASSERT_AWAIT_READY(future);
        ASSERT_TRUE(future.Get().status.IsOk());
        ASSERT_TRUE(instanceMgrActor->member_->operateCacher->IsCacheClear(INSTANCE_PATH_PREFIX));

        instanceMgrActor->member_->operateCacher->AddPutEvent(INSTANCE_PATH_PREFIX, instance1->instanceid(),
                                                              "SCHEDULING");
        instanceMgrActor->member_->instID2Instance[instance1->instanceid()] = std::make_pair(instanceKey1, instance1);

        future = instanceMgrActor->InstanceInfoSyncer();  // put failed
        ASSERT_AWAIT_READY(future);
        ASSERT_TRUE(future.Get().status.IsError());
        ASSERT_FALSE(instanceMgrActor->member_->operateCacher->IsCacheClear(INSTANCE_PATH_PREFIX));

        future = instanceMgrActor->InstanceInfoSyncer();  // put success
        ASSERT_AWAIT_READY(future);
        ASSERT_TRUE(future.Get().status.IsOk());
        ASSERT_TRUE(instanceMgrActor->member_->operateCacher->IsCacheClear(INSTANCE_PATH_PREFIX));
    }

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, ForwardKillInstanceWhenInstanceManagerNeverTakeInstance)
{
    std::string jsonString001;
    resource_view::InstanceInfo instance001 = CreateInstance(INSTANCE_PATH_PREFIX + "/001", true);
    instance001.set_functionproxyid("");
    instance001.set_function("0/0-system-faascontroller/$latest");
    instance001.mutable_instancestatus()->set_code(6);
    ASSERT_TRUE(TransToJsonFromInstanceInfo(jsonString001, instance001));
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);
    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = false });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();
    auto mockBootstrapActor = std::make_shared<MockBootstrapStubActor>("MockBootstrapStubActor1");
    litebus::Spawn(mockBootstrapActor);
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));
    messages::ForwardKillRequest req;
    req.set_requestid("kill-0001");
    req.mutable_instance()->set_functionproxyid("nodeid");
    req.mutable_instance()->set_function("0/0-system-faasfrontend/$latest");
    req.mutable_instance()->set_instanceid(instance001.instanceid());
    req.mutable_instance()->set_version(instance001.version());
    auto future = litebus::Async(mockBootstrapActor->GetAID(), &MockBootstrapStubActor::SendForwardKill,
                                 instanceMgrActor->GetAID(), req);
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get().IsOk());
    std::cout << "" << future.Get().ToString() << std::endl;
    litebus::Terminate(mockBootstrapActor->GetAID());
    litebus::Await(mockBootstrapActor->GetAID());
    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, QueryInstancesInfo)
{
    PutInstances(true, true);
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    auto instance001 = std::make_shared<resource_view::InstanceInfo>();
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut, "inst001", instance001);

    auto queryInstanceReq = std::make_shared<messages::QueryInstancesInfoRequest>();
    auto expectedRsp = std::make_shared<messages::QueryInstancesInfoResponse>();
    expectedRsp->mutable_instanceinfos()->Add(resource_view::InstanceInfo(*instance001));

    auto future = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::QueryInstancesInfo, queryInstanceReq);
    EXPECT_AWAIT_READY(future);
    EXPECT_EQ(future.Get().SerializeAsString(), expectedRsp->SerializeAsString());

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, QueryNamedIns)
{
    PutInstances(true, true);
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    // no instance
    auto req = std::make_shared<messages::QueryNamedInsRequest>();
    auto expectedRsp = std::make_shared<messages::QueryNamedInsResponse>();
    auto future = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::QueryNamedIns, req);
    EXPECT_AWAIT_READY(future);
    EXPECT_EQ(future.Get().SerializeAsString(), expectedRsp->SerializeAsString());

    // 1 named ins, 1 non named ins
    auto instance001 = std::make_shared<resource_view::InstanceInfo>();
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut, "inst001", instance001);
    auto instance002 = std::make_shared<resource_view::InstanceInfo>();
    (*instance002->mutable_extensions())[NAMED] = "true";
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut, "inst002", instance002);
    expectedRsp->add_names(instance002->instanceid());
    future = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::QueryNamedIns, req);
    EXPECT_AWAIT_READY(future);
    EXPECT_EQ(future.Get().SerializeAsString(), expectedRsp->SerializeAsString());

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, QueryDebugInstancesInfo)
{

    MetaStoreClient client(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ });
    client.Init();

    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(NODES));
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    auto ready = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                                GetLeaderInfo(instanceMgrActor->GetAID()));
    EXPECT_AWAIT_READY(ready);

    auto req = std::make_shared<messages::QueryDebugInstanceInfosRequest>();
    auto requestID = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    req->set_requestid(requestID);

    // metastore中没有debug instance info
    auto future = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::QueryDebugInstancesInfo, req);
    EXPECT_AWAIT_READY(future);
    auto rsp = future.Get();
    EXPECT_EQ(rsp.debuginstanceinfos().size(), 0);

    // 添加一个debug instance info 到metastore中
    messages::DebugInstanceInfo info1;
    info1.set_instanceid("inst1");
    info1.set_debugserver(localAddress_);
    info1.set_pid(111);
    info1.set_status("S");

    std::string jsonStr1;
    EXPECT_TRUE(google::protobuf::util::MessageToJsonString(info1, &jsonStr1).ok());
    EXPECT_TRUE(client.Put("/yr/debug/inst1",jsonStr1,{}).Get()->status.IsOk());
    // wait for put envent callback func finished
    auto member = GetMemberFromInstanceMgrActor(instanceMgrActor);
    ASSERT_AWAIT_TRUE([&]() -> bool { return member->debugInstInfoMap.size() == 1; });
    auto future1 = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::QueryDebugInstancesInfo, req);
    EXPECT_AWAIT_READY(future1);
    auto rsp1 = future1.Get();
    EXPECT_EQ(rsp1.debuginstanceinfos().size(), 1);
    EXPECT_EQ(rsp1.debuginstanceinfos(0).instanceid(), "inst1");
    EXPECT_EQ(rsp1.debuginstanceinfos(0).debugserver(), localAddress_);

    // 删除其中某个debug instance info
    EXPECT_TRUE(client.Delete("/yr/debug/inst1", {}).Get()->status.IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool { return member->debugInstInfoMap.size() == 0; });
    auto future2 = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::QueryDebugInstancesInfo, req);
    EXPECT_AWAIT_READY(future2);
    auto rsp2 = future2.Get();
    EXPECT_EQ(rsp2.debuginstanceinfos_size(), 0);

    //再次添加一个instance
    EXPECT_TRUE(client.Put("/yr/debug/inst1",jsonStr1,{}).Get()->status.IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool { return member->debugInstInfoMap.size() == 1; });
    auto future3 = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::QueryDebugInstancesInfo, req);
    EXPECT_AWAIT_READY(future3);
    auto rsp3 = future3.Get();
    EXPECT_EQ(rsp3.debuginstanceinfos().size(), 1);
    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}


TEST_F(DISABLED_InstanceManagerTest, CompleteKillInstance)
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    std::unordered_set<std::string> nodes = {"siaphisprg00912"};
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(nodes));
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    // test for instance not found
    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceMgrActor->member_->instanceOpt = mockInstanceOpt;

    std::string requestID = "d4f050f90ee2b90b00";
    std::string instanceID = "609d910b-f65d-4efc-8000-000000000046";

    auto instanceKey1 =
        R"(/sn/instance/business/yrk/tenant/default/function/0-system-faasExecutorPython3.9/version/$latest/defaultaz/d4f050f90ee2b90b00/609d910b-f65d-4efc-8000-000000000046)";
    auto instanceInfoJson1 =
        R"({"instanceID":"609d910b-f65d-4efc-8000-000000000046","requestID":"d4f050f90ee2b90b00","runtimeID":"runtime-6de59705-0000-4000-8000-00abf61502f6","runtimeAddress":"10.42.2.100:22771","functionAgentID":"functionagent-pool1-776c6db574-nnmrn","functionProxyID":"siaphisprg00912","function":"default/0-system-faasExecutorPython3.9/$latest","scheduleTimes":1,"instanceStatus":{"code":1,"msg":"scheduling"},"jobID":"job-12345678","parentID":"4e7cd507-8645-4600-b33c-f045f13e4beb","deployTimes":1,"version":"1"})";

    std::shared_ptr<resource_view::InstanceInfo> instance1 = std::make_shared<resource_view::InstanceInfo>();
    ASSERT_TRUE(TransToInstanceInfoFromJson(*instance1, instanceInfoJson1));

    auto aid = litebus::AID("aid1");

    auto killResponse = GenKillResponse(common::ErrorCode::ERR_INSTANCE_NOT_FOUND, "instance not found, the instance may have been killed");
    auto forwardKillResponse = GenForwardKillResponse(requestID, killResponse.code(), killResponse.message());
    auto status = Status{ StatusCode::ERR_INSTANCE_NOT_FOUND, forwardKillResponse.message() };
    auto promise = std::make_shared<litebus::Promise<Status>>();
    instanceMgrActor->member_->killReqPromises.emplace(requestID, promise);

    // mock ForwardCustomSignalResponse, and set promise
    instanceMgrActor->ForwardCustomSignalResponse(aid, "local", forwardKillResponse.SerializeAsString());

    // wait promise is set
    ASSERT_AWAIT_TRUE([=]() -> bool { return promise->GetFuture().Get().StatusCode() == StatusCode::ERR_INSTANCE_NOT_FOUND ; });
    instanceMgrActor->CompleteKillInstance(status, requestID, instanceID);
    EXPECT_TRUE(instanceMgrActor->member_->killReqPromises.find(requestID) ==
                instanceMgrActor->member_->killReqPromises.end());

    // test must call ForceDelete
    instanceMgrActor->member_->instID2Instance[instanceID] = std::make_pair(instanceKey1, instance1);
    EXPECT_CALL(*mockInstanceOpt, ForceDelete).WillOnce(testing::Return(OperateResult{ Status::OK(), "", 3 }));
    instanceMgrActor->CompleteKillInstance(status, requestID, instanceID);
    EXPECT_TRUE(instanceMgrActor->member_->killReqPromises.find(requestID) == instanceMgrActor->member_->killReqPromises.end());

    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, NodesTest)
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    EXPECT_CALL(*scheduler, LocalSchedAbnormalCallback).Times(1);
    std::unordered_set<std::string> nodes = {"nodeA"};
    EXPECT_CALL(*scheduler, QueryNodes).WillOnce(::testing::Return(nodes));

    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();

    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::UpdateLeaderInfo,
                   GetLeaderInfo(instanceMgrActor->GetAID()));

    auto instance001 = std::make_shared<resource_view::InstanceInfo>();
    instance001->set_functionproxyid("nodeA");
    instance001->set_instanceid("instanceA");
    instance001->set_requestid("941e253514a11c24");
    instance001->set_function("default/0-system-faasscheduler/$latest");
    std::string key = "/sn/instance/business/yrk/tenant/default/function/0-system"
        "-faasscheduler/version/$latest/defaultaz/941e253514a11c24/instanceA";
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut, key, instance001);

    auto instance002 = std::make_shared<resource_view::InstanceInfo>();
    instance002->set_functionproxyid("nodeB");
    instance002->set_instanceid("instanceB");
    instance002->set_requestid("941e253514a11c25");
    instance002->set_function("default/0-system-faasscheduler/$latest");
    std::string key1 = "/sn/instance/business/yrk/tenant/default/function/0-system"
        "-faasscheduler/version/$latest/defaultaz/941e253514a11c25/instanceB";
    litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::OnInstancePut, key1, instance002);

    EXPECT_AWAIT_TRUE([&](){
        return !instanceMgrActor->business_->NodeExists("nodeB");
    });
    auto queryInstanceReq = std::make_shared<messages::QueryInstancesInfoRequest>();
    auto expectedRsp = std::make_shared<messages::QueryInstancesInfoResponse>();
    instance002->set_functionproxyid("InstanceManagerOwner");
    instance002->mutable_instancestatus()->set_msg("nodeB is exited");
    instance002->mutable_instancestatus()->set_code(6);

    auto future = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::QueryInstancesInfo, queryInstanceReq);
    EXPECT_AWAIT_READY(future);
    auto rsp = future.Get();
    EXPECT_EQ(rsp.instanceinfos_size(), (int)2);
    for (auto instance : *rsp.mutable_instanceinfos()) {
        if (instance.instanceid() == instance001->instanceid()) {
            EXPECT_EQ(instance.functionproxyid(), instance001->functionproxyid());
        }
        if (instance.instanceid() == instance002->instanceid()) {
            EXPECT_EQ(instance.functionproxyid(), instance002->functionproxyid());
            EXPECT_EQ(instance.instancestatus().code(), instance002->instancestatus().code());
        }
    }
    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, ExecuteTest)
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    litebus::Spawn(instanceMgrActor);
    auto count = std::make_shared<int>();
    *count = 0;
    auto fn = [count]() {
        (*count)++;
        return Status::OK();
    };
    auto future1 = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Execute, fn);
    auto future2 = litebus::Async(instanceMgrActor->GetAID(), &InstanceManagerActor::Execute, fn);
    EXPECT_AWAIT_READY(future1);
    EXPECT_AWAIT_READY(future2);
    EXPECT_EQ(*count, 2);
    litebus::Terminate(instanceMgrActor->GetAID());
    litebus::Await(instanceMgrActor->GetAID());
}

TEST_F(DISABLED_InstanceManagerTest, ForwardQueryDebugInstancesInfo)
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();
    {
        std::string empty = "";
        std::string name = "ForwardQueryDebugInstancesInfoHandler";
        instanceMgrActor->ForwardQueryDebugInstancesInfoHandler(litebus::AID(), std::move(name), std::move(empty));
    }

    {
        messages::QueryDebugInstanceInfosRequest req;
        std::string name = "ForwardQueryDebugInstancesInfoHandler";
        instanceMgrActor->ForwardQueryDebugInstancesInfoHandler(litebus::AID(), std::move(name), req.SerializeAsString());
    }
    {
        auto future = litebus::Future<messages::QueryDebugInstanceInfosResponse>();
        future.SetFailed(StatusCode::FAILED);
        instanceMgrActor->OnQueryDebugInstancesInfoFinished(litebus::AID(), future);
    }

    {
        std::string empty = "";
        std::string name = "ForwardQueryDebugInstancesInfoHandler";
        instanceMgrActor->ForwardQueryDebugInstancesInfoResponseHandler(litebus::AID(), std::move(name), std::move(empty));
    }
    {
        auto promise = std::make_shared<litebus::Promise<messages::QueryDebugInstanceInfosResponse>>();
        instanceMgrActor->member_->queryDebugInstancesPromise = promise;
        messages::QueryDebugInstanceInfosResponse rsp;
        std::string name = "ForwardQueryDebugInstancesInfoHandler";
        instanceMgrActor->ForwardQueryDebugInstancesInfoResponseHandler(litebus::AID(), std::move(name), rsp.SerializeAsString());
        EXPECT_AWAIT_READY(promise->GetFuture());
        EXPECT_EQ(instanceMgrActor->member_->queryDebugInstancesPromise, nullptr);
    }
    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, ForwardQueryInstancesInfo)
{
    auto scheduler = std::make_shared<MockGlobalSched>();
    auto groupMgrActor = std::make_shared<MockGroupManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler);
    auto groupMgr = std::make_shared<MockGroupManager>(groupMgrActor);

    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        MetaStoreClient::Create(MetaStoreConfig{ .etcdAddress = metaStoreServerHost_ }), scheduler, groupMgr,
        nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto instanceMgrDriver = std::make_shared<InstanceManagerDriver>(instanceMgrActor, groupMgrActor);
    instanceMgrDriver->Start();
    {
        std::string empty = "";
        std::string name = "ForwardQueryInstancesInfoHandler";
        instanceMgrActor->ForwardQueryInstancesInfoHandler(litebus::AID(), std::move(name), std::move(empty));
    }

    {
        messages::QueryInstancesInfoRequest req;
        std::string name = "ForwardQueryInstancesInfoHandler";
        instanceMgrActor->ForwardQueryInstancesInfoHandler(litebus::AID(), std::move(name), req.SerializeAsString());
    }
    {
        auto future = litebus::Future<messages::QueryInstancesInfoResponse>();
        future.SetFailed(StatusCode::FAILED);
        instanceMgrActor->OnQueryInstancesInfoFinished(litebus::AID(), future);
    }

    {
        std::string empty = "";
        std::string name = "ForwardQueryDebugInstancesInfoHandler";
        instanceMgrActor->ForwardQueryInstancesInfoResponseHandler(litebus::AID(), std::move(name), std::move(empty));
    }
    {
        auto promise = std::make_shared<litebus::Promise<messages::QueryInstancesInfoResponse>>();
        instanceMgrActor->member_->queryInstancesPromise = promise;
        messages::QueryInstancesInfoResponse rsp;
        std::string name = "ForwardQueryInstancesInfoResponseHandler";
        instanceMgrActor->ForwardQueryInstancesInfoResponseHandler(litebus::AID(), std::move(name), rsp.SerializeAsString());
        EXPECT_AWAIT_READY(promise->GetFuture());
        EXPECT_EQ(instanceMgrActor->member_->queryInstancesPromise, nullptr);
    }
    instanceMgrDriver->Stop();
    instanceMgrDriver->Await();
}

TEST_F(DISABLED_InstanceManagerTest, IsInstanceShouldBeKilledTest)
{
    auto member = std::make_shared<InstanceManagerActor::Member>();
    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        nullptr, nullptr, nullptr, nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    auto masterBusiness = std::make_shared<InstanceManagerActor::MasterBusiness>(member, instanceMgrActor);
    masterBusiness->member_->family = std::make_shared<InstanceFamilyCaches>();
    std::string instanceIDA = "instanceA";
    std::shared_ptr<resource_view::InstanceInfo> instanceInfoA = std::make_shared<resource_view::InstanceInfo>();
    instanceInfoA->set_instanceid("instanceA");
    instanceInfoA->mutable_instancestatus()->set_code(int32_t(InstanceState::RUNNING));
    instanceInfoA->set_lowreliability(true);
    instanceInfoA->set_parentid("123");
    EXPECT_TRUE(masterBusiness->IsInstanceShouldBeKilled(instanceInfoA));
    instanceInfoA->mutable_instancestatus()->set_code(int32_t(InstanceState::FATAL));
    EXPECT_TRUE(masterBusiness->IsInstanceShouldBeKilled(instanceInfoA));
    instanceInfoA->set_lowreliability(false);
    instanceInfoA->set_detached(true);
    EXPECT_FALSE(masterBusiness->IsInstanceShouldBeKilled(instanceInfoA));

    instanceInfoA->mutable_instancestatus()->set_code(int32_t(InstanceState::RUNNING));
    instanceInfoA->set_detached(false);
    EXPECT_TRUE(masterBusiness->IsInstanceShouldBeKilled(instanceInfoA));
    (*instanceInfoA->mutable_extensions())["source"] = "frontend";
    EXPECT_FALSE(masterBusiness->IsInstanceShouldBeKilled(instanceInfoA));
    (*instanceInfoA->mutable_extensions())["source"] = "";
    (*instanceInfoA->mutable_createoptions())["resource.owner"] = "static_function";
    EXPECT_FALSE(masterBusiness->IsInstanceShouldBeKilled(instanceInfoA));
    masterBusiness->member_->exitingInstances.insert("123");
    EXPECT_TRUE(masterBusiness->IsInstanceShouldBeKilled(instanceInfoA));
}

TEST_F(DISABLED_InstanceManagerTest, IsInstanceManagedByJobTest)
{
    auto member = std::make_shared<InstanceManagerActor::Member>();
    auto instanceMgrActor = std::make_shared<InstanceManagerActor>(
        nullptr, nullptr, nullptr, nullptr, InstanceManagerStartParam{ .runtimeRecoverEnable = true });
    instanceMgrActor->member_->family = std::make_shared<InstanceFamilyCaches>();
    std::string instanceIDA = "instanceA";
    std::shared_ptr<resource_view::InstanceInfo> instanceInfoA = std::make_shared<resource_view::InstanceInfo>();
    instanceInfoA->set_instanceid("instanceA");
    instanceInfoA->mutable_instancestatus()->set_code(int32_t(InstanceState::RUNNING));
    EXPECT_FALSE(instanceMgrActor->IsInstanceManagedByJob(instanceInfoA));
    instanceInfoA->set_jobid("job-123");
    instanceInfoA->set_detached(true);
    EXPECT_FALSE(instanceMgrActor->IsInstanceManagedByJob(instanceInfoA));
    instanceInfoA->set_detached(false);
    instanceInfoA->set_parentid("instanceB");
    EXPECT_TRUE(instanceMgrActor->IsInstanceManagedByJob(instanceInfoA));
    std::shared_ptr<resource_view::InstanceInfo> instanceInfoB = std::make_shared<resource_view::InstanceInfo>();
    instanceInfoB->set_instanceid("instanceB");
    instanceInfoB->set_function("0/0-system-faasfrontend/$latest");
    instanceMgrActor->member_->family->AddInstance(instanceInfoB);
    EXPECT_TRUE(instanceMgrActor->IsInstanceManagedByJob(instanceInfoA));
    instanceInfoB->set_detached(true);
    instanceInfoB->set_function("0/hello/$latest");
    EXPECT_FALSE(instanceMgrActor->IsInstanceManagedByJob(instanceInfoA));
    std::shared_ptr<resource_view::InstanceInfo> instanceInfoC = std::make_shared<resource_view::InstanceInfo>();
    instanceInfoC->set_instanceid("instanceC");
    instanceInfoC->set_function("0/hello$latest");
    instanceInfoC->set_parentid("instanceB");
    instanceMgrActor->member_->family->AddInstance(instanceInfoC);
    instanceInfoB->set_detached(false);
    instanceInfoA->set_parentid("instanceC");
    EXPECT_TRUE(instanceMgrActor->IsInstanceManagedByJob(instanceInfoA));
    instanceMgrActor->member_->jobID2InstanceIDs["job-123"] = {"instanceC"};
    EXPECT_TRUE(instanceMgrActor->IsInstanceManagedByJob(instanceInfoA));
}
}  // namespace functionsystem::instance_manager::test
