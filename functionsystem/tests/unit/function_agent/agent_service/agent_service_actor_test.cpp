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

#include "function_agent/agent_service_actor.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <memory>

#include "agent_service_test_actor.h"
#include "common/constants/actor_name.h"
#include "common/logs/logging.h"
#include "common/metadata/metadata.h"
#define private public  // only for test
#include "common/metrics/metrics_adapter.h"
#undef private  // reset
#include "common/types/instance_state.h"
#include "common/utils/exec_utils.h"
#include "common/utils/hash_util.h"
#include "common/hex/hex.h"
#include "common/utils/struct_transfer.h"
#include "function_agent/code_deployer/copy_deployer.h"
#include "function_agent/code_deployer/local_deployer.h"
#include "function_agent/code_deployer/working_dir_deployer.h"
#include "function_agent/code_deployer/shared_dir_deployer.h"
#include "function_agent/common/constants.h"
#include "mocks/mock_agent_s3_deployer.h"
#include "mocks/mock_test_agent_s3_deployer.h"
#include "mocks/mock_exec_utils.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {

using ::testing::_;
using ::testing::Return;

namespace {
    const std::string TEST_AGENT_ID = "127.0.0.1-129866";
    const std::string TEST_LOCALSCHD_AID = "local_scheduler:111";
    const std::string TEST_INSTANCE_ID = "testInstanceID";
    const std::string TEST_INSTANCE_ID_2 = "testInstanceID2";
    const std::string TEST_INSTANCE_ID_3 = "testInstanceID3";
    const std::string TEST_RUNTIME_ID = "testRuntimeID";
    const std::string TEST_RUNTIME_ID_2 = "testRuntimeID2";
    const std::string TEST_RUNTIME_ID_3 = "testRuntimeID3";
    const std::string TEST_REQUEST_ID = "testRequestID";
    const std::string TEST_REQUEST_ID_2 = "testRequestID2";
    const std::string TEST_REQUEST_ID_3 = "testRequestID3";
    const std::string TEST_BUCKET_ID = "testBucketID";
    const std::string TEST_OBJECT_ID = "testObjectID";
    const std::string TEST_LAYER_OBJECT_ID = "testObjectID-layer";
    const std::string TEST_LAYER_OBJECT_ID_2 = "testObjectID-layer2";
    const std::string LOCAL_DEPLOY_DIR = "/tmp/home/local/test";
    const std::string TEST_WRONG_FIREWALL_CONFIG = R"([{"firewallConfig": {"chain": "OUTPUT", "table": "", "operation": "add", "target": "127.0.0.1", "args": "-j ACCEPT"}}])";
    const std::string TEST_WRONG_TUNNEL_CONFIG = R"([{"tunnelConfig": {"tunnelName": "", "remoteIP": "127.0.0.1", "mode": "ipip"}}])";
    const std::string TEST_WRONG_ROUTE_CONFIG = R"([{"routeConfig": {"gateway": "127.0.0.1", "cidr": ""}}])";
    const std::string TEST_WRONG_PROBER_CONFIG = R"([{"protocol": "ICMP", "address": "127.0.0.1", "interval": 1, "timeout": 1, "failureThreshold": 1}])";
    const std::string TEST_PROBER_CONFIG = R"([{"protocol": "ICMP", "address": "127.0.0.1", "interval": 1, "timeout": 1, "failureThreshold": 1}])";
    const std::string TEST_PODIP_IPSET_NAME = "test-podip-whitelist"; // length cannot exceed 31
    const std::string TEST_TENANT_ID = "tenant001";

    size_t JudgeCodeReferNum(const std::shared_ptr<std::unordered_map<std::string, CodeReferInfo>> &codeReferMgr,
                           const std::string &dir)
    {
        auto ite = codeReferMgr->find(dir);
        if (ite == codeReferMgr->end()) {
            return 0;
        }
        return ite->second.instanceIDs.size();
    }
    void AddLayer(messages::Layer* layer, const std::string &bucketID, const std::string &objectID)
    {
        functionsystem::messages::Layer tempLayer;
        tempLayer.set_bucketid(bucketID);
        tempLayer.set_objectid(objectID);
        layer->CopyFrom(tempLayer);
    }

    void GenCodePkgThresholdsCfgFile(const std::string &configDir, const std::string &content)
    {
        if (!litebus::os::ExistPath(configDir)) {
            litebus::os::Mkdir(configDir);
        }
        auto configPath = litebus::os::Join(configDir, "code-package-thresholds.json");
        if (litebus::os::ExistPath(configPath)) {
            litebus::os::Rm(configPath);
        }
        std::ofstream outfile;
        outfile.open(configPath.c_str());
        outfile << content << std::endl;
        outfile.close();
    }
}
class AgentServiceActorTest : public ::testing::Test {
public:
    void SetUp() override
    {
        auto deployer = std::make_shared<function_agent::LocalDeployer>();
        auto workingDirDeployer = std::make_shared<function_agent::WorkingDirDeployer>();
        auto sharedDirDeployer = std::make_shared<function_agent::SharedDirDeployer>();
        auto s3Config = std::make_shared<S3Config>();
        messages::CodePackageThresholds codePackageThresholds;
        auto mockDeployer = std::make_shared<MockAgentS3Deployer>(s3Config, codePackageThresholds);
        function_agent::AgentServiceActor::Config config{ TEST_LOCALSCHD_AID, *(s3Config.get()),
                                                          codePackageThresholds };
        dstActor_ = std::make_shared<function_agent::AgentServiceActor>("dstAgentServiceActor", TEST_AGENT_ID, config);
        dstActor_->SetClearCodePackageInterval(100);    // to reduce LLT cost time
        dstActor_->SetRetrySendCleanStatusInterval(100);    // to reduce LLT cost time
        dstActor_->SetRetryRegisterInterval(100);    // to reduce LLT cost time
        dstActor_->SetDeployers(function_agent::S3_STORAGE_TYPE, mockDeployer);
        dstActor_->SetDeployers(function_agent::LOCAL_STORAGE_TYPE, deployer);
        dstActor_->SetDeployers(function_agent::WORKING_DIR_STORAGE_TYPE, workingDirDeployer);
        dstActor_->SetDeployers(SHARED_DIR_STORAGE_TYPE, sharedDirDeployer);
        dstActor_->isRegisterCompleted_ = true;
        dstActor_->isUnitTestSituation_ = true;
        dstActor_->SetIpsetName(TEST_PODIP_IPSET_NAME);
        dstActor_->SetPingTimeoutMs(1000);
        std::shared_ptr<IpsetIpv4NetworkIsolation> isolation = std::make_shared<IpsetIpv4NetworkIsolation>(dstActor_->GetIpsetName());
        commandRunner_ = std::make_shared<MockCommandRunner>();
        isolation->SetCommandRunner(commandRunner_);
        dstActor_->SetIpsetIpv4NetworkIsolation(isolation);
        litebus::Spawn(dstActor_, true);

        testFuncAgentMgrActor_ = std::make_shared<function_agent::test::MockFunctionAgentMgrActor>("testFuncAgentMgrActor");
        testFuncAgentMgrActor_->actorMessageList_.emplace("DeployInstance");
        testFuncAgentMgrActor_->actorMessageList_.emplace("KillInstance");
        testFuncAgentMgrActor_->actorMessageList_.emplace("Registered");
        testFuncAgentMgrActor_->actorMessageList_.emplace("QueryInstanceStatusInfo");
        testFuncAgentMgrActor_->actorMessageList_.emplace("CleanStatus");
        testFuncAgentMgrActor_->actorMessageList_.emplace("UpdateCred");
        testFuncAgentMgrActor_->actorMessageList_.emplace("SetNetworkIsolationRequest");
        testFuncAgentMgrActor_->actorMessageList_.emplace("QueryDebugInstanceInfos");
        testFuncAgentMgrActor_->actorMessageList_.emplace("NotifyFunctionStatusChange");
        litebus::Spawn(testFuncAgentMgrActor_, true);

        testMetricsActor_ = std::make_shared<function_agent::test::MockMetricsActor>("testMetricsActor");
        testMetricsActor_->actorMessageList_.emplace("UpdateRuntimeStatus");
        testMetricsActor_->actorMessageList_.emplace("UpdateResources");
        litebus::Spawn(testMetricsActor_, true);

        testRuntimeManager_ = std::make_shared<function_agent::test::MockRuntimeManagerActor>("testRuntimeManager");
        testRuntimeManager_->actorMessageList_.emplace("StartInstanceResponse");
        testRuntimeManager_->actorMessageList_.emplace("StopInstanceResponse");
        testRuntimeManager_->actorMessageList_.emplace("QueryInstanceStatusInfoResponse");
        testRuntimeManager_->actorMessageList_.emplace("CleanStatusResponse");
        testRuntimeManager_->actorMessageList_.emplace("GracefulShutdownFinish");
        testRuntimeManager_->actorMessageList_.emplace("QueryDebugInstanceInfosResponse");
        litebus::Spawn(testRuntimeManager_, true);

        testRegisterHelperActor_ = std::make_shared<function_agent::test::MockRegisterHelperActor>("testRuntimeManager-RegisterHelper");
        testRegisterHelperActor_->actorMessageList_.emplace("Register");
        litebus::Spawn(testRegisterHelperActor_, true);

        dstActor_->SetLocalSchedFuncAgentMgrAID(testFuncAgentMgrActor_->GetAID());
        dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID());
    }

    void PrepareFiles(const std::string &unzipedAppWorkingDir)
    {
        // module script
        (void)litebus::os::Mkdir(unzipedAppWorkingDir);
        std::string myPackageDir = litebus::os::Join(unzipedAppWorkingDir, "my_package");
        (void)litebus::os::Mkdir(myPackageDir);
        std::string initFile = litebus::os::Join(myPackageDir, "__init__.py");
        (void)litebus::os::Rm(initFile);
        TouchFile(initFile);
        std::string moduleFile = litebus::os::Join(myPackageDir, "hello.py");
        (void)litebus::os::Rm(moduleFile);
        TouchFile(moduleFile);
        std::ofstream outfile;
        outfile.open(moduleFile);
        outfile << R"(def greet(name):)" << std::endl;
        outfile << R"(    return f"Hello, {name}!")" << std::endl << std::endl;
        outfile << R"(def say_bye(name):)" << std::endl;
        outfile << R"(    return f"Goodbye, {name}!")" << std::endl << std::endl;
        outfile.close();

        // entrypoint script
        std::string entrypointPath = litebus::os::Join(unzipedAppWorkingDir, "script.py");
        (void)litebus::os::Rm(entrypointPath);
        TouchFile(entrypointPath);
        outfile.open(entrypointPath);
        outfile << "import sys" << std::endl;
        outfile << "import os" << std::endl;
        outfile << "import time" << std::endl;
        outfile << "from my_package import hello" << std::endl;
        outfile << R"(print("Python executable path:", sys.executable))" << std::endl;
        outfile << R"(print("Python module search path (sys.path):", sys.path))" << std::endl;
        outfile << R"(print(hello.greet("World")))" << std::endl;
        outfile << R"(print("Environment Variables:"))" << std::endl;
        outfile << R"(for key, value in os.environ.items():)" << std::endl;
        outfile << R"(    print(f"{key}={value}"))" << std::endl << std::endl;
        outfile << R"(time.sleep(3))" << std::endl;
        outfile.close();
    }

    void ZipWorkingDir(const std::string &unzipedAppWorkingDir)
    {
        // zip working dir file
        std::string zipFile = litebus::os::Join(unzipedAppWorkingDir, "file.zip");
        std::string cmd = "cd " + unzipedAppWorkingDir + "; zip -r " + zipFile + " my_package/ script.py";
        if (auto code(std::system(cmd.c_str())); code) {
            YRLOG_ERROR("failed to execute zip cmd({}). code: {}", cmd, code);
        }
    }

    void PrepareWorkingDir(const std::string &unzipedAppWorkingDir)
    {
        PrepareFiles(unzipedAppWorkingDir);
        ZipWorkingDir(unzipedAppWorkingDir);
    }

    void ModifyWorkingDir(const std::string &unzipedAppWorkingDir)
    {
        // modify entrypoint script
        std::string entrypointPath = litebus::os::Join(unzipedAppWorkingDir, "script.py");
        std::ofstream outfile;
        outfile.open(entrypointPath, std::ios::app);
        if (outfile.is_open()) {
            outfile << R"(print("=====modified=====");)";
            outfile.close();
        } else {
            std::cerr << "Failed to open file for appending: " << entrypointPath << std::endl;
        }

        ZipWorkingDir(unzipedAppWorkingDir);
    }

    void DestroyWorkingDir(const std::string &unzipedAppWorkingDir)
    {
        (void)litebus::os::Rmdir(unzipedAppWorkingDir);
    }

    void TearDown() override
    {
        litebus::Terminate(dstActor_->GetAID());
        litebus::Terminate(testFuncAgentMgrActor_->GetAID());
        litebus::Terminate(testRuntimeManager_->GetAID());
        litebus::Terminate(testMetricsActor_->GetAID());
        litebus::Terminate(testRegisterHelperActor_->GetAID());

        litebus::Await(dstActor_);
        litebus::Await(testFuncAgentMgrActor_);
        litebus::Await(testRuntimeManager_);
        litebus::Await(testMetricsActor_);
        litebus::Await(testRegisterHelperActor_);

        // Explicitly terminate RegisterHelperActor spawned by tests that call RegisterHelper("dstAgentServiceActor").
        // That actor is stored in dstActor_->registerHelper_ and its destructor may be delayed by thread pool
        // shared_ptr refs after Await(), causing name conflicts when the next test creates one.
        litebus::Terminate(litebus::AID("dstAgentServiceActor-RegisterHelper"));
        litebus::Await(litebus::AID("dstAgentServiceActor-RegisterHelper"));

        dstActor_ = nullptr;
        testFuncAgentMgrActor_ = nullptr;
        testRuntimeManager_ = nullptr;
        testMetricsActor_ = nullptr;
        testRegisterHelperActor_ = nullptr;
    }

protected:
    std::shared_ptr<function_agent::AgentServiceActor> dstActor_;
    std::shared_ptr<function_agent::test::MockFunctionAgentMgrActor> testFuncAgentMgrActor_;
    std::shared_ptr<function_agent::test::MockRuntimeManagerActor> testRuntimeManager_;
    std::shared_ptr<function_agent::test::MockMetricsActor> testMetricsActor_;
    std::shared_ptr<function_agent::test::MockRegisterHelperActor> testRegisterHelperActor_;
    std::shared_ptr<MockCommandRunner> commandRunner_;

private:
};

inline std::shared_ptr<messages::DeployInstanceRequest> GetDeployInstanceRequest(const std::string &requestID,
                                                                                 const std::string &instanceID,
                                                                                 const std::string &bucketID,
                                                                                 const std::string &objectID)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(requestID);
    deployInstanceReq->set_instanceid(instanceID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_deploydir("/tmp/home");
    spec->set_bucketid(bucketID);
    spec->set_objectid(objectID);
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    deployInstanceReq->mutable_scheduleoption()->set_schedpolicyname("shared");
    return deployInstanceReq;
}

/**
 * Feature: AgentServiceActor--DeployInstanceErrorRequest
 * Description: deploy instance fail with error request or create other wrong situation
 * Steps:
 * 1. Create error request msg and send DeployInstance request
 * 2. Set AgentServiceActor unregister and then send DeployInstance request
 * 3. Create DeployInstance request with empty instanceid and then send
 * 4. Create DeployInstance request with empty storagetype and then send
 * 5. Create a complete DeployInstance request and send, and simulate RuntimeManager to send err StartInstanceResponse
 * 6. send the same request like step 5
 * Expectation:
 * 1. Cause ParseFromString failed, AgentServiceActor will not send StartInstance request to RuntimeManager and
 * return DeployInstanceResponse to FunctionAgentMgrActor
 * 2. Cause registration not complete err, AgentServiceActor will not send StartInstance request to RuntimeManager
 * or return DeployInstanceResponse to FunctionAgentMgrActor
 * 3. Cause illegal request err, AgentServiceActor will send DeployInstanceResponse with errcode FUNC_AGENT_REQUEST_ID_ILLEGAL_ERROR
 * back to FunctionAgentMgrActor but not send StartInstance request to RuntimeManager
 * 4. Cause not find deployer err, AgentServiceActor will send DeployInstanceResponse with errcode FUNC_AGENT_INVALID_DEPLOYER_ERROR
 * back to FunctionAgentMgrActor but not send StartInstance request to RuntimeManager
 * 5. RuntimeManager will receive StartInstance request from AgentServiceActor, but FunctionAgentMgrActor won't receive DeployInstanceResponse
 * 6. Cause repeatedly deploy instance request err, AgentServiceActor will not send StartInstance request to RuntimeManager
 * or return DeployInstanceResponse to FunctionAgentMgrActor
 */
TEST_F(AgentServiceActorTest, DeployInstanceErrorRequest)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    // ParseFromString failed
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString() + "err"));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), "");
    EXPECT_EQ(testRuntimeManager_->GetReceivedStartInstanceRequest(), false);
    // register not complete error
    dstActor_->isRegisterCompleted_ = false;
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), "");
    EXPECT_EQ(testRuntimeManager_->GetReceivedStartInstanceRequest(), false);
    // empty instanceid error
    dstActor_->isRegisterCompleted_ = true;
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(),
              StatusCode::FUNC_AGENT_REQUEST_ID_ILLEGAL_ERROR);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    EXPECT_EQ(testRuntimeManager_->GetReceivedStartInstanceRequest(), false);
    // error(empty) storaget type
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(),
              StatusCode::FUNC_AGENT_INVALID_DEPLOYER_ERROR);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    EXPECT_EQ(testRuntimeManager_->GetReceivedStartInstanceRequest(), false);
    // success (will not receive DeployInstance Response, but receive StartInstance Request)
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);

    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse).WillOnce(Return("invalid msg")); // send err response
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), "");
    EXPECT_EQ(testRuntimeManager_->GetReceivedStartInstanceRequest(), true);
    testRuntimeManager_->ResetReceivedStartInstanceRequest();

    // package validation failed
    dstActor_->SetFailedDownloadRequests(TEST_REQUEST_ID);
    spec->set_deploydir("/tmp/home");
    spec->set_bucketid(TEST_BUCKET_ID);
    spec->set_objectid(TEST_OBJECT_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRuntimeManager_->GetReceivedStartInstanceRequest(), false);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(),
              StatusCode::ERR_USER_CODE_LOAD);
    // package validation failed when downloading other
    auto destination = "/tmp/home";
    dstActor_->SetFailedDeployingObjects(destination);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRuntimeManager_->GetReceivedStartInstanceRequest(), false);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(),
              StatusCode::ERR_USER_CODE_LOAD);

}

/**
 * Feature: AgentServiceActor--DeployInstanceSetNetworkFailed
 * Description: deploy instance fail with error network configuration
 * Steps:
 * 1. Create wrong networkConfig json value and set createoptions in DeployInstanceRequest and then send request
 * 2. Create empty networkConfig json value and wrong proberConfig json value and set createoptions in DeployInstanceRequest
 * and then send request
 * 3. Create empty networkConfig json value and set createoptions in DeployInstanceRequest and then send request
 * and simulate RuntimeManager to send err StartInstanceResponse
 * Expectation:
 * 1. Cause set network err, gentServiceActor will send DeployInstanceResponse with errcode FUNC_AGENT_SET_NETWORK_ERROR
 * back to FunctionAgentMgrActor but not send StartInstance request to RuntimeManager
 * 2. Cause set network err, gentServiceActor will send DeployInstanceResponse with errcode FUNC_AGENT_NETWORK_WORK_ERROR
 * back to FunctionAgentMgrActor but not send StartInstance request to RuntimeManager
 * 3. RuntimeManager will receive StartInstance request from AgentServiceActor, but FunctionAgentMgrActor won't receive DeployInstanceResponse
 */
TEST_F(AgentServiceActorTest, DeployInstanceSetNetworkFailed)
{
    messages::DeployInstanceRequest deployInstance;
    deployInstance.set_requestid(TEST_REQUEST_ID);
    deployInstance.set_instanceid(TEST_INSTANCE_ID);
    deployInstance.mutable_funcdeployspec()->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse).WillRepeatedly(Return("invalid msg")); // send err response
    auto deployer = std::make_shared<function_agent::LocalDeployer>();
    dstActor_->SetDeployers(function_agent::LOCAL_STORAGE_TYPE, deployer);
    // error network config --> SetFireWall Failed
    deployInstance.mutable_createoptions()->operator[]("networkConfig") = TEST_WRONG_FIREWALL_CONFIG;
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstance.SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::FUNC_AGENT_SET_NETWORK_ERROR);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    EXPECT_EQ(testRuntimeManager_->GetReceivedStartInstanceRequest(), false);
    // error prober config
    deployInstance.mutable_createoptions()->operator[]("networkConfig") = "";
    deployInstance.mutable_createoptions()->operator[]("proberConfig") = "invalid config $$";
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstance.SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::FUNC_AGENT_NETWORK_WORK_ERROR);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    EXPECT_EQ(testRuntimeManager_->GetReceivedStartInstanceRequest(), false);
    // success
    deployInstance.mutable_createoptions()->erase("proberConfig");
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstance.SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), "");
    EXPECT_EQ(testRuntimeManager_->GetReceivedStartInstanceRequest(), true);
}

/**
 * Feature: AgentServiceActor--DeployInstanceAboutRuntimeMgrRegistration
 * Description: deploy instance with and without Runtime Manager Registration
 * Steps:
 * 1. Create a complete DeployInstance request and send, but set RuntimeManager unregister
 * 2. Set RuntimeManager registered, and then send the request again with a different requestid, and
 * simulate RuntimeManager to send success StartInstanceResponse
 * Expectation:
 * 1. Cause failed to start runtime, AgentServiceActor will send DeployInstanceResponse with errcode ERR_INNER_COMMUNICATION
 * back to FunctionAgentMgrActor but not send StartInstance request to RuntimeManager
 * 2. AgentServiceActor will send StartInstance request to RuntimeManager and
 * send DeployInstanceResponse Successfully back to FunctionAgentMgrActor
 */
TEST_F(AgentServiceActorTest, DeployInstanceAboutRuntimeMgrRegistration)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    auto deployer = std::make_shared<function_agent::LocalDeployer>();
    dstActor_->SetDeployers(function_agent::LOCAL_STORAGE_TYPE, deployer);
    // set err delegate code info
    deployInstanceReq->mutable_createoptions()->insert(
        { "DELEGATE_DOWNLOAD",
          "invalid $$" });

    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    // StartRuntime with unregister runtime manager, will not receive StartInstance Request, but receive DeployInstance Response
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), false);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRuntimeManager_->GetReceivedStartInstanceRequest(), false);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::ERR_INNER_COMMUNICATION);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    // StartRuntime with registered runtime manager, will receive StartInstance Request
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), true);
    deployInstanceReq->set_requestid(TEST_REQUEST_ID_2); // need be different with last one, or will cause repeadtedly request error
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRuntimeManager_->GetReceivedStartInstanceRequest(), true);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::SUCCESS);
}

/**
 * Feature: AgentServiceActor--DeployInstanceWithLocalDeployer
 * Description: Deploy instance with LocalDeployer, and receive StartInstanceResponse from RuntimeManager
 * Steps:
 * 1. Create DeployInstanceRequest, set stroragetype as LOCAL_STORAGE_TYPE
 * 2. Mock FunctionAgentMgrActor to send DeployInstance, mock RuntimeManager to return StartInstanceReponse
 * 3. Send DeployInstance request, simulate RuntimeManager return StartInstanceReponse with code SUCCESS
 * 4. Send DeployInstance request, simulate RuntimeManager return StartInstanceReponse with code RUNTIME_MANAGER_INSTANCE_HAS_BEEN_DEPLOYED
 * 5. Send DeployInstance request, simulate RuntimeManager return StartInstanceReponse with code RUNTIME_MANAGER_PARAMS_INVALID
 * Expectation:
 * 1. First deploy, runtime code has one code refer, FunctionAgentMgrActor will receive DeployInstanceResponse with code SUCCESS
 * 2. Second deploy, runtime code still has one code refer, FunctionAgentMgrActor will receive DeployInstanceResponse with code SUCCESS again
 * 3. Third deploy, runtime code still has one code refer, FunctionAgentMgrActor will receive DeployInstanceResponse with code RUNTIME_MANAGER_PARAMS_INVALID
 */
TEST_F(AgentServiceActorTest, DeployInstanceWithLocalDeployer)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    spec->set_deploydir(LOCAL_DEPLOY_DIR);
    auto deployer = std::make_shared<function_agent::LocalDeployer>();
    dstActor_->SetDeployers(function_agent::LOCAL_STORAGE_TYPE, deployer);

    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    // DeployInstance Request Send to Agent
    // 1. code is SUCCESS
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::SUCCESS);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(1));
    // 2. code is RUNTIME_MANAGER_INSTANCE_HAS_BEEN_DEPLOYED
    startInstanceResponse.set_requestid(TEST_REQUEST_ID_2);
    startInstanceResponse.set_code(StatusCode::RUNTIME_MANAGER_INSTANCE_HAS_BEEN_DEPLOYED);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    deployInstanceReq->set_requestid(TEST_REQUEST_ID_2);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID_2);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::SUCCESS);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(1));
    // 3. code is other ERROR CODE
    startInstanceResponse.set_requestid(TEST_REQUEST_ID_3);
    startInstanceResponse.set_code(StatusCode::RUNTIME_MANAGER_PARAMS_INVALID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    deployInstanceReq->set_requestid(TEST_REQUEST_ID_3);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID_2);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID_3);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->instanceid(), TEST_INSTANCE_ID_2);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::RUNTIME_MANAGER_PARAMS_INVALID);
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(1));
}

/**
 * Feature: AgentServiceActor--DeployInstanceWithTwoLayersOfSameDirViaS3
 * Description: deploy instance with S3Deployer and two layer code packages with same directory
 * Steps:
 * 1. Create DeployInstanceRequest, set stroragetype as S3_STORAGE_TYPE
 * 2. Add two layer with same bucketid and objectid to request
 * 3. Mock FunctionAgentMgrActor to send request, mock RuntimeManager to return StartInstanceReponse, and mock
 * S3Deployer to download code packages and create dir.
 * Expectation:
 * 1. Runtime code package dir and layer code packager dir will be created, and runtime code dir has code refer number as 1 while
 * layer code dir has code refer number as 1
 * 2. FunctionAgentMgrActor will receive DeployInstanceResponse from AgentServiceActor
 */
TEST_F(AgentServiceActorTest, DeployInstanceWithTwoLayersOfSameDirViaS3)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    spec->set_deploydir("/tmp/home");
    spec->set_bucketid(TEST_BUCKET_ID);
    spec->set_objectid(TEST_OBJECT_ID);
    // add layer one code
    AddLayer(spec->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    // add layer two code
    AddLayer(spec->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    auto destination = "/tmp/home/layer/func/" + TEST_BUCKET_ID + "/" + TEST_OBJECT_ID;
    auto layerDestination = "/tmp/home/layer/" + TEST_BUCKET_ID + "/" + TEST_LAYER_OBJECT_ID;
    litebus::os::Rmdir(destination);
    litebus::os::Rmdir(layerDestination);

    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    // have two same dir layer, three deployers
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(layerDestination); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination), static_cast<uint32_t>(1));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->instanceid(), TEST_INSTANCE_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->runtimeid(), TEST_RUNTIME_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::SUCCESS);
    EXPECT_TRUE(litebus::os::Rmdir(destination).IsNone());
    EXPECT_TRUE(litebus::os::Rmdir(layerDestination).IsNone());
}

/**
 * Feature: AgentServiceActor--DeployInstanceWithTwoLayersOfSameDirViaS3AtSameTime
 * Description: deploy instance with S3Deployer and two layer code packages with same directory and send this request twice
 * Steps:
 * 1. Create DeployInstanceRequest, set stroragetype as S3_STORAGE_TYPE
 * 2. Add two layer with same bucketid and objectid to request
 * 3. Mock FunctionAgentMgrActor to send request, mock RuntimeManager to return StartInstanceReponse, and mock
 * S3Deployer to download code packages and create dir.
 * 4. Send DeployInstance request twice, the second request should change requestid
 * Expectation:
 * 1. Runtime code package dir and layer code packager dir will be created, and runtime code dir has code refer number as 2 while
 * layer code dir has code refer number as 2
 */
TEST_F(AgentServiceActorTest, DeployInstanceWithTwoLayersOfSameDirViaS3AtSameTime)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    spec->set_deploydir("/tmp/home");
    spec->set_bucketid(TEST_BUCKET_ID);
    spec->set_objectid(TEST_OBJECT_ID);
    // add layer one code
    AddLayer(spec->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    // add layer two code
    AddLayer(spec->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    auto destination = "/tmp/home/layer/func/" + TEST_BUCKET_ID + "/" + TEST_OBJECT_ID;
    auto layerDestination = "/tmp/home/layer/" + TEST_BUCKET_ID + "/" + TEST_LAYER_OBJECT_ID;
    litebus::os::Rmdir(destination);
    litebus::os::Rmdir(layerDestination);

    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    messages::StartInstanceResponse startInstanceResponseDuplica;
    startInstanceResponseDuplica.set_code(StatusCode::SUCCESS);
    startInstanceResponseDuplica.set_requestid(TEST_REQUEST_ID);
    startInstanceResponseDuplica.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()))
        .WillOnce(Return(startInstanceResponseDuplica.SerializeAsString()));
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID_2);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(layerDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination) == 2; });
    ASSERT_AWAIT_TRUE([&]() -> bool { return JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination) == 2; });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(litebus::os::Rmdir(destination).IsNone());
    EXPECT_TRUE(litebus::os::Rmdir(layerDestination).IsNone());
}

/**
 * Feature: AgentServiceActor--RepeatedlyDeployInstanceWithOneLayersAndDelegateViaS3
 * Description: Deploy instance twice with one layer code and delegate code, configuring S3_DEPLOY_DIR
 * Steps:
 * 1. Create DeployInstanceRequest, set stroragetype as S3_STORAGE_TYPE
 * 2. Add one layer, set DELEGATE_DOWNLOAD and S3_DEPLOY_DIR
 * 3. Mock FunctionAgentMgrActor to send DeployInstance, mock RuntimeManager to return StartInstanceReponse, and mock
 * S3Deployer to download code packages and create dir.
 * 4. Send DeployInstance request twice, the second request should change requestid
 * Expectation:
 * 1. First deploy, runtime code, layer code and delegate code should create dir respectively, each one has one code refer,
 * FunctionAgentMgrActor will receive DeployInstanceResponse
 * 2. Second deploy, runtime code, layer code and delegate code have two code refer respectively,
 * FunctionAgentMgrActor will receive DeployInstanceResponse again
 */
TEST_F(AgentServiceActorTest, RepeatedlyDeployInstanceWithOneLayersAndDelegateViaS3)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    spec->set_deploydir("/tmp/home");
    spec->set_bucketid(TEST_BUCKET_ID);
    spec->set_objectid(TEST_OBJECT_ID);
    // add layer code
    AddLayer(spec->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    // add delegate code
    deployInstanceReq->mutable_createoptions()->insert(
        { "DELEGATE_DOWNLOAD",
          R"({"appId": "userCode", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID"})" });
    // set extractly layer deploy dir
    deployInstanceReq->mutable_createoptions()->insert(
        { "S3_DEPLOY_DIR",
          "/tmp/home/test" });
    std::string destination = "/tmp/home/layer/func/" + TEST_BUCKET_ID + "/" + TEST_OBJECT_ID;
    std::string layerDestination = "/tmp/home/test/layer/" + TEST_BUCKET_ID + "/" + TEST_LAYER_OBJECT_ID;
    std::string delegateDestination = "/tmp/home/test/layer/func/testUserCodeBucketID/testUserCodeObjectID";
    litebus::os::Rmdir(destination);
    litebus::os::Rmdir(layerDestination);
    litebus::os::Rmdir(delegateDestination);

    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(layerDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(delegateDestination); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), delegateDestination), static_cast<uint32_t>(1));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->instanceid(), TEST_INSTANCE_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->runtimeid(), TEST_RUNTIME_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::SUCCESS);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    // repeatedly deploy with different requestuid
    startInstanceResponse.set_requestid(TEST_REQUEST_ID_2);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    deployInstanceReq->set_requestid(TEST_REQUEST_ID_2);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID_2);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(2));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination), static_cast<uint32_t>(2));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), delegateDestination), static_cast<uint32_t>(2));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID_2);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->instanceid(), TEST_INSTANCE_ID_2);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->runtimeid(), TEST_RUNTIME_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::SUCCESS);

    EXPECT_TRUE(litebus::os::Rmdir(destination).IsNone());
    EXPECT_TRUE(litebus::os::Rmdir(layerDestination).IsNone());
    EXPECT_TRUE(litebus::os::Rmdir(delegateDestination).IsNone());
}

std::string DELEGATE_LAYER_DESTINATION = "/tmp/home/test/layer/testUserLibCodeBucketID/testUserLibCodeObjectID";
std::string DELEGATE_DESTINATION = "/tmp/home/test/layer/func/testUserCodeBucketID/testUserCodeObjectID";
std::string DESTINATION = "/tmp/home/layer/func/" + TEST_BUCKET_ID + "/" + TEST_OBJECT_ID;

void BuildCreateOptions(const std::unique_ptr<messages::DeployInstanceRequest> &request, const std::string &code,
                        const std::string &layer)
{
    request->mutable_createoptions()->erase("DELEGATE_DOWNLOAD");
    request->mutable_createoptions()->erase("DELEGATE_LAYER_DOWNLOAD");
    // add delegate code
    request->mutable_createoptions()->insert({ "DELEGATE_DOWNLOAD", code });
    request->mutable_createoptions()->insert({ "DELEGATE_LAYER_DOWNLOAD", layer });
}

/**
 * Feature: AgentServiceActor--DeployInstanceWithDelegateCode
 * Description: Deploy instance with user delegate code and lib
 * Steps:
 * 1. Create DeployInstanceRequest, set stroragetype as S3_STORAGE_TYPE
 * 2. set DELEGATE_DOWNLOAD, DELEGATE_LAYER_DOWNLOAD and S3_DEPLOY_DIR
 * 3. Mock FunctionAgentMgrActor to send DeployInstance, mock RuntimeManager to return StartInstanceReponse, and mock
 * S3Deployer to download code packages and create dir.
 * 4. First send, DELEGATE_DOWNLOAD and DELEGATE_LAYER_DOWNLOAD both have hostName, token, temporayAccessKey and
 * temporarySecretKey
 * 5. Second send, DELEGATE_DOWNLOAD and DELEGATE_LAYER_DOWNLOAD both have token, temporayAccessKey and
 * temporarySecretKey, without hostName
 * 6. Third send, DELEGATE_DOWNLOAD and DELEGATE_LAYER_DOWNLOAD both just have hostName
 * 7. Forth send, DELEGATE_DOWNLOAD with local file
 * Expectation:
 * 1. First deploy, runtime code, delegate lib code and delegate code should create dir respectively, each one has one
 * code refer, FunctionAgentMgrActor will receive DeployInstanceResponse
 * 2. Second deploy, runtime code, delegate lib code and delegate code have two code refer respectively,
 * FunctionAgentMgrActor will receive DeployInstanceResponse again
 * 4. Third deploy, runtime code, delegate lib code and delegate code have three code refer respectively,
 * FunctionAgentMgrActor will receive DeployInstanceResponse again
 * 5. Forth deploy, runtime code, local delegate lib code have one code refer,
 * FunctionAgentMgrActor will receive DeployInstanceResponse again
 */
TEST_F(AgentServiceActorTest, DeployInstanceWithDelegateCode)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    spec->set_deploydir("/tmp/home");
    spec->set_bucketid(TEST_BUCKET_ID);
    spec->set_objectid(TEST_OBJECT_ID);

    BuildCreateOptions(
        deployInstanceReq,
        R"({"appId":"userCode", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID", "hostName":"xx", "securityToken":"xxx", "temporayAccessKey":"xxx", "temporarySecretKey":"xxx","sha256":"","sha512":"aaaaaaaa"})",
        R"([{"appId":"userCode-layer", "bucketId":"testUserLibCodeBucketID", "objectId":"testUserLibCodeObjectID", "hostName":"xx", "securityToken":"xxx", "temporayAccessKey":"xxx", "temporarySecretKey":"xxx","sha256":"","sha512":"aaaaaaaa"}])");
    deployInstanceReq->mutable_createoptions()->insert({ "S3_DEPLOY_DIR", "/tmp/home/test" });
    litebus::os::Rmdir(DESTINATION);
    litebus::os::Rmdir(DELEGATE_LAYER_DESTINATION);
    litebus::os::Rmdir(DELEGATE_DESTINATION);

    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(DESTINATION); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(DELEGATE_LAYER_DESTINATION); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(DELEGATE_DESTINATION); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), DESTINATION), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), DELEGATE_LAYER_DESTINATION), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), DELEGATE_DESTINATION), static_cast<uint32_t>(1));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    // set DELEGATE_DOWNLOAD and DELEGATE_LAYER_DOWNLOAD with empty hostName
    BuildCreateOptions(
        deployInstanceReq,
        R"({"appId":"userCode", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID", "securityToken":"xxx", "temporayAccessKey":"xxx", "temporarySecretKey":"xxx"})",
        R"([{"appId":"userCode-layer", "bucketId":"testUserLibCodeBucketID", "objectId":"testUserLibCodeObjectID", "securityToken":"xxx", "temporayAccessKey":"xxx", "temporarySecretKey":"xxx"}])");
    startInstanceResponse.set_requestid(TEST_REQUEST_ID_2);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    deployInstanceReq->set_requestid(TEST_REQUEST_ID_2);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID_2);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), DESTINATION), static_cast<uint32_t>(2));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), DELEGATE_LAYER_DESTINATION), static_cast<uint32_t>(2));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), DELEGATE_DESTINATION), static_cast<uint32_t>(2));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID_2);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    // set DELEGATE_DOWNLOAD and DELEGATE_LAYER_DOWNLOAD with just hostName
    BuildCreateOptions(
        deployInstanceReq,
        R"({"appId":"userCode", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID", "hostName":"xx"})",
        R"([{"appId":"userCode-layer", "bucketId":"testUserLibCodeBucketID", "objectId":"testUserLibCodeObjectID", "hostName":"xx"}])");
    startInstanceResponse.set_requestid(TEST_REQUEST_ID_3);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    deployInstanceReq->set_requestid(TEST_REQUEST_ID_3);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID_3);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), DESTINATION), static_cast<uint32_t>(3));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), DELEGATE_LAYER_DESTINATION), static_cast<uint32_t>(3));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), DELEGATE_DESTINATION), static_cast<uint32_t>(3));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID_3);
    // set DELEGATE_DOWNLOAD with local file
    BuildCreateOptions(
        deployInstanceReq,
        R"({"appId":"", "bucketId":"", "objectId":"", "hostName":"xx", "storage_type": "local", "code_path": "/tmp/home/test/function-packages"})",
        R"([{"appId":"userCode-layer", "bucketId":"testUserLibCodeBucketID", "objectId":"testUserLibCodeObjectID", "hostName":"xx"}])");
    startInstanceResponse.set_requestid("testRequestID4");
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    deployInstanceReq->set_requestid("testRequestID4");
    deployInstanceReq->set_instanceid("testInstanceID4");
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE(
        [&]() -> bool { return testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid() == "testRequestID4"; });
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), "/tmp/home/test/function-packages"),
              static_cast<uint32_t>(1));

    EXPECT_TRUE(litebus::os::Rmdir(DESTINATION).IsNone());
    EXPECT_TRUE(litebus::os::Rmdir(DELEGATE_LAYER_DESTINATION).IsNone());
    EXPECT_TRUE(litebus::os::Rmdir(DELEGATE_DESTINATION).IsNone());
}

/**
 * Feature: AgentServiceActor--StartInstanceResponseError
 * Description: Mock RuntimeManager to send StartInstanceResponse to AgentServiceActor
 * Steps:
 * 1. Create error response msg and send StartInstanceResponse
 * 2. Send correct reponse to AgentServiceActor Directly
 * Expectation:
 * 1. Cause ParseFromString failed, AgentServiceActor will not return DeployInstanceResponse to FunctionAgentMgrActor
 * 2. deployingRequest_ does not store DeployInstanceRequest with the same requestid, so
 * AgentServiceActor will not return DeployInstanceResponse to FunctionAgentMgrActor
 */
TEST_F(AgentServiceActorTest, StartInstanceResponseError)
{
    auto startInstanceResponse = std::make_unique<messages::StartInstanceResponse>();
    startInstanceResponse->set_requestid(TEST_REQUEST_ID);
    // ParseFromString failed
    testRuntimeManager_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                        "StartInstanceResponse",
                                                        std::move(startInstanceResponse->SerializeAsString() + "err"));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), "");
    // Request has been killed
    testRuntimeManager_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                        "StartInstanceResponse",
                                                        std::move(startInstanceResponse->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), "");
}

/**
 * Feature: AgentServiceActor--KillInstanceErrorRequest
 * Description: Mock FunctionAgentMgrActor to send KillInstance to AgentServiceActor with err request
 * Steps:
 * 1. Create error request msg and send KillInstance
 * 2. Set AgentServiceActor unregister and then send KillInstance request
 * 3. Create KillInstance request with empty storagetype and then send
 * 4. Create a complete KillInstance request and send, and simulate RuntimeManager to send err StartInstanceResponse
 * Expectation:
 * 1. Cause ParseFromString failed, AgentServiceActor will not send StopInstance request to RuntimeManager or
 * return KillInstanceResponse to FunctionAgentMgrActor
 * 2. Cause registration not complete err, AgentServiceActor will not send StopInstance request to RuntimeManager
 * or return KillInstanceResponse to FunctionAgentMgrActor
 * 3. Cause invalid function storage type err, AgentServiceActor will send KillInstanceResponse with errcode FUNC_AGENT_INVALID_STORAGE_TYPE
 * back to FunctionAgentMgrActor but not send StopInstance request to RuntimeManager
 * 4. AgentServiceActor will send StopInstance request to RuntimeManager but not send KillInstanceResponse to FunctionAgentMgrActor
 */
TEST_F(AgentServiceActorTest, KillInstanceErrorRequest)
{
    auto killInstanceReq = std::make_unique<messages::KillInstanceRequest>();
    killInstanceReq->set_requestid(TEST_REQUEST_ID);
    // ParseFromString failed
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString() + "err"));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->requestid(), "");
    EXPECT_EQ(testRuntimeManager_->GetReceivedStopInstanceRequest(), false);
    // register not complete error
    dstActor_->isRegisterCompleted_ = false;
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->requestid(), "");
    EXPECT_EQ(testRuntimeManager_->GetReceivedStopInstanceRequest(), false);
    // error(empty) storaget type
    dstActor_->isRegisterCompleted_ = true;
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->code(),
              StatusCode::FUNC_AGENT_INVALID_STORAGE_TYPE);
    testFuncAgentMgrActor_->ResetKillInstanceResponse();
    EXPECT_EQ(testRuntimeManager_->GetReceivedStopInstanceRequest(), false);
    // success (will receive StopInstance Request, but not receive KillInstance Response)
    killInstanceReq->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    auto deployer = std::make_shared<function_agent::LocalDeployer>();
    dstActor_->SetDeployers(function_agent::LOCAL_STORAGE_TYPE, deployer);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse).WillOnce(Return("invalid msg")); // send err response
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->requestid(), "");
    EXPECT_EQ(testRuntimeManager_->GetReceivedStopInstanceRequest(), true);
}

/**
 * Feature: AgentServiceActor--StartInstanceResponseError
 * Description: Mock RuntimeManager to send StopInstanceResponse to AgentServiceActor
 * Steps:
 * 1. Create error response msg and send StopInstanceResponse
 * 2. Send correct reponse to AgentServiceActor Directly
 * Expectation:
 * 1. Cause ParseFromString failed, AgentServiceActor will not return KillInstanceResponse to FunctionAgentMgrActor
 * 2. killingRequest_ does not store KillInstanceRequest with the same requestid, so
 * AgentServiceActor will not return KillInstanceResponse to FunctionAgentMgrActor
 */
TEST_F(AgentServiceActorTest, StopInstanceResponseError)
{
    auto stopInstanceResponse = std::make_unique<messages::StopInstanceResponse>();
    stopInstanceResponse->set_requestid(TEST_REQUEST_ID);
    // ParseFromString failed
    testRuntimeManager_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                        "StopInstanceResponse",
                                                        std::move(stopInstanceResponse->SerializeAsString() + "err"));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->requestid(), "");
    // Request already killed
    testRuntimeManager_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                        "StopInstanceResponse",
                                                        std::move(stopInstanceResponse->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->requestid(), "");
}

/**
 * Feature: AgentServiceActor--KillInstanceWithRespose
 * Description: Mock FunctionAgentMgrActor and RuntimeManager to send KillInstance and StopInstanceResponse to AgentServiceActor
 * and receive StopInstance and KillInstanceResponse from AgentServiceActor
 * Steps:
 * 1. Create correct KillInstanceRequest with LocalDeployer
 * 2. When receive StopInstance request, simulate RuntimeManager to send StopInstanceResponse back to AgentServiceActor
 * Expectation:
 * 1. RuntimeManager will receive StopInstance request from AgentServiceActor
 * 2. FunctionAgentMgrActor will receive KillInstanceResponse from AgentServiceActor
 */
TEST_F(AgentServiceActorTest, KillInstanceWithRespose)
{
    auto killInstanceReq = std::make_unique<messages::KillInstanceRequest>();
    killInstanceReq->set_requestid(TEST_REQUEST_ID);
    killInstanceReq->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    killInstanceReq->set_ismonopoly(true);
    auto deployer = std::make_shared<function_agent::LocalDeployer>();
    dstActor_->SetDeployers(function_agent::LOCAL_STORAGE_TYPE, deployer);

    messages::StopInstanceResponse stopInstanceResponse;
    stopInstanceResponse.set_code(StatusCode::SUCCESS);
    stopInstanceResponse.set_requestid(TEST_REQUEST_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse).WillOnce(Return(stopInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRuntimeManager_->GetReceivedStopInstanceRequest(), true);
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->code(), StatusCode::SUCCESS);
    EXPECT_EQ(dstActor_->monopolyUsed_, true);
}

TEST_F(AgentServiceActorTest, KillInstanceWithResposeCancelHeartbeat)
{
    auto killInstanceReq = std::make_unique<messages::KillInstanceRequest>();
    killInstanceReq->set_requestid(TEST_REQUEST_ID);
    killInstanceReq->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    killInstanceReq->set_ismonopoly(true);
    dstActor_->SetDeployers(function_agent::LOCAL_STORAGE_TYPE, std::make_shared<function_agent::LocalDeployer>());
    dstActor_->enableRestartForReuse_ = true;
    messages::Registered registered;
    dstActor_->StartPingPong(registered);
    auto status = dstActor_->pingPongDriver_->GetStatus();
    ASSERT_AWAIT_READY(status);
    EXPECT_TRUE(status.Get().IsOk());

    messages::StopInstanceResponse stopInstanceResponse;
    stopInstanceResponse.set_code(StatusCode::SUCCESS);
    stopInstanceResponse.set_requestid(TEST_REQUEST_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse)
        .WillOnce(Return(stopInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() { return testRuntimeManager_->GetReceivedStopInstanceRequest(); });
    EXPECT_EQ(dstActor_->monopolyUsed_, true);
    status = dstActor_->pingPongDriver_->GetStatus();
    ASSERT_AWAIT_READY(status);
    EXPECT_EQ(status.Get().StatusCode(), StatusCode::FAILED);
    EXPECT_THAT(status.Get().ToString(), testing::HasSubstr("HeartbeatClient is stopped"));
}

TEST_F(AgentServiceActorTest, KillInstanceWithoutRuntimeMgrRegistration)
{
    auto killInstanceReq = std::make_unique<messages::KillInstanceRequest>();
    killInstanceReq->set_requestid(TEST_REQUEST_ID);
    killInstanceReq->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    auto deployer = std::make_shared<function_agent::LocalDeployer>();
    dstActor_->SetDeployers(function_agent::LOCAL_STORAGE_TYPE, deployer);
    // lost connection with local scheduler
    dstActor_->isRegisterCompleted_ = false;
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRuntimeManager_->GetReceivedStopInstanceRequest(), false);
    // lost connection with runtime manager
    dstActor_->isRegisterCompleted_ = true;
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), false);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRuntimeManager_->GetReceivedStopInstanceRequest(), false);
    // recover connection
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), true);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRuntimeManager_->GetReceivedStopInstanceRequest(), true);

}

/**
 * Feature: AgentServiceActor--DeployAndKillInstanceWithTwoLayerViaS3
 * Description: deploy and kill instance with S3Deployer and two layer code packages with same directory
 * Steps:
 * 1. Create DeployInstanceRequest, set stroragetype as S3_STORAGE_TYPE
 * 2. Add two layer with same bucketid and objectid to request
 * 3. Send DeployInstance to AgentServiceActor
 * 4. Send KillInstance to AgentServiceActor
 * Expectation:
 * 1. While deploying, runtime code package dir and layer code packager dir will be created, and layer code dir has code refer number as 2
 * 2. While deploying, FunctionAgentMgrActor will receive DeployInstanceResponse from AgentServiceActor
 * 3. While killing, runtime code package dir and layer code packager dir will be removed, and layer code dir has code refer number as 0
 * 4. While killing, FunctionAgentMgrActor will receive KillInstanceResponse from AgentServiceActor
 */
TEST_F(AgentServiceActorTest, DeployAndKillInstanceWithTwoLayerViaS3)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    spec->set_deploydir("/tmp/home");
    spec->set_bucketid(TEST_BUCKET_ID);
    spec->set_objectid(TEST_OBJECT_ID);
    // add layer code
    AddLayer(spec->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    AddLayer(spec->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    auto layerDestination = "/tmp/home/layer/" + TEST_BUCKET_ID + "/" + TEST_OBJECT_ID + "-layer";
    // DeployInstance
    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    auto destination = "/tmp/home/layer/func/" + TEST_BUCKET_ID + "/" + TEST_OBJECT_ID;
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(layerDestination); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination), static_cast<uint32_t>(1));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    // KillInstance
    auto killInstanceReq = std::make_unique<messages::KillInstanceRequest>();
    killInstanceReq->set_requestid(TEST_REQUEST_ID);
    killInstanceReq->set_storagetype(function_agent::S3_STORAGE_TYPE);

    messages::StopInstanceResponse stopInstanceResponse;
    stopInstanceResponse.set_code(StatusCode::SUCCESS);
    stopInstanceResponse.set_requestid(TEST_REQUEST_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse).WillOnce(Return(stopInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(layerDestination); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination), static_cast<uint32_t>(0));
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->requestid(), TEST_REQUEST_ID);
}

/**
 * Feature: AgentServiceActor--DeployAndKillInstanceWithTwoLayersTwoDelegateLayersAndDelegate
 * Description: deploy and kill instance with S3Deployer, two layer code, two delegate layer code
 * and delegate code, setting S3_DEPLOY_DIR additionally
 * Steps:
 * 1. Create DeployInstanceRequest, set stroragetype as S3_STORAGE_TYPE
 * 2. Add two layer, add two delegate layer, set DELEGATE_DOWNLOAD and S3_DEPLOY_DIR
 * 3. Mock FunctionAgentMgrActor to send DeployInstance, mock RuntimeManager to return StartInstanceReponse, and mock
 * S3Deployer to download code packages and create dir
 * 4. Mock FunctionAgentMgrActor to send KillInstance, mock RuntimeManager to return StopInstanceReponse, and mock
 * S3Deployer to clear code packages and remove dir
 * Expectation:
 * 1. While deploying, runtime code, layer code, delegate layer code and delegate code should create dir respectively, runtime code, delegate code
 * , layer code and delegate layer code have one code refer, FunctionAgentMgrActor will receive DeployInstanceResponse
 * 2. While killing, dir of runtime code, layer code, delegate layer code and delegate code should be removed respectively, each one have zero code refer,
 * FunctionAgentMgrActor will receive KillInstanceResponse
 */
TEST_F(AgentServiceActorTest, DeployAndKillInstanceWithTwoLayersTwoDelegateLayersAndDelegate)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    spec->set_deploydir("/tmp/home");
    spec->set_bucketid(TEST_BUCKET_ID);
    spec->set_objectid(TEST_OBJECT_ID);
    // add two layers
    AddLayer(spec->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    AddLayer(spec->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID_2);
    // add delegate code
    deployInstanceReq->mutable_createoptions()->insert(
        { "DELEGATE_LAYER_DOWNLOAD",
          R"([{"appId": "userCode-layer", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID-layer"}, {"appId": "userCode-layer2", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID-layer2"}])" });
    // add two delegate layers
    deployInstanceReq->mutable_createoptions()->insert(
        { "DELEGATE_DOWNLOAD",
          R"({"appId": "userCode", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID"})" });
    // set extractly layer deploy dir
    deployInstanceReq->mutable_createoptions()->insert(
        { "S3_DEPLOY_DIR",
          "/tmp/home/test" });
    std::string destination = "/tmp/home/layer/func/" + TEST_BUCKET_ID + "/" + TEST_OBJECT_ID;
    std::string layerDestination = "/tmp/home/test/layer/" + TEST_BUCKET_ID + "/" + TEST_LAYER_OBJECT_ID;
    std::string layerDestination2 = "/tmp/home/test/layer/" + TEST_BUCKET_ID + "/" + TEST_LAYER_OBJECT_ID_2;
    std::string delegateDestination = "/tmp/home/test/layer/func/testUserCodeBucketID/testUserCodeObjectID";
    std::string delegateLayerDestination = "/tmp/home/test/layer/testUserCodeBucketID/testUserCodeObjectID-layer";
    std::string delegateLayerDestination2 = "/tmp/home/test/layer/testUserCodeBucketID/testUserCodeObjectID-layer2";
    litebus::os::Rmdir(destination);
    litebus::os::Rmdir(layerDestination);
    litebus::os::Rmdir(layerDestination2);
    litebus::os::Rmdir(delegateLayerDestination);
    litebus::os::Rmdir(delegateLayerDestination2);
    litebus::os::Rmdir(delegateDestination);

    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    // deploy instance
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));

    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(layerDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(layerDestination2); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(delegateLayerDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(delegateLayerDestination2); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(delegateDestination); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination2), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), delegateLayerDestination), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), delegateLayerDestination2), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), delegateDestination), static_cast<uint32_t>(1));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->instanceid(), TEST_INSTANCE_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->runtimeid(), TEST_RUNTIME_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::SUCCESS);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    // kill instance
    auto killInstanceReq = std::make_unique<messages::KillInstanceRequest>();
    killInstanceReq->set_requestid(TEST_REQUEST_ID);
    killInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    killInstanceReq->set_runtimeid(TEST_RUNTIME_ID);
    killInstanceReq->set_storagetype(function_agent::S3_STORAGE_TYPE);

    messages::StopInstanceResponse stopInstanceResponse;
    stopInstanceResponse.set_code(StatusCode::SUCCESS);
    stopInstanceResponse.set_requestid(TEST_REQUEST_ID);
    stopInstanceResponse.set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse).WillOnce(Return(stopInstanceResponse.SerializeAsString()));
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(layerDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(layerDestination2); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(delegateLayerDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(delegateLayerDestination2); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(delegateDestination); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(0));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination), static_cast<uint32_t>(0));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination2), static_cast<uint32_t>(0));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), delegateLayerDestination), static_cast<uint32_t>(0));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), delegateLayerDestination2), static_cast<uint32_t>(0));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), delegateDestination), static_cast<uint32_t>(0));
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->requestid(), TEST_REQUEST_ID);
}

/**
 * Feature: AgentServiceActor--RepeatedlyDeployAndKillInstanceWithOneLayersAndDelegateViaS3
 * Description: Deploy and kill (each operation twice) instance with S3Deployer, one layer code and delegate code
 * Steps:
 * 1. Create DeployInstanceRequest, set stroragetype as S3_STORAGE_TYPE, configurate DELEGATE_DOWNLOAD, add one layer
 * 2. Mock FunctionAgentMgrActor to send DeployInstance, mock RuntimeManager to return StartInstanceReponse, and mock
 * S3Deployer to download code packages and create dir
 * 3. Mock FunctionAgentMgrActor to send DeployInstance again, mock RuntimeManager to return StartInstanceReponse
 * 4. Mock FunctionAgentMgrActor to send KillInstance, mock RuntimeManager to return StopInstanceReponse
 * 5. Mock FunctionAgentMgrActor to send KillInstance again, mock RuntimeManager to return StopInstanceReponse, and mock
 * S3Deployer to clear code packages and remove dir
 * Expectation:
 * 1. While first deploying, runtime code, layer code and delegate code should create dir respectively, each one
 * has one code refer, FunctionAgentMgrActor will receive DeployInstanceResponse
 * 2. While second deploying, runtime code, layer code and delegate code should have two code refer,
 * FunctionAgentMgrActor will receive DeployInstanceResponse again
 * 3. While first killing, dir of runtime code, layer code and delegate code should have one code refer,
 * FunctionAgentMgrActor will receive KillInstanceResponse
 * 3. While second killing, dir of runtime code, layer code and delegate code should be removed respectively, each one have zero code refer,
 * FunctionAgentMgrActor will receive KillInstanceResponse again
 */
TEST_F(AgentServiceActorTest, RepeatedlyDeployAndKillInstanceWithOneLayersAndDelegateViaS3)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    spec->set_deploydir("/tmp/home");
    spec->set_bucketid(TEST_BUCKET_ID);
    spec->set_objectid(TEST_OBJECT_ID);
    // add layer code
    AddLayer(spec->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    // add delegate code
    deployInstanceReq->mutable_createoptions()->insert(
        { "DELEGATE_DOWNLOAD",
          R"({"appId": "userCode", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID"})" });
    std::string destination = "/tmp/home/layer/func/" + TEST_BUCKET_ID + "/" + TEST_OBJECT_ID;
    std::string layerDestination = "/tmp/home/layer/" + TEST_BUCKET_ID + "/" + TEST_LAYER_OBJECT_ID;
    std::string delegateDestination = "/tmp/home/layer/func/testUserCodeBucketID/testUserCodeObjectID";

    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    // have one layer and DELEGATE_DOWNLOAD, three deployers
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(layerDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(delegateDestination); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), delegateDestination), static_cast<uint32_t>(1));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->runtimeid(), TEST_RUNTIME_ID);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    // repeatedly deploy
    startInstanceResponse.set_requestid(TEST_REQUEST_ID_2);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID_2);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse).WillOnce(Return(startInstanceResponse.SerializeAsString()));
    deployInstanceReq->set_requestid(TEST_REQUEST_ID_2);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID_2);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(2));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination), static_cast<uint32_t>(2));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), delegateDestination), static_cast<uint32_t>(2));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID_2);
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->runtimeid(), TEST_RUNTIME_ID_2);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    // KillInstance
    auto killInstanceReq = std::make_unique<messages::KillInstanceRequest>();
    killInstanceReq->set_requestid(TEST_REQUEST_ID);
    killInstanceReq->set_runtimeid(TEST_RUNTIME_ID);
    killInstanceReq->set_storagetype(function_agent::S3_STORAGE_TYPE);

    messages::StopInstanceResponse stopInstanceResponse;
    stopInstanceResponse.set_code(StatusCode::SUCCESS);
    stopInstanceResponse.set_requestid(TEST_REQUEST_ID);
    stopInstanceResponse.set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse)
        .WillOnce(Return(stopInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), delegateDestination), static_cast<uint32_t>(1));
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->requestid(), TEST_REQUEST_ID);
    testFuncAgentMgrActor_->ResetKillInstanceResponse();
    // Repeatedly kill instance
    stopInstanceResponse.set_requestid(TEST_REQUEST_ID_2);
    stopInstanceResponse.set_instanceid(TEST_INSTANCE_ID);
    stopInstanceResponse.set_runtimeid(TEST_RUNTIME_ID_2);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse).WillOnce(Return(stopInstanceResponse.SerializeAsString()));
    killInstanceReq->set_requestid(TEST_REQUEST_ID_2);
    killInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(layerDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(delegateDestination); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(0));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination), static_cast<uint32_t>(0));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), delegateDestination), static_cast<uint32_t>(0));
    EXPECT_EQ(testFuncAgentMgrActor_->GetKillInstanceResponse()->requestid(), TEST_REQUEST_ID_2);
}

/**
 * Feature: AgentServiceActor--UpdateResourcesRequest
 * Description: RuntimeManager send UpdateResources to AgentServiceActor, AgentServiceActor transfer this request to FunctionAgentMgrActor
 * Steps:
 * 1. Create invalid request msg and send UpdateResources request
 * 2. Send correct UpdateResources request but send invalid UpdateInstanceStatusResponse
 * Expectation:
 * 1. FunctionAgentMgrActor will not receive UpdateResources request from AgentServiceActor
 * 2. FunctionAgentMgrActor will receive UpdateInstanceStatus request from AgentServiceActor
 */
TEST_F(AgentServiceActorTest, UpdateResourcesRequest)
{
    auto updateResourcesReq = std::make_unique<messages::UpdateResourcesRequest>();
    // ParseFromString failed
    testMetricsActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                        "UpdateResources",
                                                        "invalid $$");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetReceivedUpdateResource(), false);
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), true);
    // success
    testMetricsActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                        "UpdateResources",
                                                        std::move(updateResourcesReq->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetReceivedUpdateResource(), true);
}

/**
 * Feature: AgentServiceActor--UpdateInstanceStatusRequestAndResponse
 * Description: Mock RuntimeManager to send UpdateInstanceStatus and mock FunctionAgentMgrActor to send UpdateInstanceStatusResponse
 * Steps:
 * 1. Create invalid request msg and send UpdateInstanceStatus request
 * Expectation:
 * 1. RuntimeManager will receive UpdateInstanceStatusResponse
 */
TEST_F(AgentServiceActorTest, UpdateInstanceStatusRequestAndResponse)
{
    auto testHealthCheckActor =
        std::make_shared<function_agent::test::MockHealthCheckActor>(RUNTIME_MANAGER_HEALTH_CHECK_ACTOR_NAME);
    testHealthCheckActor->actorMessageList_.emplace("UpdateInstanceStatus");
    litebus::Spawn(testHealthCheckActor, true);

    auto req = std::make_unique<messages::UpdateInstanceStatusRequest>();
    req->set_requestid(TEST_REQUEST_ID);

    // lost connection with local scheduler
    dstActor_->isRegisterCompleted_ = false;
    testHealthCheckActor->SendRequestToAgentServiceActor(dstActor_->GetAID(), "UpdateInstanceStatus",
                                                         std::move(req->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetReceivedUpdateInstanceStatus(), false);

    // success
    dstActor_->isRegisterCompleted_ = true;
    testHealthCheckActor->SendRequestToAgentServiceActor(dstActor_->GetAID(), "UpdateInstanceStatus",
                                                         std::move(req->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() { return testFuncAgentMgrActor_->GetReceivedUpdateInstanceStatus(); });
    ASSERT_AWAIT_TRUE(
        [&]() { return testHealthCheckActor->GetUpdateInstanceStatusResponse()->requestid() == TEST_REQUEST_ID; });

    litebus::Terminate(testHealthCheckActor->GetAID());
    litebus::Await(testHealthCheckActor);
}

/**
 * Feature: AgentServiceActor--UpdateRuntimeStatusRequestAndResponse
 * Description: Mock RuntimeManager to send UpdateRuntimeStatus and mock FunctionAgentMgrActor to send UpdateAgentStatusResponse
 * Steps:
 * 1. Create invalid request msg and send UpdateRuntimeStatus request
 * 2. Send correct UpdateRuntimeStatus request but send invalid UpdateAgentStatusResponse
 * 3. wait 1s for AgentServiceActor to retry send UpdateAgentStatus to FunctionAgentMgrActor, this time with wrong request id
 * 4. wait 1s for AgentServiceActor to retry send UpdateAgentStatus to FunctionAgentMgrActor, this time with correct UpdateAgentStatusResponse
 * Expectation:
 * 1. FunctionAgentMgrActor will receive UpdateAgentStatus request from AgentServiceActor, RuntimeManager will not receive UpdateInstanceStatusResponse
 * 2. FunctionAgentMgrActor will receive UpdateInstanceStatus request from AgentServiceActor, RuntimeManager will receive UpdateInstanceStatusResponse
 * 3. After waiting for 1s, FunctionAgentMgrActor will receive UpdateInstanceStatus request from AgentServiceActor
 * 4. After waiting for 1s, FunctionAgentMgrActor will receive UpdateInstanceStatus request from AgentServiceActor
 * 5. After waiting for 1s, FunctionAgentMgrActor will not receive UpdateInstanceStatus request from AgentServiceActor
 */
TEST_F(AgentServiceActorTest, UpdateRuntimeStatusRequestAndResponse)
{
    auto updateRuntimeStatusRequest = std::make_unique<messages::UpdateRuntimeStatusRequest>();
    updateRuntimeStatusRequest->set_requestid(TEST_REQUEST_ID);

    messages::UpdateAgentStatusResponse updateAgentStatusRsp;
    updateAgentStatusRsp.set_requestid(TEST_REQUEST_ID);
    EXPECT_CALL(*testFuncAgentMgrActor_, MockUpdateAgentStatusResponse)
        .WillOnce(Return("invalid $$"))
        .WillOnce(Return(updateAgentStatusRsp.SerializeAsString()));
    // ParseFromString failed
    testMetricsActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                        "UpdateRuntimeStatus",
                                                        "invalid $$");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetReceivedUpdateAgentStatus(), false);
    EXPECT_EQ(testMetricsActor_->GetUpdateRuntimeStatusResponse()->requestid(), "");
    // lost connection with local scheduler
    dstActor_->isRegisterCompleted_ = false;
    testMetricsActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                      "UpdateRuntimeStatus",
                                                      std::move(updateRuntimeStatusRequest->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetReceivedUpdateAgentStatus(), false);
    // lost connection with runtime manager
    dstActor_->isRegisterCompleted_ = true;
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), false);
    testMetricsActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                      "UpdateRuntimeStatus",
                                                      std::move(updateRuntimeStatusRequest->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetReceivedUpdateAgentStatus(), false);
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), true);
    // success request but error response
    testMetricsActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                        "UpdateRuntimeStatus",
                                                        std::move(updateRuntimeStatusRequest->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetReceivedUpdateAgentStatus(), true);
    testFuncAgentMgrActor_->ResetReceivedUpdateAgentStatus();
    EXPECT_EQ(testMetricsActor_->GetUpdateRuntimeStatusResponse()->requestid(), TEST_REQUEST_ID);
    EXPECT_EQ(testMetricsActor_->GetUpdateRuntimeStatusResponse()->message(), "update runtime status success");
    testMetricsActor_->ResetUpdateRuntimeStatusResponse();
    // wait for retry send UpdateAgentStatus request
    EXPECT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetReceivedUpdateAgentStatus(); });

    testFuncAgentMgrActor_->ResetReceivedUpdateAgentStatus();
    // wait for retry send UpdateAgentStatus request, because of wrong request id
    updateAgentStatusRsp.set_requestid(testFuncAgentMgrActor_->GetUpdateAgentStatusRequest()->requestid());
    EXPECT_CALL(*testFuncAgentMgrActor_, MockUpdateAgentStatusResponse)
        .WillOnce(Return(updateAgentStatusRsp.SerializeAsString()));
    EXPECT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetReceivedUpdateAgentStatus(); });

    testFuncAgentMgrActor_->ResetReceivedUpdateAgentStatus();
    // will not retry
    EXPECT_AWAIT_TRUE([&]() -> bool { return !testFuncAgentMgrActor_->GetReceivedUpdateAgentStatus(); });
}

/**
 * Feature: AgentServiceActor--ReceiveRuntimeManagerRegisterRequest
 * Description: Runtime send Register to AgentServiceActor, callback to method ReceiveRegister of AgentServiceActor
 * Steps:
 * 1. Construct RegisterHelper and SetRegisterHelper for AgentServiceActor
 * 2. Mock RegisterHelper of RuntimeManager to send Register request to RegisterHelper of AgentServiceActor with invalid msg
 * 3. Mock RegisterHelper of RuntimeManager to send Register request to RegisterHelper of AgentServiceActor with RuntimeManager already registered
 * 4. Set RuntimeManager unregister, then mock RegisterHelper of RuntimeManager to send Register request to RegisterHelper of
 * AgentServiceActor without resourceUnit (so that AgentServiceActor will not send Register to FunctionAgentMgrActor)
 * Expectation:
 * 1. First register, cause ParseFromString err, RegisterHelper of RuntimeManager will not receive Registered response
 * 2. Second register, will discard this request, RegisterHelper of RuntimeManager will receive Registered response
 * 3. Third register, will discard this request, RegisterHelper of RuntimeManager will receive Registered response, RuntimeManager will be set registered
 */
TEST_F(AgentServiceActorTest, ReceiveRuntimeManagerRegisterRequest)
{
    messages::RegisterRuntimeManagerRequest req;
    req.set_name(testRuntimeManager_->GetAID().Name());
    req.set_address(testRuntimeManager_->GetAID().Url());
    auto registerHelper = std::make_shared<RegisterHelper>("dstAgentServiceActor");
    dstActor_->SetRegisterHelper(registerHelper);
    litebus::AID dstAid(dstActor_->GetAID().Name() + "-RegisterHelper", dstActor_->GetAID().Url());
    // ParseFromString failed
    testRegisterHelperActor_->SendRequestToAgentServiceActor(dstAid, "Register", " invalid $$");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRegisterHelperActor_->GetReceivedRegisterRuntimeManagerResponse(), false);
    // Resiter for already registered runtime manager
    testRegisterHelperActor_->SendRequestToAgentServiceActor(dstAid, "Register", req.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRegisterHelperActor_->GetReceivedRegisterRuntimeManagerResponse(), true);
    testRegisterHelperActor_->ResetReceivedRegisterRuntimeManagerResponse();
    // Register Success and Start Heartbeat (registeredResourceUnit_ is null, will not send Agent Register Request)
    dstActor_->MarkRuntimeManagerUnavailable("error_id");
    dstActor_->MarkRuntimeManagerUnavailable("");
    messages::RuntimeInstanceInfo runtimeInstanceInfo;
    runtimeInstanceInfo.set_instanceid(TEST_INSTANCE_ID);
    runtimeInstanceInfo.set_requestid(TEST_REQUEST_ID);
    req.mutable_runtimeinstanceinfos()->insert({ TEST_RUNTIME_ID, runtimeInstanceInfo });
    auto uuid = litebus::uuid_generator::UUID::GetRandomUUID();
    req.set_id(uuid.ToString());
    EXPECT_CALL(*testFuncAgentMgrActor_, MockRegisteredResponse).WillOnce(Return(""));
    testRegisterHelperActor_->SendRequestToAgentServiceActor(dstAid, "Register", req.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_EQ(dstActor_->GetRuntimeManagerContext().registered, true);
    EXPECT_EQ(dstActor_->GetRuntimeManagerContext().id, uuid.ToString());
    EXPECT_EQ(testRegisterHelperActor_->GetReceivedRegisterRuntimeManagerResponse(), true);
    EXPECT_EQ(testRegisterHelperActor_->registeredMsg_.code(), StatusCode::SUCCESS);
    testRegisterHelperActor_->ResetReceivedRegisterRuntimeManagerResponse();
    // runtime-manager retry register failed
    dstActor_->MarkRuntimeManagerUnavailable("invalid id");
    dstActor_->MarkRuntimeManagerUnavailable(uuid.ToString());
    testRegisterHelperActor_->SendRequestToAgentServiceActor(dstAid, "Register", req.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_EQ(dstActor_->GetRuntimeManagerContext().registered, false);
    EXPECT_EQ(testRegisterHelperActor_->GetReceivedRegisterRuntimeManagerResponse(), true);
    EXPECT_EQ(testRegisterHelperActor_->registeredMsg_.code(), StatusCode::REGISTER_ERROR);
}

/**
 * Feature: AgentServiceActor--AgentRegisterRequestAndResponse
 * Description: Runtime send Register to AgentServiceActor, callback to method ReceiveRegister of AgentServiceActor,
 * after registered AgentServiceActor send Register to FunctionAgentMgrActor
 * Steps:
 * 1. Construct RegisterHelper and SetRegisterHelper for AgentServiceActor and set AgentServiceActor unregister
 * 2. Mock RegisterHelper of RuntimeManager to send Register request to RegisterHelper of AgentServiceActor with resourceunit
 * 3. Mock FunctionAgentMgrActor send Registered response with invalid msg
 * 4. Mock FunctionAgentMgrActor send Registered response with code -1
 * 5. Mock FunctionAgentMgrActor send Registered response with code SUCCESS
 * 6. Set AgentServiceActor unregister, and then Mock FunctionAgentMgrActor send Registered response with code SUCCESS again
 * Expectation:
 * 1. First register, cause ParseFromString err, RegisterHelper of RuntimeManager will receive Registered response and FunctionAgentMgrActor will
 * receive Register request
 * 2. 6s after, AgentServiceActor first retry register to FunctionAgentMgrActor, FunctionAgentMgrActor will receive Register request
 * 3. 6s after, AgentServiceActor second retry register to FunctionAgentMgrActor, FunctionAgentMgrActor will receive Register request and AgentServiceActor
 * will be registered
 * 4. FunctionAgentMgrActor send Registered response again will not set AgentServiceActor registered
 */
TEST_F(AgentServiceActorTest, AgentRegisterRequestAndResponse)
{
    messages::RegisterRuntimeManagerRequest req;
    req.set_name(testRuntimeManager_->GetAID().Name());
    req.set_address(testRuntimeManager_->GetAID().Url());
    req.set_id(testRuntimeManager_->GetRuntimeManagerID());
    req.mutable_resourceunit()->set_id("dstAgentServiceActor");

    auto registerHelper = std::make_shared<RegisterHelper>("dstAgentServiceActor");
    dstActor_->SetRegisterHelper(registerHelper);
    litebus::AID dstAid(dstActor_->GetAID().Name() + "-RegisterHelper", dstActor_->GetAID().Url());
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), false);  // runtime manager set unregistered
    dstActor_->isRegisterCompleted_ = false; // agent set unregistered
    messages::Registered registeredErrorCode;
    registeredErrorCode.set_code(-1);
    messages::Registered registeredSuccess;
    registeredSuccess.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*testFuncAgentMgrActor_, MockRegisteredResponse)
        .WillOnce(Return("invalid $$"))
        .WillOnce(Return(registeredErrorCode.SerializeAsString()))
        .WillOnce(Return(registeredSuccess.SerializeAsString()));
    testRegisterHelperActor_->SendRequestToAgentServiceActor(dstAid, "Register", std::move(req.SerializeAsString()));
    // first time reponse with invalid msg
    ASSERT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetReceivedRegisterRequest(); });
    EXPECT_EQ(testRegisterHelperActor_->GetReceivedRegisterRuntimeManagerResponse(), true);
    testFuncAgentMgrActor_->ResetReceivedRegisterRequest();
    EXPECT_EQ(dstActor_->isRegisterCompleted_, false);
    // wait for retry
    ASSERT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetReceivedRegisterRequest(); });
    testFuncAgentMgrActor_->ResetReceivedRegisterRequest();
    EXPECT_EQ(dstActor_->isRegisterCompleted_, false);
    // will not retry
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    EXPECT_EQ(testFuncAgentMgrActor_->GetReceivedRegisterRequest(), false);
    // send register again
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), false, "ignore_id");  // runtime manager set unregistered
    testRegisterHelperActor_->SendRequestToAgentServiceActor(dstAid, "Register", std::move(req.SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return dstActor_->isRegisterCompleted_; });
    // already received registered msg error
    dstActor_->isRegisterCompleted_ = false;
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "Registered",
                                                           std::move(registeredSuccess.SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return !dstActor_->isRegisterCompleted_; });
}

/**
 * Feature: AgentServiceActor--QueryInstanceStatusInfoTest
 * Description: FunctionAgentMgrActor send QueryInstanceStatusInfo to AgentServiceActor, AgentServiceActor transfer this request to RuntimeManager
 * Steps:
 * 1. Send UpdateResources request
 * Expectation:
 * 1. FunctionAgentMgrActor will receive QueryInstanceStatusResponse from AgentServiceActor
 */
TEST_F(AgentServiceActorTest, QueryInstanceStatusInfoTest)
{
    auto req = std::make_unique<messages::QueryInstanceStatusRequest>();
    req->set_requestid(TEST_REQUEST_ID);
    // lost connection with runtime manager
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), false);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "QueryInstanceStatusInfo",
                                                           std::move(req->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRuntimeManager_->GetReceiveQueryInstanceStatusInfo(), false);
    // success
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), true);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "QueryInstanceStatusInfo",
                                                           std::move(req->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRuntimeManager_->GetReceiveQueryInstanceStatusInfo(), true);
    EXPECT_EQ(testFuncAgentMgrActor_->GetQueryInstanceStatusResponse()->requestid(), TEST_REQUEST_ID);
}

TEST_F(AgentServiceActorTest, QueryDebugInstanceInfosTest)
{
    auto req = std::make_unique<messages::QueryDebugInstanceInfosRequest>();
    req->set_requestid(TEST_REQUEST_ID);
    // lost connection with runtime manager
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), false);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "QueryDebugInstanceInfos",
                                                           std::move(req->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRuntimeManager_->GetReceiveQueryDebugInstanceInfos(), false);

    // success
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), true);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "QueryDebugInstanceInfos",
                                                           std::move(req->SerializeAsString()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testRuntimeManager_->GetReceiveQueryDebugInstanceInfos(), true);
    EXPECT_EQ(testFuncAgentMgrActor_->GetQueryDebugInstanceInfosResponse()->requestid(), TEST_REQUEST_ID);
}

/**
 * Feature: AgentServiceActor--UpdateTokenTest
 * Description: test UpdateToken interface of AgentServiceActor
 * Steps:
 * 1. receive UpdateToken request from FunctionAgentMgrActor and send it to RuntimeManger,
 * then receive UpdateTokenResponse from RuntimeManager and will send it to FunctionAgentMgrActor
 */
TEST_F(AgentServiceActorTest, UpdateTokenTest)
{
    auto req = std::make_unique<messages::UpdateCredRequest>();
    req->set_requestid(TEST_REQUEST_ID);

    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), false);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "UpdateCred",
                                                           std::move(req->SerializeAsString()));
    EXPECT_EQ(testFuncAgentMgrActor_->GetUpdateTokenResponse()->requestid(), "");
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), true);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "UpdateCred",
                                                           std::move(req->SerializeAsString()));
    ASSERT_AWAIT_TRUE(
        [&]() { return testFuncAgentMgrActor_->GetUpdateTokenResponse()->requestid() == TEST_REQUEST_ID; });
}

/**
 * Feature: AgentServiceActor--ProtectedSetNetworkTest
 * Description: Config network, including firewall, tunnel and route
 * Steps:
 * 1. Construct wrong firewall config and set network
 * 2. Construct wrong tunnel config and set network
 * 2. Construct wrong route config and set network
 * 2. Construct empty network config and set network
 * Expectation:
 * 1. Set firewall cause err, return false
 * 1. Set tunnel cause err, return false
 * 1. Set route cause err, return false
 * 1. Skip set network, return true
 */
TEST_F(AgentServiceActorTest, ProtectedSetNetworkTest)
{
    bool state = false;
    if (getenv("POD_IP") == nullptr) {
        setenv("POD_IP", "127.0.0.1", 0);
        state = true;
    }
    EXPECT_EQ(dstActor_->ProtectedSetNetwork(NetworkTool::ParseNetworkConfig(TEST_WRONG_FIREWALL_CONFIG)), false);
    EXPECT_EQ(dstActor_->ProtectedSetNetwork(NetworkTool::ParseNetworkConfig(TEST_WRONG_TUNNEL_CONFIG)), false);
    EXPECT_EQ(dstActor_->ProtectedSetNetwork(NetworkTool::ParseNetworkConfig(TEST_WRONG_ROUTE_CONFIG)), false);
    EXPECT_EQ(dstActor_->ProtectedSetNetwork(NetworkTool::ParseNetworkConfig("")), true);
    if (state) {
        unsetenv("POD_IP");
    }
}

/**
 * Feature: AgentServiceActor--StartPingPongSuccess
 * Description: When AgentServiceActor registered, start pingpong to receive heartbeat from FunctionAgentMgrActor
 * Steps:
 * 1. Start PingPong
 * Expectation:
 * 1. PingPongDriver will be constructed
 */
TEST_F(AgentServiceActorTest, StartPingPongSuccess)
{
    messages::Registered registered;
    auto registerResponseFuture = dstActor_->StartPingPong(registered);
    EXPECT_TRUE(dstActor_->GetPingPongDriver() != nullptr);
}

/**
 * Feature: AgentServiceActor--TimeOutEventTest
 * Description: When PingPongActor of AgentServiceActor do not receieve heartbeat from FunctionAgentMgrActor over than 12 times
 * cause TimeOutEvent
 * Steps:
 * 1. Start TimeOutEvent
 * 2. Start TimeOutEvent with registeredPromise failed
 * Expectation:
 * 1. cause RegisterAgent
 * 2. PingPongDriver will be set nullptr
 */
TEST_F(AgentServiceActorTest, TimeOutEventTest)
{
    RegisterInfo registerInfo;
    registerInfo.registeredPromise = litebus::Promise<messages::Registered>();
    dstActor_->SetRegisterInfo(registerInfo);
    dstActor_->TimeOutEvent();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    messages::Registered registered;
    auto registerResponseFuture = dstActor_->StartPingPong(registered);
    registerInfo.registeredPromise.SetFailed(static_cast<int32_t>(StatusCode::FUNC_AGENT_PING_PONG_IS_NULL));
    dstActor_->SetRegisterInfo(registerInfo);
    dstActor_->TimeOutEvent();
    EXPECT_TRUE(dstActor_->GetPingPongDriver() != nullptr);
}

/**
 * Feature: DeployInstanceSuccessWithS3WithLayerWithUserCodeDownload
 * Description: deploy instance success when s3 deploy with user code and user code layer
 * Steps:
 * 1. set executor code
 * 2. set executor layer code
 * 3. set user code
 * 4. set user layer code
 * Expectation:
 * 1. start Instance
 * 1.1 deploy executor code
 * 1.2 deploy executor layer code
 * 1.3 deploy user code
 * 1.4 deploy user layer code
 * 1.5 request set env ENV_DELEGATE_DOWNLOAD and LAYER_LIB_PATH
 * 2. kill Instance
 * 2.1 clear executor code
 * 2.2 clear executor layer code
 * 2.3 clear user code
 * 2.4 clear user layer code
 */

TEST_F(AgentServiceActorTest, DeployInstanceSuccessWithS3WithLayerWithUserCodeDownload)
{
    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(0);
    startInstanceResponse.set_message(TEST_REQUEST_ID);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));

    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    // executor destination
    deployInstanceReq->mutable_funcdeployspec()->set_deploydir("/tmp/home");
    deployInstanceReq->mutable_funcdeployspec()->set_bucketid("testBucketID");
    deployInstanceReq->mutable_funcdeployspec()->set_objectid("testObjectID");
    std::string executorDestination = "/tmp/home/layer/func/testBucketID/testObjectID";

    // layer destination
    AddLayer(deployInstanceReq->mutable_funcdeployspec()->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    std::string layer1Destination = "/tmp/home/layer/" + TEST_BUCKET_ID + "/" + TEST_LAYER_OBJECT_ID;

    // user code destination
    deployInstanceReq->mutable_createoptions()->insert(
        { "DELEGATE_DOWNLOAD",
          R"({"appId": "userCode", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID"})" });
    std::string userCodeDestination = "/tmp/home/layer/func/testUserCodeBucketID/testUserCodeObjectID";

    // user code layer destination
    deployInstanceReq->mutable_createoptions()->insert(
        { "DELEGATE_LAYER_DOWNLOAD",
          R"([{"appId": "userCode-layer1", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID-layer1"}])" });
    std::string userCodeLayer1Destination = "/tmp/home/layer/testUserCodeBucketID/testUserCodeObjectID-layer1";
    std::string userCodeLayer2Destination = "/tmp/home/layer/testUserCodeBucketID/testUserCodeObjectID-layer2";

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));

    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(executorDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(layer1Destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(userCodeDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(userCodeLayer1Destination); });
    ASSERT_AWAIT_TRUE(
        [&]() -> bool { return testFuncAgentMgrActor_->GetDeployInstanceResponse()->message() == TEST_REQUEST_ID; });
    auto startInstanceRequest = std::make_shared<messages::StartInstanceRequest>();
    startInstanceRequest->ParseFromString(testRuntimeManager_->promiseOfStartInstanceRequest.GetFuture().Get());
    EXPECT_EQ(
        startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find("ENV_DELEGATE_DOWNLOAD")->second,
        "/tmp/home/layer/func/testUserCodeBucketID/testUserCodeObjectID");

    // start to kill instances
    messages::StopInstanceResponse stopInstanceResponse;
    stopInstanceResponse.set_code(0);
    stopInstanceResponse.set_requestid(TEST_REQUEST_ID);
    auto deploymentConfigs = dstActor_->GetRuntimesDeploymentCache();
    for (const auto& deploymentConfigIter : deploymentConfigs->runtimes) {
        stopInstanceResponse.set_runtimeid(deploymentConfigIter.first);
    }
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse)
        .WillOnce(Return(stopInstanceResponse.SerializeAsString()));

    auto killInstanceReq = std::make_unique<messages::KillInstanceRequest>();
    killInstanceReq->set_storagetype(function_agent::S3_STORAGE_TYPE);
    killInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    killInstanceReq->set_requestid(TEST_REQUEST_ID);

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));

    ASSERT_AWAIT_TRUE(
        [&]() -> bool { return testFuncAgentMgrActor_->GetKillInstanceResponse()->requestid() == TEST_REQUEST_ID; });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(executorDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(layer1Destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(userCodeDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(userCodeLayer1Destination); });

    (void)litebus::os::Rmdir("/tmp/home/layer");
}

/**
 * Feature: DeployInstanceFailedWithS3WithLayerWithUserCodeDownload
 * Description: deploy instance failed when s3 deploy with user code and user code layer
 * Steps:
 * 1. set executor code
 * 2. set executor layer code
 * 3. set user code
 * 4. set user layer code
 * Expectation:
 * 1. start Instance
 * 1.1 deploy executor code
 * 1.2 deploy executor layer code
 * 1.3 deploy user code
 * 1.4 deploy user layer code
 * 1.5 request set env ENV_DELEGATE_DOWNLOAD and LAYER_LIB_PATH
 * 2. start failed
 * 2.1 clear executor code
 * 2.2 clear executor layer code
 * 2.3 clear user code
 * 2.4 clear user layer code
 */

TEST_F(AgentServiceActorTest, DeployInstanceFailedWithS3WithLayerWithUserCodeDownload)
{
    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(-1);
    startInstanceResponse.set_message(TEST_REQUEST_ID);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse).WillOnce(Return(startInstanceResponse.SerializeAsString()));

    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    // executor destination
    deployInstanceReq->mutable_funcdeployspec()->set_deploydir("/tmp/home");
    deployInstanceReq->mutable_funcdeployspec()->set_bucketid("testBucketID");
    deployInstanceReq->mutable_funcdeployspec()->set_objectid("testObjectID");
    std::string executorDestination = "/tmp/home/layer/func/testBucketID/testObjectID";

    // layer destination
    AddLayer(deployInstanceReq->mutable_funcdeployspec()->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    std::string layer1Destination = "/tmp/home/layer/" + TEST_BUCKET_ID + "/" + TEST_LAYER_OBJECT_ID;

    // user code destination
    deployInstanceReq->mutable_createoptions()->insert({"DELEGATE_DOWNLOAD", R"({"appId": "userCode", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID"})"});
    std::string userCodeDestination = "/tmp/home/layer/func/testUserCodeBucketID/testUserCodeObjectID";

    // user code layer destination
    deployInstanceReq->mutable_createoptions()->insert({"DELEGATE_LAYER_DOWNLOAD", R"([{"appId": "userCode-layer1", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID-layer1"}])"});
    std::string userCodeLayer1Destination = "/tmp/home/layer/testUserCodeBucketID/testUserCodeObjectID-layer1";

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(executorDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(layer1Destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(userCodeDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(userCodeLayer1Destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetDeployInstanceResponse()->message() == TEST_REQUEST_ID; });
    auto startInstanceRequest = std::make_shared<messages::StartInstanceRequest>();
    startInstanceRequest->ParseFromString(testRuntimeManager_->promiseOfStartInstanceRequest.GetFuture().Get());
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find("ENV_DELEGATE_DOWNLOAD")->second, "/tmp/home/layer/func/testUserCodeBucketID/testUserCodeObjectID");
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().layers_size(), 2);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().layers(0).bucketid(), TEST_BUCKET_ID);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().layers(0).objectid(), TEST_LAYER_OBJECT_ID);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().layers(1).bucketid(), "testUserCodeBucketID");
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().layers(1).objectid(), "testUserCodeObjectID-layer1");

    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(executorDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(layer1Destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(userCodeDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(userCodeLayer1Destination); });

    (void) litebus::os::Rmdir("/tmp/home/layer");
}

TEST_F(AgentServiceActorTest, CodeReferAddAndDeleteTest)
{
    auto deployer = std::make_shared<function_agent::LocalDeployer>();
    dstActor_->SetDeployers(function_agent::LOCAL_STORAGE_TYPE, deployer);
    dstActor_->SetRuntimeManagerAID(testRuntimeManager_->GetAID(), false);
    messages::RegisterRuntimeManagerRequest registerReq;
    registerReq.set_name(testRuntimeManager_->GetAID().Name());
    registerReq.set_address(testRuntimeManager_->GetAID().Url());
    registerReq.set_id(testRuntimeManager_->GetRuntimeManagerID());
    auto registerHelper = std::make_shared<RegisterHelper>("dstAgentServiceActor");
    dstActor_->SetRegisterHelper(registerHelper);
    litebus::AID dstAid(dstActor_->GetAID().Name() + "-RegisterHelper", dstActor_->GetAID().Url());
    messages::RuntimeInstanceInfo runtimeInstanceInfo;
    auto deploymentConfig = runtimeInstanceInfo.mutable_deploymentconfig();
    deploymentConfig->set_deploydir(LOCAL_DEPLOY_DIR);
    deploymentConfig->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    runtimeInstanceInfo.set_instanceid(TEST_INSTANCE_ID);
    runtimeInstanceInfo.set_requestid(TEST_REQUEST_ID);
    registerReq.mutable_runtimeinstanceinfos()->insert({ TEST_RUNTIME_ID, runtimeInstanceInfo });
    testRegisterHelperActor_->SendRequestToAgentServiceActor(dstAid, "Register", registerReq.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_EQ(dstActor_->GetRuntimeManagerContext().registered, true);
    EXPECT_EQ(testRegisterHelperActor_->GetReceivedRegisterRuntimeManagerResponse(), true);
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(1));

    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_requestid(TEST_REQUEST_ID_2);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID_2);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    messages::DeployInstanceRequest deployReq;
    deployReq.mutable_funcdeployspec()->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    deployReq.mutable_funcdeployspec()->set_deploydir(LOCAL_DEPLOY_DIR);
    deployReq.set_requestid(TEST_REQUEST_ID_2);
    deployReq.set_instanceid(TEST_INSTANCE_ID_2);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           deployReq.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(2));

    startInstanceResponse.set_requestid(TEST_REQUEST_ID_3);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID_3);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    deployReq.set_requestid(TEST_REQUEST_ID_3);
    deployReq.set_instanceid(TEST_INSTANCE_ID_3);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           deployReq.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(3));

    messages::StopInstanceResponse stopRsp;
    stopRsp.set_requestid(TEST_REQUEST_ID);
    stopRsp.set_instanceid(TEST_INSTANCE_ID);
    stopRsp.set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse).WillOnce(Return(stopRsp.SerializeAsString()));
    messages::KillInstanceRequest killReq;
    killReq.set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    killReq.set_requestid(TEST_REQUEST_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           killReq.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(2));

    stopRsp.set_requestid(TEST_REQUEST_ID_2);
    stopRsp.set_instanceid(TEST_INSTANCE_ID_2);
    stopRsp.set_runtimeid(TEST_RUNTIME_ID_2);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse).WillOnce(Return(stopRsp.SerializeAsString()));
    killReq.set_requestid(TEST_REQUEST_ID_2);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           killReq.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(1));

    stopRsp.set_requestid(TEST_REQUEST_ID_3);
    stopRsp.set_instanceid(TEST_INSTANCE_ID_3);
    stopRsp.set_runtimeid(TEST_RUNTIME_ID_3);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse).WillOnce(Return(stopRsp.SerializeAsString()));
    killReq.set_requestid(TEST_REQUEST_ID_3);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           killReq.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(0));
}

TEST_F(AgentServiceActorTest, TestCodeReferWhenRetryDeployAndKillInstance)
{
    auto deployer = std::make_shared<function_agent::LocalDeployer>();
    dstActor_->SetDeployers(function_agent::LOCAL_STORAGE_TYPE, deployer);

    testRuntimeManager_->SetIsNeedToResponse(false);
    messages::DeployInstanceRequest deployReq;
    deployReq.mutable_funcdeployspec()->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    deployReq.mutable_funcdeployspec()->set_deploydir(LOCAL_DEPLOY_DIR);
    deployReq.set_requestid(TEST_REQUEST_ID);
    deployReq.set_instanceid(TEST_INSTANCE_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           deployReq.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_TRUE(testRuntimeManager_->GetReceivedStartInstanceRequest());
    testRuntimeManager_->ResetReceivedStartInstanceRequest();
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(1));

    testRuntimeManager_->SetIsNeedToResponse(true);
    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           deployReq.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->instanceid(), TEST_INSTANCE_ID);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(1));

    testRuntimeManager_->SetIsNeedToResponse(false);
    messages::KillInstanceRequest killReq;
    killReq.set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    killReq.set_requestid(TEST_REQUEST_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           killReq.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_TRUE(testRuntimeManager_->GetReceivedStopInstanceRequest());
    testRuntimeManager_->ResetReceivedStopInstanceRequest();
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(1));

    testRuntimeManager_->SetIsNeedToResponse(true);
    messages::StopInstanceResponse stopRsp;
    stopRsp.set_requestid(TEST_REQUEST_ID);
    stopRsp.set_instanceid(TEST_INSTANCE_ID);
    stopRsp.set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse).WillOnce(Return(stopRsp.SerializeAsString()));
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           killReq.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), LOCAL_DEPLOY_DIR), static_cast<uint32_t>(0));
}

TEST_F(AgentServiceActorTest, TestCodeReferDeployKillDeploySuccessive)
{
    messages::DeployInstanceRequest deployReq;
    deployReq.mutable_funcdeployspec()->set_storagetype(function_agent::S3_STORAGE_TYPE);
    deployReq.mutable_funcdeployspec()->set_deploydir("/tmp/home");
    deployReq.mutable_funcdeployspec()->set_bucketid(TEST_BUCKET_ID);
    deployReq.mutable_funcdeployspec()->set_objectid(TEST_OBJECT_ID);
    auto destination = "/tmp/home/layer/func/" + TEST_BUCKET_ID + "/" + TEST_OBJECT_ID;
    litebus::os::Rmdir(destination);

    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .Times(2)
        .WillRepeatedly(Return(startInstanceResponse.SerializeAsString()));

    deployReq.set_requestid(TEST_REQUEST_ID);
    deployReq.set_instanceid(TEST_INSTANCE_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           deployReq.SerializeAsString());
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(destination); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(1));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();

    messages::StopInstanceResponse stopInstanceResponse;
    stopInstanceResponse.set_requestid(TEST_REQUEST_ID);
    stopInstanceResponse.set_instanceid(TEST_INSTANCE_ID);
    stopInstanceResponse.set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse)
        .WillOnce(Return(stopInstanceResponse.SerializeAsString()));
    messages::KillInstanceRequest killReq;
    killReq.set_storagetype(function_agent::S3_STORAGE_TYPE);
    killReq.set_requestid(TEST_REQUEST_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           killReq.SerializeAsString());
    deployReq.set_requestid(TEST_REQUEST_ID_2);
    deployReq.set_instanceid(TEST_INSTANCE_ID_2);
    AddLayer(deployReq.mutable_funcdeployspec()->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    auto layerDestination = "/tmp/home/layer/" + TEST_BUCKET_ID + "/" + TEST_LAYER_OBJECT_ID;
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           deployReq.SerializeAsString());
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(layerDestination); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(1));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), layerDestination), static_cast<uint32_t>(1));
    EXPECT_TRUE(litebus::os::ExistPath(destination));
    EXPECT_TRUE(litebus::os::Rmdir(destination).IsNone());
    EXPECT_TRUE(litebus::os::Rmdir(layerDestination).IsNone());
}

TEST_F(AgentServiceActorTest, CleanStatusRequestRetryTest)
{
    messages::CleanStatusRequest cleanStatusRequest;
    cleanStatusRequest.set_name("invalid agentID");
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "CleanStatus",
                                                           cleanStatusRequest.SerializeAsString());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(testFuncAgentMgrActor_->GetReceivedCleanStatusResponse());

    testRuntimeManager_->SetIsNeedToResponse(false);
    cleanStatusRequest.set_name(TEST_AGENT_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "CleanStatus",
                                                           cleanStatusRequest.SerializeAsString());

    ASSERT_AWAIT_TRUE([&]() -> bool { return testRuntimeManager_->GetReceiveCleanStatusRequest(); });
    testRuntimeManager_->ResetReceiveCleanStatusRequest();
    ASSERT_AWAIT_TRUE([&]() -> bool { return testRuntimeManager_->GetReceiveCleanStatusRequest(); });
    testRuntimeManager_->ResetReceiveCleanStatusRequest();
    ASSERT_AWAIT_TRUE([&]() -> bool { return testRuntimeManager_->GetReceiveCleanStatusRequest(); });
    testRuntimeManager_->ResetReceiveCleanStatusRequest();

    EXPECT_TRUE(testFuncAgentMgrActor_->GetReceivedCleanStatusResponse());
}

TEST_F(AgentServiceActorTest, CleanStatusWithExistedInstanceTest)
{
    messages::DeployInstanceRequest deployInstanceReq;
    deployInstanceReq.set_requestid(TEST_REQUEST_ID);
    deployInstanceReq.set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq.mutable_funcdeployspec();
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    spec->set_deploydir("/tmp/home/test");
    spec->set_bucketid(TEST_BUCKET_ID);
    spec->set_objectid(TEST_OBJECT_ID);
    AddLayer(spec->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID);
    auto destination = "/tmp/home/test/layer/func/" + TEST_BUCKET_ID + "/" + TEST_OBJECT_ID;
    auto layerDestination = "/tmp/home/test/layer/" + TEST_BUCKET_ID + "/" + TEST_LAYER_OBJECT_ID;

    auto deployInstanceReq2 = deployInstanceReq;
    deployInstanceReq2.set_instanceid(TEST_INSTANCE_ID_2);
    deployInstanceReq2.mutable_funcdeployspec()->set_deploydir("/tmp/home/test2");
    AddLayer(deployInstanceReq2.mutable_funcdeployspec()->add_layers(), TEST_BUCKET_ID, TEST_LAYER_OBJECT_ID_2);
    auto destination2 = "/tmp/home/test2/layer/func/" + TEST_BUCKET_ID + "/" + TEST_OBJECT_ID;
    auto layerDestination2 = "/tmp/home/test2/layer/" + TEST_BUCKET_ID + "/" + TEST_LAYER_OBJECT_ID;

    litebus::os::Rmdir(destination);
    litebus::os::Rmdir(destination2);
    litebus::os::Rmdir(layerDestination);
    litebus::os::Rmdir(layerDestination2);

    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillRepeatedly(Return(startInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq.SerializeAsString()));
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq2.SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(layerDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(destination2); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(layerDestination2); });

    messages::CleanStatusRequest cleanStatusRequest;
    cleanStatusRequest.set_name(TEST_AGENT_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "CleanStatus",
                                                           std::move(cleanStatusRequest.SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(layerDestination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(destination2); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(layerDestination2); });
}

TEST_F(AgentServiceActorTest, RegisterAgentFailedTest)
{
    std::string msg = "AgentServiceActor nullptr of registeredResourceUnit_! Maybe runtime_manager is not registered.";
    dstActor_->SetRegisteredResourceUnit(nullptr);
    auto rsp = dstActor_->ProtectedRegisterAgent();
    EXPECT_EQ(rsp.Get().code(), StatusCode::FUNC_AGENT_RESOURCE_UNIT_IS_NULL);
    EXPECT_EQ(rsp.Get().message(), msg);
}

TEST_F(AgentServiceActorTest, GracefulShutdown)
{
    testFuncAgentMgrActor_->ResetReceivedUpdateAgentStatus();
    auto fut = litebus::Async(dstActor_->GetAID(), &AgentServiceActor::GracefulShutdown);
    testRuntimeManager_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                        "GracefulShutdownFinish", "");
    EXPECT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetReceivedUpdateAgentStatus(); });
    messages::UpdateAgentStatusResponse updateAgentStatusRsp;
    updateAgentStatusRsp.set_requestid(testFuncAgentMgrActor_->GetUpdateAgentStatusRequest()->requestid());
    EXPECT_CALL(*testFuncAgentMgrActor_, MockUpdateAgentStatusResponse)
        .WillRepeatedly(Return(updateAgentStatusRsp.SerializeAsString()));
    dstActor_->TimeOutEvent();
    EXPECT_TRUE(fut.Get());
}

TEST_F(AgentServiceActorTest, RestartForReuse)
{
    dstActor_->monopolyUsed_ = true;
    dstActor_->enableRestartForReuse_ = true;
    litebus::Async(dstActor_->GetAID(), &AgentServiceActor::TimeOutEvent);
    EXPECT_TRUE(dstActor_->runtimeManagerGracefulShutdown_.GetFuture().Get());
}

TEST_F(AgentServiceActorTest, SetNetworkIsolationPodIpSuccessAddDelete)
{
    CommandExecResult result;
    result.output = "Name: test-podip-whitelist\nMembers:\n";
    result.error = "";
    CommandExecResult result3;
    result3.output = "Name: test-podip-whitelist\nMembers:\n192.168.1.1\n192.168.2.1";
    result3.error = "";
    CommandExecResult result5;
    result5.output = "Name: test-podip-whitelist\nMembers:\n192.168.1.1";
    result5.error = "";
    EXPECT_CALL(*commandRunner_, ExecuteCommandWrapper(_))
        .WillOnce(Return(result))    // ipset list result #1
        .WillOnce(Return(result3))   // #3
        .WillOnce(Return(result5));  // #5

    EXPECT_CALL(*commandRunner_, CheckAndRunCommandWrapper(_)).WillRepeatedly(Return(true));  // #2, #4

    // add more
    messages::SetNetworkIsolationRequest req;
    req.set_requestid(TEST_REQUEST_ID);
    req.set_ruletype(messages::RuleType::IPSET_ADD);
    (*req.mutable_rules()->Add()) = "192.168.1.1";
    (*req.mutable_rules()->Add()) = "192.168.2.1";

    auto response = testFuncAgentMgrActor_->GetSetNetworkIsolationResponse();
    response->set_code(StatusCode::FAILED);  // must do reset
    response->set_requestid("");             // must do reset
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "SetNetworkIsolationRequest",  // #1, #2
                                                           req.SerializeAsString());
    EXPECT_AWAIT_TRUE([&]() -> bool {
        return response->requestid() == TEST_REQUEST_ID;
    });
    EXPECT_EQ(response->code(), StatusCode::SUCCESS);

    // delete
    messages::SetNetworkIsolationRequest req2;
    req2.set_requestid(TEST_REQUEST_ID_3);
    req2.set_ruletype(messages::RuleType::IPSET_DELETE);
    (*req2.mutable_rules()->Add()) = "192.168.2.1";

    response->set_code(StatusCode::FAILED);  // must do reset
    response->set_requestid("");             // must do reset
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "SetNetworkIsolationRequest",  // #3, #4
                                                           req2.SerializeAsString());
    EXPECT_AWAIT_TRUE([&]() -> bool {
        return response->code() == StatusCode::SUCCESS;
    });

    auto ipsetIsolation = dstActor_->GetIpsetIpv4NetworkIsolation();
    EXPECT_EQ(ipsetIsolation->GetAllRules().size(), static_cast<size_t>(1));  // #5
}

TEST_F(AgentServiceActorTest, SetNetworkIsolationRuleFlush)
{
    CommandExecResult result;
    result.output = "Name: test-podip-whitelist\nMembers:\n";
    result.error = "";
    CommandExecResult result3;
    result3.output = "Name: test-podip-whitelist\nMembers:\n192.168.1.1";
    result3.error = "";
    EXPECT_CALL(*commandRunner_, ExecuteCommandWrapper(_))
        .WillOnce(Return(result))   // #1
        .WillOnce(Return(result3))  // #3
        .WillOnce(Return(result));  // #5

    EXPECT_CALL(*commandRunner_, CheckAndRunCommandWrapper(_))
        .WillOnce(Return(true))   // #2
        .WillOnce(Return(true));  // #4

    // add one
    messages::SetNetworkIsolationRequest req;
    req.set_requestid(TEST_REQUEST_ID);
    req.set_ruletype(messages::RuleType::IPSET_ADD);
    (*req.mutable_rules()->Add()) = "192.168.1.1";

    const auto response = testFuncAgentMgrActor_->GetSetNetworkIsolationResponse();
    response->set_code(StatusCode::FAILED);  // must do reset
    response->set_requestid("");             // must do reset
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "SetNetworkIsolationRequest",  // #1, #2
                                                           req.SerializeAsString());
    EXPECT_AWAIT_TRUE([&]() -> bool {
        return response->requestid() == TEST_REQUEST_ID;
    });
    EXPECT_AWAIT_TRUE([&]() -> bool {
        return response->code() == StatusCode::SUCCESS;
    });

    // flush
    messages::SetNetworkIsolationRequest req2;
    req2.set_requestid(TEST_REQUEST_ID_3);
    req2.set_ruletype(messages::RuleType::IPSET_FLUSH);

    response->set_code(StatusCode::FAILED);  // must do reset
    response->set_requestid("");             // must do reset
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "SetNetworkIsolationRequest",  // #3, #4
                                                           req2.SerializeAsString());
    EXPECT_AWAIT_TRUE([&]() -> bool {
        return response->code() == StatusCode::SUCCESS;
    });

    const auto ipsetIsolation = dstActor_->GetIpsetIpv4NetworkIsolation();
    EXPECT_EQ(ipsetIsolation->GetAllRules().size(), static_cast<long unsigned int>(0));  // #5
}

TEST_F(AgentServiceActorTest, RegisteredEvictedTest)
{
    messages::Registered registered;
    registered.set_code(static_cast<int32_t>(StatusCode::LS_AGENT_EVICTED));
    dstActor_->Registered(testFuncAgentMgrActor_->GetAID(), "Registered", registered.SerializeAsString());
}

TEST_F(AgentServiceActorTest, DeployInstanceWithCopyDeployer)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::COPY_STORAGE_TYPE);
    litebus::os::Mkdir(LOCAL_DEPLOY_DIR);
    spec->set_deploydir(LOCAL_DEPLOY_DIR);
    auto deployer = std::make_shared<function_agent::CopyDeployer>();
    deployer->SetBaseDeployDir("/tmp/copy");
    dstActor_->SetDeployers(function_agent::COPY_STORAGE_TYPE, deployer);

    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    // DeployInstance Request Send to Agent
    // 1. code is SUCCESS
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid() == TEST_REQUEST_ID; });
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::SUCCESS);
    auto destination = deployer->GetDestination("", "", LOCAL_DEPLOY_DIR);
    EXPECT_TRUE(litebus::os::ExistPath(destination));
    deployer->Clear(destination, "test");
    EXPECT_FALSE(litebus::os::ExistPath(destination));
    // code deployer with error
    destination = deployer->GetDestination("", "", "/tmp/home/local/test1");
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(0));
    deployInstanceReq->set_requestid("request123");
    deployInstanceReq->set_instanceid("inst123");
    spec->set_objectid("");
    spec->set_deploydir("/tmp/home/local/test1");
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid() == "request123"; });
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->code(), StatusCode::ERR_USER_CODE_LOAD);
    EXPECT_FALSE(litebus::os::ExistPath(destination));
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination), static_cast<uint32_t>(0));
}

TEST_F(AgentServiceActorTest, DeployMonopolyInstanceWithS3Deployer)
{
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    // add delegate code
    deployInstanceReq->mutable_createoptions()->insert(
        { "DELEGATE_DOWNLOAD",
          R"({"appId":"userCode", "bucketId":"testUserCodeBucketID", "objectId":"testUserCodeObjectID", "hostName":"xx", "securityToken":"xxx", "temporayAccessKey":"xxx", "temporarySecretKey":"xxx"})" });
    deployInstanceReq->mutable_createoptions()->insert({ "S3_DEPLOY_DIR","/tmp/home/test" });
    deployInstanceReq->mutable_scheduleoption()->set_schedpolicyname("monopoly");
    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    std::string destination = "/tmp/home/layer/func/" + TEST_BUCKET_ID + "/" + TEST_OBJECT_ID;
    litebus::os::Rmdir(destination);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid() == TEST_REQUEST_ID; });
    EXPECT_FALSE(litebus::os::ExistPath(destination));
}

TEST_F(AgentServiceActorTest, PythonRuntime_Support_WorkingDirFileZip_WithOut_EntryPoint)
{
    PrepareWorkingDir("/tmp/working_dir-tmp");
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);  // as appID
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    deployInstanceReq->set_language("/usr/bin/python3.9");
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::WORKING_DIR_STORAGE_TYPE);
    auto deployDir = "/tmp/home/sn/function/package/xxxz";
    std::string workingDirFile = "file:///tmp/working_dir-tmp/file.zip";
    auto destination = "/tmp/home/sn/function/package/xxxz/app/working_dir/" + CalculateFileMD5(workingDirFile.substr(7));
    (void)litebus::os::Rmdir(deployDir);
    spec->set_deploydir(deployDir);
    std::string optionDetail =
        "{\"appId\":\"userWorkingDirCode001\", \"storage_type\":\"working_dir\", \"code_path\":\"";
    optionDetail += workingDirFile;
    optionDetail += "\"}";
    deployInstanceReq->mutable_createoptions()->insert({ "DELEGATE_DOWNLOAD", optionDetail });
    deployInstanceReq->mutable_createoptions()->insert({ CONDA_CONFIG, "{'test_conda_config': 'confit_content'}" });
    deployInstanceReq->mutable_createoptions()->insert({ CONDA_COMMAND, "conda create -n test_env python=3.11" });
    std::string testCondaPrefix = "/tmp/conda";
    std::string testCondaDefaultEnv = "env_name_copy";
    deployInstanceReq->mutable_createoptions()->insert({ CONDA_PREFIX, testCondaPrefix });
    deployInstanceReq->mutable_createoptions()->insert({ CONDA_DEFAULT_ENV, testCondaDefaultEnv });
    std::string testVirtualenvKind = "venv";
    std::string testVirtualenvName = "testVenv";
    std::string testVirtualenvCommand = "pip install \"matplotlib\" \"msgpack-python==1.0.5\"";
    std::string testVirtualenvPath = "{\"site_package_path\":\"obs://mini-kernel/site-package.zip\"}";
    deployInstanceReq->mutable_createoptions()->insert({ VIRTUALENV_NAME, testVirtualenvName });
    deployInstanceReq->mutable_createoptions()->insert({ VIRTUALENV_COMMAND, testVirtualenvCommand });
    deployInstanceReq->mutable_createoptions()->insert({ VIRTUALENV_PATH, testVirtualenvPath });
    std::string testExecPath = "/home/sn/.yr/venv/testVenv/bin/python";
    deployInstanceReq->mutable_createoptions()->insert({ EXEC_PATH, testExecPath });
    deployInstanceReq->set_tenantid(TEST_TENANT_ID);
    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE(
        [&]() -> bool { return testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid() == TEST_REQUEST_ID; });
    EXPECT_TRUE(litebus::os::ExistPath(destination));  // app deployed

    auto startInstanceRequest = std::make_shared<messages::StartInstanceRequest>();
    startInstanceRequest->ParseFromString(testRuntimeManager_->promiseOfStartInstanceRequest.GetFuture().Get());
    YRLOG_DEBUG(startInstanceRequest->ShortDebugString());
    EXPECT_EQ(
        startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(UNZIPPED_WORKING_DIR)->second,
        destination);  // startInstance param posixenvs should contain UNZIPPED_WORKING_DIR
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(YR_WORKING_DIR)->second,
              workingDirFile);  // startInstance param posixenvs should contain YR_WORKING_DIR
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(YR_TENANT_ID)->second,
              TEST_TENANT_ID);
    auto iter = startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().end();
    EXPECT_TRUE(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().find(CONDA_CONFIG)
                != iter);
    EXPECT_TRUE(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().find(CONDA_COMMAND)
                != iter);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().find(CONDA_PREFIX)->second,
              testCondaPrefix);
    EXPECT_EQ(
        startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().find(CONDA_DEFAULT_ENV)->second,
        testCondaDefaultEnv);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(CONDA_PREFIX)->second,
              testCondaPrefix);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(CONDA_DEFAULT_ENV)->second,
              testCondaDefaultEnv);
    EXPECT_TRUE(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().find(VIRTUALENV_NAME)
                != iter);
    EXPECT_TRUE(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().find(VIRTUALENV_COMMAND)
                != iter);
    EXPECT_TRUE(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().find(VIRTUALENV_PATH)
                != iter);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().find(VIRTUALENV_NAME)->second,
              testVirtualenvName);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().find(VIRTUALENV_COMMAND)->second,
              testVirtualenvCommand);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().find(VIRTUALENV_PATH)->second,
              testVirtualenvPath);
    EXPECT_TRUE(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().find(EXEC_PATH)
                != iter);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().deploymentconfig().deployoptions().find(EXEC_PATH)->second,
              testExecPath);

    DestroyWorkingDir("/tmp/working_dir-tmp");
}

TEST_F(AgentServiceActorTest, AppDriver_Support_DeployInstanceWithWorkingDirDeployer_And_KillInstance)
{
    PrepareWorkingDir("/tmp/working_dir-tmp");
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID); // as appID
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    deployInstanceReq->set_language("posix-custom-runtime");
    auto appEntryPoint = "python script.py";
    deployInstanceReq->set_entryfile(appEntryPoint);  // app entrypoint set from proxy, For working_dir, the presence or
                                                      // absence of an entryfile is not used as a judgment criterion.
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::WORKING_DIR_STORAGE_TYPE);
    auto deployDir = "/tmp/home/sn/function/package/xxxz";
    std::string workingDirFile = "file:///tmp/working_dir-tmp/file.zip";
    auto destination = "/tmp/home/sn/function/package/xxxz/app/working_dir/" + CalculateFileMD5(workingDirFile.substr(7));
    (void)litebus::os::Rmdir(deployDir);
    spec->set_deploydir(deployDir);
    // add create options delegate code working_dir zip file
    deployInstanceReq->mutable_createoptions()->insert({APP_ENTRYPOINT, appEntryPoint});
    std::string optionDetail = "{\"appId\":\"userWorkingDirCode001\", \"storage_type\":\"working_dir\", \"code_path\":\"";
    optionDetail += workingDirFile;
    optionDetail += "\"}";
    deployInstanceReq->mutable_createoptions()->insert({"DELEGATE_DOWNLOAD", optionDetail});
    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid() == TEST_REQUEST_ID; });
    EXPECT_TRUE(litebus::os::ExistPath(destination)); // app deployed

    auto startInstanceRequest = std::make_shared<messages::StartInstanceRequest>();
    startInstanceRequest->ParseFromString(testRuntimeManager_->promiseOfStartInstanceRequest.GetFuture().Get());
    YRLOG_DEBUG(startInstanceRequest->ShortDebugString());
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().entryfile(), appEntryPoint);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(UNZIPPED_WORKING_DIR)->second,
        destination); // startInstance param posixenvs should contains UNZIPPED_WORKING_DIR
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(YR_WORKING_DIR)->second,
        workingDirFile); // startInstance param posixenvs should contains YR_WORKING_DIR
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(YR_APP_MODE)->second,
              "true");

    auto killInstanceReq = std::make_unique<messages::KillInstanceRequest>();
    killInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    killInstanceReq->set_requestid(TEST_REQUEST_ID);
    killInstanceReq->set_storagetype(function_agent::WORKING_DIR_STORAGE_TYPE);
    auto deployer = std::make_shared<function_agent::WorkingDirDeployer>();
    dstActor_->SetDeployers(function_agent::WORKING_DIR_STORAGE_TYPE, deployer);

    messages::StopInstanceResponse stopInstanceResponse;
    stopInstanceResponse.set_code(StatusCode::SUCCESS);
    stopInstanceResponse.set_requestid(TEST_REQUEST_ID);
    stopInstanceResponse.set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse).WillOnce(Return(stopInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool {
        return testRuntimeManager_->GetReceivedStopInstanceRequest() &&
               testFuncAgentMgrActor_->GetKillInstanceResponse()->code() == StatusCode::SUCCESS;
    });
    dstActor_->RemoveCodePackageAsync();
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(destination); }); // clean after app killed
    DestroyWorkingDir("/tmp/working_dir-tmp");
}

TEST_F(AgentServiceActorTest, MultiInstance_WithSameWorkingDirFileZip)
{
    PrepareWorkingDir("/tmp/working_dir-tmp");
    auto deployDir = "/tmp/home/sn/function/package/xxxz";
    std::string workingDirFile = "file:///tmp/working_dir-tmp/file.zip";
    auto destination = "/tmp/home/sn/function/package/xxxz/app/working_dir/" + CalculateFileMD5(workingDirFile.substr(7));
    for (int i = 0; i < 2; ++i) {
        auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
        deployInstanceReq->set_requestid(TEST_REQUEST_ID + std::to_string(i));
        deployInstanceReq->set_instanceid(TEST_INSTANCE_ID + std::to_string(i));
        deployInstanceReq->set_language("python3.9");
        auto spec = deployInstanceReq->mutable_funcdeployspec();
        spec->set_storagetype(function_agent::WORKING_DIR_STORAGE_TYPE);
        (void)litebus::os::Rmdir(deployDir);
        spec->set_deploydir(deployDir);
        std::string optionDetail =
            "{\"appId\":\"userWorkingDirCode001\", \"storage_type\":\"working_dir\", \"code_path\":\"";
        optionDetail += workingDirFile;
        optionDetail += "\"}";
        deployInstanceReq->mutable_createoptions()->insert({ "DELEGATE_DOWNLOAD", optionDetail });
        messages::StartInstanceResponse startInstanceResponse;
        startInstanceResponse.set_code(StatusCode::SUCCESS);
        startInstanceResponse.set_requestid(TEST_REQUEST_ID + std::to_string(i));
        startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
        EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
            .WillOnce(Return(startInstanceResponse.SerializeAsString()));

        testFuncAgentMgrActor_->ResetDeployInstanceResponse();
        testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "DeployInstance",
                                                               std::move(deployInstanceReq->SerializeAsString()));
        ASSERT_AWAIT_TRUE([&]() -> bool {
            return testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid()
                   == TEST_REQUEST_ID + std::to_string(i);
        });
    }

    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination) == 2; });

    // kill one instance
    auto killInstanceReq = std::make_unique<messages::KillInstanceRequest>();
    killInstanceReq->set_instanceid(TEST_INSTANCE_ID + std::to_string(0));
    killInstanceReq->set_requestid(TEST_REQUEST_ID + std::to_string(0));
    killInstanceReq->set_storagetype(function_agent::WORKING_DIR_STORAGE_TYPE);
    auto deployer = std::make_shared<function_agent::WorkingDirDeployer>();
    dstActor_->SetDeployers(function_agent::WORKING_DIR_STORAGE_TYPE, deployer);

    messages::StopInstanceResponse stopInstanceResponse;
    stopInstanceResponse.set_code(StatusCode::SUCCESS);
    stopInstanceResponse.set_requestid(TEST_REQUEST_ID + std::to_string(0));
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse)
        .WillOnce(Return(stopInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool {
        return testRuntimeManager_->GetReceivedStopInstanceRequest() &&
               testFuncAgentMgrActor_->GetKillInstanceResponse()->code() == StatusCode::SUCCESS;
    });

    messages::CleanStatusRequest cleanStatusRequest;
    cleanStatusRequest.set_name(TEST_AGENT_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "CleanStatus",
                                                           std::move(cleanStatusRequest.SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(destination); }); // clean after app killed
    // after clean
    ASSERT_AWAIT_TRUE([&]() -> bool { return JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination) == 0; });
    DestroyWorkingDir("/tmp/working_dir-tmp");
}

TEST_F(AgentServiceActorTest, MultiInstance_ModifiedWorkingDirFileZip)
{
    PrepareWorkingDir("/tmp/working_dir-tmp");
    auto deployDir = "/tmp/home/sn/function/package/xxxz";
    std::string workingDirFile = "file:///tmp/working_dir-tmp/file.zip";
    std::string destination;
    for (int i = 0; i < 2; ++i) {
        if (i == 1) {
            ModifyWorkingDir("/tmp/working_dir-tmp");
            destination = "/tmp/home/sn/function/package/xxxz/app/working_dir/" + CalculateFileMD5(workingDirFile.substr(7));
        }
        auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
        deployInstanceReq->set_requestid(TEST_REQUEST_ID + std::to_string(i));
        deployInstanceReq->set_instanceid(TEST_INSTANCE_ID + std::to_string(i));
        deployInstanceReq->set_language("python3.9");
        auto spec = deployInstanceReq->mutable_funcdeployspec();
        spec->set_storagetype(function_agent::WORKING_DIR_STORAGE_TYPE);
        (void)litebus::os::Rmdir(deployDir);
        spec->set_deploydir(deployDir);
        std::string optionDetail =
            "{\"appId\":\"userWorkingDirCode001\", \"storage_type\":\"working_dir\", \"code_path\":\"";
        optionDetail += workingDirFile;
        optionDetail += "\"}";
        deployInstanceReq->mutable_createoptions()->insert({ "DELEGATE_DOWNLOAD", optionDetail });
        messages::StartInstanceResponse startInstanceResponse;
        startInstanceResponse.set_code(StatusCode::SUCCESS);
        startInstanceResponse.set_requestid(TEST_REQUEST_ID + std::to_string(i));
        startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
        EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
            .WillOnce(Return(startInstanceResponse.SerializeAsString()));

        testFuncAgentMgrActor_->ResetDeployInstanceResponse();
        testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "DeployInstance",
                                                               std::move(deployInstanceReq->SerializeAsString()));
        ASSERT_AWAIT_TRUE([&]() -> bool {
            return testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid()
                   == TEST_REQUEST_ID + std::to_string(i);
        });
    }

    ASSERT_AWAIT_TRUE([&]() -> bool { return litebus::os::ExistPath(destination); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination) == 1; });

    // kill one instance
    auto killInstanceReq = std::make_unique<messages::KillInstanceRequest>();
    killInstanceReq->set_instanceid(TEST_INSTANCE_ID + std::to_string(0));
    killInstanceReq->set_requestid(TEST_REQUEST_ID + std::to_string(0));
    killInstanceReq->set_storagetype(function_agent::WORKING_DIR_STORAGE_TYPE);
    auto deployer = std::make_shared<function_agent::WorkingDirDeployer>();
    dstActor_->SetDeployers(function_agent::WORKING_DIR_STORAGE_TYPE, deployer);

    messages::StopInstanceResponse stopInstanceResponse;
    stopInstanceResponse.set_code(StatusCode::SUCCESS);
    stopInstanceResponse.set_requestid(TEST_REQUEST_ID + std::to_string(0));
    EXPECT_CALL(*testRuntimeManager_, MockStopInstanceResponse)
        .WillOnce(Return(stopInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "KillInstance",
                                                           std::move(killInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool {
        return testRuntimeManager_->GetReceivedStopInstanceRequest() &&
               testFuncAgentMgrActor_->GetKillInstanceResponse()->code() == StatusCode::SUCCESS;
    });

    messages::CleanStatusRequest cleanStatusRequest;
    cleanStatusRequest.set_name(TEST_AGENT_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "CleanStatus",
                                                           std::move(cleanStatusRequest.SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(destination); }); // clean after app killed
    // after clean
    ASSERT_AWAIT_TRUE([&]() -> bool { return JudgeCodeReferNum(dstActor_->GetCodeReferManager(), destination) == 0; });
    DestroyWorkingDir("/tmp/working_dir-tmp");
}

TEST_F(AgentServiceActorTest, DeployInstanceWithWorkingDirDeployer_Ray_Serve_Without_createOptions_APP_ENTRYPOINT)
{
    PrepareWorkingDir("/tmp/working_dir-tmp");
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID); // as appID
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    deployInstanceReq->set_language("posix-custom-runtime");
    auto appEntryPoint = "python script.py";
    deployInstanceReq->set_entryfile(appEntryPoint);  // app entrypoint set from proxy, For working_dir, the presence or
                                                      // absence of an entryfile is not used as a judgment criterion.
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::S3_STORAGE_TYPE);
    auto deployDir = "/tmp/home/sn/function/package/xxxz";
    std::string workingDirFile = "file:///tmp/working_dir-tmp/file.zip";
    auto destination = "/tmp/home/sn/function/package/xxxz/app/working_dir/" + CalculateFileMD5(workingDirFile.substr(7));
    (void)litebus::os::Rmdir(deployDir);
    spec->set_deploydir(deployDir);
    // add create options delegate code working_dir zip file, but without APP_ENTRYPOINT in createOptions
    std::string optionDetail = "{\"appId\":\"userWorkingDirCode001\", \"storage_type\":\"working_dir\", \"code_path\":\"";
    optionDetail += workingDirFile;
    optionDetail += "\"}";
    deployInstanceReq->mutable_createoptions()->insert({"DELEGATE_DOWNLOAD", optionDetail});
    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid() == TEST_REQUEST_ID; });
    EXPECT_TRUE(litebus::os::ExistPath(destination)); // app deployed

    auto startInstanceRequest = std::make_shared<messages::StartInstanceRequest>();
    startInstanceRequest->ParseFromString(testRuntimeManager_->promiseOfStartInstanceRequest.GetFuture().Get());
    YRLOG_DEBUG(startInstanceRequest->ShortDebugString());
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().entryfile(), appEntryPoint);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(UNZIPPED_WORKING_DIR)->second,
        destination); // startInstance param posixenvs should contains UNZIPPED_WORKING_DIR
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(YR_WORKING_DIR)->second,
        workingDirFile); // startInstance param posixenvs should contains YR_WORKING_DIR
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(YR_APP_MODE)->second,
              "false");
    DestroyWorkingDir("/tmp/working_dir-tmp");
}

TEST_F(AgentServiceActorTest, DeployInstanceWithWorkingDir_ErrorInputFile_Createoption_WorkingDirFile)
{
    PrepareWorkingDir("/tmp/working_dir-tmp");
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);  // as appID
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    deployInstanceReq->set_language("posix-custom-runtime");
    auto appEntryPoint = "python script.py";
    deployInstanceReq->set_entryfile(appEntryPoint);  // app entrypoint set from proxy
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::WORKING_DIR_STORAGE_TYPE);
    auto deployDir = "/tmp/home/sn/function/package/xxxz";
    auto destination = "/tmp/home/sn/function/package/xxxz/app/working_dir/" + TEST_INSTANCE_ID;
    (void)litebus::os::Rmdir(deployDir);
    EXPECT_TRUE(!litebus::os::ExistPath(destination));
    spec->set_deploydir(deployDir);
    deployInstanceReq->mutable_createoptions()->insert({ APP_ENTRYPOINT, appEntryPoint });
    // file is not zip file , it is dir
    deployInstanceReq->mutable_createoptions()->insert(
        { "DELEGATE_DOWNLOAD",
          R"({"appId":"userWorkingDirCode001", "storage_type":"working_dir", "code_path":"file:///tmp/working_dir-tmp/"})" });
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse).Times(0);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool {
        return (testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid() == TEST_REQUEST_ID
                && testFuncAgentMgrActor_->GetDeployInstanceResponse()->code()
                       == StatusCode::FUNC_AGENT_INVALID_WORKING_DIR_FILE);
    });
    EXPECT_TRUE(!litebus::os::ExistPath(destination));
    DestroyWorkingDir("/tmp/working_dir-tmp");
}

TEST_F(AgentServiceActorTest, DeployInstanceWithWorkingDir_ErrorInput_Createoption_WorkingDirFile)
{
    PrepareWorkingDir("/tmp/working_dir-tmp");
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);  // as appID
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    deployInstanceReq->set_language("posix-custom-runtime");
    auto appEntryPoint = "python script.py";
    deployInstanceReq->set_entryfile(appEntryPoint);  // app entrypoint set from proxy
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::WORKING_DIR_STORAGE_TYPE);
    auto deployDir = "/tmp/home/sn/function/package/xxxz";
    auto destination = "/tmp/home/sn/function/package/xxxz/app/working_dir/" + TEST_INSTANCE_ID;
    (void)litebus::os::Rmdir(deployDir);
    EXPECT_TRUE(!litebus::os::ExistPath(destination));
    spec->set_deploydir(deployDir);
    deployInstanceReq->mutable_createoptions()->insert({ APP_ENTRYPOINT, appEntryPoint });
    deployInstanceReq->mutable_createoptions()->insert(
        { "DELEGATE_DOWNLOAD",
          R"({"appId":"userWorkingDirCode001", "storage_type":"working_dir", "code_path":"ftp:///tmp/working_dir-tmp/file.zip"})" });  // error ftp
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse).Times(0);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool {
        return (testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid() == TEST_REQUEST_ID
                && testFuncAgentMgrActor_->GetDeployInstanceResponse()->code()
                       == StatusCode::FUNC_AGENT_UNSUPPORTED_WORKING_DIR_SCHEMA);
    });
    messages::CleanStatusRequest cleanStatusRequest;
    cleanStatusRequest.set_name(TEST_AGENT_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "CleanStatus",
                                                           std::move(cleanStatusRequest.SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return !litebus::os::ExistPath(destination); }); // app deploy error
    EXPECT_TRUE(!litebus::os::ExistPath(destination));
    DestroyWorkingDir("/tmp/working_dir-tmp");
}


TEST_F(AgentServiceActorTest, DeployInstanceWithWorkingDirCpp)
{
    PrepareWorkingDir("/tmp/working_dir-tmp");
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);  // as appID
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    deployInstanceReq->set_language("cpp11");

    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::WORKING_DIR_STORAGE_TYPE);
    auto deployDir = "/tmp/home/sn/function/package/xxxz";

    std::string destination = "/tmp/working_dir-tmp/";
    (void)litebus::os::Rmdir(deployDir);
    EXPECT_TRUE(litebus::os::ExistPath(destination));
    spec->set_deploydir(deployDir);

    // path
    deployInstanceReq->mutable_createoptions()->insert(
        { "DELEGATE_DOWNLOAD",
          R"({"appId":"userWorkingDirCode001", "storage_type":"working_dir", "code_path":"/tmp/working_dir-tmp/"})" });
    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE(
        [&]() -> bool { return testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid() == TEST_REQUEST_ID; });
    EXPECT_TRUE(litebus::os::ExistPath(destination));  // app deployed

    auto startInstanceRequest = std::make_shared<messages::StartInstanceRequest>();
    startInstanceRequest->ParseFromString(testRuntimeManager_->promiseOfStartInstanceRequest.GetFuture().Get());
    YRLOG_DEBUG(startInstanceRequest->ShortDebugString());
    EXPECT_EQ(
        startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(UNZIPPED_WORKING_DIR)->second,
        destination);  // startInstance param posixenvs should contain UNZIPPED_WORKING_DIR
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(YR_WORKING_DIR)->second,
              destination);  // startInstance param posixenvs should contain YR_WORKING_DIR
    DestroyWorkingDir("/tmp/working_dir-tmp");
}

TEST_F(AgentServiceActorTest, DeployInstanceWithNonExistWorkingDirCpp)
{
    PrepareWorkingDir("/tmp/working_dir-tmp");
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);  // as appID
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    deployInstanceReq->set_language("cpp11");

    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::WORKING_DIR_STORAGE_TYPE);
    auto deployDir = "/tmp/home/sn/function/package/xxxz";

    std::string destination = "/tmp/working_dir-tmp-fake/";
    (void)litebus::os::Rmdir(deployDir);
    (void)litebus::os::Rmdir(destination);
    EXPECT_FALSE(litebus::os::ExistPath(destination));
    spec->set_deploydir(deployDir);

    // path
    deployInstanceReq->mutable_createoptions()->insert(
        { "DELEGATE_DOWNLOAD",
          R"({"appId":"userWorkingDirCode001", "storage_type":"working_dir", "code_path":"/tmp/working_dir-tmp-fake/"})" });
    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_code(StatusCode::SUCCESS);
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    startInstanceResponse.mutable_startruntimeinstanceresponse()->set_runtimeid(TEST_RUNTIME_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));

    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(),
                                                           "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE(
        [&]() -> bool { return testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid() == TEST_REQUEST_ID; });
    EXPECT_TRUE(litebus::os::ExistPath(destination));  // app deployed

    auto startInstanceRequest = std::make_shared<messages::StartInstanceRequest>();
    startInstanceRequest->ParseFromString(testRuntimeManager_->promiseOfStartInstanceRequest.GetFuture().Get());
    YRLOG_DEBUG(startInstanceRequest->ShortDebugString());
    EXPECT_EQ(
        startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(UNZIPPED_WORKING_DIR)->second,
        destination);  // startInstance param posixenvs should contain UNZIPPED_WORKING_DIR
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().runtimeconfig().posixenvs().find(YR_WORKING_DIR)->second,
              destination);  // startInstance param posixenvs should contain YR_WORKING_DIR
    EXPECT_TRUE(litebus::os::ExistPath(destination));
    DestroyWorkingDir("/tmp/working_dir-tmp");
    (void)litebus::os::Rmdir(destination);
}

TEST_F(AgentServiceActorTest, SendS3Alarm)
{
    metrics::MetricsAdapter::GetInstance().enabledInstruments_.insert(metrics::YRInstrument::YR_S3_ALARM);

    auto request = std::make_shared<messages::DeployRequest>();
    auto promise = std::make_shared<litebus::Promise<DeployResult>>();

    auto s3Config = std::make_shared<S3Config>();
    messages::CodePackageThresholds codePackageThresholds;
    auto mockTestDeployer = std::make_shared<MockTestAgentS3Deployer>(s3Config, codePackageThresholds);
    dstActor_->SetDeployers(function_agent::S3_STORAGE_TYPE, mockTestDeployer);
    dstActor_->retryDownloadInterval_ = 50;
    EXPECT_CALL(*mockTestDeployer, Deploy)
        .WillRepeatedly(Return(DeployResult{ .status = Status(StatusCode::FUNC_AGENT_OBS_ERROR_NEED_RETRY) }));
    dstActor_->DownloadCode(request, mockTestDeployer, promise, 1);
    ASSERT_AWAIT_TRUE([&]() -> bool {
        promise->GetFuture().Get();
        auto alarmMap = metrics::MetricsAdapter::GetInstance().GetAlarmHandler().GetAlarmMap();
        return alarmMap.find(metrics::S3_ALARM) != alarmMap.end();
    });
}

TEST_F(AgentServiceActorTest, ConfigCodeAgingTimeTest)
{
    dstActor_->codePackageThresholds_.set_codeagingtime(10);
    dstActor_->codeReferInfos_->clear();
    dstActor_->AddCodeRefer("/tmp/test1", "testIns001", dstActor_->deployers_[function_agent::S3_STORAGE_TYPE]);
    dstActor_->AddCodeRefer("/tmp/test2", "testIns002", dstActor_->deployers_[function_agent::S3_STORAGE_TYPE]);
    dstActor_->AddCodeRefer("/tmp/test2", "testIns003", dstActor_->deployers_[function_agent::S3_STORAGE_TYPE]);
    dstActor_->AddCodeRefer("/tmp/test3", "testIns004", dstActor_->deployers_[function_agent::S3_STORAGE_TYPE]);
    dstActor_->DeleteFunction("/tmp/test2", "testIns003");
    dstActor_->DeleteFunction("/tmp/test3", "testIns004");
    (*dstActor_->codeReferInfos_)["/tmp/test1"].lastAccessTimestamp = 1700000;
    (*dstActor_->codeReferInfos_)["/tmp/test3"].lastAccessTimestamp = 1700000;
    dstActor_->RemoveCodePackageAsync();
    EXPECT_TRUE(dstActor_->codeReferInfos_->find("/tmp/test1") != dstActor_->codeReferInfos_->end());
    EXPECT_TRUE(dstActor_->codeReferInfos_->find("/tmp/test2") != dstActor_->codeReferInfos_->end());
    EXPECT_TRUE(dstActor_->codeReferInfos_->find("/tmp/test3") == dstActor_->codeReferInfos_->end());
}

const std::string STATIC_FUNCTION_META_JSON = R"delim({"funcMetaData":{"layers":[],"name":"0@functest@functest","description":"empty function","functionUrn":"sn:cn:yrk:default:function:0@functest@functest","reversedConcurrency":0,"tenantId":"default","tags":null,"functionUpdateTime":"","functionVersionUrn":"sn:cn:yrk:default:function:0@functest@functest:latest","revisionId":"20250410134701376","codeSize":2911,"codeSha512":"9508f59eb7231bb7577ecc123c5c27317e6563a434dd83d33501f55dfe76b80b58228da0d8f6e37f6cf6d54ccd6dba0eb506bdc10a6acc2ee43e7aa83028b8e3","handler":"handler.my_handler","runtime":"python3.9","timeout":900,"version":"latest","versionDescription":"latest","deadLetterConfig":"","businessId":"yrk","functionType":"","func_id":"","func_name":"functest","domain_id":"","project_name":"","service":"functest","dependencies":"","enable_cloud_debug":"","isStatefulFunction":false,"isBridgeFunction":false,"isStreamEnable":false,"type":"","created":"2025-04-09 08:58:48.655 UTC","enable_auth_in_header":false,"dns_domain_cfg":null,"vpcTriggerImage":""},"codeMetaData":{"codeUploadType":"s3","sha512":"9508f59eb7231bb7577ecc123c5c27317e6563a434dd83d33501f55dfe76b80b58228da0d8f6e37f6cf6d54ccd6dba0eb506bdc10a6acc2ee43e7aa83028b8e3","storage_type":"s3","code_path":"","appId":"61022","bucketId":"bucket-test-log1","objectId":"0@functest@functest-1744292821408","bucketUrl":"http://bucket-test-log1.hwcloudtest.cn:18085","code_type":"","code_url":"","code_filename":"","func_code":{"file":"","link":""}},"envMetaData":{"envKey":"abca007b01f5b1f1fc2e9b3a:86c55b48dc81cac2813f080bd214ea83bf8d79430892c54fdbcd353b29daf165d342943a45411b4b83b01cce27b3a0a51a22d09b6fee1fd729872e5e46893640d5bd870e1051eeb7c90e7e31df5af1ee","environment":"757be5c1d3725bedf25692ea:e4a8c7eeb322fcc5397a498ca9bd83f45597","encrypted_user_data":"","cryptoAlgorithm":"GCM"},"resourceMetaData":{"cpu":600,"memory":512,"gpu_memory":0,"enable_dynamic_memory":false,"customResources":"","enable_tmp_expansion":false,"ephemeral_storage":0},"instanceMetaData":{"maxInstance":100,"minInstance":0,"concurrentNum":100,"instanceType":"","idleMode":false,"poolLabel":"","poolId":"","scalePolicy":"staticFunction"},"extendedMetaData":{"image_name":"","role":{"xrole":"","app_xrole":""},"func_vpc":null,"endpoint_tenant_vpc":null,"mount_config":null,"strategy_config":{"concurrency":0},"extend_config":"","initializer":{"initializer_handler":"handler.init","initializer_timeout":30},"pre_stop":{"pre_stop_handler":"","pre_stop_timeout":0},"heartbeat":{"heartbeat_handler":""},"enterprise_project_id":"","log_tank_service":{"logGroupId":"","logStreamId":""},"tracing_config":{"tracing_ak":"","tracing_sk":"","project_name":""},"custom_container_config":{"control_path":"","image":"","command":null,"args":null,"working_dir":"","uid":0,"gid":0},"async_config_loaded":false,"restore_hook":{},"network_controller":{"disable_public_network":false,"trigger_access_vpcs":null},"user_agency":{"accessKey":"","secretKey":"","token":""}}})delim";

TEST_F(AgentServiceActorTest, CreateStaticFunctionInstanceTest)
{
    const std::string user_dir = "/tmp/home/static-function-test";

    auto res = dstActor_->CreateStaticFunctionInstance();
    EXPECT_EQ(res.Get().StatusCode(), -1);  // env not exist

    litebus::os::SetEnv("SF_CONFIG_PATH", user_dir + "/functionMeta.json");

    litebus::os::SetEnv("SF_SCHEDULE_TIMEOUT_MS", "300");
    litebus::os::SetEnv("SF_INSTANCE_TYPE_NOTE", "reserved");
    litebus::os::SetEnv("SF_DELEGATE_DIRECTORY_INFO", "/tmp");
    litebus::os::SetEnv("SF_INVOKE_LABELS", "");
    litebus::os::SetEnv("SF_FUNCTION_SIGNATURE", "20250822095005");

    litebus::os::SetEnv("POD_DEPLOYMENT_NAME", "pod-deployment-name");
    litebus::os::SetEnv("POD_NAMESPACE", "pod-namespace");
    litebus::os::SetEnv("POD_NAME", "pod-name");

    (void)litebus::os::Rmdir(user_dir);

    res = dstActor_->CreateStaticFunctionInstance();
    EXPECT_EQ(res.Get().StatusCode(), -1);  // function meta file not exist

    (void)litebus::os::Mkdir(user_dir);  // why /tmp/home/test/config/ ?

    Write(user_dir + "/functionMeta.json", "");
    res = dstActor_->CreateStaticFunctionInstance();
    EXPECT_EQ(res.Get().StatusCode(), -1);  // function meta file is empty


    Write(user_dir + "/functionMeta.json", STATIC_FUNCTION_META_JSON);
    bool isFinished = false;
    EXPECT_CALL(*testFuncAgentMgrActor_, MockStaticFunctionScheduleResponse)
        .WillOnce(testing::DoAll(testing::Assign(&isFinished, true), testing::Return("")));
    dstActor_->retryScheduleInterval_ = 50;

    res = dstActor_->CreateStaticFunctionInstance();
    EXPECT_EQ(res.IsOK(), true);

    ASSERT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetReceivedScheduleRequest(); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return isFinished; });
    auto req = testFuncAgentMgrActor_->GetScheduleRequest();
    ASSERT_NE(req, nullptr);
    EXPECT_NE(req->instance().scheduleoption().resourceselector().find(RESOURCE_OWNER_KEY),
              req->instance().scheduleoption().resourceselector().end());
    EXPECT_EQ(req->instance().scheduleoption().resourceselector().find(RESOURCE_OWNER_KEY)->second, TEST_AGENT_ID);

    testFuncAgentMgrActor_->ResetReceivedScheduleRequest();
    ::messages::ScheduleResponse response;
    response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    response.set_requestid(testFuncAgentMgrActor_->GetScheduleRequest()->requestid());
    response.set_instanceid("instance1");
    EXPECT_CALL(*testFuncAgentMgrActor_, MockStaticFunctionScheduleResponse)
        .WillRepeatedly(Return(response.SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetReceivedScheduleRequest(); });
    ASSERT_AWAIT_TRUE([&]() -> bool { return dstActor_->scheduleResponsePromise_->GetFuture().IsOK(); });

    litebus::os::Rmdir(user_dir);
}

TEST_F(AgentServiceActorTest, NotifyFunctionStatusTest)
{
    auto staticFunctionChangeRequest = std::make_shared<messages::StaticFunctionChangeRequest>();
    std::string instanceID = "Test-InstanceID";
    staticFunctionChangeRequest->set_instanceid(instanceID);
    staticFunctionChangeRequest->set_status(static_cast<int32_t>(InstanceState::RUNNING));

    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "NotifyFunctionStatusChange",
                                                           staticFunctionChangeRequest->SerializeAsString());

    ASSERT_AWAIT_TRUE([&]() -> bool {
        return dstActor_->instanceHealthyMap_.find(instanceID) != dstActor_->instanceHealthyMap_.end();
    });
    EXPECT_TRUE(dstActor_->instanceHealthyMap_[instanceID] == static_cast<int32_t>(InstanceState::RUNNING));

    staticFunctionChangeRequest->set_status(static_cast<int32_t>(InstanceState::EXITING));
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "NotifyFunctionStatusChange",
                                                           staticFunctionChangeRequest->SerializeAsString());
    ASSERT_AWAIT_TRUE([&]() -> bool {
        return dstActor_->instanceHealthyMap_.find(instanceID) == dstActor_->instanceHealthyMap_.end();
    });

    staticFunctionChangeRequest->set_status(static_cast<int32_t>(InstanceState::FATAL));
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "NotifyFunctionStatusChange",
                                                           staticFunctionChangeRequest->SerializeAsString());
    ASSERT_AWAIT_TRUE([&]() -> bool {
        return dstActor_->instanceHealthyMap_.find(instanceID) == dstActor_->instanceHealthyMap_.end();
    });
}

TEST_F(AgentServiceActorTest, CheckReadinessTest)
{
    EXPECT_TRUE(dstActor_->IsAgentReadiness().IsError());

    dstActor_->instanceHealthyMap_["xx"] = static_cast<int32_t>(InstanceState::CREATING);
    EXPECT_TRUE(dstActor_->IsAgentReadiness().IsError());

    dstActor_->instanceHealthyMap_["xx"] = static_cast<int32_t>(InstanceState::RUNNING);
    const auto future = dstActor_->IsAgentReadiness();  // SetValue
    EXPECT_TRUE(future.IsOK() && future.Get().IsOk());
}

TEST_F(AgentServiceActorTest, DISABLED_DeployInstanceWithSharedDir)
{
    std::string sharedDir = "test_abc";
    std::string dest = "/dcache/shared/" + sharedDir;
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    deployInstanceReq->mutable_createoptions()->insert({ "DELEGATE_SHARED_DIRECTORY", sharedDir });
    deployInstanceReq->mutable_createoptions()->insert({ "DELEGATE_SHARED_DIRECTORY_TTL", "9223372036854775808" });
    litebus::os::Rmdir(dest);
    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    EXPECT_CALL(*testRuntimeManager_, MockStartInstanceResponse)
        .WillOnce(Return(startInstanceResponse.SerializeAsString()));
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE_FOR([&]() -> bool { return litebus::os::ExistPath(dest); }, 5000);
    EXPECT_EQ(dstActor_->deployers_[SHARED_DIR_STORAGE_TYPE]->GetTTL(dest), std::numeric_limits<int>::max());
    EXPECT_EQ(JudgeCodeReferNum(dstActor_->GetCodeReferManager(), dest), static_cast<uint32_t>(1));
    EXPECT_EQ(testFuncAgentMgrActor_->GetDeployInstanceResponse()->requestid(), TEST_REQUEST_ID);
    testFuncAgentMgrActor_->ResetDeployInstanceResponse();
    EXPECT_TRUE(litebus::os::Rmdir(dest).IsNone());
}

TEST_F(AgentServiceActorTest, DeployInstanceWithSharedDirAndInvalidTTL)
{
    std::string sharedDir = "test_abc";
    std::string dest = "/dcache/shared/" + sharedDir;
    auto deployInstanceReq = std::make_unique<messages::DeployInstanceRequest>();
    deployInstanceReq->set_requestid(TEST_REQUEST_ID);
    deployInstanceReq->set_instanceid(TEST_INSTANCE_ID);
    auto spec = deployInstanceReq->mutable_funcdeployspec();
    spec->set_storagetype(function_agent::LOCAL_STORAGE_TYPE);
    deployInstanceReq->mutable_createoptions()->insert({ "DELEGATE_SHARED_DIRECTORY", sharedDir });
    deployInstanceReq->mutable_createoptions()->insert({ "DELEGATE_SHARED_DIRECTORY_TTL", "abc" });
    messages::StartInstanceResponse startInstanceResponse;
    startInstanceResponse.set_requestid(TEST_REQUEST_ID);
    testFuncAgentMgrActor_->SendRequestToAgentServiceActor(dstActor_->GetAID(), "DeployInstance",
                                                           std::move(deployInstanceReq->SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() -> bool { return testFuncAgentMgrActor_->GetDeployInstanceResponse()->code() == 70052; });
}

TEST_F(AgentServiceActorTest, LoadConfigPkgThresholdsCfgTest)
{
    messages::CodePackageThresholds defaultThresholds;
    defaultThresholds.set_filecountsmax(30000);
    defaultThresholds.set_zipfilesizemaxmb(300);
    defaultThresholds.set_unzipfilesizemaxmb(600);
    defaultThresholds.set_dirdepthmax(10);
    auto thresholdConfigPath = "/tmp/home/download/config";
    dstActor_->codePkgThresholdsCfgPath_ = thresholdConfigPath;
    {
        dstActor_->codePackageThresholds_.Clear();
        std::string content = R"({"file_count_max":"20000","zip_file_size_max_MB":1000,
            "unzip_file_size_max_MB":2000,"dir_depth_max":50})";
        GenCodePkgThresholdsCfgFile(thresholdConfigPath, content);
        dstActor_->LoadCodePkgThresholdsCfg();
        EXPECT_EQ(dstActor_->codePackageThresholds_.filecountsmax(), 20000);
        EXPECT_EQ(dstActor_->codePackageThresholds_.zipfilesizemaxmb(), 1000);
        EXPECT_EQ(dstActor_->codePackageThresholds_.unzipfilesizemaxmb(), 2000);
        EXPECT_EQ(dstActor_->codePackageThresholds_.dirdepthmax(), 50);
        auto s3Deployer = std::dynamic_pointer_cast<S3Deployer>(dstActor_->deployers_["s3"]);
        EXPECT_EQ(s3Deployer->codePackageThresholds_.filecountsmax(), 20000);
        EXPECT_EQ(s3Deployer->codePackageThresholds_.zipfilesizemaxmb(), 1000);
        EXPECT_EQ(s3Deployer->codePackageThresholds_.unzipfilesizemaxmb(), 2000);
        EXPECT_EQ(s3Deployer->codePackageThresholds_.dirdepthmax(), 50);
    }
    {
        dstActor_->codePackageThresholds_.CopyFrom(defaultThresholds);
        litebus::os::Rm(litebus::os::Join(thresholdConfigPath, "code-package-thresholds.json"));
        dstActor_->LoadCodePkgThresholdsCfg();
        EXPECT_EQ(dstActor_->codePackageThresholds_.filecountsmax(), 30000);
        EXPECT_EQ(dstActor_->codePackageThresholds_.zipfilesizemaxmb(), 300);
        EXPECT_EQ(dstActor_->codePackageThresholds_.unzipfilesizemaxmb(), 600);
        EXPECT_EQ(dstActor_->codePackageThresholds_.dirdepthmax(), 10);
    }
    {
        dstActor_->codePackageThresholds_.CopyFrom(defaultThresholds);
        std::string content = R"({})";
        GenCodePkgThresholdsCfgFile(thresholdConfigPath, content);
        dstActor_->LoadCodePkgThresholdsCfg();
        EXPECT_EQ(dstActor_->codePackageThresholds_.filecountsmax(), 30000);
        EXPECT_EQ(dstActor_->codePackageThresholds_.zipfilesizemaxmb(), 300);
        EXPECT_EQ(dstActor_->codePackageThresholds_.unzipfilesizemaxmb(), 600);
        EXPECT_EQ(dstActor_->codePackageThresholds_.dirdepthmax(), 10);
    }
    {
        dstActor_->codePackageThresholds_.CopyFrom(defaultThresholds);
        std::string content = R"({"file_count_max":"0","zip_file_size_max_MB":true,
            "unzip_file_size_max_MB":2.2,"dir_depth_max":500})";
        GenCodePkgThresholdsCfgFile(thresholdConfigPath, content);
        dstActor_->LoadCodePkgThresholdsCfg();
        EXPECT_EQ(dstActor_->codePackageThresholds_.filecountsmax(), 30000);
        EXPECT_EQ(dstActor_->codePackageThresholds_.zipfilesizemaxmb(), 300);
        EXPECT_EQ(dstActor_->codePackageThresholds_.unzipfilesizemaxmb(), 600);
        EXPECT_EQ(dstActor_->codePackageThresholds_.dirdepthmax(), 10);
    }
    {
        dstActor_->codePackageThresholds_.Clear();
        dstActor_->codePackageThresholds_.set_filecountsmax(30000);
        dstActor_->codePackageThresholds_.set_dirdepthmax(50);
        std::string content = R"({"file_count_max":"20000","zip_file_size_max_MB":1000,"unzip_file_size_max_MB":2000})";
        GenCodePkgThresholdsCfgFile(thresholdConfigPath, content);
        dstActor_->CodePkgThresholdsCfgCallback(thresholdConfigPath, "..2026_02_11_03_37_09.275559235",
            uint32_t{1073742336});
        std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        EXPECT_EQ(dstActor_->codePackageThresholds_.filecountsmax(), 20000);
        EXPECT_EQ(dstActor_->codePackageThresholds_.zipfilesizemaxmb(), 1000);
        EXPECT_EQ(dstActor_->codePackageThresholds_.unzipfilesizemaxmb(), 2000);
        EXPECT_EQ(dstActor_->codePackageThresholds_.dirdepthmax(), 50);
    }
}
}  // namespace functionsystem::test
