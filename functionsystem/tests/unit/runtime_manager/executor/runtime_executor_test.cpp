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

#include "runtime_manager/executor/runtime_executor.h"

#include <gmock/gmock.h>
#include <yaml-cpp/yaml.h>

#include <cstdio>
#include <fstream>

#include "common/constants/constants.h"
#include "common/status/status.h"
#include "common/utils/exec_utils.h"
#include "common/utils/files.h"
#include "common/utils/path.h"
#include "gtest/gtest.h"
#include "mocks/mock_cmdtool.h"
#include "port/port_manager.h"
#include "runtime_manager/healthcheck/health_check.h"
#include "runtime_manager/metrics/mock_function_agent_actor.h"
#include "runtime_manager/utils/std_redirector.h"
#include "utils/future_test_helper.h"
#include "utils/os_utils.hpp"
#include "utils/port_helper.h"

namespace functionsystem::runtime_manager {
using namespace functionsystem::test;
const int INITIAL_PORT = 1000;
const int PORT_NUM = 1000;
const std::string testDeployDir = "/tmp/layer/func/bucket-test-log1/yr-test-runtime-executor";
const std::string funcObj = testDeployDir + "/" + "funcObj";
const std::string TEST_TENANT_ID = "tenant001";
const std::string TEST_RUNTIME_METRICS_CONFIG_FILE = "/tmp/runtime_executor_metrics_config.json";
const std::string TEST_METRICS_ROOT_CERT_FILE = "/tmp/runtime_executor_metrics_root_cert.pem";
const std::string TEST_METRICS_CERT_FILE = "/tmp/runtime_executor_metrics_cert.pem";
const std::string TEST_METRICS_KEY_FILE = "/tmp/runtime_executor_metrics_key.pem";
const std::vector<std::string> condaEnvCreateResult{
    "Channels:",
    " - conda-forge",
    " - defaults",
    "Platform: linux-64",
    "Collecting package metadata (repodata.json): done",
    "Solving environment: done",
    "",
    "Downloading and Extracting Packages:",
    "",
    "Preparing transaction: done",
    "Verifying transaction: done",
    "Executing transaction: done",
    "#",
    "# To activate this environment, use",
    "#",
    "#     $ conda activate env_name_file",
    "#",
    "# To deactivate an active environment, use",
    "#",
    "#     $ conda deactivate",
};

const std::string testCondaConfig = R"(
{
    "name": "env_name_file",
    "channels": [
        "conda-forge",
        "defaults"
    ],
    "dependencies": [
        "_libgcc_mutex=0.1=main",
        "_openmp_mutex=5.1=1_gnu",
        "bzip2=1.0.8=h5eee18b_6",
        "ca-certificates=2025.2.25=h06a4308_0",
        "ld_impl_linux-64=2.40=h12ee557_0",
        "libffi=3.4.4=h6a678d5_1",
        "libgcc-ng=11.2.0=h1234567_1",
        "libgomp=11.2.0=h1234567_1",
        "libstdcxx-ng=11.2.0=h1234567_1",
        "libuuid=1.41.5=h5eee18b_0",
        "ncurses=6.4=h6a678d5_0",
        "openssl=3.0.16=h5eee18b_0",
        "pip=25.0=py310h06a4308_0",
        "python=3.10.16=he870216_1",
        "readline=8.2=h5eee18b_0",
        "setuptools=75.8.0=py310h06a4308_0",
        "sqlite=3.45.3=h5eee18b_0",
        "tk=8.6.14=h39e8969_0",
        "tzdata=2025a=h04d1e81_0",
        "wheel=0.45.1=py310h06a4308_0",
        "xz=5.6.4=h5eee18b_1",
        "zlib=1.2.13=h5eee18b_1"
    ],
    "prefix": "/usr/local/conda/envs/env_name_file"
}
)";

std::shared_ptr<messages::StartInstanceRequest> BuildStartInstanceRequest(const std::string &language);

class MockRuntimeExecutor : public RuntimeExecutor {
public:
    explicit MockRuntimeExecutor(const std::string &name, const litebus::AID &functionAgentAID) : RuntimeExecutor(name, functionAgentAID)
    {
    }

    void SetDisConvCallStack(bool enable)
    {
        config_.enableDisConvCallStack = enable;
    }
};

class RuntimeExecutorTest : public ::testing::Test {
public:
    [[maybe_unused]] static void SetUpTestSuite()
    {
        if (!litebus::os::ExistPath("/tmp/cpp/bin")) {
            litebus::os::Mkdir("/tmp/cpp/bin");
        }

        auto fd = open("/tmp/cpp/bin/runtime", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
        EXPECT_NE(fd, -1);
        close(fd);

        std::ofstream outfile;
        outfile.open("/tmp/cpp/bin/runtime");
        outfile << "sleep 2" << std::endl;
        outfile.close();
    }

    void SetUp() override
    {
        PortManager::GetInstance().InitPortResource(INITIAL_PORT, PORT_NUM);
        (void)litebus::os::Mkdir(testDeployDir);
        (void)TouchFile(funcObj);
        system("echo \"testDeployDir in runtime_executor_test\""
            "> /tmp/layer/func/bucket-test-log1/yr-test-runtime-executor/funcObj");
        mockAgent_ = std::make_shared<runtime_manager::test::MockFunctionAgentActor>();
        litebus::Spawn(mockAgent_);
        executor_ = std::make_shared<MockRuntimeExecutor>("RuntimeExecutorTestActor", mockAgent_->GetAID());
        litebus::Spawn(executor_);
        litebus::os::SetEnv("YR_BARE_MENTAL", "1");
        litebus::os::UnSetEnv("RUNTIME_METRICS_CONFIG");
        litebus::os::UnSetEnv("RUNTIME_METRICS_CONFIG_FILE");

        // memory origin env
        auto optionEnv = litebus::os::GetEnv("PATH");
        std::string env;
        if (optionEnv.IsSome()) {
            env_ = optionEnv.Get();
        }
    }

    void TearDown() override
    {
        (void)litebus::os::Rmdir(testDeployDir);
        litebus::Terminate(executor_->GetAID());
        litebus::Terminate(mockAgent_->GetAID());
        litebus::Await(executor_->GetAID());
        litebus::os::SetEnv("PATH", env_);
        litebus::os::UnSetEnv("RUNTIME_METRICS_CONFIG");
        litebus::os::UnSetEnv("RUNTIME_METRICS_CONFIG_FILE");
        (void)std::remove(TEST_RUNTIME_METRICS_CONFIG_FILE.c_str());
        (void)std::remove(TEST_METRICS_ROOT_CERT_FILE.c_str());
        (void)std::remove(TEST_METRICS_CERT_FILE.c_str());
        (void)std::remove(TEST_METRICS_KEY_FILE.c_str());
        pidArray.clear();
    }

    void RecordRuntimePID(pid_t pid) {
        pidArray.push_back(pid);
    }

    void ClearRuntimePID() {
        for(pid_t pid: pidArray) {
            executor_->UpdatePrestartRuntimePromise(pid);
        }
        pidArray.clear();
    }

    int CheckPrestartRuntimePromise()
    {
        int cnt = 0;
        for (auto iter = executor_->prestartRuntimePromiseMap_.begin();
             iter != executor_->prestartRuntimePromiseMap_.end(); ++iter) {
            if (iter->second->GetFuture().IsError() || iter->second->GetFuture().IsOK()) {
                cnt++;
            } else {
                return 0;
            }
        }
        return cnt;
    }

protected:
    std::shared_ptr<MockRuntimeExecutor> executor_;
    std::shared_ptr<runtime_manager::test::MockFunctionAgentActor> mockAgent_;
    std::string env_;
    std::vector<pid_t> pidArray;
};

TEST_F(RuntimeExecutorTest, FillMetricsTLSConfigFromEnvCertData)
{
    litebus::os::SetEnv("RUNTIME_METRICS_CONFIG", R"({
        "backends": [{
            "immediatelyExport": {
                "exporters": [{
                    "prometheusPullExporter": {
                        "enable": true,
                        "initConfig": {
                            "isSSLEnable": true,
                            "rootCertData": "root-data",
                            "certData": "cert-data",
                            "keyData": "key-data"
                        }
                    }
                }]
            }
        }]
    })");

    ::messages::TLSConfig tlsConfig;
    auto status = executor_->ProtectedFillMetricsTLSConfig(tlsConfig);

    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(tlsConfig.metricsrootcertdata(), "root-data");
    EXPECT_EQ(tlsConfig.metricscertdata(), "cert-data");
    EXPECT_EQ(tlsConfig.metricskeydata(), "key-data");
}

TEST_F(RuntimeExecutorTest, FillMetricsTLSConfigClearsIncompleteCertData)
{
    litebus::os::SetEnv("RUNTIME_METRICS_CONFIG", R"({
        "backends": [{
            "immediatelyExport": {
                "exporters": [{
                    "prometheusPullExporter": {
                        "enable": true,
                        "initConfig": {
                            "isSSLEnable": true,
                            "certData": "cert-data"
                        }
                    }
                }]
            }
        }]
    })");

    ::messages::TLSConfig tlsConfig;
    auto status = executor_->ProtectedFillMetricsTLSConfig(tlsConfig);

    EXPECT_TRUE(status.IsOk());
    EXPECT_TRUE(tlsConfig.metricsrootcertdata().empty());
    EXPECT_TRUE(tlsConfig.metricscertdata().empty());
    EXPECT_TRUE(tlsConfig.metricskeydata().empty());
}

TEST_F(RuntimeExecutorTest, FillMetricsTLSConfigIgnoresDisabledSsl)
{
    litebus::os::SetEnv("RUNTIME_METRICS_CONFIG", R"({
        "backends": [{
            "immediatelyExport": {
                "exporters": [{
                    "prometheusPullExporter": {
                        "enable": true,
                        "initConfig": {
                            "isSSLEnable": false,
                            "rootCertData": "root-data",
                            "certData": "cert-data",
                            "keyData": "key-data"
                        }
                    }
                }]
            }
        }]
    })");

    ::messages::TLSConfig tlsConfig;
    auto status = executor_->ProtectedFillMetricsTLSConfig(tlsConfig);

    EXPECT_TRUE(status.IsOk());
    EXPECT_TRUE(tlsConfig.metricsrootcertdata().empty());
    EXPECT_TRUE(tlsConfig.metricscertdata().empty());
    EXPECT_TRUE(tlsConfig.metricskeydata().empty());
}

TEST_F(RuntimeExecutorTest, FillMetricsTLSConfigFromConfigFileCertFiles)
{
    {
        std::ofstream rootCert(TEST_METRICS_ROOT_CERT_FILE);
        rootCert << "root";
    }
    {
        std::ofstream cert(TEST_METRICS_CERT_FILE);
        cert << "cert";
    }
    {
        std::ofstream key(TEST_METRICS_KEY_FILE);
        key << "key";
    }
    {
        std::ofstream config(TEST_RUNTIME_METRICS_CONFIG_FILE);
        config << R"({
            "backends": [{
                "immediatelyExport": {
                    "exporters": [{
                        "prometheusPullExporter": {
                            "enable": true,
                            "initConfig": {
                                "isSSLEnable": true,
                                "rootCertFile": ")" << TEST_METRICS_ROOT_CERT_FILE << R"(",
                                "certFile": ")" << TEST_METRICS_CERT_FILE << R"(",
                                "keyFile": ")" << TEST_METRICS_KEY_FILE << R"("
                            }
                        }
                    }]
                }
            }]
        })";
    }
    litebus::os::SetEnv("RUNTIME_METRICS_CONFIG_FILE", TEST_RUNTIME_METRICS_CONFIG_FILE);

    ::messages::TLSConfig tlsConfig;
    auto status = executor_->ProtectedFillMetricsTLSConfig(tlsConfig);

    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(tlsConfig.metricsrootcertdata(), "cm9vdA==");
    EXPECT_EQ(tlsConfig.metricscertdata(), "Y2VydA==");
    EXPECT_EQ(tlsConfig.metricskeydata(), "a2V5");
}

TEST_F(RuntimeExecutorTest, FillMetricsTLSConfigFromStsP12DefaultReturnsOk)
{
    ::messages::TLSConfig tlsConfig;
    auto status = executor_->ProtectedFillMetricsTLSConfigFromStsP12(tlsConfig);

    EXPECT_TRUE(status.IsOk());
    EXPECT_TRUE(tlsConfig.metricsrootcertdata().empty());
    EXPECT_TRUE(tlsConfig.metricscertdata().empty());
    EXPECT_TRUE(tlsConfig.metricskeydata().empty());
}

TEST_F(RuntimeExecutorTest, VerifyCustomJvmArgs_ShouldReturnValidArgs_WhenArgsAreValid)
{
    std::vector<std::string> customArgs = {
        "-XX:InitialRAMPercentage=25.0", "--add-opens=java.base/java.text=ALL-UNNAMED", "-XX:+DisableExplicitGC",
        "-javaagent:/opt/YuanRong.1.0.0/jacoco/jacocoagent.jar="
        "destfile=/opt/YuanRong.1.0.0/jacoco/reports/"
        "coverage.exec,includes=com.**,output=file,dumponexit=true",
        "-javaagent:/opt/data/secRASP/slave_agent/loader_2.3.0.102/secrasp_slaveloader.jar="
        "dockerType=normal,masteragent.socket.port=2021,masteragent.socket.ip=127.0.0.1,featureStatus=1,,"
        "appScope=slaveagent.version=2.3.0.102,"
        "slaveagent.log.dir=/opt/logs/secRASP/slave_agent/{slaveagent.app_id}/var/logs"
    };
    std::vector<std::string> expected = {
        "-XX:InitialRAMPercentage=25.0", "--add-opens=java.base/java.text=ALL-UNNAMED",
        "-javaagent:/opt/YuanRong.1.0.0/jacoco/jacocoagent.jar="
        "destfile=/opt/YuanRong.1.0.0/jacoco/reports/"
        "coverage.exec,includes=com.**,output=file,dumponexit=true",
        "-javaagent:/opt/data/secRASP/slave_agent/loader_2.3.0.102/secrasp_slaveloader.jar="
        "dockerType=normal,masteragent.socket.port=2021,masteragent.socket.ip=127.0.0.1,featureStatus=1,,"
        "appScope=slaveagent.version=2.3.0.102,"
        "slaveagent.log.dir=/opt/logs/secRASP/slave_agent/{slaveagent.app_id}/var/logs"
    };
    std::vector<std::string> result = executor_->VerifyCustomJvmArgs(customArgs);
    EXPECT_EQ(result, expected);
}

TEST_F(RuntimeExecutorTest, StartInstanceTest)
{
    litebus::os::SetEnv("PATH", litebus::os::Join("/", env_, ':'));
    (void) litebus::os::Rm("/conda");
    auto fd = open("/conda", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    close(fd);
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();

    const char *argv[] = {
        "/runtime_manager",
        "--node_id=node1",
        "--ip=127.0.0.1",
        "--host_ip=127.0.0.1",
        port,
        "--runtime_initial_port=500",
        "--port_num=2000",
        "--runtime_dir=/tmp"
    };
    runtime_manager::Flags flags;
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp");
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(0);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(0);
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");

    executor_->SetDisConvCallStack(true);

    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    int32_t resCode = instanceResponse.code();
    std::string resMsg = instanceResponse.message();
    std::string resRequestID = instanceResponse.requestid();
    EXPECT_EQ(resCode, SUCCESS);
    EXPECT_EQ(resMsg, "start instance success");
    EXPECT_EQ(resRequestID, "test_requestID");

    auto startResponse = instanceResponse.mutable_startruntimeinstanceresponse();
    std::string resRuntimeID = startResponse->runtimeid();
    pid_t resPid = startResponse->pid();
    EXPECT_TRUE(!resRuntimeID.empty());
    EXPECT_TRUE(executor_->IsRuntimeActive(resRuntimeID));
    EXPECT_TRUE(executor_->IsRuntimeActiveByPid(resPid));

    auto startRequest1 = std::make_shared<messages::StartInstanceRequest>();
    startRequest1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo1 = startRequest1->mutable_runtimeinstanceinfo();
    runtimeInfo1->set_requestid("test_requestID");
    runtimeInfo1->set_instanceid("test_instanceID");
    runtimeInfo1->set_traceid("test_traceID");

    auto runtimeConfig1 = runtimeInfo1->mutable_runtimeconfig();
    runtimeConfig1->set_language("nodejs");
    runtimeConfig1->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(0);
    runtimeConfig1->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(0);
    auto userEnvs1 = runtimeConfig1->mutable_userenvs();
    userEnvs1->insert({ "user_env1", "user_env1_value" });
    userEnvs1->insert({ "user_env2", "user_env2_value" });

    auto deployConfig1 = runtimeInfo1->mutable_deploymentconfig();
    deployConfig1->set_objectid("test_objectID");
    deployConfig1->set_bucketid("test_bucketID");
    deployConfig1->set_deploydir(testDeployDir);
    deployConfig1->set_storagetype("s3");
    auto deployOptions = runtimeInfo1->mutable_deploymentconfig()->mutable_deployoptions();
    deployOptions->operator[](CONDA_CONFIG) = "{'test_conda_config': 'confit_content'}";
    deployOptions->operator[](CONDA_COMMAND) = "conda create -n test_env python=3.11";
    deployOptions->operator[](CONDA_PREFIX) = "/tmp/conda_path";
    deployOptions->operator[](CONDA_DEFAULT_ENV) = "env_name_copy";

    auto future1 = executor_->StartInstance(startRequest1, {});
    auto instanceResponse1 = future1.Get();
    int32_t resCode1 = instanceResponse1.code();
    std::string resMsg1 = instanceResponse1.message();
    std::string resRequestID1 = instanceResponse1.requestid();
    EXPECT_EQ(resMsg1, "Executable path of nodejs is not found");
    EXPECT_EQ(resRequestID1, "test_requestID");

    auto startResponse1 = instanceResponse1.mutable_startruntimeinstanceresponse();
    std::string resRuntimeID1 = startResponse1->runtimeid();
    EXPECT_TRUE(resRuntimeID1.empty());
    (void) litebus::os::Rm("/conda");
}

TEST_F(RuntimeExecutorTest, StartInstance_CondaNotExist)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto instanceInfo = request->mutable_runtimeinstanceinfo();
    auto runtimeConfig = instanceInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("python");

    auto deployOptions = instanceInfo->mutable_deploymentconfig()->mutable_deployoptions();
    deployOptions->operator[](CONDA_PREFIX) = "/usr/local/conda";
    deployOptions->operator[](CONDA_DEFAULT_ENV) = "env_name_copy";
    deployOptions->operator[](CONDA_COMMAND) = "conda create -n test_env python=3.11";

    auto future = executor_->StartInstance(request, {});
    auto response = future.Get();
    EXPECT_EQ(response.code(), RUNTIME_MANAGER_CONDA_PARAMS_INVALID);
    EXPECT_THAT(response.message(), testing::HasSubstr("conda not found in path"));
}

TEST_F(RuntimeExecutorTest, StartInstance_CondaSpecifiedEnvNotExist)
{
    litebus::os::SetEnv("PATH", litebus::os::Join("/", env_, ':'));
    (void) litebus::os::Rm("/conda");
    auto fd = open("/conda", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    close(fd);

    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto instanceInfo = request->mutable_runtimeinstanceinfo();
    auto runtimeConfig = instanceInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("python");
    instanceInfo->mutable_deploymentconfig()->set_deploydir(testDeployDir);

    auto deployOptions = instanceInfo->mutable_deploymentconfig()->mutable_deployoptions();
    deployOptions->operator[](CONDA_PREFIX) = "/usr/local/conda";
    deployOptions->operator[](CONDA_DEFAULT_ENV) = "nullconda";
    deployOptions->operator[](CONDA_COMMAND) = "conda activate nullconda";

    auto future = executor_->StartInstance(request, {});
    auto response = future.Get();
    EXPECT_EQ(response.code(), RUNTIME_MANAGER_CONDA_ENV_NOT_EXIST);
    EXPECT_THAT(response.message(), testing::HasSubstr("not exists"));
}

TEST_F(RuntimeExecutorTest, StartInstance_CondaCommandNotValid)
{
    litebus::os::SetEnv("PATH", litebus::os::Join("/", env_, ':'));
    (void) litebus::os::Rm("/conda");
    auto fd = open("/conda", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    close(fd);

    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto instanceInfo = request->mutable_runtimeinstanceinfo();
    auto runtimeConfig = instanceInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp");
    auto deployOptions = instanceInfo->mutable_deploymentconfig()->mutable_deployoptions();
    deployOptions->operator[](CONDA_PREFIX) = "/usr/local/conda";
    deployOptions->operator[](CONDA_DEFAULT_ENV) = "env_name_copy";
    deployOptions->operator[](CONDA_COMMAND) = "rm -rf /xxx";

    auto future = executor_->StartInstance(request, {});
    auto response = future.Get();
    EXPECT_EQ(response.code(), RUNTIME_MANAGER_CONDA_PARAMS_INVALID);
    EXPECT_THAT(response.message(), testing::HasSubstr("not valid"));
}

TEST_F(RuntimeExecutorTest, StartInstanceLanguageFailTest)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto instanceInfo = request->mutable_runtimeinstanceinfo();
    instanceInfo->set_requestid("test_requestID");
    instanceInfo->set_instanceid("test_instanceID");
    instanceInfo->set_runtimeid("test_runtimeID");
    instanceInfo->set_traceid("test_traceID");

    auto runtimeConfig = instanceInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("unknown_lang");
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto future = executor_->StartInstance(request, {});
    auto response = future.Get();
    EXPECT_EQ(response.code(), PARAMETER_ERROR);
    EXPECT_THAT(response.message(), testing::HasSubstr("runtimeExecutor does not support this language: unknown_lang"));
}

TEST_F(RuntimeExecutorTest, StartInstancePortFailTest)
{
    PortManager::GetInstance().Clear();
    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto instanceInfo = request->mutable_runtimeinstanceinfo();
    instanceInfo->set_requestid("test_requestID");
    instanceInfo->set_instanceid("test_instanceID");
    instanceInfo->set_runtimeid("test_runtimeID");
    instanceInfo->set_traceid("test_traceID");

    auto runtimeConfig = instanceInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp");
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto future = executor_->StartInstance(request, {});
    auto response = future.Get();
    EXPECT_EQ(response.code(), RUNTIME_MANAGER_PORT_UNAVAILABLE);
    // resume port resource
    PortManager::GetInstance().InitPortResource(INITIAL_PORT, PORT_NUM);
}

TEST_F(RuntimeExecutorTest, StartInstanceIdentityFailInvalidUidTest)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto instanceInfo = request->mutable_runtimeinstanceinfo();
    instanceInfo->set_requestid("test_requestID");
    instanceInfo->set_instanceid("test_instanceID");
    instanceInfo->set_runtimeid("test_runtimeID");
    instanceInfo->set_traceid("test_traceID");
    instanceInfo->mutable_deploymentconfig()->set_deploydir(testDeployDir);

    auto runtimeConfig = instanceInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp");
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(1000);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(-1);
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto future = executor_->StartInstance(request, {});
    auto response = future.Get();
    EXPECT_EQ(response.code(), RUNTIME_MANAGER_PARAMS_INVALID);
}

TEST_F(RuntimeExecutorTest, StartInstanceIdentityFailUIDOverLimitTest)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto instanceInfo = request->mutable_runtimeinstanceinfo();
    instanceInfo->set_requestid("test_requestID");
    instanceInfo->set_instanceid("test_instanceID");
    instanceInfo->set_traceid("test_traceID");
    instanceInfo->mutable_deploymentconfig()->set_deploydir(testDeployDir);

    auto runtimeConfig = instanceInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp");
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(65536);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(0);
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto future = executor_->StartInstance(request, {});
    auto response = future.Get();
    EXPECT_EQ(response.code(), RUNTIME_MANAGER_PARAMS_INVALID);
}

TEST_F(RuntimeExecutorTest, StartInstanceWithSubDirTest)
{
    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp");
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(0);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(0);
    runtimeConfig->mutable_subdirectoryconfig()->set_isenable(true);
    runtimeConfig->mutable_subdirectoryconfig()->set_parentdirectory("/");
    runtimeConfig->mutable_subdirectoryconfig()->set_quota(1);

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");

    litebus::os::Rmdir("/test_instanceID");
    auto future = executor_->StartInstance(startRequest, {});
    EXPECT_EQ(future.Get().code(), SUCCESS);
    EXPECT_TRUE(FileExists("/test_instanceID"));

    auto owner = GetOwner("/test_instanceID");
    EXPECT_TRUE(owner.IsSome());
    EXPECT_EQ(owner.Get().first, static_cast<uint32_t>(0));

    auto permission = GetPermission("/test_instanceID");
    EXPECT_TRUE(permission.IsSome());
    EXPECT_EQ(permission.Get().owner, static_cast<uint32_t>(7));
    EXPECT_EQ(permission.Get().group, static_cast<uint32_t>(5));
    EXPECT_EQ(permission.Get().others, static_cast<uint32_t>(0));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    (void)TouchFile("/test_instanceID/test_size.txt");
    system("echo \"fjwehfujwehjfnwekfjoejgwpggwegwgrgbrnmgfwejknfioji42joi34jismdkovgnqpwof2pokqpoekfpwkdopkqwpofmtdopkqwpofmt\""
        ">> /test_instanceID/test_size.txt");
    auto updateInstanceStatusMsg = mockAgent_->updateInstanceStatusMsg.GetFuture();
    EXPECT_FALSE(updateInstanceStatusMsg.IsOK());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    (void)litebus::os::Mkdir("/test_instanceID/subsub"); // 触发create
    system("/usr/bin/dd if=/dev/zero of=/test_instanceID/subsub/newfile bs=4M count=5 >/dev/null 2>&1");

    updateInstanceStatusMsg = mockAgent_->updateInstanceStatusMsg.GetFuture();
    messages::UpdateInstanceStatusRequest req;
    req.ParseFromString(updateInstanceStatusMsg.Get());
    EXPECT_TRUE(req.instancestatusinfo().status() == static_cast<int32_t>(INSTANCE_DISK_USAGE_EXCEED_LIMIT));
    EXPECT_TRUE(req.instancestatusinfo().type() == static_cast<int32_t>(EXIT_TYPE::EXCEPTION_INFO));
    EXPECT_TRUE(req.instancestatusinfo().instanceid() == "test_instanceID");

    auto instanceResponse = future.Get();
    auto stopRequest = std::make_shared<messages::StopInstanceRequest>();
    stopRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    stopRequest->set_requestid("test_requestID");
    stopRequest->set_runtimeid(instanceResponse.mutable_startruntimeinstanceresponse()->runtimeid());
    auto stopResponse = executor_->StopInstance(stopRequest);
    EXPECT_EQ(stopResponse.Get().StatusCode(), SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(FileExists("/test_instanceID"));

    litebus::os::Rmdir("/fake_dir");
    runtimeConfig->mutable_subdirectoryconfig()->set_parentdirectory("/fake_dir");
    future = executor_->StartInstance(startRequest, {});
    EXPECT_EQ(future.Get().code(), SUCCESS);
    EXPECT_TRUE(FileExists("/tmp/test_instanceID"));

    owner = GetOwner("/tmp/test_instanceID");
    EXPECT_TRUE(owner.IsSome());
    // dir is owned by runtime-manager (0 in test, sn in real environment)
    EXPECT_EQ(owner.Get().first, static_cast<uint32_t>(0));

    permission = GetPermission("/tmp/test_instanceID");
    EXPECT_TRUE(permission.IsSome());
    EXPECT_EQ(permission.Get().owner, static_cast<uint32_t>(7));
    EXPECT_EQ(permission.Get().group, static_cast<uint32_t>(5));
    EXPECT_EQ(permission.Get().others, static_cast<uint32_t>(0));

    instanceResponse = future.Get();
    stopRequest->set_runtimeid(instanceResponse.mutable_startruntimeinstanceresponse()->runtimeid());
    stopResponse = executor_->StopInstance(stopRequest);
    EXPECT_EQ(stopResponse.Get().StatusCode(), SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(FileExists("/tmp/test_instanceID"));

    runtimeConfig->mutable_subdirectoryconfig()->set_parentdirectory("");
    future = executor_->StartInstance(startRequest, {});
    EXPECT_EQ(future.Get().code(), SUCCESS);
    EXPECT_TRUE(FileExists("/tmp/test_instanceID"));
    (void)litebus::os::Rmdir("/test_instanceID");
}

TEST_F(RuntimeExecutorTest, StopInstanceTest)
{
    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp");
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");

    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    int32_t resCode = instanceResponse.code();
    std::string resMsg = instanceResponse.message();
    std::string resRequestID = instanceResponse.requestid();
    EXPECT_EQ(resCode, SUCCESS);
    EXPECT_EQ(resMsg, "start instance success");
    EXPECT_EQ(resRequestID, "test_requestID");

    auto startResponse = instanceResponse.mutable_startruntimeinstanceresponse();
    std::string resRuntimeID = startResponse->runtimeid();
    pid_t resPid = startResponse->pid();
    EXPECT_TRUE(!resRuntimeID.empty());

    auto stopRequest = std::make_shared<messages::StopInstanceRequest>();
    stopRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    stopRequest->set_requestid("test_requestID");
    stopRequest->set_runtimeid(resRuntimeID);

    auto stopResponse = executor_->StopInstance(stopRequest);
    EXPECT_EQ(stopResponse.Get().StatusCode(), SUCCESS);
    EXPECT_FALSE(executor_->IsRuntimeActive(resRuntimeID));
    EXPECT_FALSE(executor_->IsRuntimeActiveByPid(resPid));
}

TEST_F(RuntimeExecutorTest, StopInstanceFailTest)
{
    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("unknown_runtimeID");
    request->set_requestid("test_requestID");

    auto response = executor_->StopInstance(request);
    EXPECT_EQ(response.Get().StatusCode(), RUNTIME_MANAGER_RUNTIME_PROCESS_NOT_FOUND);
}

/**
* Feature:
* Description: PosixCustomRuntime
* Steps:
* 1. Create StartInstanceRequest
* 2. Set start request
* 3. Call StartInstance
* 4. Check response
* Expectation:
* 1. Receive correct posix string.
 */
TEST_F(RuntimeExecutorTest, PosixCustomRuntimeTest)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto instanceInfo = request->mutable_runtimeinstanceinfo();
    instanceInfo->set_requestid("test_requestID");
    instanceInfo->set_instanceid("test_instanceID");
    instanceInfo->set_traceid("test_traceID");
    instanceInfo->mutable_deploymentconfig()->set_objectid("stdout");
    auto runtimeConfig = instanceInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("posix-custom-runtime");
    const std::string tmpFilePath = "/tmp/runtime_executor_entryfile";
    runtimeConfig->set_entryfile(tmpFilePath);

    const std::string testStr = "test posix custom runtime";
    (void)litebus::os::Mkdir(tmpFilePath);
    const std::string bootstrapPath = tmpFilePath + "/bootstrap";
    (void)litebus::os::Rm(bootstrapPath);
    TouchFile(bootstrapPath);
    std::ofstream outfile;
    outfile.open(bootstrapPath);
    outfile << "#!/bin/bash" << std::endl;
    outfile << "echo " + testStr << std::endl;
    outfile.close();

    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    (void)litebus::os::Rmdir("/home/snuser/instances/");
    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();

    const char *argv[] = { "/runtime_manager",
                           "--node_id=",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--runtime_std_log_dir=instances" };
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);
    auto future = executor_->StartInstance(request, {});
    auto response = future.Get();
    auto resRuntimeID = response.startruntimeinstanceresponse().runtimeid();
    sleep(1);
    ASSERT_AWAIT_TRUE([=](){
        auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log");
        return output.IsSome() && output.Get().find(testStr) != std::string::npos;
    });
    EXPECT_EQ(response.requestid(), "test_requestID");
    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
}

/**
* Feature:
* Description: Start instance with cachePool
* Steps:
* 1. Create runtime manager flags
* 2. Set start request
* 3. Call StartInstance
* 4. Check response
* Expectation:
* 1. Receive response with RUNTIME_MANAGER_CREATE_EXEC_FAILED.
 */
TEST_F(RuntimeExecutorTest, StartInstanceWithCachePoolTest)
{
    auto client = std::make_shared<runtime_manager::HealthCheck>();
    client->RegisterProcessExitCallback(std::bind(&RuntimeExecutor::UpdatePrestartRuntimePromise, executor_, std::placeholders::_1));
    const char *argv[] = { "./runtime-manager", "--runtime_log_level=DEBUG",
                           "--runtime_prestart_config={\"java1.8\": {\"prestartCount\": -1, \"customArgs\": "
                           "[\"-XX:+PrintGC\",\"-XX:+UseParallelGC\"]}, \"java11\": {\"prestartCount\": -1}, "
                           "\"cpp11\": {\"prestartCount\": 1}, \"python3.9\": {\"prestartCount\": 1}}" };
    runtime_manager::Flags flags;
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);
    sleep(3);
    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp11");
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");

    auto scheduleOptions = startRequest->mutable_scheduleoption();
    scheduleOptions->set_schedpolicyname( "shared");
    // make sure all prestart process has exit
    ASSERT_AWAIT_TRUE([&]() -> bool { return CheckPrestartRuntimePromise() == 6; });
    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    int32_t resCode = instanceResponse.code();
    std::string resMsg = instanceResponse.message();
    std::string resRequestID = instanceResponse.requestid();
    EXPECT_EQ(resCode, SUCCESS);
    EXPECT_EQ(resMsg, "start instance success");
    EXPECT_EQ(resRequestID, "test_requestID");

    runtimeInfo->set_requestid("test_requestID_monopoly");
    runtimeInfo->set_instanceid("test_instanceID_monopoly");
    runtimeInfo->set_traceid("test_traceID_monopoly");
    runtimeConfig->set_language("python3.9");
    deployConfig->set_objectid("test_objectID_monopoly");
    deployConfig->set_bucketid("test_bucketID_monopoly");
    scheduleOptions->set_schedpolicyname( "monopoly");

    future = executor_->StartInstance(startRequest, {});
    instanceResponse = future.Get();
    resCode = instanceResponse.code();
    resMsg = instanceResponse.message();
    resRequestID = instanceResponse.requestid();
    EXPECT_EQ(resCode, SUCCESS);
    EXPECT_EQ(resMsg, "start instance success");
    EXPECT_EQ(resRequestID, "test_requestID_monopoly");
}

/**
* Feature:
* Description: Start instance with prestart runtime
* Steps:
* 1. Init Prestart Runtime Flag
* 2. Wait prestart start runtime ready
* 3. Call StartInstance, and select runtime from pool
* 4. Check response
* 5. Wait all the prestart runtime exit
* Expectation:
* 1. Receive response with RUNTIME_MANAGER_CREATE_EXEC_FAILED.
 */
TEST_F(RuntimeExecutorTest, StartInstanceWithPrestartRuntime)
{
    auto client = std::make_shared<runtime_manager::HealthCheck>();
    client->RegisterProcessExitCallback(std::bind(&RuntimeExecutorTest::RecordRuntimePID, this, std::placeholders::_1));
    const char *argv[] = { "./runtime-manager",
                           "--runtime_log_level=DEBUG",
                           "--runtime_prestart_config={\"cpp11\": {\"prestartCount\": 1}}"  };
    runtime_manager::Flags flags;
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);
    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp11");
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");

    auto scheduleOptions = startRequest->mutable_scheduleoption();
    scheduleOptions->set_schedpolicyname( "shared");
    ASSERT_AWAIT_TRUE([&]() -> bool { return pidArray.size() > 0; });
    // after prestart runtime exit
    client->RegisterProcessExitCallback(std::bind(&RuntimeExecutor::UpdatePrestartRuntimePromise, executor_, std::placeholders::_1));
    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    int32_t resCode = instanceResponse.code();
    std::string resMsg = instanceResponse.message();
    std::string resRequestID = instanceResponse.requestid();
    EXPECT_EQ(resCode,  RUNTIME_MANAGER_CREATE_EXEC_FAILED);
    EXPECT_EQ(resRequestID, "test_requestID");
    ClearRuntimePID();
    ASSERT_AWAIT_TRUE([&]() -> bool { return CheckPrestartRuntimePromise() == 3; });
}

/**
* Feature: KillOtherPrestartRuntimeProcessTest
* Description: Start monopoly instance, kill prestart runtime process
* Steps:
* 1. Create runtime manager flags with prestart config
* 2. Set start request, schedule_policy is monopoly
* 3. Call StartInstance
* 4. Kill other prestart runtime
* Expectation:
* 1. prestart runtime pool size will be 0
 */
TEST_F(RuntimeExecutorTest, KillOtherPrestartRuntimeProcessTest)
{
    auto client = std::make_shared<runtime_manager::HealthCheck>();
    client->RegisterProcessExitCallback(std::bind(&RuntimeExecutor::UpdatePrestartRuntimePromise, executor_, std::placeholders::_1));
    const char *argv[] = { "./runtime-manager",
                           "--runtime_log_level=DEBUG",
                           "--runtime_prestart_config={\"python3.9\": {\"prestartCount\": 1}}" };
    runtime_manager::Flags flags;
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);
    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");
    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp11");
    auto scheduleOptions = startRequest->mutable_scheduleoption();
    scheduleOptions->set_schedpolicyname( "monopoly");
    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");
    // make sure all prestart process has exit
    ASSERT_AWAIT_TRUE([&]() -> bool { return CheckPrestartRuntimePromise() == 3; });
    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    ASSERT_AWAIT_TRUE_FOR([&]() -> bool { return CheckPrestartRuntimePromise() == 0; }, 3000);
}

/**
* Feature:
* Description: Cover HookRuntimeCredentialByID Method
* Steps:
* 1. Create function
* 2. Add to vector
* 3. Call HookRuntimeCredentialByID
* Expectation:
* 1. None.
 */
TEST_F(RuntimeExecutorTest, HookRuntimeCredentialByIDTest)
{
    std::function<void()> func = []() {
        std::cout << "Hello, world!" << std::endl;
    };
    std::vector<std::function<void()>> initHook;
    initHook.push_back(func);
    executor_->HookRuntimeCredentialByID(initHook, 0, 0);
    EXPECT_TRUE(true);
}

/**
* Feature:
* Description: Test GetPythonBuildArgs
* Steps:
* 1. Add fake execution path
* 2. Set start request with language python
* 3. Call StartInstance
* 4. Check runtimeID
* 5. Remove execution path
* Expectation:
* 1. RuntimeID is not empty.
 */
TEST_F(RuntimeExecutorTest, GetPythonBuildArgsTest)
{
    litebus::os::SetEnv("PATH", litebus::os::Join("/", env_, ':'));
    (void) litebus::os::Rm("/conda");
    auto fd = open("/conda", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    fd = open("/python", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    close(fd);
    auto cmdTool = std::make_shared<MockCmdTools>();
    EXPECT_CALL(*cmdTool.get(), GetCmdResultWithError).WillRepeatedly(testing::Return(condaEnvCreateResult));
    executor_->cmdTool_ = cmdTool; // mock

    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("python");
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(0);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(0);
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto deployOptions = runtimeInfo->mutable_deploymentconfig()->mutable_deployoptions();
    deployOptions->operator[](CONDA_PREFIX) = "/tmp/conda2";
    deployOptions->operator[](CONDA_DEFAULT_ENV) = "env_name_copy";
    deployOptions->operator[](CONDA_COMMAND) = "conda env create -f env.yaml"; // Note env name in file
    deployOptions->operator[](CONDA_CONFIG) = testCondaConfig;

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");

    auto future = executor_->StartInstance(startRequest, {}); // start
    auto instanceResponse = future.Get();
    auto startResponse = instanceResponse.mutable_startruntimeinstanceresponse();
    std::string resRuntimeID = startResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());

    ASSERT_AWAIT_TRUE([=]() {
        auto output = litebus::os::Read("/nodeID-user_func_std.log");
        return output.IsSome()
               && (output.Get().find("conda activate failed") != std::string::npos);  // fail for fake conda
    });

    // conda yaml file
    EXPECT_TRUE(FileExists(testDeployDir + "/env.yaml"));
    std::ifstream fin(testDeployDir + "/env.yaml");
    YAML::Node node = YAML::Load(std::string(std::istreambuf_iterator<char>(fin), std::istreambuf_iterator<char>()));
    ASSERT_TRUE(node.IsMap());
    ASSERT_TRUE(node["name"]);
    std::string value = node["name"].as<std::string>();
    EXPECT_EQ(value, "env_name_file");

    (void)litebus::os::Rm("/python");
    (void)litebus::os::Rm("/conda");
}

/**
* Feature:
* Description: Test GetJavaBuildArgs
* Steps:
* 1. Add fake execution path
* 2. Set start request with language java1.8
* 3. Call StartInstance
* 4. Check runtimeID
* 5. Remove execution path
* Expectation:
* 1. RuntimeID is not empty.
 */
TEST_F(RuntimeExecutorTest, GetJavaBuildArgsTest)
{
    litebus::os::SetEnv("PATH", litebus::os::Join("/tmp", env_, ':'));
    (void) litebus::os::Rm("/tmp/java1.8");
    auto fd = open("/tmp/java1.8", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    close(fd);

    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("java1.8");
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(0);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(0);
    resources::Resource memResource;
    memResource.set_type(::resources::Value_Type::Value_Type_SCALAR);
    memResource.mutable_scalar()->set_value(500.0);
    (*runtimeConfig->mutable_resources()->mutable_resources())["Memory"] = memResource;
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");

    const char *argv[] = { "./runtime-manager", "--runtime_log_level=DEBUG","--runtime_prestart_config={}","--proc_metrics_memory=1000"};
    runtime_manager::Flags flags;
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);
    auto prestartArgs = executor_->GetBuildArgsForPrestart("runtime11", "java1.8", "8080");
    EXPECT_TRUE(std::count(prestartArgs.begin(), prestartArgs.end(), "-XX:+CMSClassUnloadingEnabled") == 1);
    std::vector<std::string> args;
    executor_->GetBuildArgs("java1.8", "8080", startRequest, args);
    EXPECT_TRUE(std::count(args.begin(), args.end(), "-Xmx500m") == 1);
    resources::Resource memResource1;
    memResource1.set_type(::resources::Value_Type::Value_Type_SCALAR);
    memResource1.mutable_scalar()->set_value(1000.0);
    (*runtimeConfig->mutable_resources()->mutable_resources())["Memory"] = memResource1;
    args = {};
    executor_->GetBuildArgs("java1.8", "8080", startRequest, args);
    EXPECT_TRUE(std::count(args.begin(), args.end(), "-Xmx800m") == 1);
    EXPECT_TRUE(std::count(args.begin(), args.end(), "-XX:+CMSClassUnloadingEnabled") == 1);
    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    auto startResponse = instanceResponse.mutable_startruntimeinstanceresponse();
    std::string resRuntimeID = startResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());

    (void) litebus::os::Rm("/tmp/java1.8");
}

/**
* Feature:
* Description: Test GetJava11BuildArgs
* Steps:
* 1. Add fake execution path
* 2. Set start request with language java11
* 3. Call StartInstance
* 4. Check runtimeID
* 5. Remove execution path
* Expectation:
* 1. RuntimeID is not empty.
 */
TEST_F(RuntimeExecutorTest, GetJava11BuildArgsTest)
{
    litebus::os::SetEnv("PATH", litebus::os::Join("/tmp", env_, ':'));
    (void) litebus::os::Rm("/tmp/java11");
    auto fd = open("/tmp/java11", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    close(fd);

    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("java11");
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(0);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(0);
    resources::Resource memResource;
    memResource.set_type(::resources::Value_Type::Value_Type_SCALAR);
    memResource.mutable_scalar()->set_value(500.0);
    (*runtimeConfig->mutable_resources()->mutable_resources())["Memory"] = memResource;
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");
    const char *argv[] = { "./runtime-manager", "--runtime_log_level=DEBUG","--runtime_prestart_config={}","--proc_metrics_memory=1000"};
    runtime_manager::Flags flags;
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);
    auto prestartArgs = executor_->GetBuildArgsForPrestart("runtime11", "java11", "8080");
    EXPECT_TRUE(std::count(prestartArgs.begin(), prestartArgs.end(), "-XX:+UseG1GC") == 1);
    std::vector<std::string> args;
    executor_->GetBuildArgs("java11", "8080", startRequest, args);
    EXPECT_TRUE(std::count(args.begin(), args.end(), "-Xmx500m") == 1);
    resources::Resource memResource1;
    memResource1.set_type(::resources::Value_Type::Value_Type_SCALAR);
    memResource1.mutable_scalar()->set_value(1000.0);
    (*runtimeConfig->mutable_resources()->mutable_resources())["Memory"] = memResource1;
    args = {};
    executor_->GetBuildArgs("java11", "8080", startRequest, args);
    EXPECT_TRUE(std::count(args.begin(), args.end(), "-Xmx800m") == 1);
    EXPECT_TRUE(std::count(args.begin(), args.end(), "-XX:+UseG1GC") == 1);
    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    auto startResponse = instanceResponse.mutable_startruntimeinstanceresponse();
    std::string resRuntimeID = startResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());

    (void) litebus::os::Rm("/tmp/java11");
}

TEST_F(RuntimeExecutorTest, GetJava17BuildArgsTest)
{
    litebus::os::SetEnv("PATH", litebus::os::Join("/tmp", env_, ':'));
    (void) litebus::os::Rm("/tmp/java17");
    auto fd = open("/tmp/java17", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    close(fd);

    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("java17");
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(0);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(0);
    resources::Resource memResource;
    memResource.set_type(::resources::Value_Type::Value_Type_SCALAR);
    memResource.mutable_scalar()->set_value(500.0);
    (*runtimeConfig->mutable_resources()->mutable_resources())["Memory"] = memResource;
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");
    const char *argv[] = { "./runtime-manager", "--runtime_log_level=DEBUG","--runtime_prestart_config={}","--proc_metrics_memory=1000"};
    runtime_manager::Flags flags;
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    std::vector<std::string> args;
    executor_->GetBuildArgs("java17", "8080", startRequest, args);
    EXPECT_TRUE(std::count(args.begin(), args.end(), "-Xmx500m") == 1);
    resources::Resource memResource1;
    memResource1.set_type(::resources::Value_Type::Value_Type_SCALAR);
    memResource1.mutable_scalar()->set_value(1000.0);
    (*runtimeConfig->mutable_resources()->mutable_resources())["Memory"] = memResource1;

    args = {};
    executor_->GetBuildArgs("java17", "8080", startRequest, args);
    EXPECT_TRUE(std::count(args.begin(), args.end(), "-Xmx800m") == 1);
    EXPECT_TRUE(std::count(args.begin(), args.end(), "-XX:+UseZGC") == 1);
    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    auto startResponse = instanceResponse.mutable_startruntimeinstanceresponse();
    std::string resRuntimeID = startResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());

    (void) litebus::os::Rm("/tmp/java17");
}

TEST_F(RuntimeExecutorTest, GetJava21BuildArgsTest)
{
    litebus::os::SetEnv("PATH", litebus::os::Join("/tmp", env_, ':'));
    (void) litebus::os::Rm("/tmp/java21");
    auto fd = open("/tmp/java21", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    close(fd);

    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("java21");
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(0);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(0);
    resources::Resource memResource;
    memResource.set_type(::resources::Value_Type::Value_Type_SCALAR);
    memResource.mutable_scalar()->set_value(500.0);
    (*runtimeConfig->mutable_resources()->mutable_resources())["Memory"] = memResource;
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");
    const char *argv[] = { "./runtime-manager", "--runtime_log_level=DEBUG","--runtime_prestart_config={}","--proc_metrics_memory=1000"};
    runtime_manager::Flags flags;
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    std::vector<std::string> args;
    executor_->GetBuildArgs("java21", "8080", startRequest, args);
    EXPECT_TRUE(std::count(args.begin(), args.end(), "-Xmx500m") == 1);
    resources::Resource memResource1;
    memResource1.set_type(::resources::Value_Type::Value_Type_SCALAR);
    memResource1.mutable_scalar()->set_value(1000.0);
    (*runtimeConfig->mutable_resources()->mutable_resources())["Memory"] = memResource1;

    args = {};
    executor_->GetBuildArgs("java21", "8080", startRequest, args);
    EXPECT_TRUE(std::count(args.begin(), args.end(), "-Xmx800m") == 1);
    EXPECT_TRUE(std::count(args.begin(), args.end(), "-XX:+UseZGC") == 1);
    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    auto startResponse = instanceResponse.mutable_startruntimeinstanceresponse();
    std::string resRuntimeID = startResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());

    (void) litebus::os::Rm("/tmp/java21");
}

/**
* Feature:
* Description: Test GetNoneExistedGoExecPathTest
* Steps:
* 1. Add fake execution path
* 2. Set start request with language go
* 3. Call StartInstance
* 4. Check runtimeID
* Expectation:
* 1. RuntimeID is not empty.
 */
TEST_F(RuntimeExecutorTest, GetNoneExistedGoExecPathTest)
{
    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("go");
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(0);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(0);
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();

    const char *argv[] = {
        "/runtime_manager",
        "--node_id=node1",
        "--ip=127.0.0.1",
        "--host_ip=127.0.0.1",
        port,
        "--runtime_initial_port=500",
        "--port_num=2000",
        "--runtime_dir=/tmp",
        "--agent_address=127.0.0.1:8080",
        "--runtime_ld_library_path=/tmp",
        "--proc_metrics_cpu=2000",
        "--proc_metrics_memory=2000",
        R"(--log_config={"filepath": "/home/yr/log", "level": "DEBUG", "rolling": {"maxsize": 100, "maxfiles": 1},"alsologtostderr":true})"
    };
    functionsystem::runtime_manager::Flags flags;
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    auto startResponse = instanceResponse.mutable_startruntimeinstanceresponse();
    std::string resRuntimeID = startResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());
}

/**
* Feature:
* Description: Test GetNoneExistedCPPExecPathTest
* Steps:
* 1. Add fake execution path
* 2. Set start request with language cpp
* 3. Call StartInstance
* 4. Check runtimeID
* Expectation:
* 1. RuntimeID is not empty.
 */
TEST_F(RuntimeExecutorTest, GetNoneExistedCPPExecPathTest)
{
    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp");
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(0);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(0);
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");

    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();
    const char *argv[] = {
        "/runtime_manager",
        "--node_id=node1",
        "--ip=127.0.0.1",
        "--host_ip=127.0.0.1",
        port,
        "--runtime_initial_port=500",
        "--port_num=2000",
        "--runtime_dir=/tmp",
        "--agent_address=127.0.0.1:8080",
        "--runtime_ld_library_path=/tmp",
        "--proc_metrics_cpu=2000",
        "--proc_metrics_memory=2000",
        R"(--log_config={"filepath": "/home/yr/log", "level": "DEBUG", "rolling": {"maxsize": 100, "maxfiles": 1},"alsologtostderr":true})"
    };
    functionsystem::runtime_manager::Flags flags;
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    auto startResponse = instanceResponse.mutable_startruntimeinstanceresponse();
    std::string resRuntimeID = startResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());
}

/**
* Feature:
* Description: Test GetGoBuildArgs
* Steps:
* 1. Add fake execution path
* 2. Set start request with language go
* 3. Call StartInstance
* 4. Check runtimeID
* 5. Remove execution path
* Expectation:
* 1. RuntimeID is not empty.
 */
TEST_F(RuntimeExecutorTest, GetGoBuildArgsTest)
{
    litebus::os::SetEnv("PATH", litebus::os::Join("/tmp", env_, ':'));
    (void) litebus::os::Rm("/tmp/go");
    auto fd = open("/tmp/go", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    close(fd);

    auto startRequest = BuildStartInstanceRequest(GO_LANGUAGE);

    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    auto startResponse = instanceResponse.mutable_startruntimeinstanceresponse();
    std::string resRuntimeID = startResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());

    (void) litebus::os::Rm("/tmp/go");
}

/**
* Feature:
* Description: Test GetJavaBuildArgsForPrestart
* Steps:
* 1. Call GetJavaBuildArgsForPrestart
* Expectation:
* 1. Size of result equals to 0.
 */
TEST_F(RuntimeExecutorTest, GetJavaBuildArgsForPrestartTest)
{
    auto result = executor_->GetJavaBuildArgsForPrestart("runtimeID", "30660", "java1.8");
    EXPECT_TRUE(result.size() > 0);
}

/**
* Feature:
* Description: Test CheckPrestartRuntimeRetry
* Steps:
* 1. Call CheckPrestartRuntimeRetry
* Expectation:
* 1. Receive false.
* 2. Receive false.
 */
TEST_F(RuntimeExecutorTest, CheckPrestartRuntimeRetryTest)
{
    EXPECT_FALSE(executor_->CheckPrestartRuntimeRetry("runtimeID", "java1.8", 3));
    EXPECT_FALSE(executor_->CheckPrestartRuntimeRetry("runtimeID", "java1.8", 2));
}

std::shared_ptr<messages::StartInstanceRequest> BuildStartInstanceRequest(const std::string &language)
{
    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language(language);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_userid(0);
    runtimeConfig->mutable_funcmountconfig()->mutable_funcmountuser()->set_groupid(0);
    auto userEnvs = runtimeConfig->mutable_userenvs();
    userEnvs->insert({ "user_env1", "user_env1_value" });
    userEnvs->insert({ "user_env2", "user_env2_value" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");
    return startRequest;
}

/**
* Feature:
* Description: Test GetValgrindMassifBuildArgs
* Steps:
* 1. Add fake execution path
* 2. Set start request with valgrind
* 3. Call StartInstance
* 4. Check runtimeID
* 5. Remove execution path
* Expectation:
* 1. RuntimeID is not empty.
 */
TEST_F(RuntimeExecutorTest, GetValgrindMassifBuildArgs)
{
    litebus::os::SetEnv("PATH", litebus::os::Join("/tmp", env_, ':'));
    (void) litebus::os::Rm("/tmp/valgrind");
    auto fd = open("/tmp/valgrind", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    close(fd);
    functionsystem::runtime_manager::Flags flags;
    const char *argv[] = {
        "/runtime_manager",
        "--massif_enable=true",
    };
    flags.ParseFlags(std::size(argv), argv);
    executor_->Executor::SetRuntimeConfig(flags);
    auto startRequest = BuildStartInstanceRequest(GO_LANGUAGE);
    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();
    auto startResponse = instanceResponse.mutable_startruntimeinstanceresponse();
    std::string resRuntimeID = startResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());
    (void) litebus::os::Rm("/tmp/valgrind");
}

TEST_F(RuntimeExecutorTest, SetLD_LIBRARY_PATH)
{
    const std::string tmpFilePath = "/tmp/runtime_executor_entryfile";
    (void)litebus::os::Mkdir(tmpFilePath);
    const std::string bootstrapPath = tmpFilePath + "/bootstrap";
    (void)litebus::os::Rm(bootstrapPath);
    TouchFile(bootstrapPath);
    std::ofstream outfile;
    outfile.open(bootstrapPath);
    outfile << "#!/bin/bash" << std::endl;
    outfile << R"(echo LD_LIBRARY_PATH="${LD_LIBRARY_PATH}")" << std::endl;
    outfile.close();

    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();

    const char *argv[] = { "/runtime_manager",
                           "--node_id=",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--runtime_std_log_dir=instances" };
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    auto request1 = std::make_shared<messages::StartInstanceRequest>();
    request1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    request1->mutable_runtimeinstanceinfo()->set_requestid("test_requestID");
    request1->mutable_runtimeinstanceinfo()->set_instanceid("test_instanceID");
    request1->mutable_runtimeinstanceinfo()->set_traceid("test_traceID");
    request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_objectid("stdout");
    request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_deploydir("/dacache/bucket/object");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("posix-custom-runtime");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_entryfile(tmpFilePath);
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert({"LD_LIBRARY_PATH", "${LD_LIBRARY_PATH}:${FUNCTION_LIB_PATH}/tmp:/opt/${NOT_EXISTED_PATH}/tmp"});
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_userenvs()->insert({"func-LD_LIBRARY_PATH", "/dcache"});

    auto response = executor_->StartInstance(request1, {}).Get();
    ASSERT_AWAIT_TRUE([=](){
        auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log");
        return output.IsSome() && (output.Get().find("LD_LIBRARY_PATH=") != std::string::npos);
    });
    auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log").Get();
    EXPECT_TRUE(
        output.find(
            "LD_LIBRARY_PATH=/dacache/bucket/object:/dacache/bucket/object/lib:/dacache/bucket/object/tmp:/opt//tmp:/dcache") !=
        output.npos);
    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
}

TEST_F(RuntimeExecutorTest, SetEmptyLD_LIBRARY_PATH)
{
    const std::string tmpFilePath = "/tmp/runtime_executor_entryfile";
    (void)litebus::os::Mkdir(tmpFilePath);
    const std::string bootstrapPath = tmpFilePath + "/bootstrap";
    (void)litebus::os::Rm(bootstrapPath);
    TouchFile(bootstrapPath);
    std::ofstream outfile;
    outfile.open(bootstrapPath);
    outfile << "#!/bin/bash" << std::endl;
    outfile << R"(echo LD_LIBRARY_PATH="${LD_LIBRARY_PATH}")" << std::endl;
    outfile.close();

    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();
    const char *argv[] = { "/runtime_manager",
                           "--node_id=",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--runtime_std_log_dir=instances" };
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    auto request1 = std::make_shared<messages::StartInstanceRequest>();
    request1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    request1->mutable_runtimeinstanceinfo()->set_requestid("test_requestID");
    request1->mutable_runtimeinstanceinfo()->set_instanceid("test_instanceID");
    request1->mutable_runtimeinstanceinfo()->set_traceid("test_traceID");
    request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_objectid("stdout");
    request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_deploydir("/dacache/bucket/object");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("posix-custom-runtime");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_entryfile(tmpFilePath);
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert({"LD_LIBRARY_PATH", ""});

    auto response = executor_->StartInstance(request1, {}).Get();
    ASSERT_AWAIT_TRUE([=](){
        auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log");
        return output.IsSome() && (output.Get().find("LD_LIBRARY_PATH=") != std::string::npos);
    });
    auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log").Get();
    EXPECT_TRUE(output.find("LD_LIBRARY_PATH=") != output.npos);
    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
}

TEST_F(RuntimeExecutorTest, SetErrorLD_LIBRARY_PATH)
{
    const std::string tmpFilePath = "/tmp/runtime_executor_entryfile";
    (void)litebus::os::Mkdir(tmpFilePath);
    const std::string bootstrapPath = tmpFilePath + "/bootstrap";
    (void)litebus::os::Rm(bootstrapPath);
    TouchFile(bootstrapPath);
    std::ofstream outfile;
    outfile.open(bootstrapPath);
    outfile << "#!/bin/bash" << std::endl;
    outfile << R"(echo LD_LIBRARY_PATH="${LD_LIBRARY_PATH}")" << std::endl;
    outfile.close();

    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();
    const char *argv[] = { "/runtime_manager",
                           "--node_id=",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--runtime_std_log_dir=instances" };
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    auto request1 = std::make_shared<messages::StartInstanceRequest>();
    request1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    request1->mutable_runtimeinstanceinfo()->set_requestid("test_requestID");
    request1->mutable_runtimeinstanceinfo()->set_instanceid("test_instanceID");
    request1->mutable_runtimeinstanceinfo()->set_traceid("test_traceID");
    request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_objectid("stdout");
    request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_deploydir("/dacache/bucket/object");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("posix-custom-runtime");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_entryfile(tmpFilePath);
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert({"LD_LIBRARY_PATH", "{ABC}:${LD_LIBRARY_PATH"});

    auto response = executor_->StartInstance(request1, {}).Get();
    ASSERT_AWAIT_TRUE([=](){
        auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log");
        return output.IsSome() && (output.Get().find("LD_LIBRARY_PATH=") != std::string::npos);
    });
    auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log").Get();
    EXPECT_TRUE(output.find("LD_LIBRARY_PATH={ABC}:${LD_LIBRARY_PATH") != output.npos);
    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
}

TEST_F(RuntimeExecutorTest, StartPosixCustomInstanceTest)
{
    const std::string tmpFilePath = "/tmp/posix-custom-runtime/";
    (void)litebus::os::Mkdir(tmpFilePath);
    const std::string bootstrapPath = tmpFilePath + "start.sh";
    (void)litebus::os::Rm(bootstrapPath);
    if (!FileExists(bootstrapPath)) {
        int fd = open(bootstrapPath.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IXUSR);
        if (fd > 0) {
            Close(fd);
        } else {
            EXPECT_TRUE(false);
        }
    }
    TouchFile(bootstrapPath);
    std::ofstream outfile;
    outfile.open(bootstrapPath);
    outfile << "#!/bin/bash" << std::endl;
    outfile << R"(echo 111)" << std::endl;
    outfile << R"(env)" << std::endl;
    outfile.close();
    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");

    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();
    const char *argv[] = { "/runtime_manager",
                           "--node_id=",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--runtime_std_log_dir=instances" };
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);
    auto startRequest = std::make_shared<messages::StartInstanceRequest>();
    startRequest->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest->mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("posix-custom-runtime");

    auto posixEnvs = runtimeConfig->mutable_posixenvs();
    posixEnvs->insert({ "ENV_DELEGATE_BOOTSTRAP", "start.sh" });
    posixEnvs->insert({ "ENV_DELEGATE_DOWNLOAD", "/tmp/posix-custom-runtime" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");
    auto deployFilePath = testDeployDir + "/layer/func/test_bucketID/test_objectID";

    auto future = executor_->StartInstance(startRequest, {});
    auto instanceResponse = future.Get();

    ASSERT_AWAIT_TRUE([=]() {
        auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log");
        if (output.IsNone()) {
            return false;
        }
        bool found = false;
        std::istringstream iss(output.Get());
        std::string line;
        while (std::getline(iss, line, '\n')) {
            size_t ldPos = line.find("LD_LIBRARY_PATH=");
            if (ldPos != std::string::npos) {
                std::string value = line.substr(ldPos + 16);  // 16 is lenth of "LD_LIBRARY_PATH="
                size_t colonPos = value.find(':');
                std::string firstPart = value.substr(0, colonPos);
                if (firstPart == deployFilePath) { // deployFilePath lib in LD_LIBRARY_PATH
                    found = true;
                    std::cout << "found: " << firstPart << std::endl;
                }
            }
        }
        return output.IsSome() && (output.Get().find("111") != std::string::npos) && found;
    });
    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
}

TEST_F(RuntimeExecutorTest, InheritEnvTest)
{
    litebus::os::SetEnv("Inherit_env", "123456");
    executor_->config_.inheritEnv = true;
    executor_->config_.pythonDependencyPath = "/python/path";
    executor_->config_.runtimeLogLevel = "DEBUG";
    executor_->config_.runtimePath = "/path/to/python_runtime";
    executor_->config_.runtimeDsConnectTimeout = 10;
    auto env = Envs{};
    env.userEnvs.insert({ "user_env1", "user_env1_value" });
    env.userEnvs.insert({ "user_env2", "user_env2_value" });
    env.userEnvs.insert({ "PYTHONPATH", "/userdefined/pythonpath" });
    env.posixEnvs.insert({YR_TENANT_ID, TEST_TENANT_ID});
    env.posixEnvs.insert({"LD_LIBRARY_PATH", "/usr/posix/path"});
    env.posixEnvs.insert({"PATH", "/usr/local/bin"});
    env.posixEnvs.insert({"YR_WORKING_DIR", "/home/snuser/function/package/file.zip"});
    env.posixEnvs.insert({"UNZIPPED_WORKING_DIR", "/home/sn/function/package/xxx/working_dir/"});
    env.posixEnvs.insert({CONDA_PREFIX, "/usr/local/conda"});
    env.posixEnvs.insert({CONDA_DEFAULT_ENV, "env_name_file"});
    litebus::os::SetEnv("PATH", "/inherit/path");
    litebus::os::SetEnv("YR_FRONTEND_ADDRESS", "127.0.0.1:0");
    auto combineEnv = executor_->CombineEnvs(env);
    EXPECT_EQ(combineEnv["Inherit_env"], "123456");
    EXPECT_EQ(combineEnv["user_env1"], "user_env1_value");
    EXPECT_EQ(combineEnv["user_env2"], "user_env2_value");
    EXPECT_EQ(combineEnv["LD_LIBRARY_PATH"], "/usr/posix/path");
    EXPECT_EQ(combineEnv["YR_FRONTEND_ADDRESS"], "127.0.0.1:0");
    // append UNZIPPED_WORKING_DIR Into PYTHONPATH
    EXPECT_EQ(
        combineEnv["PYTHONPATH"], "/userdefined/pythonpath:/path/to/python_runtime:/python/path:/home/sn/function/package/xxx/working_dir/");
    EXPECT_EQ(combineEnv["PATH"], "/usr/local/bin:/inherit/path");
    EXPECT_EQ(combineEnv.count("UNZIPPED_WORKING_DIR"), 0);

    // inherit overriden by user define
    litebus::os::SetEnv("user_env1", "user_env1_valuexxx");
    // when UNZIPPED_WORKING_DIR is empty
    env.posixEnvs.insert({"UNZIPPED_WORKING_DIR", ""});
    env.posixEnvs.insert({"YR_LOG_LEVEL", "ReleaseXX"}); // user env confilct with framework env pass to runtime
    combineEnv = executor_->CombineEnvs(env);
    EXPECT_EQ(combineEnv["Inherit_env"], "123456");
    EXPECT_EQ(combineEnv["user_env1"], "user_env1_value");
    EXPECT_EQ(combineEnv["user_env2"], "user_env2_value");
    // inherit env
    EXPECT_EQ(combineEnv[YR_TENANT_ID], TEST_TENANT_ID);
    EXPECT_EQ(combineEnv["YR_WORKING_DIR"], "/home/snuser/function/package/file.zip");
    EXPECT_EQ(combineEnv.count("UNZIPPED_WORKING_DIR"), 0);
    EXPECT_EQ(combineEnv["DS_CONNECT_TIMEOUT_SEC"], "10");
    EXPECT_EQ(combineEnv[CONDA_PREFIX], "/usr/local/conda");
    EXPECT_EQ(combineEnv[CONDA_DEFAULT_ENV], "env_name_file");
    EXPECT_EQ(combineEnv["YR_LOG_LEVEL"], "DEBUG"); // framework env pass to runtime

    // YR_NOSET_ASCEND_RT_VISIBLE_DEVICES is set, ASCEND_RT_VISIBLE_DEVICES will delete
    litebus::os::SetEnv("YR_NOSET_ASCEND_RT_VISIBLE_DEVICES", "1");
    env.userEnvs["ASCEND_RT_VISIBLE_DEVICES"] = "0,1";
    combineEnv = executor_->CombineEnvs(env);
    EXPECT_TRUE(combineEnv.find("ASCEND_RT_VISIBLE_DEVICES") == combineEnv.end());

    // YR_NOSET_CUDA_VISIBLE_DEVICES is set, CUDA_VISIBLE_DEVICES will delete
    litebus::os::SetEnv("YR_NOSET_CUDA_VISIBLE_DEVICES", "1");
    env.userEnvs["CUDA_VISIBLE_DEVICES"] = "4,5,6";
    combineEnv = executor_->CombineEnvs(env);
    EXPECT_TRUE(combineEnv.find("CUDA_VISIBLE_DEVICES") == combineEnv.end());
}

TEST_F(RuntimeExecutorTest, InheritEnvFalseTest)
{
    executor_->config_.inheritEnv = false;
    auto env = Envs{};
    litebus::os::SetEnv("YR_FRONTEND_ADDRESS", "127.0.0.1:0");
    litebus::os::SetEnv("POD_FRONTEND_ADDRESS", "127.0.0.1:0");
    auto combineEnv = executor_->CombineEnvs(env);
    EXPECT_EQ(combineEnv["YR_FRONTEND_ADDRESS"], "127.0.0.1:0");
    EXPECT_EQ(combineEnv["POD_FRONTEND_ADDRESS"], "");
}

TEST_F(RuntimeExecutorTest, SeparatedRuntimeStdRedirected)
{
    litebus::ExecIO stdOut = litebus::ExecIO::CreatePipeIO();
    auto stdErr = stdOut;
    std::string runtimeID = "runtime-123456";
    executor_->config_.runtimeLogPath = "/home/snuser";
    executor_->config_.runtimeStdLogDir = "log";
    executor_->ConfigRuntimeRedirectLog(stdOut, stdErr, runtimeID);
    auto out = "/home/snuser/log/" + runtimeID + ".out";
    auto err = "/home/snuser/log/" + runtimeID + ".err";
    EXPECT_EQ(litebus::os::ExistPath(out), true);
    EXPECT_EQ(litebus::os::ExistPath(err), true);
    (void)litebus::os::Rm(out);
    (void)litebus::os::Rm(err);
}

inline void CreatePythonEnvInfoScript(const std::string& entrypointPath)
{
    (void)litebus::os::Rm(entrypointPath);
    TouchFile(entrypointPath);

    std::ofstream outfile;
    outfile.open(entrypointPath);
    outfile << "import sys" << std::endl;
    outfile << "import os" << std::endl;
    outfile << R"(print("Python version:", sys.version))" << std::endl;
    outfile << R"(print("Python executable path:", sys.executable))" << std::endl;
    outfile << R"(print("Python module search path (sys.path):", sys.path))" << std::endl;
    outfile << R"(print("Environment Variables:"))" << std::endl;
    outfile << R"(for key, value in os.environ.items():)" << std::endl;
    outfile << R"(    print(f"{key}={value}"))" << std::endl;
    outfile.close();
}

TEST_F(RuntimeExecutorTest, StartJobEntrypoint_WithoutWorkingDirTest)
{
    const std::string workingDirFile = "/home/snuser/function/package/file.zip";
    const std::string unzipedAppWorkingDir = "/home/sn/function/package/xxx/working_dir/";
    (void)litebus::os::Mkdir(unzipedAppWorkingDir);
    const std::string entrypointPath = unzipedAppWorkingDir + "script.py";
    CreatePythonEnvInfoScript(entrypointPath);

    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();
    const char *argv[] = { "/runtime_manager",
                           "--node_id=",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--runtime_std_log_dir=instances" };
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    auto request1 = std::make_shared<messages::StartInstanceRequest>();
    request1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    request1->mutable_runtimeinstanceinfo()->set_requestid("test_requestID");
    request1->mutable_runtimeinstanceinfo()->set_instanceid("test_instanceID");
    request1->mutable_runtimeinstanceinfo()->set_traceid("test_traceID");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("posix-custom-runtime");
    // without UNZIPPED_WORKING_DIR in posixenvs ->  entry path + '/bootstrap' case
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_entryfile(
        "python3 script.py");

    auto response = executor_->StartInstance(request1, {}).Get();
    EXPECT_EQ(response.code(), RUNTIME_MANAGER_EXECUTABLE_PATH_INVALID);
    (void)litebus::os::Rmdir(unzipedAppWorkingDir);
}

TEST_F(RuntimeExecutorTest, StartJobEntrypointInWorkingDirTest)
{
    (void)litebus::os::Rm("/home/snuser/instances/node1-user_func_std.log");
    const std::string workingDirFile = "/home/snuser/function/package/file.zip";
    const std::string unzipedAppWorkingDir = "/home/sn/function/package/xxx/working_dir/";
    (void)litebus::os::Mkdir(unzipedAppWorkingDir);
    const std::string entrypointPath = unzipedAppWorkingDir + "script.py";
    CreatePythonEnvInfoScript(entrypointPath);

    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();

    const char *argv[] = { "/runtime_manager",
                           "--node_id=node1",
                           "--runtime_ld_library_path=/tmp",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--agent_address=127.0.0.1:1234",
                           "--runtime_std_log_dir=instances",
                           "--data_system_port=24560",
                           "--proxy_grpc_server_port=20258" };
    auto ret = flags.ParseFlags(11, argv);
    ASSERT_TRUE(ret.IsNone()) << ret.Get();
    executor_->SetRuntimeConfig(flags);

    auto request1 = std::make_shared<messages::StartInstanceRequest>();
    request1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    request1->mutable_runtimeinstanceinfo()->set_requestid("test_requestID");
    request1->mutable_runtimeinstanceinfo()->set_instanceid("test_instanceID");
    request1->mutable_runtimeinstanceinfo()->set_traceid("test_traceID");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("posix-custom-runtime");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        { YR_TENANT_ID, TEST_TENANT_ID });
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        {"UNZIPPED_WORKING_DIR", unzipedAppWorkingDir});
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        {"YR_WORKING_DIR", workingDirFile});
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        {"YR_APP_MODE", "true"}); // job submisson
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        {"YR_DS_ADDRESS", "127.0.0.1:24560"});
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        {"YR_SERVER_ADDRESS", "127.0.0.1:20258"});
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        {"LD_LIBRARY_PATH", "${LD_LIBRARY_PATH}:/opt/buildtools/python3.9/lib/"});
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_entryfile(
        "python3 script.py");

    auto response = executor_->StartInstance(request1, {}).Get();
    EXPECT_AWAIT_TRUE([=]() {
        auto output = litebus::os::Read("/home/snuser/instances/node1-user_func_std.log");
        return output.IsSome() && (output.Get().find("Python module search path (sys.path):") != std::string::npos)
               && (output.Get().find("YR_WORKING_DIR") != std::string::npos
                   && output.Get().find(workingDirFile) != std::string::npos)
               && output.Get().find("UNZIPPED_WORKING_DIR") == std::string::npos
               && output.Get().find(YR_TENANT_ID + "=" + TEST_TENANT_ID) != std::string::npos
               && output.Get().find("YR_DS_ADDRESS=127.0.0.1:24560") != std::string::npos
               && output.Get().find("YR_APP_MODE=true") != std::string::npos
               && output.Get().find("YR_SERVER_ADDRESS=127.0.0.1:20258") != std::string::npos;
    });
    auto output = litebus::os::Read("/home/snuser/instances/node1-user_func_std.log").Get();
    YRLOG_DEBUG("output: {}", output);
    EXPECT_TRUE(output.find(unzipedAppWorkingDir) != output.npos); // in PYTHONPATH
    (void)litebus::os::Rmdir(unzipedAppWorkingDir);
    (void)litebus::os::Rm("/home/snuser/instances/node1-user_func_std.log");
}

TEST_F(RuntimeExecutorTest, StartPythonConda_WithWorkingDirTest)
{
    auto path = LookPath("conda");
    if (path.IsNone()) {
        GTEST_SKIP() << "without conda installed";
    }

    system("conda env remove --name env_name_file -y");
    (void)litebus::os::Rm("/home/snuser/instances/node1-user_func_std.log");
    const std::string workingDirFile = "/home/snuser/function/package/file.zip";
    const std::string unzipedAppWorkingDir = "/home/sn/function/package/xxx/working_dir/";
    (void)litebus::os::Mkdir(unzipedAppWorkingDir);
    const std::string entrypointPath = unzipedAppWorkingDir + "script.py";
    CreatePythonEnvInfoScript(entrypointPath);
    (void)litebus::os::Mkdir("/home/snuser/python/fnruntime");
    CreatePythonEnvInfoScript("/home/snuser/python/fnruntime/server.py");

    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();

    const char *argv[] = { "/runtime_manager",
                           "--node_id=node1",
                           "--runtime_ld_library_path=/tmp",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--agent_address=127.0.0.1:1234",
                           "--runtime_std_log_dir=instances",
                           "--data_system_port=24560",
                           "--proxy_grpc_server_port=20258" };
    auto ret = flags.ParseFlags(std::size(argv), argv);
    ASSERT_TRUE(ret.IsNone()) << ret.Get();
    executor_->SetRuntimeConfig(flags);

    auto request1 = std::make_shared<messages::StartInstanceRequest>();
    request1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    request1->mutable_runtimeinstanceinfo()->set_requestid("test_requestID");
    request1->mutable_runtimeinstanceinfo()->set_instanceid("test_instanceID");
    request1->mutable_runtimeinstanceinfo()->set_traceid("test_traceID");
    auto deployConfig = request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(unzipedAppWorkingDir);
    deployConfig->set_storagetype("working_dir");
    auto deployOptions = deployConfig->mutable_deployoptions();
    deployOptions->operator[](CONDA_CONFIG) = testCondaConfig;
    deployOptions->operator[](CONDA_COMMAND) = "conda env create -f env.yaml";
    deployOptions->operator[](CONDA_PREFIX) = "/usr/local/conda";
    deployOptions->operator[](CONDA_DEFAULT_ENV) = "env_name_file";
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("python");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        { YR_TENANT_ID, TEST_TENANT_ID });
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        {"UNZIPPED_WORKING_DIR", unzipedAppWorkingDir});
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        {"YR_WORKING_DIR", workingDirFile});
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        {CONDA_PREFIX, "/usr/local/conda"});
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        {CONDA_DEFAULT_ENV, "env_name_file"});

    auto response = executor_->StartInstance(request1, {}).Get();
    EXPECT_AWAIT_TRUE([=]() {
        auto output = litebus::os::Read("/home/snuser/instances/node1-user_func_std.log");
        return output.IsSome() && (output.Get().find("Python module search path (sys.path):") != std::string::npos)
               && (output.Get().find("YR_WORKING_DIR") != std::string::npos
                   && output.Get().find(workingDirFile) != std::string::npos)
               && output.Get().find("UNZIPPED_WORKING_DIR") == std::string::npos
               && output.Get().find("CONDA_DEFAULT_ENV") != std::string::npos
               && output.Get().find("CONDA_PREFIX") != std::string::npos
               && output.Get().find(YR_TENANT_ID + "=" + TEST_TENANT_ID) != std::string::npos;
    });
    (void)litebus::os::Rm("/home/snuser/instances/node1-user_func_std.log");
    (void)litebus::os::Rmdir("/home/snuser/python/fnruntime");
    (void)litebus::os::Rmdir(unzipedAppWorkingDir);
}

TEST_F(RuntimeExecutorTest, StartJobEntrypoint_InvalidWorkingDirTest)
{
    const std::string unzipedAppWorkingDir = "/home/sn/function/package/xxxy/working_dir/";
    const std::string workingDirFile = "/home/sn/function/package/file.zip";

    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();

    const char *argv[] = { "/runtime_manager",
                           "--node_id=",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--runtime_std_log_dir=instances" };
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    auto request1 = std::make_shared<messages::StartInstanceRequest>();
    request1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    request1->mutable_runtimeinstanceinfo()->set_requestid("test_requestID");
    request1->mutable_runtimeinstanceinfo()->set_instanceid("test_instanceID");
    request1->mutable_runtimeinstanceinfo()->set_traceid("test_traceID");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("posix-custom-runtime");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        {"UNZIPPED_WORKING_DIR", unzipedAppWorkingDir});
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs()->insert(
        {"YR_WORKING_DIR", workingDirFile});
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_entryfile(
        "python3 script.py");

    auto response = executor_->StartInstance(request1, {}).Get();
    EXPECT_EQ(response.code(), RUNTIME_MANAGER_WORKING_DIR_FOR_APP_NOTFOUND);
}

TEST_F(RuntimeExecutorTest, TestStartRuntimeStdRedirection)
{
    (void)litebus::os::Rm("/home/snuser/instances/test_runtime_id.out");
    runtime_manager::Flags flags;
    const char *argv[] = { "/runtime_manager", "--node_id=testid", "--ip=127.0.0.1","--host_ip=127.0.0.1", "--port=32233",
                           "--runtime_initial_port=500", "--runtime_std_log_dir=instances",
                           "--runtime_direct_connection_enable=false",
                           "--enable_separated_redirect_runtime_std=true"};
    flags.ParseFlags(9, argv);
    executor_->SetRuntimeConfig(flags);
    auto stdOut = litebus::ExecIO::CreatePipeIO();
    auto stdOErr = stdOut;
    litebus::Try<std::shared_ptr<litebus::Exec>> s = litebus::Exec::CreateExec(
            "echo start; cp a b; echo end;", litebus::None(), litebus::ExecIO::CreateFDIO(STDIN_FILENO),
            stdOut, stdOErr);
    executor_->StartRuntimeStdRedirection("test_runtime_id", s.Get()->GetOut(), s.Get()->GetErr());
    ASSERT_AWAIT_TRUE([=]() {
        auto output = litebus::os::Read("/home/snuser/instances/test_runtime_id.out");
        return output.IsSome() &&
               (output.Get().find("start") != std::string::npos) &&
               (output.Get().find("No such file or directory") != std::string::npos) &&
               (output.Get().find("end") != std::string::npos);
    });
    (void)litebus::os::Rm("/home/snuser/instances/test_runtime_id.out");
}

// Note: this case run more than 30s, set `export NOT_SKIP_LONG_TESTS=1` when run it, and not run on CI by default
TEST_F(RuntimeExecutorTest, EpollRedirectorStdLogRollingCompressTest)
{
    const char* skip_test = std::getenv("NOT_SKIP_LONG_TESTS");
    if (skip_test == nullptr || std::string(skip_test) != "1") {
        GTEST_SKIP() << "Long-running tests are skipped by default";
    }

    // 1.init StdRedirectParam
    std::string command = "rm -rf /home/snuser/instances/test_runtime_id*";
    (void)std::system(command.c_str());
    executor_->config_.userLogBufferFlushThreshold = 1024;
    executor_->config_.userLogAutoFlushIntervalMs = 10;
    executor_->config_.userLogRollingSizeLimitMb = 1;
    executor_->config_.userLogRollingFileCountLimit = 3;

    // 2.StartRuntimeStdRedirection
    runtime_manager::Flags flags;
    const char *argv[] = { "/runtime_manager", "--node_id=testid", "--ip=127.0.0.1","--host_ip=127.0.0.1", "--port=32233",
                           "--runtime_initial_port=500", "--runtime_std_log_dir=instances",
                           "--runtime_direct_connection_enable=false",
                           "--enable_separated_redirect_runtime_std=true"};
    flags.ParseFlags(9, argv);
    executor_->SetRuntimeConfig(flags);
    auto stdOut = litebus::ExecIO::CreatePipeIO();
    auto stdErr = stdOut;
    litebus::Try<std::shared_ptr<litebus::Exec>> s = litebus::Exec::CreateExec(
            "for i in {1..15000}; do echo output1; cp a b; cp a b; done", litebus::None(),
            litebus::ExecIO::CreateFDIO(STDIN_FILENO), stdOut, stdErr);
    executor_->StartRuntimeStdRedirection("test_runtime_id", s.Get()->GetOut(), s.Get()->GetErr());

    // 3.validate
    sleep(30);
    {
        // find rolling compression log files
        std::string command = "ls /home/snuser/instances/test_runtime_id* | wc -l";
        auto result = ExecuteCommand(command);
        if (!result.error.empty()) {
            YRLOG_ERROR("execute command {} failed, error: {}", command, result.error);
            return;
        }

        YRLOG_INFO("command {} output is {}", command, result.output);
        std::istringstream iss(result.output);
        std::string line;
        std::getline(iss, line);
        std::istringstream linestream(line);
        int count;
        linestream >> count;
        EXPECT_FALSE(line.empty());
        EXPECT_TRUE(count == 1);
    }

    {
        // find rolling compression log files
        std::string command = "ls /home/snuser/instances/test_runtime_id*.out.gz | wc -l";
        auto result = ExecuteCommand(command);
        if (!result.error.empty()) {
            YRLOG_ERROR("execute command {} failed, error: {}", command, result.error);
            return;
        }

        YRLOG_INFO("command {} output is {}", command, result.output);
        std::istringstream iss(result.output);
        std::string line;
        std::getline(iss, line);
        std::istringstream linestream(line);
        int count;
        linestream >> count;
        EXPECT_FALSE(line.empty());
        EXPECT_TRUE(count > 0);
    }

    // std::string command = "rm -rf /home/snuser/instances/testid-user_func_std*";
    // (void)std::system(command.c_str());
}

TEST_F(RuntimeExecutorTest, SetRuntimeEnv_runtime_direct_connection_enable_false)
{
    const std::string tmpFilePath = "/tmp/runtime_executor_entryfile";
    (void)litebus::os::Mkdir(tmpFilePath);
    const std::string bootstrapPath = tmpFilePath + "/bootstrap";
    (void)litebus::os::Rm(bootstrapPath);
    TouchFile(bootstrapPath);
    std::ofstream outfile;
    outfile.open(bootstrapPath);
    outfile << "#!/bin/bash" << std::endl;
    outfile << R"(echo subprocess env:\"$(env)\")" << std::endl;
    outfile.close();

    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();

    const char *argv[] = { "/runtime_manager",
                           "--node_id=",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--runtime_std_log_dir=instances",
                           "--runtime_direct_connection_enable=false" };
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    auto request1 = std::make_shared<messages::StartInstanceRequest>();
    request1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    request1->mutable_runtimeinstanceinfo()->set_requestid("test_requestID");
    request1->mutable_runtimeinstanceinfo()->set_instanceid("test_instanceID");
    request1->mutable_runtimeinstanceinfo()->set_traceid("test_traceID");
    request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_objectid("stdout");
    request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_deploydir("/dacache/bucket/object");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("posix-custom-runtime");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_entryfile(tmpFilePath);

    auto response = executor_->StartInstance(request1, {}).Get();
    ASSERT_AWAIT_TRUE([=]() {
        auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log");
        return output.IsSome() && (output.Get().find("subprocess env:") != std::string::npos);
    });
    auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log").Get();
    EXPECT_TRUE(output.find("RUNTIME_DIRECT_CONNECTION_ENABLE") == output.npos);
    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
}

TEST_F(RuntimeExecutorTest, SetRuntimeEnv_runtime_direct_connection_enable_servermode_false)
{
    const std::string tmpFilePath = "/tmp/runtime_executor_entryfile";
    (void)litebus::os::Mkdir(tmpFilePath);
    const std::string bootstrapPath = tmpFilePath + "/bootstrap";
    (void)litebus::os::Rm(bootstrapPath);
    TouchFile(bootstrapPath);
    std::ofstream outfile;
    outfile.open(bootstrapPath);
    outfile << "#!/bin/bash" << std::endl;
    outfile << R"(echo subprocess env:\"$(env)\")" << std::endl;
    outfile.close();

    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();

    const char *argv[] = { "/runtime_manager",
                           "--node_id=",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--runtime_std_log_dir=instances",
                           "--runtime_direct_connection_enable=true" };
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    auto request1 = std::make_shared<messages::StartInstanceRequest>();
    request1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    request1->mutable_runtimeinstanceinfo()->set_requestid("test_requestID");
    request1->mutable_runtimeinstanceinfo()->set_instanceid("test_instanceID");
    request1->mutable_runtimeinstanceinfo()->set_traceid("test_traceID");
    request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_objectid("stdout");
    request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_deploydir("/dacache/bucket/object");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("posix-custom-runtime");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_entryfile(tmpFilePath);
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_tlsconfig()->set_enableservermode(false);

    auto response = executor_->StartInstance(request1, {}).Get();
    ASSERT_AWAIT_TRUE([=]() {
        auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log");
        return output.IsSome() && (output.Get().find("subprocess env:") != std::string::npos);
    });
    auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log").Get();
    EXPECT_TRUE(output.find("RUNTIME_DIRECT_CONNECTION_ENABLE=true") != output.npos);
    EXPECT_TRUE(output.find("DERICT_RUNTIME_SERVER_PORT=600") != output.npos);
    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
}

TEST_F(RuntimeExecutorTest, SetRuntimeEnv_runtime_direct_connection_enable_tls_servermode_true)
{
    const std::string tmpFilePath = "/tmp/runtime_executor_entryfile";
    (void)litebus::os::Mkdir(tmpFilePath);
    const std::string bootstrapPath = tmpFilePath + "/bootstrap";
    (void)litebus::os::Rm(bootstrapPath);
    TouchFile(bootstrapPath);
    std::ofstream outfile;
    outfile.open(bootstrapPath);
    outfile << "#!/bin/bash" << std::endl;
    outfile << R"(echo subprocess env:\"$(env)\")" << std::endl;
    outfile.close();

    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();

    const char *argv[] = { "/runtime_manager",
                           "--node_id=",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--runtime_std_log_dir=instances",
                           "--runtime_direct_connection_enable=true" };
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    auto request1 = std::make_shared<messages::StartInstanceRequest>();
    request1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    request1->mutable_runtimeinstanceinfo()->set_requestid("test_requestID");
    request1->mutable_runtimeinstanceinfo()->set_instanceid("test_instanceID");
    request1->mutable_runtimeinstanceinfo()->set_traceid("test_traceID");
    request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_objectid("stdout");
    request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_deploydir("/dacache/bucket/object");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("posix-custom-runtime");
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_entryfile(tmpFilePath);
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_tlsconfig()->set_enableservermode(true);
    request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_tlsconfig()->set_posixport("99999");

    auto response = executor_->StartInstance(request1, {}).Get();
    ASSERT_AWAIT_TRUE([=]() {
        auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log");
        return output.IsSome() && (output.Get().find("subprocess env:") != std::string::npos);
    });
    auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log").Get();
    EXPECT_TRUE(output.find("RUNTIME_DIRECT_CONNECTION_ENABLE=true") != output.npos);
    EXPECT_TRUE(output.find("DERICT_RUNTIME_SERVER_PORT=600") != output.npos);
    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
}

TEST_F(RuntimeExecutorTest, SetRuntimeEnv_runtime_direct_connection_enable_tls_servermode_false_error)
{
    const std::string tmpFilePath = "/tmp/runtime_executor_entryfile";
    (void)litebus::os::Mkdir(tmpFilePath);
    const std::string bootstrapPath = tmpFilePath + "/bootstrap";
    (void)litebus::os::Rm(bootstrapPath);
    TouchFile(bootstrapPath);
    std::ofstream outfile;
    outfile.open(bootstrapPath);
    outfile << "#!/bin/bash" << std::endl;
    outfile << R"(echo subprocess env:\"$(env)\")" << std::endl;
    outfile.close();

    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();

    const char *argv[] = { "/runtime_manager",
                           "--node_id=",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--port_num=10",
                           "--runtime_std_log_dir=instances",
                           "--runtime_direct_connection_enable=true" };
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    for (int k = 0; k < 11; k++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        auto request1 = std::make_shared<messages::StartInstanceRequest>();
        request1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
        request1->mutable_runtimeinstanceinfo()->set_requestid("test_requestID");
        request1->mutable_runtimeinstanceinfo()->set_instanceid("test_instanceID");
        request1->mutable_runtimeinstanceinfo()->set_traceid("test_traceID");
        request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_objectid("stdout");
        request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_deploydir("/dacache/bucket/object");
        request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("posix-custom-runtime");
        request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_entryfile(tmpFilePath);

        auto response = executor_->StartInstance(request1, {}).Get();
        ASSERT_AWAIT_TRUE(([k, response]() {
            auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log");
            if (k < 10) {
                return output.IsSome() &&
                       (output.Get().find("RUNTIME_DIRECT_CONNECTION_ENABLE=true") != std::string::npos);
            }
            return response.code() ==
                   RUNTIME_MANAGER_PORT_UNAVAILABLE;  // port resource is not available, can not start instanceID
        }));
    }
    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
    // resume port resource
    PortManager::GetInstance().InitPortResource(INITIAL_PORT, PORT_NUM);
}

TEST_F(RuntimeExecutorTest, SetRuntimeEnv_runtime_direct_connection_enable_tls_servermode_true_error)
{
    const std::string tmpFilePath = "/tmp/runtime_executor_entryfile";
    (void)litebus::os::Mkdir(tmpFilePath);
    const std::string bootstrapPath = tmpFilePath + "/bootstrap";
    (void)litebus::os::Rm(bootstrapPath);
    TouchFile(bootstrapPath);
    std::ofstream outfile;
    outfile.open(bootstrapPath);
    outfile << "#!/bin/bash" << std::endl;
    outfile << R"(echo subprocess env:\"$(env)\")" << std::endl;
    outfile.close();

    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
    runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();

    const char *argv[] = { "/runtime_manager",
                           "--node_id=",
                           "--ip=127.0.0.1",
                           "--host_ip=127.0.0.1",
                           port,
                           "--runtime_initial_port=500",
                           "--port_num=10",
                           "--runtime_std_log_dir=instances",
                           "--runtime_direct_connection_enable=true" };
    flags.ParseFlags(std::size(argv), argv);
    executor_->SetRuntimeConfig(flags);

    for (int k = 0; k < 11; k++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        auto request1 = std::make_shared<messages::StartInstanceRequest>();
        request1->set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
        request1->mutable_runtimeinstanceinfo()->set_requestid("test_requestID");
        request1->mutable_runtimeinstanceinfo()->set_instanceid("test_instanceID");
        request1->mutable_runtimeinstanceinfo()->set_traceid("test_traceID");
        request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_objectid("stdout");
        request1->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_deploydir("/dacache/bucket/object");
        request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("posix-custom-runtime");
        request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_entryfile(tmpFilePath);
        request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_tlsconfig()->set_enableservermode(
            true);
        request1->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_tlsconfig()->set_posixport("99999");

        auto response = executor_->StartInstance(request1, {}).Get();
        ASSERT_AWAIT_TRUE([k]() {
            const auto output = litebus::os::Read("/home/snuser/instances/-user_func_std.log");
            if (k < 11) { // 11th ok
                return output.IsSome() &&
                       (output.Get().find("RUNTIME_DIRECT_CONNECTION_ENABLE=true") != std::string::npos);
            }
            return false;
        });
    }
    (void)litebus::os::Rm("/home/snuser/instances/-user_func_std.log");
}
}  // namespace functionsystem::test
