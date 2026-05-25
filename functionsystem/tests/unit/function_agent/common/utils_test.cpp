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

#include "function_agent/common/utils.h"

#include <gtest/gtest.h>

#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <utils/os_utils.hpp>
#include "common/utils/sensitive_value.h"
#include "common/logs/logging.h"
#include "common/hex/hex.h"
#include "function_agent/common/constants.h"

namespace functionsystem::test {

const std::string TEST_ENV_KEY =
    "de8b633fdc18e4b530fb3161:"
    "40bea9070402bd31c80b1616362c1579515f68d39e491932ee99e79008d8d402e49a5a10e3b5d54946d4e4fd3ed9aee29cc89850372f67"
    "c8e18a63b49d5d493b73469641a3c4729aec985308127857d6";
const std::string TEST_ENV_INFO =
    "{\"func-FAAS_FUNCTION_LANGUAGE\":\"python3.8\",\"func-FAAS_FUNCTION_LD_LIBRARY_PATH\":\"/lib64:/usr/lib64\","
    "\"func-FAAS_FUNCTION_MEMORY\":\"500\",\"func-FAAS_FUNCTION_PYTHON_PATH\":\"/usr/lib/python3.7/lib-dynload:"
    "/usr/local/lib/python3.7/dist-packages:/usr/local/lib/python3.7/dist-packages/pip-20.1.1-py3.7.egg:"
    "/usr/lib/python3/dist-packages\",\"func-FAAS_FUNCTION_REGION\":\"cn\",\"func-FAAS_FUNCTION_TIMEZONE\":"
    "\"Asia/Shanghai\",\"func-adminFuncLoad\":\"true\",\"func-stream\":\"true\"}";
const std::string TEST_BUCKET_ID = "testBucketID";
const std::string TEST_OBJECT_ID = "testObjectID";

const std::string K1_HEX_STR =
    "f48f9d5a9706088947ac438ebe005aa26c9370579f2231c538b28894a315562182da0eb18002c86728c4cdc0df5efb19e1c2060e93"
    "370fd891d4f3d9e5b2b61376643f86d0210ce996446a985759b15112037a5a2f6463cf5fd6afc7ff30fe814bf960eb0c16c5059407"
    "c74d6a93a8b3110405cbc935dff672da3b648d62e0d5cecd91bc7063211e6b33210afb6899e8322eabffe167318a5ac5d591aa7579"
    "efd37e9e4c7fcf390e97c1151b7c1bf00b4a18764a1a0cac1fda1ea6389b39d755127f0e5bc072e6d5936738be1585535dc63b71ad"
    "58686f71c821325009de36bdbac31c1c044845bd1bb41230ec9815695ef3f9e7143a16410113ff3286147a76";
const std::string K2_HEX_STR =
    "5d3da9f432be72b34951c737053eb2c816aaccae2b390d092046288aa5ce2cc5b16529f8197de316303735fbc0c041ccc3885b9be5"
    "fef4933b6806febb940b6bb609b3bf1d1501110e3ba62c6d8b2cf4388a08a8e123a3cea96daec619fbca177bdf092461f5701b02e5"
    "af83ddf0f6ce40deb279cda3ec7d6805237d229e26e30555f3dd890b7306b42bdef0ca1f963dbe25cd00d75018ab3216fcd3b7002b"
    "8a493d015306bf264cca12718890ef11c8d9e54721ebd6bdecab6c7084442f45611f249d9b5d703414770a46380d0b97c018718524"
    "1e9b6187c8168414370649fe6e7afef83a0df645424c4b6c0631dc3ef50c30af37eda905a1886ca12474c68a";
const std::string K3_HEX_STR =
    "43b0d158d9dcf4ffd416eb4e6a89d1b7a66d595c43329bb5c1c66d5befe33c37f31da53aaf539e43238457c46e1f28339cb9dda461c71c"
    "0ea2dba3dc8006684ff0d8d59ee2192582983c155e400d5b7cadcb65bbe682e61d175af54549796e447f3174b95f1f50998ae7785b5c0c"
    "359746e1ee6eeb989284fbe9e0f801ce5a7267285afbab7694c0e8434d6b86991298a46039de4d1fbfd824b8337b11c2d0b2f30ed4d463"
    "12e315cd9042abddc09ea73169f9e1f5baa496d44ed5cac9659cab076212499ef09a56db69e7444d665195a0562a7c82d176d027b0ecc7"
    "f4a26215e003fd463bf3911633baf85ee98f9187357a65ee2869b3d93a3871d830b4034e";
const std::string SALT_HEX_STR =
    "37a1b37efbb9bb6beadb4446f40aa2c4bcaeb298192fa390ed03ee65bfcd54e55da39bae9961b9fa0d4b89591e41eed835ed01cca3"
    "15eab75ebaf8a9e7b02287a468ec6d0c61f9f8e4d58dad90fb8a6a13bee7fe4685dbb535bfdb7e76b328d66b4d4bc7aa48791b205d"
    "1d2f2ef176f2b5b80a8ddc34ed9514372130eb896bc18745facf059a7fa37ef5e2ef413d0030f5bca581055eb3b3565dca642651cb"
    "802530e2e4964ab3c8a37370adfd65c80483398a1a8668caed455deabae0dbae7fb2bcdeeee4c2a2d9431ed93c6527985ef6841276"
    "91904c799e13f37daeb1cb7ebfb0904d61796362514e521ac0fed682fd952ca3e9ce9a7a4407aaaa44f8aab6";

const std::string PARSED_JSON =
    "{\"func-FAAS_FUNCTION_LANGUAGE\":\"python3.8\",\"func-FAAS_FUNCTION_LD_LIBRARY_PATH\":\"/lib64:/usr/"
    "lib64\",\"func-FAAS_FUNCTION_MEMORY\":\"500\",\"func-FAAS_FUNCTION_PYTHON_PATH\":\"/usr/lib/python3.7/"
    "lib-dynload:/usr/local/lib/python3.7/dist-packages:/usr/local/lib/python3.7/dist-packages/pip-20.1.1-py3.7.egg:/"
    "usr/lib/python3/dist-packages\",\"func-FAAS_FUNCTION_REGION\":\"cn\",\"func-FAAS_FUNCTION_TIMEZONE\":\"Asia/"
    "Shanghai\",\"func-adminFuncLoad\":\"true\",\"func-stream\":\"true\"}";

const std::string RUNTIME_ENV_PREFIX = "func-";

class FunctionAgentUtilsTest : public ::testing::Test {
public:
    void SetUp() override
    {
    }

    void TearDown() override
    {
    }

    static std::string LoadRootKey(const std::string &k1HexStr, const std::string &k2HexStr,
                                   const std::string &saltHexStr, const std::string &k3HexStr)
    {
        char path[RESOURCE_PATH_MAX_SIZE];
        (void)getcwd(path, sizeof(path));
        auto resourcePath = litebus::os::Join(std::string(path), RESOURCE_DIRECTORY);

        std::string k1DirPath = JoinDirPath(resourcePath, RDO, ROOT_KEY_VERSION, APPLE);
        (void)litebus::os::Mkdir(k1DirPath);
        std::ofstream outfileA(litebus::os::Join(k1DirPath, A_TXT));
        outfileA << k1HexStr;
        outfileA.close();

        std::string k2DirPath = JoinDirPath(resourcePath, RDO, ROOT_KEY_VERSION, BOY);
        (void)litebus::os::Mkdir(k2DirPath);
        std::ofstream outfileB(litebus::os::Join(k2DirPath, B_TXT));
        outfileB << k2HexStr;
        outfileB.close();

        std::string saltDirPath = JoinDirPath(resourcePath, RDO, ROOT_KEY_VERSION, DOG);
        (void)litebus::os::Mkdir(saltDirPath);
        std::ofstream outfileD(litebus::os::Join(saltDirPath, D_TXT));
        outfileD << saltHexStr;
        outfileD.close();

        std::string k3DirPath = JoinDirPath(resourcePath, RDO, ROOT_KEY_VERSION, EGG);
        (void)litebus::os::Mkdir(k3DirPath);
        std::ofstream outfileE(litebus::os::Join(k3DirPath, E_TXT));
        outfileE << k3HexStr;
        outfileE.close();

        return resourcePath;
    }

protected:
    static std::string JoinDirPath(const std::string &resourcePath, const std::string &rdoName,
                                   const std::string &rdoVersion, const std::string &subPathName)
    {
        std::string rdoPath = litebus::os::Join(resourcePath, rdoName, '/');
        std::string versionPath = litebus::os::Join(rdoPath, rdoVersion, '/');
        std::string subPath = litebus::os::Join(versionPath, subPathName, '/');
        return subPath;
    }

private:
};

TEST_F(FunctionAgentUtilsTest, SetDeployRequestConfigSuccess)
{
    auto deployInstanceRequest = std::make_shared<functionsystem::messages::DeployInstanceRequest>();
    functionsystem::messages::CodePackageThresholds codePackageThresholds;
    deployInstanceRequest->set_language(function_agent::JAVA_LANGUAGE);
    deployInstanceRequest->mutable_funcdeployspec()->set_bucketurl("https://**.cn:***");
    auto deployRequest = function_agent::SetDeployRequestConfig(deployInstanceRequest, nullptr);
    EXPECT_EQ(deployRequest->runtimeconfig().language(), function_agent::JAVA_LANGUAGE);
    EXPECT_EQ(deployRequest->deploymentconfig().bucketurl(), "https://**.cn:***");
}

TEST_F(FunctionAgentUtilsTest, SetRuntimeConfigSuccess)
{
    auto resourcePath = FunctionAgentUtilsTest::LoadRootKey(K1_HEX_STR, K2_HEX_STR, SALT_HEX_STR, K3_HEX_STR);

    functionsystem::messages::FuncDeploySpec funcDeploySpec;
    funcDeploySpec.set_deploydir("/tmp/home");
    funcDeploySpec.set_bucketid(TEST_BUCKET_ID);
    funcDeploySpec.set_objectid(TEST_OBJECT_ID);

    auto deployInstanceRequest = std::make_shared<functionsystem::messages::DeployInstanceRequest>();
    deployInstanceRequest->set_entryfile(TEST_OBJECT_ID + "/test");
    deployInstanceRequest->set_tenantid("Test_TenantID");
    deployInstanceRequest->mutable_funcdeployspec()->CopyFrom(funcDeploySpec);
    deployInstanceRequest->set_envkey(TEST_ENV_KEY);
    deployInstanceRequest->set_envinfo(TEST_ENV_INFO);
    auto runtimeConfig = function_agent::SetRuntimeConfig(deployInstanceRequest);

    EXPECT_EQ(runtimeConfig.userenvs().size(), static_cast<uint32_t>(8));
    EXPECT_EQ(runtimeConfig.entryfile(), TEST_OBJECT_ID + "/test");
    deployInstanceRequest->set_language(function_agent::JAVA_LANGUAGE);
    runtimeConfig = function_agent::SetRuntimeConfig(deployInstanceRequest);
    EXPECT_EQ(runtimeConfig.entryfile(), TEST_OBJECT_ID + "/test");

    nlohmann::json val = nlohmann::json::object();
    val["LD_LIBRARY_PATH"] = R"(${LD_LIBRARY_PATH}:${FUNCTION_LIB_PATH}/depend)";
    val[DELEGATE_CONTAINER_ID_KEY] = R"(container_id_error)";
    std::cout << val.dump() << std::endl;
    (*deployInstanceRequest->mutable_createoptions())["DELEGATE_ENV_VAR"] = val.dump();
    (*deployInstanceRequest->mutable_createoptions())[DELEGATE_CONTAINER_ID_KEY] =
        "container_id";  // can not be override by user envs
    runtimeConfig = function_agent::SetRuntimeConfig(deployInstanceRequest);
    EXPECT_TRUE(runtimeConfig.posixenvs().find(DELEGATE_CONTAINER_ID_KEY) != runtimeConfig.userenvs().end());
    EXPECT_EQ(runtimeConfig.posixenvs().find(DELEGATE_CONTAINER_ID_KEY)->second, "container_id");
    EXPECT_TRUE(runtimeConfig.posixenvs().find(YR_TENANT_ID) != runtimeConfig.userenvs().end());
    EXPECT_EQ(runtimeConfig.posixenvs().find(YR_TENANT_ID)->second, "Test_TenantID");
    // not set path
    EXPECT_FALSE(runtimeConfig.subdirectoryconfig().isenable());

    // set path not set quota
    (*deployInstanceRequest->mutable_createoptions())[function_agent::DELEGATE_DIRECTORY_INFO] = "/parentDir";
    (*deployInstanceRequest->mutable_createoptions())[function_agent::DELEGATE_DIRECTORY_QUOTA] = "";
    runtimeConfig = function_agent::SetRuntimeConfig(deployInstanceRequest);
    EXPECT_TRUE(runtimeConfig.subdirectoryconfig().isenable());
    EXPECT_EQ(runtimeConfig.subdirectoryconfig().parentdirectory(), "/parentDir");
    EXPECT_EQ(runtimeConfig.subdirectoryconfig().quota(), 512);

    // set path, set illegal quota
    (*deployInstanceRequest->mutable_createoptions())[function_agent::DELEGATE_DIRECTORY_QUOTA] = "-1";
    runtimeConfig = function_agent::SetRuntimeConfig(deployInstanceRequest);
    EXPECT_TRUE(runtimeConfig.subdirectoryconfig().isenable());
    EXPECT_EQ(runtimeConfig.subdirectoryconfig().parentdirectory(), "/parentDir");
    EXPECT_EQ(runtimeConfig.subdirectoryconfig().quota(), -1);

    (*deployInstanceRequest->mutable_createoptions())[function_agent::DELEGATE_DIRECTORY_QUOTA] = "-2";
    runtimeConfig = function_agent::SetRuntimeConfig(deployInstanceRequest);
    EXPECT_TRUE(runtimeConfig.subdirectoryconfig().isenable());
    EXPECT_EQ(runtimeConfig.subdirectoryconfig().parentdirectory(), "/parentDir");
    EXPECT_EQ(runtimeConfig.subdirectoryconfig().quota(), 512);

    (*deployInstanceRequest->mutable_createoptions())[function_agent::DELEGATE_DIRECTORY_QUOTA] = "1048577";
    runtimeConfig = function_agent::SetRuntimeConfig(deployInstanceRequest);
    EXPECT_TRUE(runtimeConfig.subdirectoryconfig().isenable());
    EXPECT_EQ(runtimeConfig.subdirectoryconfig().parentdirectory(), "/parentDir");
    EXPECT_EQ(runtimeConfig.subdirectoryconfig().quota(), 512);

    // set path, monopoly scene, normal quota
    (*deployInstanceRequest->mutable_createoptions())[function_agent::DELEGATE_DIRECTORY_QUOTA] = "355";
    (*deployInstanceRequest->mutable_createoptions())[function_agent::DELEGATE_DIRECTORY_INFO] = "/tmp";
    (*deployInstanceRequest->mutable_scheduleoption()).set_schedpolicyname("monopoly");
    runtimeConfig = function_agent::SetRuntimeConfig(deployInstanceRequest);
    EXPECT_TRUE(runtimeConfig.subdirectoryconfig().isenable());
    EXPECT_EQ(runtimeConfig.subdirectoryconfig().parentdirectory(), "/tmp");
    EXPECT_EQ(runtimeConfig.subdirectoryconfig().quota(), 355);

    // set disk mount point
    auto diskEnvKey = RUNTIME_ENV_PREFIX + resource_view::DISK_MOUNT_POINT;
    (*deployInstanceRequest->mutable_createoptions())[diskEnvKey] = "/tmp/abc/";
    runtimeConfig = function_agent::SetRuntimeConfig(deployInstanceRequest);
    EXPECT_TRUE(runtimeConfig.userenvs().find(diskEnvKey) != runtimeConfig.userenvs().end());
    (void)litebus::os::Rmdir(resourcePath);
}

TEST_F(FunctionAgentUtilsTest, SetRuntimeConfigWithMountConfig)
{
    auto deployInstanceRequest = std::make_shared<functionsystem::messages::DeployInstanceRequest>();
    deployInstanceRequest->set_entryfile(TEST_OBJECT_ID + "/test");
    (*deployInstanceRequest->mutable_createoptions())[DELEGATE_MOUNT] = R"(
{
	"mount_user": {
		"user_id": 1004,
		"user_group_id": 1004
	},
	"func_mounts": [{
		"id": "ccc6f799-96f9-4f47-9d67-ce4d267d90b9",
		"mount_type": "sfs",
		"mount_resource": "eb4ebf7a-db82-4602-82ce-7e1e57a8ef46",
		"mount_share_path": "sfs-nas01.test.com:/share-77644e2e",
		"local_mount_path": "/home/fs",
		"status": "active"
	}]
}
)";
    auto runtimeConfig = function_agent::SetRuntimeConfig(deployInstanceRequest);
    EXPECT_EQ(runtimeConfig.funcmountconfig().funcmountuser().userid(), 1004);
    EXPECT_EQ(runtimeConfig.funcmountconfig().funcmounts().size(), 1);

    (*deployInstanceRequest->mutable_createoptions())[DELEGATE_MOUNT] = R"(
{
	"mount_user": {
		"user_id": "",
		"user_group_id": ""
	},
	"func_mounts": [{
		"id": "ccc6f799-96f9-4f47-9d67-ce4d267d90b9",
		"mount_type": "sfs",
		"mount_resource": "eb4ebf7a-db82-4602-82ce-7e1e57a8ef46",
		"mount_share_path": "sfs-nas01.test.com:/share-77644e2e",
		"local_mount_path": "/home/fs",
		"status": "active"
	}]
}
)";
    runtimeConfig = function_agent::SetRuntimeConfig(deployInstanceRequest);
    EXPECT_EQ(runtimeConfig.funcmountconfig().funcmountuser().userid(), 0);
}

TEST_F(FunctionAgentUtilsTest, SetStartRuntimeInstanceRequestConfigSuccess)
{
    const std::string requestId = "job-de930e46-task-9603b5de-090c-4fe0-89fa-94307a3ad4ce-97da54ee-0";
    auto startInstanceRequest = std::make_unique<messages::StartInstanceRequest>();
    auto *runtimeInstanceInfo = new messages::RuntimeInstanceInfo;
    auto *runtimeConfig = new messages::RuntimeConfig;
    runtimeConfig->set_language(function_agent::JAVA_LANGUAGE);
    runtimeInstanceInfo->set_allocated_runtimeconfig(runtimeConfig);
    startInstanceRequest->set_allocated_runtimeinstanceinfo(runtimeInstanceInfo);

    auto deployInstanceRequest = std::make_shared<functionsystem::messages::DeployInstanceRequest>();
    deployInstanceRequest->set_language(function_agent::JAVA_LANGUAGE);
    deployInstanceRequest->set_requestid(requestId);

    function_agent::SetStartRuntimeInstanceRequestConfig(startInstanceRequest, deployInstanceRequest);
    EXPECT_EQ(startInstanceRequest->runtimeinstanceinfo().requestid(), requestId);
}

TEST_F(FunctionAgentUtilsTest, SetStopRuntimeInstanceRequestSuccess)
{
    auto runtimeID = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    messages::StopInstanceRequest stopInstanceRequest;
    auto req = std::make_shared<messages::KillInstanceRequest>();
    req->set_runtimeid(runtimeID);

    function_agent::SetStopRuntimeInstanceRequest(stopInstanceRequest, req);
    EXPECT_EQ(stopInstanceRequest.runtimeid(), runtimeID);
}

/**
 * Feature: FieldFuncTest
 * Description: Test String Field
 * Steps:
 * Expectation:
 */
TEST_F(FunctionAgentUtilsTest, FieldTest)
{
    // given
    std::string str = "  123 abc   456  efg";

    // want
    std::vector<std::string> want = { "123", "abc", "456", "efg" };

    // got
    std::vector<std::string> got = function_agent::Field(str, ' ');

    EXPECT_TRUE(want.size() == got.size());
    for (size_t i = 0; i < want.size(); i++) {
        EXPECT_TRUE(want[i] == got[i]);
    }
}

TEST_F(FunctionAgentUtilsTest, ParseJsonSuccess)
{
    messages::RuntimeConfig runtimeConf;
    functionsystem::function_agent::ParseEnvInfoJson(PARSED_JSON, runtimeConf);
    EXPECT_EQ((*runtimeConf.mutable_userenvs())["func-FAAS_FUNCTION_LANGUAGE"], "python3.8");
}

/**
 * Feature: SetUnencryptedUserEnvWithoutEnvKeySuccess
 * Description: Test case when envkey and secret keys are all empty with unencrypted envinfo
 * Expectation: correctly get envinfo
 */
TEST_F(FunctionAgentUtilsTest, SetUnencryptedUserEnvWithoutEnvKeySuccess)
{
    auto deployInstanceRequest = std::make_shared<functionsystem::messages::DeployInstanceRequest>();
    deployInstanceRequest->set_envinfo(PARSED_JSON);

    messages::RuntimeConfig runtimeConf;

    functionsystem::function_agent::SetUserEnv(deployInstanceRequest, runtimeConf);
    EXPECT_EQ((*runtimeConf.mutable_userenvs())["func-FAAS_FUNCTION_LANGUAGE"], "python3.8");
}

/**
 * Feature: SetUserEnvGCMWithEnvKeySuccess
 * Description: Test case when has envkey and GCM secret key is loaded with encrypted envinfo
 * Expectation: correctly get envinfo
 */
TEST_F(FunctionAgentUtilsTest, SetUserEnvGCMWithEnvKeySuccess)
{
    auto deployInstanceRequest = std::make_shared<functionsystem::messages::DeployInstanceRequest>();
    deployInstanceRequest->set_envkey(TEST_ENV_KEY);
    deployInstanceRequest->set_envinfo(TEST_ENV_INFO);

    messages::RuntimeConfig runtimeConf;

    auto resourcePath = FunctionAgentUtilsTest::LoadRootKey(K1_HEX_STR, K2_HEX_STR, SALT_HEX_STR, K3_HEX_STR);

    functionsystem::function_agent::SetUserEnv(deployInstanceRequest, runtimeConf);
    EXPECT_EQ((*runtimeConf.mutable_userenvs())["func-FAAS_FUNCTION_LANGUAGE"], "python3.8");
}

TEST_F(FunctionAgentUtilsTest, DecryptEnvKeyFromRepoSuccess)
{
    auto deployInstanceRequest = std::make_shared<functionsystem::messages::DeployInstanceRequest>();
    std::string envKey =
        "c8f99fab42a15dcf25dbc3cd:"
        "1ef78f3c86a80d4a5794c1cb282687e1178f9cc964b3e858a3c08fad70ba7fb50f09fbca7f5166e76eb140222dbda9b26a0e101c15377c"
        "3ad3f4b0e71695cd81afab7503cb8d117ea51e7ddbe804b5a4";
    std::string envInfo =
        "{\"func-dataTestCount\":\"100\",\"func-timeInterval\":\"10\"}";
    deployInstanceRequest->set_envkey(envKey);
    deployInstanceRequest->set_envinfo(envInfo);

    messages::RuntimeConfig runtimeConf;

    auto resourcePath = FunctionAgentUtilsTest::LoadRootKey(K1_HEX_STR, K2_HEX_STR, SALT_HEX_STR, K3_HEX_STR);

    functionsystem::function_agent::SetUserEnv(deployInstanceRequest, runtimeConf);
    EXPECT_EQ((*runtimeConf.mutable_userenvs())["func-dataTestCount"], "100");
}

TEST_F(FunctionAgentUtilsTest, SetUserEnvGCMWithCryptotoolEnvKeySuccess)
{
    std::string envKeyEncrypted =
        "5a00f7925740edc0e49981d9:"
        "1e62bfceaf874866ab7eacbaf1ebbae8f5dd3244fe8e6823d44a5c90ba1713da8bb3800f6cfea5b8f9687cc3c2b0585b686e9c9c028d4d"
        "4b5a2070467daea01868671c9a4b5e97bef1ed21e7a716cf50";
    std::string envInfoEncrypted =
        "{\"dataTestCount\":100,\"timeInterval\":10}";
    auto deployInstanceRequest = std::make_shared<functionsystem::messages::DeployInstanceRequest>();
    deployInstanceRequest->set_envkey(envKeyEncrypted);
    deployInstanceRequest->set_envinfo(envInfoEncrypted);

    messages::RuntimeConfig runtimeConf;
    const std::string k1HexStr =
        "e1876810a37bc15783dd5ed4ef09aae09fca6b6c358a125c32db51d41728ca43531230a095ed087a4026d18456f7901eb626a2a954cc8c"
        "c302e2e7fa8b4d8d134f8105f13e760c0010fd571ae952917f2f69a461d94f56d1794980c8bb12d4d93c3a7b7466c7beddaaa952dfc04c"
        "cb365bd4651fb11dd1b2debde964019052fde624ef8fcdfda5fe8d441c0ce229965a31e6039fddc47bc1f68a5f462c19e7de95e8e5132d"
        "0aa7ddd95ae41f224ffa2d7f22239926f84d9a36be9774a8dc7268d3b00e7290f5b645a183687a10efe680194e7ef14530278432cb2b42"
        "76b1f90ad02b4bb833fb0b975999f961e94bcb62169e9d697ff8861b0662525a11a1d124";
    const std::string k2HexStr =
        "bc1657c2884aa1108ff33bc2f8a9f2e0a0e42dc86998291882452b515ce2671bc3ba458ee0abc071bd8fdfd26a5c3077e54061527276b4"
        "f663b1c20fecf442967a10878a583e0da95943caf80a78fc0351e225d88e9d7bfab6c977da489ff8a13a967ece5bf431d8d032083cb043"
        "1a2f06f2378b5bfd9b7f53aa7ddf8bc425a086641b7b441a2ad32948f323b17f6319e4e791736627fa0bdd8c501cba2490307bbdfba896"
        "b5253b5cc7911fd163cea544d1a3a305e554b195f9d8483ba00f48b3dd0aa2fe56cd21ed3009a1b8d35ee21b677a9574558ae9a9918c5d"
        "9bdef1d3fa55d6504058f4060ea6e28d112bf703430e2b05935486d8afe473f72b248d57";
    const std::string k3HexStr =
        "7f9a368f4268e850979218c9af8f2df4ccdb17cf4139ae928012572248d7dc28634713b76aa4f93b681c6f7b314625ec4b529fdfae9c59"
        "f666c00419b082805948c7cf0bdd3dd6645a52b103b46df105fde29457c8af166e2ce54b0006c4462476e0c3796e19c70a4ec5e7085e43"
        "083f6b2c4d31a7200232abf79a891c79f1315bf1cd8a8fefc9026271bf741d1304bac6c01193ab15dc2b5e5a3a141462228d289bf5f94a"
        "73f9b90f9f247b174caf92a2b4d42b312f455a4233c375ebd33ee8326e7e9deb2a4eaba72e52f11f61f6047aaa4b68513e6bd7bf99f372"
        "32ec7b87e947cd2d9dc93362b25d7dd723a0fe6359244e529a6a6a65313bd22ab677960e";
    const std::string saltHexStr =
        "69fdec8731c7ba8fff8535e389313238fd68f07eb78f5ad8d979fd45b41c84d53020f39a4bdb647b9b3eb88bcbc6816a1ae6d6752e0859"
        "25bd072bc9bd8230f9707ded3c96b1bdbd5899769b746a134798525a5e5363c79fb82e8886137f3280b1cc49f78cab2a46623aa7555d60"
        "b97c7fa51beddd0a727e622dc0be3fb3951ca4db66c8419590da32f4ee29e3da9a91fa6a2cb4239e63516408c59dacd1ebed52ce750777"
        "b453828061060c770c25754543a56ff8555bbd0eb842543987ea6f5930036dc98b67d580e4ced82f124366f69e6ef06fae3254a821b200"
        "001c772b3c9d2cede3cf33fb01277d8bfe915285120ec751c820bf52d58a7ac05524e883";

    auto resourcePath = FunctionAgentUtilsTest::LoadRootKey(k1HexStr, k2HexStr, saltHexStr, k3HexStr);

    functionsystem::function_agent::SetUserEnv(deployInstanceRequest, runtimeConf);
    EXPECT_EQ((*runtimeConf.mutable_userenvs())["func-dataTestCount"], "100");
    EXPECT_EQ((*runtimeConf.mutable_userenvs())["func-timeInterval"], "10");
}

TEST_F(FunctionAgentUtilsTest, HasSuffixSuccess)
{
    std::string source = "source-suffix";
    std::string suffix = "suffix";
    EXPECT_EQ(function_agent::HasSuffix(source, suffix), true);

    source = "src";
    EXPECT_EQ(function_agent::HasSuffix(source, suffix), false);
}

TEST_F(FunctionAgentUtilsTest, IsDirSuccess)
{
    std::string dir = "/tmp/";
    std::string noneDir = "noneDir";
    EXPECT_EQ(function_agent::IsDir(dir), true);
    EXPECT_EQ(function_agent::IsDir(noneDir), false);
}

TEST_F(FunctionAgentUtilsTest, SetDeployingRequestLayersSuccess)
{
    auto spec = messages::FuncDeploySpec();
    spec.set_storagetype(function_agent::S3_STORAGE_TYPE);
    auto layer = spec.add_layers();
    layer->set_appid("appid-1");
    layer->set_bucketid("bucketid-1");
    layer->set_objectid("objectID-1");
    layer->set_bucketurl("bucketURL-1");
    layer->set_sha256("sha256-1");

    auto layer2 = spec.add_layers();
    layer2->set_appid("appid-2");
    layer2->set_bucketid("bucketid-2");
    layer2->set_objectid("objectID-2");
    layer2->set_bucketurl("bucketURL-2");
    layer2->set_sha256("sha256-2");

    auto result = function_agent::SetDeployingRequestLayers(spec);
    const int32_t RESULT_SIZE = 2;
    EXPECT_EQ(static_cast<int32_t>(result.size()), RESULT_SIZE);
}

TEST_F(FunctionAgentUtilsTest, AddDefaultEnvWithDELEGATE_ENV_VAR)
{
    nlohmann::json build_in = nlohmann::json::object();
    build_in["YR-RUNTIME_ENABLE"] = R"(true)";
    // CreateOptions has the highest priority
    build_in["LD_LIBRARY_PATH"] = R"(from build-in)";
    litebus::os::SetEnv("DELEGATE_ENV_VAR", build_in.dump());

    nlohmann::json val = nlohmann::json::object();
    val["LD_LIBRARY_PATH"] = R"(${LD_LIBRARY_PATH}:${FUNCTION_LIB_PATH}/depend)";
    val["key1"] = R"(value1)";
    std::cout << val.dump() << std::endl;
    auto deployInstanceRequest1 = std::make_shared<functionsystem::messages::DeployInstanceRequest>();
    (*deployInstanceRequest1->mutable_createoptions())["DELEGATE_ENV_VAR"] = val.dump();
    messages::RuntimeConfig runtimeConf1;
    functionsystem::function_agent::AddDefaultEnv(deployInstanceRequest1, runtimeConf1);
    std::cout << (*runtimeConf1.mutable_posixenvs()).size() << std::endl;
    // CreateOptions has the highest priority
    EXPECT_EQ((*runtimeConf1.mutable_posixenvs())["LD_LIBRARY_PATH"], R"(${LD_LIBRARY_PATH}:${FUNCTION_LIB_PATH}/depend)");
    EXPECT_EQ((*runtimeConf1.mutable_posixenvs())["key1"], R"(value1)");
    EXPECT_EQ((*runtimeConf1.mutable_posixenvs())["YR-RUNTIME_ENABLE"], R"(true)");

    litebus::os::UnSetEnv("DELEGATE_ENV_VAR");  // unset build-it delegate env

    auto deployInstanceRequest2 = std::make_shared<functionsystem::messages::DeployInstanceRequest>();
    deployInstanceRequest2->set_tenantid("Test_TenantID");
    (*deployInstanceRequest2->mutable_createoptions())["DELEGATE_ENV_VAR"] = R"({"LD_LIBRARY_PATH":"${LD_LIBRARY_PATH}:${FUNCTION_LIB_PATH}/depend")";
    messages::RuntimeConfig runtimeConf2;
    functionsystem::function_agent::AddDefaultEnv(deployInstanceRequest2, runtimeConf2);
    EXPECT_EQ((*runtimeConf2.mutable_posixenvs()).size(), size_t{1});
}

TEST_F(FunctionAgentUtilsTest, AddDefaultEnvInjectsPodMetadataForAllRuntimes)
{
    litebus::os::SetEnv("POD_NAME", "pod-name");
    litebus::os::SetEnv("POD_NAMESPACE", "pod-namespace");

    auto deployInstanceRequest = std::make_shared<functionsystem::messages::DeployInstanceRequest>();
    messages::RuntimeConfig runtimeConf;
    functionsystem::function_agent::AddDefaultEnv(deployInstanceRequest, runtimeConf);

    EXPECT_EQ((*runtimeConf.mutable_posixenvs())["POD_NAME"], "pod-name");
    EXPECT_EQ((*runtimeConf.mutable_posixenvs())["POD_NAMESPACE"], "pod-namespace");

    litebus::os::UnSetEnv("POD_NAME");
    litebus::os::UnSetEnv("POD_NAMESPACE");
}

TEST_F(FunctionAgentUtilsTest, SensitiveValueHashTest)
{
    const std::string plaint = "secret-key";

    const auto value = SensitiveValue(plaint);

    ::messages::TenantCredentials credentials;
    credentials.set_secretkey(value.GetData());

    ::messages::TenantCredentials credentials2;
    credentials2.set_secretkey(value.GetData(), value.GetSize());

    const auto value_hash = std::hash<std::string>()(value.GetData());
    EXPECT_EQ(value_hash, std::hash<std::string>()(credentials.secretkey()));
    EXPECT_EQ(value_hash, std::hash<std::string>()(credentials2.secretkey()));
}
TEST_F(FunctionAgentUtilsTest, DecryptDelegateDataTest)
{
    std::string delegateData = "{\"accessKey\":\"\",\"authToken\":\"\",\"cryptoAlgorithm\":\"NO_CRYPTO\",\"encrypted_user_data\":\"\",\"envKey\":\"\",\"environment\":\"{\\\"key1\\\":\\\"val111\\\",\\\"key2\\\":\\\"val222\\\"}\",\"secretKey\":\"\",\"securityAk\":\"\",\"securitySk\":\"\",\"securityToken\":\"\"}";
    std::string delegateDataEnc = "{\"accessKey\":\"\",\"authToken\":\"\",\"encrypted_user_data\":\"\",\"envKey\":\"\",\"environment\":\"{\\\"key1\\\":\\\"val111\\\",\\\"key2\\\":\\\"val222\\\"}\",\"secretKey\":\"\",\"securityAk\":\"\",\"securitySk\":\"\",\"securityToken\":\"\"}";
    auto decryptData = function_agent::DecryptDelegateData(delegateDataEnc, "");
    EXPECT_TRUE(decryptData.IsSome());
    decryptData = function_agent::DecryptDelegateData(delegateData, "");
    EXPECT_TRUE(decryptData.IsSome());
}

}  // namespace functionsystem::test
