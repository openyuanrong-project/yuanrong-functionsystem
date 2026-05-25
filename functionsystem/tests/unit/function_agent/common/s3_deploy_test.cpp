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
#include <gtest/gtest.h>

#include <utility>

#include "common/logs/logging.h"
#include "common/metrics/metrics_adapter.h"
#include "common/proto/pb/message_pb.h"
#include "common/utils/exec_utils.h"
#include "common/utils/files.h"
#include "function_agent/code_deployer/s3_deployer.h"
#include "mocks/mock_obs_wrapper.h"
#include "s3_bucket_opt.h"
#include "s3_object_del.h"
#include "s3_object_put.h"
#include "utils/os_utils.hpp"

namespace functionsystem::test::function_agent {
using namespace functionsystem::function_agent;

const std::string AK = "root";          // NOLINT
const std::string SK = "MFDe8ZR7zqSk";  // NOLINT

const std::string BUCKET_ID = "yr-cxx-refactor-bucket-test";  // NOLINT

const std::string OBJECT_ID = "yr-cxx-refactor-object-test";     // NOLINT
const std::string OBJECT_BUFFER = "YuanRong C++ refactor test";  // NOLINT

class DISABLED_S3DeployerTest : public ::testing::Test {
public:
    [[maybe_unused]] static void SetUpTestSuite()
    {
        codePackageThresholds_.set_filecountsmax(3);
        codePackageThresholds_.set_zipfilesizemaxmb(1);
        codePackageThresholds_.set_unzipfilesizemaxmb(1);
        codePackageThresholds_.set_dirdepthmax(3);

        s3Config_ = std::make_shared<S3Config>();
        s3Config_->endpoint = "10.243.31.23:30110";
        s3Config_->protocol = "http";

        s3Config_->accessKey = AK;  // user
        s3Config_->secretKey = SK;  // pwd

        obs_initialize(OBS_INIT_ALL);

        if (S3BucketOpt::CreateBucket(BUCKET_ID, *s3Config_).IsOk() &&
            S3ObjectPut::PutObjectFromBuffer(OBJECT_BUFFER, OBJECT_ID, BUCKET_ID, *s3Config_).IsOk()) {
            YRLOG_DEBUG("s3 service is available.");
        } else {
            YRLOG_ERROR("s3 service is not available.");
            s3ServiceAvailable = false;
        }

        obs_deinitialize();
    }

    [[maybe_unused]] static void TearDownTestSuite()
    {
        if (s3ServiceAvailable) {
            obs_initialize(OBS_INIT_ALL);

            S3ObjectDel::DeleteObject(BUCKET_ID, OBJECT_ID, *s3Config_);
            (void)S3BucketOpt::DeleteBucket(BUCKET_ID, *s3Config_);

            obs_deinitialize();
        }

        s3Config_ = nullptr;
    }

protected:
    void SetUp() override
    {
    }

    void TearDown() override
    {
    }

protected:
    inline static bool s3ServiceAvailable = true;

    inline static std::shared_ptr<S3Config> s3Config_;

    inline static messages::CodePackageThresholds codePackageThresholds_;
};

TEST_F(DISABLED_S3DeployerTest, DirectoryTest)  // NOLINT
{
    std::string layerDir = litebus::os::Join("/home/s3", "layer");
    std::string funcDir = litebus::os::Join(layerDir, "func");
    std::string bucketDir = litebus::os::Join(funcDir, "bucket");

    EXPECT_TRUE(litebus::os::Mkdir(bucketDir).IsNone());
    EXPECT_TRUE(litebus::os::Mkdir(bucketDir).IsNone());
    EXPECT_TRUE(litebus::os::ExistPath(bucketDir));

    std::string objectFile = litebus::os::Join(bucketDir, "xxx.zip");
    FILE *file = fopen(objectFile.c_str(), "wb");
    EXPECT_TRUE(file != nullptr);
    EXPECT_TRUE(litebus::os::ExistPath(objectFile));
    EXPECT_TRUE(fclose(file) == 0);

    const std::string bucketDirTmp = bucketDir + "_tmp";
    EXPECT_EQ(rename(bucketDir.c_str(), bucketDirTmp.c_str()), 0);
    EXPECT_TRUE(litebus::os::ExistPath(bucketDirTmp));

    EXPECT_TRUE(litebus::os::Rmdir(bucketDirTmp).IsNone());
    EXPECT_FALSE(litebus::os::ExistPath(bucketDirTmp));
    EXPECT_FALSE(litebus::os::ExistPath(bucketDir));

    EXPECT_TRUE(litebus::os::ExistPath(funcDir));
    EXPECT_TRUE(litebus::os::Rmdir(funcDir).IsNone());

    EXPECT_TRUE(litebus::os::ExistPath(layerDir));
    EXPECT_TRUE(litebus::os::Rmdir(layerDir).IsNone());
}

TEST_F(DISABLED_S3DeployerTest, IsDeployedTest)  // NOLINT
{
    S3Deployer deployer(s3Config_, codePackageThresholds_);
    std::string destination = "/tmp/home/s3/layer/func/bucketid/objectid";
    litebus::os::Mkdir(destination);
    EXPECT_TRUE(deployer.IsDeployed(destination, false));
    litebus::os::Rmdir(destination);
    EXPECT_FALSE(deployer.IsDeployed(destination, false));
    litebus::os::Mkdir(destination);
    EXPECT_FALSE(deployer.IsDeployed(destination, true));
    EXPECT_TRUE(deployer.IsDeployed("/tmp/home/s3/layer/func/bucketid", true));
    litebus::os::Rmdir(destination);
}

TEST_F(DISABLED_S3DeployerTest, GetDestinationTest)  // NOLINT
{
    S3Deployer deployer(s3Config_, codePackageThresholds_);
    {
        std::string destination = "/tmp/home/s3/layer/func/bucketid/objectid";
        EXPECT_EQ(deployer.GetDestination("/tmp/home/s3", "bucketid", "objectid"), destination);
    }
    {
        std::string destination = "/tmp/home/s3/layer/func/bucketid/a-b-c-objectid";
        EXPECT_EQ(deployer.GetDestination("/tmp/home/s3", "bucketid", "a/b/c/objectid"), destination);
    }

}

class UnzipMockS3Deployer : public S3Deployer {
public:
    explicit UnzipMockS3Deployer(std::shared_ptr<S3Config> config, messages::CodePackageThresholds msg)
        : S3Deployer(std::move(config), std::move(msg))
    {
    }

    explicit UnzipMockS3Deployer(std::shared_ptr<S3Config> config, const std::shared_ptr<ObsWrapper> &obsWrapper,
                                 messages::CodePackageThresholds msg)
        : S3Deployer(std::move(config), std::move(obsWrapper), std::move(msg))
    {
    }

public:
    MOCK_METHOD(Status, UnzipFile, (const std::string &, const std::string &), (override));
    MOCK_METHOD2(CollectFunctionInfo, void(const std::string &, const std::shared_ptr<::messages::DeployRequest> &));
};

/**
 * Feature: S3DeployerTest--S3DeployDownloadFailed
 * Description: Download user code with error param
 * Steps:
 * 1. Create DeploymentConfig, set hostname
 * 2. Download with just hostname
 * 3. Download with just hostname and security token
 * 4. Download without secret access key
 * Expectation:
 * 1. First download, failed when obs init options
 * 2. Second download, failed when obs init options
 * 4. Third download, failed when obs init options
 */
TEST_F(DISABLED_S3DeployerTest, S3DeployDownloadFailed)  // NOLINT
{
    messages::DeploymentConfig deploymentConfig;
    deploymentConfig.set_deploydir("/tmp/home/s3/");
    deploymentConfig.set_bucketid(BUCKET_ID);
    deploymentConfig.set_objectid(OBJECT_ID);
    deploymentConfig.set_storagetype("s3");

    deploymentConfig.set_hostname("10.175.127.111:9000");

    UnzipMockS3Deployer deployer(s3Config_, codePackageThresholds_);

    std::string layerDir = litebus::os::Join("/tmp/home/s3/", "layer");
    std::string funcDir = litebus::os::Join(layerDir, "func");
    std::string bucketDir = litebus::os::Join(funcDir, deploymentConfig.bucketid());
    std::string objectDir = litebus::os::Join(bucketDir, deploymentConfig.objectid());

    EXPECT_EQ(deployer.DownloadCode(objectDir, deploymentConfig), StatusCode::FUNC_AGENT_OBS_INIT_OPTIONS_ERROR);
    deploymentConfig.set_securitytoken("xxx");
    EXPECT_EQ(deployer.DownloadCode(objectDir, deploymentConfig), StatusCode::FUNC_AGENT_OBS_INIT_OPTIONS_ERROR);
    deploymentConfig.set_temporaryaccesskey("xxx");
    EXPECT_EQ(deployer.DownloadCode(objectDir, deploymentConfig), StatusCode::FUNC_AGENT_OBS_INIT_OPTIONS_ERROR);
    deploymentConfig.set_temporarysecretkey("xxx");
    EXPECT_NE(deployer.DownloadCode(objectDir, deploymentConfig), StatusCode::FUNC_AGENT_OBS_INIT_OPTIONS_ERROR);
}

class DownloadMockS3Deployer : public S3Deployer {
public:
    explicit DownloadMockS3Deployer(std::shared_ptr<S3Config> config, messages::CodePackageThresholds msg)
        : S3Deployer(std::move(config), std::move(msg))
    {
    }

    explicit DownloadMockS3Deployer(std::shared_ptr<S3Config> config, const std::shared_ptr<ObsWrapper> &obsWrapper,
                                 messages::CodePackageThresholds msg)
        : S3Deployer(std::move(config), std::move(obsWrapper), std::move(msg))
    {
    }

public:
    MOCK_METHOD(Status, DownloadCode, (const std::string &, const ::messages::DeploymentConfig &), (override));
    MOCK_METHOD(Status, PackageValidation, (const std::string &, const std::string &, const std::string &), (override));
    MOCK_METHOD(Status, UnzipFile, (const std::string &, const std::string &), (override));
};


TEST_F(DISABLED_S3DeployerTest, S3DeployProcess)  // NOLINT
{
    DownloadMockS3Deployer deployer(s3Config_, codePackageThresholds_);
    EXPECT_CALL(deployer, DownloadCode)
        .WillRepeatedly(testing::Invoke([](const std::string &destFile, const messages::DeploymentConfig &config) {
            litebus::os::Mkdir(destFile);
            return Status::OK();
        }));
    EXPECT_CALL(deployer, PackageValidation)
        .WillRepeatedly(testing::Return(Status::OK()));
    EXPECT_CALL(deployer, UnzipFile)
        .WillOnce(testing::Return(Status::OK()))
        .WillOnce(testing::Return(Status(StatusCode::FUNC_AGENT_OBS_OPEN_FILE_ERROR, "failed to unzip file")));

    auto request = std::make_shared<messages::DeployRequest>();
    auto *deploymentConfig = request->mutable_deploymentconfig();
    deploymentConfig->set_deploydir("/tmp/home/s3/");
    deploymentConfig->set_bucketid(BUCKET_ID);
    deploymentConfig->set_objectid(OBJECT_ID);
    deploymentConfig->set_storagetype("s3");

    std::string layerDir = litebus::os::Join("/tmp/home/s3/", "layer");
    std::string funcDir = litebus::os::Join(layerDir, "func");
    std::string bucketDir = litebus::os::Join(funcDir, deploymentConfig->bucketid());
    std::string objectDir = litebus::os::Join(bucketDir, deploymentConfig->objectid());
    std::string objectFile = litebus::os::Join(objectDir, deploymentConfig->objectid());
    std::string objectInnerDir = litebus::os::Join(objectDir, "tmp");

    // EXPECT true with already created tmp directory
    EXPECT_TRUE(deployer.Deploy(request).status.IsOk());

    EXPECT_TRUE(deployer.Clear(objectDir, deploymentConfig->objectid()));
    EXPECT_FALSE(litebus::os::ExistPath(objectDir));
    EXPECT_TRUE(litebus::os::Rmdir(layerDir).IsNone());
    EXPECT_FALSE(litebus::os::ExistPath(layerDir));

    // EXPECT true with UnzipFile return error status
    EXPECT_TRUE(deployer.Deploy(request).status.IsError());
    EXPECT_TRUE(deployer.Clear(objectDir, deploymentConfig->objectid()));
    EXPECT_FALSE(litebus::os::ExistPath(objectDir));
    EXPECT_TRUE(litebus::os::Rmdir(layerDir).IsNone());
    EXPECT_FALSE(litebus::os::ExistPath(layerDir));
}

TEST_F(DISABLED_S3DeployerTest, S3DeployProcessWithMultiDir)
{
    DownloadMockS3Deployer deployer(s3Config_, codePackageThresholds_);
    std::string expected;
    EXPECT_CALL(deployer, DownloadCode)
        .WillOnce(testing::Invoke([&expected](const std::string &destFile, const messages::DeploymentConfig &config) {
            litebus::os::Mkdir(destFile);
            expected = destFile;
            return Status::OK();
        }));
    EXPECT_CALL(deployer, PackageValidation)
        .WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(deployer, UnzipFile)
        .WillOnce(testing::Return(Status::OK()));

    auto request = std::make_shared<messages::DeployRequest>();
    auto *deploymentConfig = request->mutable_deploymentconfig();
    deploymentConfig->set_deploydir("/tmp/home/s3/");
    deploymentConfig->set_bucketid(BUCKET_ID);
    deploymentConfig->set_objectid("a/b/c/" + OBJECT_ID);
    deploymentConfig->set_storagetype("s3");

    std::string layerDir = litebus::os::Join("/tmp/home/s3/", "layer");
    std::string funcDir = litebus::os::Join(layerDir, "func");
    std::string bucketDir = litebus::os::Join(funcDir, deploymentConfig->bucketid());
    std::string objectDir = litebus::os::Join(bucketDir, "a-b-c-" + OBJECT_ID);
    std::string objectFile = litebus::os::Join(objectDir, OBJECT_ID);
    std::string objectInnerDir = objectDir + "-tmp";
    std::string objectFileTmp = litebus::os::Join(objectInnerDir, OBJECT_ID);

    // EXPECT true with already created tmp directory
    EXPECT_TRUE(deployer.Deploy(request).status.IsOk());
    EXPECT_TRUE(deployer.Clear(objectDir, deploymentConfig->objectid()));
    EXPECT_FALSE(litebus::os::ExistPath(objectDir));
    EXPECT_TRUE(litebus::os::Rmdir(layerDir).IsNone());
    EXPECT_FALSE(litebus::os::ExistPath(layerDir));
    EXPECT_STREQ(expected.c_str(), objectFileTmp.c_str());
}

TEST_F(DISABLED_S3DeployerTest, S3DeployWithEmptyDownload)
{
    auto request = std::make_shared<messages::DeployRequest>();
    auto *deploymentConfig = request->mutable_deploymentconfig();
    deploymentConfig->set_deploydir("/tmp/home/s3/");
    deploymentConfig->set_bucketid("");
    deploymentConfig->set_objectid("");
    deploymentConfig->set_storagetype("");
    auto deployer = std::make_shared<function_agent::S3Deployer>(s3Config_, codePackageThresholds_);
    EXPECT_TRUE(deployer->Deploy(request).status.IsOk());
}

TEST_F(DISABLED_S3DeployerTest, S3DeployCheckObsErrorNeedRetry)
{
    auto deployer = std::make_shared<function_agent::S3Deployer>(s3Config_, codePackageThresholds_);
    EXPECT_TRUE(deployer->CheckObsErrorNeedRetry(OBS_STATUS_FailedToConnect));
    EXPECT_TRUE(deployer->CheckObsErrorNeedRetry(OBS_STATUS_InternalError));
    EXPECT_TRUE(deployer->CheckObsErrorNeedRetry(OBS_STATUS_PartialFile));
    EXPECT_FALSE(deployer->CheckObsErrorNeedRetry(OBS_STATUS_BadMd5));
}

TEST_F(DISABLED_S3DeployerTest, S3RetryDownloadCodeSuccess)  // NOLINT
{
    auto request = std::make_shared<messages::DeployRequest>();

    auto *deploymentConfig = request->mutable_deploymentconfig();
    deploymentConfig->set_deploydir("/tmp/home/s3/");
    deploymentConfig->set_bucketid(BUCKET_ID);
    deploymentConfig->set_objectid(OBJECT_ID);
    deploymentConfig->set_storagetype("s3");

    std::shared_ptr<MockObsWrapper> mockObsWrapper = std::make_shared<MockObsWrapper>();
    EXPECT_CALL(*mockObsWrapper, DeinitializeObs()).WillRepeatedly(testing::Return());
    EXPECT_CALL(*mockObsWrapper, InitializeObs())
        .WillOnce(testing::Return(OBS_STATUS_ConnectionFailed))
        .WillOnce(testing::Return(OBS_STATUS_ConnectionFailed))
        .WillOnce(testing::Return(OBS_STATUS_OK))
        .WillOnce(testing::Return(OBS_STATUS_OK));

    DownloadMockS3Deployer deployer(s3Config_, std::move(mockObsWrapper), codePackageThresholds_);
    EXPECT_CALL(deployer, DownloadCode).WillOnce(testing::Return(Status::OK()));

    std::string layerDir = litebus::os::Join("/tmp/home/s3/", "layer");
    std::string funcDir = litebus::os::Join(layerDir, "func");
    std::string bucketDir = litebus::os::Join(funcDir, deploymentConfig->bucketid());
    std::string objectDir = litebus::os::Join(bucketDir, deploymentConfig->objectid());

    const ::messages::DeploymentConfig &config = request->deploymentconfig();
    std::string objectFile = litebus::os::Join(objectDir, deploymentConfig->objectid());
    EXPECT_TRUE(deployer.RetryDownloadCode(objectFile, config).IsOk());

    EXPECT_TRUE(deployer.Clear(objectDir, deploymentConfig->objectid()));
    EXPECT_FALSE(litebus::os::ExistPath(objectDir));
    EXPECT_FALSE(litebus::os::Rmdir(layerDir).IsNone());
    EXPECT_FALSE(litebus::os::ExistPath(layerDir));
}

TEST_F(DISABLED_S3DeployerTest, S3RetryDownloadCodeFailedWhenConnect)  // NOLINT
{
    auto request = std::make_shared<messages::DeployRequest>();

    auto *deploymentConfig = request->mutable_deploymentconfig();
    deploymentConfig->set_deploydir("/tmp/home/s3/");
    deploymentConfig->set_bucketid(BUCKET_ID);
    deploymentConfig->set_objectid(OBJECT_ID);
    deploymentConfig->set_storagetype("s3");

    std::shared_ptr<MockObsWrapper> mockObsWrapper = std::make_shared<MockObsWrapper>();
    EXPECT_CALL(*mockObsWrapper, DeinitializeObs()).WillRepeatedly(testing::Return());
    EXPECT_CALL(*mockObsWrapper, InitializeObs())
        .WillOnce(testing::Return(OBS_STATUS_OK))
        .WillOnce(testing::Return(OBS_STATUS_ConnectionFailed));

    DownloadMockS3Deployer deployer(s3Config_, std::move(mockObsWrapper), codePackageThresholds_);

    std::string layerDir = litebus::os::Join("/tmp/home/s3/", "layer");
    std::string funcDir = litebus::os::Join(layerDir, "func");
    std::string bucketDir = litebus::os::Join(funcDir, deploymentConfig->bucketid());
    std::string objectDir = litebus::os::Join(bucketDir, deploymentConfig->objectid());

    const ::messages::DeploymentConfig &config = request->deploymentconfig();
    std::string objectFile = litebus::os::Join(objectDir, deploymentConfig->objectid());
    auto status = deployer.RetryDownloadCode(objectFile, config);
    EXPECT_EQ(status.StatusCode(), StatusCode::FUNC_AGENT_OBS_CONNECTION_ERROR);
}

TEST_F(DISABLED_S3DeployerTest, S3RetryDownloadCodeFailedWhenDownload)  // NOLINT
{
    auto request = std::make_shared<messages::DeployRequest>();

    auto *deploymentConfig = request->mutable_deploymentconfig();
    deploymentConfig->set_deploydir("/tmp/home/s3/");
    deploymentConfig->set_bucketid(BUCKET_ID);
    deploymentConfig->set_objectid(OBJECT_ID);
    deploymentConfig->set_storagetype("s3");

    std::shared_ptr<MockObsWrapper> mockObsWrapper = std::make_shared<MockObsWrapper>();
    EXPECT_CALL(*mockObsWrapper, DeinitializeObs()).WillRepeatedly(testing::Return());
    EXPECT_CALL(*mockObsWrapper, InitializeObs()).WillRepeatedly(testing::Return(OBS_STATUS_OK));

    DownloadMockS3Deployer deployer(s3Config_, std::move(mockObsWrapper), codePackageThresholds_);
    EXPECT_CALL(deployer, DownloadCode)
        .WillOnce(testing::Return(Status(StatusCode::FUNC_AGENT_OBS_OPEN_FILE_ERROR, "failed to open file")));

    std::string layerDir = litebus::os::Join("/tmp/home/s3/", "layer");
    std::string funcDir = litebus::os::Join(layerDir, "func");
    std::string bucketDir = litebus::os::Join(funcDir, deploymentConfig->bucketid());
    std::string objectDir = litebus::os::Join(bucketDir, deploymentConfig->objectid());

    const ::messages::DeploymentConfig &config = request->deploymentconfig();
    std::string objectFile = litebus::os::Join(objectDir, deploymentConfig->objectid());
    auto status = deployer.RetryDownloadCode(objectFile, config);
    EXPECT_EQ(status.StatusCode(), StatusCode::FUNC_AGENT_OBS_OPEN_FILE_ERROR);
}

TEST_F(DISABLED_S3DeployerTest, S3InitFailedTest)  // NOLINT
{
    std::shared_ptr<MockObsWrapper> mockObsWrapper = std::make_shared<MockObsWrapper>();
    EXPECT_CALL(*mockObsWrapper, DeinitializeObs()).WillRepeatedly(testing::Return());
    EXPECT_CALL(*mockObsWrapper, InitializeObs()).Times(3).WillRepeatedly(testing::Return(OBS_STATUS_ConnectionFailed));

    std::shared_ptr<S3Config> s3Config = std::make_shared<S3Config>();
    s3Config_->endpoint = "10.247.5.237:19002";
    s3Config_->protocol = "http";
    s3Config_->credentialType = CREDENTIAL_TYPE_PERMANENT_CREDENTIALS;

    s3Config_->accessKey = AK;  // user
    s3Config_->secretKey = SK;
    auto s3Deployer = std::make_shared<function_agent::S3Deployer>(s3Config, std::move(mockObsWrapper),
                                                                   codePackageThresholds_);
}

TEST_F(DISABLED_S3DeployerTest, S3InitSuccessAfterRetryTest)  // NOLINT
{
    std::shared_ptr<MockObsWrapper> mockObsWrapper = std::make_shared<MockObsWrapper>();
    EXPECT_CALL(*mockObsWrapper, DeinitializeObs()).WillRepeatedly(testing::Return());
    EXPECT_CALL(*mockObsWrapper, InitializeObs())
        .WillOnce(testing::Return(OBS_STATUS_ConnectionFailed))
        .WillOnce(testing::Return(OBS_STATUS_ConnectionFailed))
        .WillOnce(testing::Return(OBS_STATUS_OK));

    std::shared_ptr<S3Config> s3Config = std::make_shared<S3Config>();
    s3Config_->endpoint = "10.247.5.237:19002";
    s3Config_->protocol = "http";
    s3Config_->credentialType = CREDENTIAL_TYPE_PERMANENT_CREDENTIALS;

    s3Config_->accessKey = AK;  // user
    s3Config_->secretKey = SK;
    auto s3Deployer = std::make_shared<function_agent::S3Deployer>(s3Config, std::move(mockObsWrapper),
                                                                   codePackageThresholds_);
}

TEST_F(DISABLED_S3DeployerTest, S3InitSuccessTest)  // NOLINT
{
    std::shared_ptr<MockObsWrapper> mockObsWrapper = std::make_shared<MockObsWrapper>();
    EXPECT_CALL(*mockObsWrapper, DeinitializeObs()).WillOnce(testing::Return());
    EXPECT_CALL(*mockObsWrapper, InitializeObs()).WillOnce(testing::Return(OBS_STATUS_OK));

    std::shared_ptr<S3Config> s3Config = std::make_shared<S3Config>();
    s3Config_->endpoint = "10.247.5.237:19002";
    s3Config_->protocol = "https";
    s3Config_->credentialType = CREDENTIAL_TYPE_PERMANENT_CREDENTIALS;

    s3Config_->accessKey = AK;  // user
    s3Config_->secretKey = SK;
    auto s3Deployer = std::make_shared<function_agent::S3Deployer>(s3Config, std::move(mockObsWrapper),
                                                                   codePackageThresholds_);
}

TEST_F(DISABLED_S3DeployerTest, S3CredentialRotationInitSuccessTest)
{
    std::shared_ptr<MockObsWrapper> mockObsWrapper = std::make_shared<MockObsWrapper>();
    EXPECT_CALL(*mockObsWrapper, DeinitializeObs()).WillOnce(testing::Return());
    EXPECT_CALL(*mockObsWrapper, InitializeObs()).WillOnce(testing::Return(OBS_STATUS_OK));

    std::shared_ptr<S3Config> s3Config = std::make_shared<S3Config>();
    s3Config_->credentialType = CREDENTIAL_TYPE_ROTATING_CREDENTIALS;
    s3Config_->endpoint = "10.247.5.237:19002";

    s3Config_->accessKey = AK;  // user
    s3Config_->secretKey = SK;
    auto s3Deployer = std::make_shared<function_agent::S3Deployer>(s3Config, std::move(mockObsWrapper),
                                                                   codePackageThresholds_);
}

TEST_F(DISABLED_S3DeployerTest, UnzipFileTest)  // NOLINT
{
    std::shared_ptr<MockObsWrapper> mockObsWrapper = std::make_shared<MockObsWrapper>();
    EXPECT_CALL(*mockObsWrapper, DeinitializeObs()).WillOnce(testing::Return());
    EXPECT_CALL(*mockObsWrapper, InitializeObs()).WillOnce(testing::Return(OBS_STATUS_OK));

    std::shared_ptr<S3Config> s3Config = std::make_shared<S3Config>();
    s3Config_->endpoint = "10.247.5.237:19002";
    s3Config_->protocol = "https";
    s3Config_->credentialType = CREDENTIAL_TYPE_PERMANENT_CREDENTIALS;

    s3Config_->accessKey = AK;  // user
    s3Config_->secretKey = SK;
    auto s3Deployer = std::make_shared<function_agent::S3Deployer>(s3Config, std::move(mockObsWrapper),
                                                                   codePackageThresholds_);
    const std::string DEST_DIR = "object_id-tmp";
    const std::string DEST_FILE = "object_id-tmp";
    auto status = s3Deployer->UnzipFile(DEST_DIR, DEST_FILE);
    EXPECT_TRUE(status.IsError());
}

// test for interface Clear
TEST_F(DISABLED_S3DeployerTest, ClearTest)
{
    std::shared_ptr<S3Config> s3Config = std::make_shared<S3Config>();
    S3Deployer deployer(s3Config, codePackageThresholds_);
    std::string bucketDir = litebus::os::Join("/tmp/home/s3/", BUCKET_ID);
    std::string objectDir = litebus::os::Join(bucketDir, OBJECT_ID);
    std::string objectTmpDir = objectDir + "_tmp";
    std::string objectTmpInnerDir = litebus::os::Join(objectTmpDir, "tmp");

    litebus::os::Mkdir(objectDir);
    EXPECT_TRUE(deployer.Clear(objectDir, OBJECT_ID));
    EXPECT_FALSE(litebus::os::ExistPath(objectDir));

    // rename failed
    litebus::os::Mkdir(objectDir);
    litebus::os::Mkdir(objectTmpInnerDir);
    EXPECT_TRUE(deployer.Clear(objectDir, OBJECT_ID));
    EXPECT_FALSE(litebus::os::ExistPath(objectDir));
    EXPECT_TRUE(litebus::os::ExistPath(objectTmpInnerDir));

    litebus::os::Rmdir(bucketDir);
}

TEST_F(DISABLED_S3DeployerTest, CheckPackageContentTest)
{
    std::shared_ptr<S3Config> s3Config = std::make_shared<S3Config>();
    S3Deployer deployer(s3Config, codePackageThresholds_);

    std::string objDir = "/tmp/home/s3/layer/func/bucket/files";
    litebus::os::Mkdir(objDir);
    auto file1 = objDir + "/a.txt";
    EXPECT_TRUE(Write(file1, "a"));
    auto file2 = objDir + "/b.txt";
    EXPECT_TRUE(Write(file2, "b"));
    auto file3 = objDir + "/c.txt";
    std::string largeStr;
    for (int i = 0; i < 1000000; i++) {
        largeStr += "cccccccccccccccccccccccccccccc";
    }
    EXPECT_TRUE(Write(file3, largeStr));
    auto file4 = objDir + "/d.txt";
    EXPECT_TRUE(Write(file4, "d"));
    auto file5 = objDir + "/e.txt";
    EXPECT_TRUE(Write(file5, "e"));

    EXPECT_EQ(ExecuteCommand("zip -r /tmp/home/s3/layer/func/bucket/test1.zip " + objDir + "/a.txt").error.empty(), true);
    auto status = deployer.PackageValidation("/tmp/home/s3/layer/func/bucket/test1.zip", "", "");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.ToString(), testing::HasSubstr("the depth of dir exceeds maximum limit"));

    EXPECT_EQ(ExecuteCommand("zip -r /tmp/home/s3/layer/func/bucket/test2.zip " + objDir + "/c.txt").error.empty(), true);
    status = deployer.PackageValidation("/tmp/home/s3/layer/func/bucket/test2.zip", "", "");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.ToString(), testing::HasSubstr("check zip file failed, error: file(tmp/home/s3/layer/func/bucket/files/c.txt) is bigger than 1048576"));

    EXPECT_EQ(ExecuteCommand("zip -r /tmp/home/s3/layer/func/bucket/test3.zip " + objDir + "/a.txt " + objDir + "/b.txt " + objDir + "/d.txt " + objDir + "/e.txt").error.empty(), true);
    status = deployer.PackageValidation("/tmp/home/s3/layer/func/bucket/test3.zip", "", "");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.ToString(), testing::HasSubstr("the number of files exceeds maximum limit"));
    EXPECT_TRUE(litebus::os::Rmdir("/tmp/home/s3/layer/func/bucket").IsNone());
}

TEST_F(DISABLED_S3DeployerTest, CheckPackageContentWithMaxThresholds)
{
    messages::CodePackageThresholds codePackageThresholds;
    codePackageThresholds.set_filecountsmax(30000);
    codePackageThresholds.set_zipfilesizemaxmb(1024 * 1024 * 10);
    codePackageThresholds.set_unzipfilesizemaxmb(1024 * 1024 * 10);
    codePackageThresholds.set_dirdepthmax(50);
    std::shared_ptr<S3Config> s3Config = std::make_shared<S3Config>();
    S3Deployer deployer(s3Config, codePackageThresholds);

    std::string objDir = "/tmp/home/s3/layer/func/bucket/files";
    litebus::os::Mkdir(objDir);
    auto file1 = objDir + "/a.txt";
    EXPECT_TRUE(Write(file1, "a"));
    auto file2 = objDir + "/b.txt";
    EXPECT_TRUE(Write(file2, "b"));
    auto file3 = objDir + "/c.txt";
    std::string largeStr;
    for (int i = 0; i < 1000000; i++) {
        largeStr += "cccccccccccccccccccccccccccccc";
    }
    EXPECT_TRUE(Write(file3, largeStr));
    auto file4 = objDir + "/d.txt";
    EXPECT_TRUE(Write(file4, "d"));
    auto file5 = objDir + "/e.txt";
    EXPECT_TRUE(Write(file5, "e"));

    EXPECT_EQ(ExecuteCommand("zip -r /tmp/home/s3/layer/func/bucket/test1.zip " + objDir + "/a.txt").error.empty(), true);
    auto status = deployer.PackageValidation("/tmp/home/s3/layer/func/bucket/test1.zip", "", "");
    EXPECT_TRUE(status.IsOk());

    EXPECT_EQ(ExecuteCommand("zip -r /tmp/home/s3/layer/func/bucket/test2.zip " + objDir + "/c.txt").error.empty(), true);
    status = deployer.PackageValidation("/tmp/home/s3/layer/func/bucket/test2.zip", "", "");
    EXPECT_TRUE(status.IsOk());

    EXPECT_EQ(ExecuteCommand("zip -r /tmp/home/s3/layer/func/bucket/test3.zip " + objDir + "/a.txt " + objDir + "/b.txt " + objDir + "/d.txt " + objDir + "/e.txt").error.empty(), true);
    status = deployer.PackageValidation("/tmp/home/s3/layer/func/bucket/test3.zip", "", "");
    EXPECT_TRUE(status.IsOk());
    EXPECT_TRUE(litebus::os::Rmdir("/tmp/home/s3/layer/func/bucket").IsNone());
}

TEST_F(DISABLED_S3DeployerTest, CheckPackageSignatureTest)
{
    std::shared_ptr<S3Config> s3Config = std::make_shared<S3Config>();
    messages::CodePackageThresholds codePackageThresholds;
    codePackageThresholds.set_filecountsmax(30);
    codePackageThresholds.set_zipfilesizemaxmb(10);
    codePackageThresholds.set_unzipfilesizemaxmb(10);
    codePackageThresholds.set_dirdepthmax(10);
    S3Deployer deployer(s3Config, codePackageThresholds, true);
    std::string objDir = "/tmp/home/s3/layer/func/bucket/files";
    litebus::os::Mkdir(objDir);
    auto file1 = objDir + "/a.txt";
    std::string largeStr;
    for (int i = 0; i < 100; i++) {
        largeStr += "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    }
    EXPECT_TRUE(Write(file1, largeStr));
    EXPECT_EQ(ExecuteCommand("zip -r /tmp/home/s3/layer/func/bucket/test1.zip " + objDir + "/a.txt").error.empty(), true);
    auto status =deployer.PackageValidation("/tmp/home/s3/layer/func/bucket/test1.zip", "", "");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.ToString(), testing::HasSubstr("package signature doesn't match"));
    status = deployer.PackageValidation("/tmp/home/s3/layer/func/bucket/test1.zip", "aaaaaaaaaaaa", "");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.ToString(), testing::HasSubstr("package signature doesn't match"));
    status = deployer.PackageValidation("/tmp/home/s3/layer/func/bucket/test1.zip", "", "aaaaaaaaaaaa");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.ToString(), testing::HasSubstr("package signature doesn't match"));
    std::stringstream ss(ExecuteCommand("sha256sum /tmp/home/s3/layer/func/bucket/test1.zip").output);
    std::string cmdResult;
    ss >> cmdResult;
    status = deployer.PackageValidation("/tmp/home/s3/layer/func/bucket/test1.zip", "", cmdResult);
    EXPECT_TRUE(status.IsOk());
    std::stringstream ss1(ExecuteCommand("sha512sum /tmp/home/s3/layer/func/bucket/test1.zip").output);
    std::string cmdResult1;
    ss1 >> cmdResult1;
    status = deployer.PackageValidation("/tmp/home/s3/layer/func/bucket/test1.zip", cmdResult1, "");
    EXPECT_TRUE(status.IsOk());
    EXPECT_TRUE(litebus::os::Rmdir("/tmp/home/s3/layer/func/bucket").IsNone());
}
}  // namespace functionsystem::test::function_agent



namespace functionsystem::function_agent {
class S3DeployerPrivateTest : public ::testing::Test {
protected:
    inline static messages::CodePackageThresholds codePackageThresholds_;
};

TEST_F(S3DeployerPrivateTest, InitObsOptions)
{
    auto s3Config = std::make_shared<S3Config>();
    s3Config->protocol = "https";

    S3Deployer deployer(s3Config, codePackageThresholds_);
    messages::DeploymentConfig deploymentConfig;
    obs_options options;
    init_obs_options(&options);

    // domain
    s3Config->endpoint = "obs.cn-north-7.ulanqab.huawei.com";
    deployer.InitObsOptions(&options, deploymentConfig, s3Config);
    const int obsType = 1;
    EXPECT_EQ(options.request_options.auth_switch, obsType);
    const int obsUriStyleVirtualhost = 0;
    EXPECT_EQ(options.bucket_options.uri_style, obsUriStyleVirtualhost);

    // ip:port
    s3Config->endpoint = "10.243.31.23:30110";
    deployer.InitObsOptions(&options, deploymentConfig, s3Config);
    const int obsUriStylePath = 1;
    EXPECT_EQ(options.bucket_options.uri_style, obsUriStylePath);
}
}  // namespace functionsystem::function_agent