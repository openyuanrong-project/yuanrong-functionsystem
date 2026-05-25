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

#include "s3_deployer.h"

#include <sys/stat.h>

#include <utility>

#include "common/constants/constants.h"
#include "common/logs/logging.h"
#include "common/utils/ssl_config.h"
#include "minizip/unzip.h"
#include "common/utils/param_check.h"

namespace functionsystem::function_agent {
const uint32_t RECOVER_RETRY_COUNT = 3;

S3Deployer::S3Deployer(std::shared_ptr<S3Config> config, messages::CodePackageThresholds codePackageThresholds,
                       bool enableSignatureValidation)
    : S3Deployer(config, std::make_shared<ObsWrapper>(), codePackageThresholds, enableSignatureValidation)
{
}

S3Deployer::~S3Deployer()
{
    if (obsWrapper_ == nullptr) {
        return;
    }
    try {
        obsWrapper_->DeinitializeObs();
    } catch (const std::exception &e) {
        // Ignore
    } catch (...) {
        // Ignore
    }
}

Status S3Deployer::InitHelper(uint32_t &retryCount)
{
    if (obsWrapper_ == nullptr) {
        return Status(StatusCode::FUNC_AGENT_OBS_INIT_OPTIONS_ERROR);
    }
    obs_status status = obsWrapper_->InitializeObs();
    if (status != OBS_STATUS_OK) {
        retryCount--;
        return Reconnect(retryCount, status);
    }
    gReconnectObsRetryCount = RECOVER_RETRY_COUNT;
    YRLOG_DEBUG("initialize obs sdk succeeded.");
    return Status::OK();
}

Status S3Deployer::Reconnect(uint32_t &retryCount, const obs_status &status)
{
    YRLOG_WARN("failed to initialize obs sdk, status is {}. retry connect, rest retry count: {}.",
               obs_get_status_name(status), retryCount);
    // obs supposed to call in obs_initialize interface once in a single process
    // so first disconnect obs, then do reconnection action
    obsWrapper_->DeinitializeObs();
    if (retryCount > 0) {
        return InitHelper(retryCount);
    } else {
        gReconnectObsRetryCount = RECOVER_RETRY_COUNT;
        YRLOG_ERROR("failed to init obs after tried 3 times.");
        return Status(StatusCode::FUNC_AGENT_OBS_CONNECTION_ERROR, "failed to connect to obs.");
    }
}

struct GetObjectCallbackData {
    FILE *outfile;
    obs_status status;
};

struct GetObjectMetaDataCallbackData {
    uint64_t objectLength;
    obs_status status;
};

obs_status OnGetObjectMetaDataBegin(const obs_response_properties *properties, void *callbackData)
{
    YRLOG_DEBUG("begin to Get object meta data, request's id is {}.", properties->request_id);
    auto *data = (GetObjectMetaDataCallbackData *)callbackData;
    data->objectLength = properties->content_length;
    return OBS_STATUS_OK;
}

void OnGetObjectMetaDataComplete(obs_status status, const obs_error_details *error, void *callbackData)
{
    if (callbackData == nullptr) {
        YRLOG_WARN("callbackData is null");
        return;
    }
    auto *data = (GetObjectMetaDataCallbackData *)callbackData;
    data->status = status;
    YRLOG_DEBUG("complete to Get object meta data, status is {}, content length is {}.", obs_get_status_name(status),
                data->objectLength);
}

obs_status OnGetObjectBegin(const obs_response_properties *properties, void *callbackData)
{
    YRLOG_DEBUG("begin to Get object, request's id is {}.", properties->request_id);
    return OBS_STATUS_OK;
}

obs_status OnGetObjectProgress(int bufferSize, const char *buffer, void *callbackData)
{
    if (callbackData == nullptr) {
        YRLOG_WARN("callbackData is null");
        return OBS_STATUS_AbortedByCallback;
    }
    auto *data = (GetObjectCallbackData *)callbackData;
    size_t bufferSizeT = static_cast<unsigned short>(bufferSize);
    size_t wrote = fwrite(buffer, 1, bufferSizeT, data->outfile);
    return (wrote < bufferSizeT) ? OBS_STATUS_AbortedByCallback : OBS_STATUS_OK;
}

void OnGetObjectComplete(obs_status status, const obs_error_details *error, void *callbackData)
{
    if (callbackData == nullptr) {
        YRLOG_WARN("callbackData is null");
        return;
    }
    YRLOG_DEBUG("complete to Get object, status is {}.", obs_get_status_name(status));
    auto *data = (GetObjectCallbackData *)callbackData;
    data->status = status;
}

bool S3Deployer::InitObsOptions(obs_options *options, const ::messages::DeploymentConfig &config,
                                const std::shared_ptr<S3Config> &s3Config) const
{
    if (s3Config == nullptr ||
        (!config.hostname().empty() && (config.securitytoken().empty() || config.temporaryaccesskey().empty() ||
                                        config.temporarysecretkey().empty()))) {
        return false;
    }
    if (config.hostname().empty()) {
        options->bucket_options.host_name = const_cast<char *>(s3Config->endpoint.c_str());  // ip:port or domain
        if (s3Config->credentialType == CREDENTIAL_TYPE_ROTATING_CREDENTIALS) {
            // use function_proxy rotation AK/SK and SecretToken
            options->bucket_options.token = const_cast<char *>(config.securitytoken().c_str());
            options->bucket_options.access_key = const_cast<char *>(config.temporaryaccesskey().c_str());
            options->bucket_options.secret_access_key = const_cast<char *>(config.temporarysecretkey().c_str());
        } else {
            // use permanent AK/SK
            options->bucket_options.access_key = const_cast<char *>(s3Config->accessKey.c_str());
            options->bucket_options.secret_access_key = const_cast<char *>(s3Config->secretKey.GetData());
        }
    } else {
        // layer use temporary AK/SK and SecretToken
        options->bucket_options.host_name = const_cast<char *>(config.hostname().c_str());
        options->bucket_options.token = const_cast<char *>(config.securitytoken().c_str());
        options->bucket_options.access_key = const_cast<char *>(config.temporaryaccesskey().c_str());
        options->bucket_options.secret_access_key = const_cast<char *>(config.temporarysecretkey().c_str());
    }

    options->bucket_options.protocol = s3Config->protocol == function_agent::S3_PROTOCOL_HTTPS
                                           ? obs_protocol::OBS_PROTOCOL_HTTPS
                                           : obs_protocol::OBS_PROTOCOL_HTTP;
    if (size_t pos = s3Config->endpoint.find(':');
        (IsIPValid(s3Config->endpoint.substr(0, pos)) || IsInnerServiceAddress(s3Config->endpoint.substr(0, pos)))) {
        // if endpoint is ip:port must be OBS_URI_STYLE_PATH, otherwise return NameLookupError(33)
        // at this time, the SDK is using the S3 protocol.
        options->bucket_options.uri_style = obs_uri_style::OBS_URI_STYLE_PATH;
    } else {
        // let the SDK be optimized to use the OBS protocol.
        YRLOG_DEBUG("use the OBS protocol");
        options->request_options.auth_switch = OBS_OBS_TYPE;
    }

    options->bucket_options.bucket_name = const_cast<char *>(config.bucketid().c_str());  // bucket id
    return true;
}

void InitObsObjectInfo(obs_object_info *objectInfo, const ::messages::DeploymentConfig &config)
{
    objectInfo->key = const_cast<char *>(config.objectid().c_str());  // [must]object id
    objectInfo->version_id = nullptr;                                 // [must]version id
}

Status S3Deployer::DownloadCode(const std::string &destFile, const ::messages::DeploymentConfig &config)
{
    obs_options options;
    init_obs_options(&options);
    if (!InitObsOptions(&options, config, s3Config_)) {
        YRLOG_ERROR("Failed to init obs options, hostname = {}, bucket = {}. object = {}.", config.hostname(),
                    config.bucketid(), config.objectid());
        return Status(StatusCode::FUNC_AGENT_OBS_INIT_OPTIONS_ERROR, "failed to int obs options");
    }

    obs_object_info objectInfo = { nullptr, nullptr };
    InitObsObjectInfo(&objectInfo, config);

    // home/layer/func/bucket id/object_id-tmp(directory)/object_id(file)
    FILE *file = fopen(destFile.c_str(), "wb");  // -rw-r--r--
    if (file == nullptr) {
        YRLOG_ERROR("failed to open file({}) for object({}).", destFile, config.objectid());
        return Status(StatusCode::FUNC_AGENT_OBS_OPEN_FILE_ERROR, "failed to open file");
    }

    GetObjectMetaDataCallbackData metaData{};
    metaData.status = OBS_STATUS_BUTT;
    metaData.objectLength = 0;

    GetObjectCallbackData data{};
    data.status = OBS_STATUS_BUTT;
    data.outfile = file;

    // get object metadata
    obs_response_handler obsResponseHandler = { &OnGetObjectMetaDataBegin, &OnGetObjectMetaDataComplete };
    get_object_metadata(&options, &objectInfo, nullptr, &obsResponseHandler, &metaData);
    if (metaData.status == OBS_STATUS_ConnectionFailed) {
        YRLOG_ERROR("lost connection with obs, try to reconnect. status code is {}.",
                    obs_get_status_name(metaData.status));
        return RetryDownloadCode(destFile, config);
    } else if (CheckObsErrorNeedRetry(metaData.status)) {
        YRLOG_ERROR("failed to Get object metadata({}), status code is {}, need retry.", config.objectid(),
                    obs_get_status_name(metaData.status));
        return Status(
            StatusCode::FUNC_AGENT_OBS_ERROR_NEED_RETRY,
            "failed to Get object metadata, obs native err: " + std::string(obs_get_status_name(metaData.status)));
    } else if (metaData.status != OBS_STATUS_OK) {
        YRLOG_ERROR("failed to Get object metadata({}), status code is {}.", config.objectid(),
                    obs_get_status_name(metaData.status));
        return Status(
            StatusCode::FUNC_AGENT_OBS_GET_OBJECT_ERROR,
            "failed to Get object metadata, obs native err: " + std::string(obs_get_status_name(metaData.status)));
    }

    if (metaData.objectLength > static_cast<uint64_t>(codePackageThresholds_.zipfilesizemaxmb()) * SIZE_MEGA_BYTES) {
        YRLOG_ERROR("Check download package validation failed. File({} bytes) exceeds the maximum size limit({} bytes)",
                    metaData.objectLength,
                    static_cast<uint64_t>(codePackageThresholds_.zipfilesizemaxmb()) * SIZE_MEGA_BYTES);
        return Status(StatusCode::ERR_USER_CODE_LOAD, "package validation failed, zip file size exceeds maximum limit");
    }

    // get object
    obs_get_conditions conditions = {};
    init_get_properties(&conditions);

    obs_get_object_handler getObjectHandler = { { &OnGetObjectBegin, &OnGetObjectComplete }, &OnGetObjectProgress };
    get_object(&options, &objectInfo, &conditions, nullptr, &getObjectHandler, &data);

    if (fclose(data.outfile) != 0) {
        YRLOG_WARN("failed to close file for object({}).", config.objectid());
    }

    if (data.status == OBS_STATUS_ConnectionFailed) {
        YRLOG_ERROR("lost connection with obs, try to reconnect. status code is {}.", obs_get_status_name(data.status));
        return RetryDownloadCode(destFile, config);
    } else if (CheckObsErrorNeedRetry(data.status)) {
        YRLOG_ERROR("failed to Get object({}), status code is {}, need retry.", config.objectid(),
                    obs_get_status_name(data.status));
        return Status(StatusCode::FUNC_AGENT_OBS_ERROR_NEED_RETRY, "failed to Get object, obs native err: " +
                                                                       std::string(obs_get_status_name(data.status)));
    } else if (data.status != OBS_STATUS_OK) {
        YRLOG_ERROR("failed to Get object({}), status code is {}.", config.objectid(),
                    obs_get_status_name(data.status));
        std::string errMsg =
            "failed to Get object " + config.objectid() + ", obs status " + obs_get_status_name(data.status);
        return Status(StatusCode::FUNC_AGENT_OBS_GET_OBJECT_ERROR, errMsg);
    }

    gDownloadCodeRetryCount = RECOVER_RETRY_COUNT;
    YRLOG_INFO("Success to Get object, object's id is {}", config.objectid());
    return Status::OK();
}

bool S3Deployer::CheckObsErrorNeedRetry(const obs_status &status)
{
    if (status == OBS_STATUS_FailedToConnect || status == OBS_STATUS_InternalError
        || status == OBS_STATUS_PartialFile) {
        return true;
    }
    return false;
}

Status S3Deployer::RetryDownloadCode(const std::string &destFile, const ::messages::DeploymentConfig &config)
{
    if (gDownloadCodeRetryCount == 0) {
        YRLOG_ERROR("failed to download code after retried 3 times.");
        gDownloadCodeRetryCount = RECOVER_RETRY_COUNT;
        return Status(StatusCode::FUNC_AGENT_OBS_CONNECTION_ERROR, "failed to connect to obs.");
    }
    // obs supposed to call in obs_initialize interface once in a single process
    // so first disconnect obs, then do reconnection action
    obsWrapper_->DeinitializeObs();
    Status status = InitHelper(gDownloadInitObsRetryTime);
    if (status != Status::OK()) {
        return Status(StatusCode::FUNC_AGENT_OBS_CONNECTION_ERROR, "failed to connect to obs.");
    }
    gDownloadCodeRetryCount--;
    YRLOG_DEBUG("retry download code. rest retry count:{}.", gDownloadCodeRetryCount);
    return DownloadCode(destFile, config);
}

}  // namespace functionsystem::function_agent