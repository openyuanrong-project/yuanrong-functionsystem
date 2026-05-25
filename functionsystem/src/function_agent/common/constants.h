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

#ifndef FUNCTION_AGENT_CONSTANTS_H
#define FUNCTION_AGENT_CONSTANTS_H

#include <string>

namespace functionsystem::function_agent {
const int32_t DEFAULT_RANDOM_PRECISE = 36;
const int32_t NUMBER_LOWER_LETTERS = 26;
const int32_t NUMBER_ARABIC_NUMERALS = 10;

constexpr int32_t MIN_FILE_COUNTS = 10;
constexpr int32_t MAX_FILE_COUNTS = 30000;
constexpr int32_t DEFAULT_FILE_LIMIT_COUNTS = 30000;

constexpr int32_t MIN_FILE_SIZE_MB = 10;
constexpr int32_t MAX_FILE_SIZE_MB = 1024 * 1024 * 10;
constexpr int32_t DEFAULT_ZIP_FILE_LIMIT_SIZE_MB = 500;
constexpr int32_t DEFAULT_UNZIP_FILE_LIMIT_SIZE_MB = 1000;

constexpr int32_t MIN_DIR_DEPTH = 1;
constexpr int32_t MAX_DIR_DEPTH = 50;
constexpr int32_t DEFAULT_DIR_LIMIT_DEPTH = 20;

constexpr uint64_t WAIT_UPDATE_THRESHOLDS_CONFIGMAP_MS = 1000;

const std::string CODE_PKG_THRESHOLDS_CFG_FILE = "code-package-thresholds.json";

const std::string LOCAL_STORAGE_TYPE = "local";
const std::string S3_STORAGE_TYPE = "s3";
const std::string COPY_STORAGE_TYPE = "copy";
const std::string WORKING_DIR_STORAGE_TYPE = "working_dir";

const std::string JAVA_LANGUAGE = "java";
const std::string RUNTIME_LAYER_DIR_NAME = "layer";
const std::string RUNTIME_FUNC_DIR_NAME = "func";

const int32_t CODE_MGR_INCREASE_TYPE = 1;
const int32_t CODE_MGR_DECREASE_TYPE = -1;

const int32_t REGISTER_AGENT_TIMEOUT = 6000;
const int32_t PING_TIME_OUT_MS = 10000;
const int32_t UPDATE_INSTANCE_STATUS_TIMEOUT = 3000;
const int32_t UPDATE_AGENT_STATUS_TIMEOUT = 500;

const int32_t UPDATE_INSTANCE_STATUS_RETRY_TIMES = 10;
const std::string RUNTIME_ID_PREFIX = "runtime-";

const std::string DELEGATE_BOOTSTRAP = "DELEGATE_BOOTSTRAP";
const std::string DELEGATE_DOWNLOAD = "DELEGATE_DOWNLOAD";
const std::string ENV_DELEGATE_DOWNLOAD_STORAGE_TYPE = "ENV_DELEGATE_DOWNLOAD_STORAGE_TYPE";

const std::string KUBERNETES_SERVICE_HOST = "KUBERNETES_SERVICE_HOST";
const std::string KUBERNETES_SERVICE_PORT = "KUBERNETES_SERVICE_PORT";
const std::string POD_NAME = "POD_NAME";
const std::string POD_NAMESPACE = "POD_NAMESPACE";
const std::string GODEBUG_KEY = "GODEBUG";
const std::string GODEBUG_VALUE = "madvdontneed=1,cgocheck=0";

const std::string LAYER_LIB_PATH = "LAYER_LIB_PATH";
const std::string DELEGATE_LAYER_DOWNLOAD = "DELEGATE_LAYER_DOWNLOAD";
const std::string ENV_DELEGATE_DECRYPT = "ENV_DELEGATE_DECRYPT";
const std::string DELEGATE_DIRECTORY_INFO = "DELEGATE_DIRECTORY_INFO";
const std::string DELEGATE_DIRECTORY_QUOTA = "DELEGATE_DIRECTORY_QUOTA";
const std::string POST_START_EXEC = "POST_START_EXEC";
const std::string S3_DEPLOY_DIR = "S3_DEPLOY_DIR";

const std::string S3_PROTOCOL_HTTP = "http";
const std::string S3_PROTOCOL_HTTPS = "https";

const std::string GCM_CRYPTO_ALGORITHM = "GCM";
const std::string CBC_CRYPTO_ALGORITHM = "CBC";
const std::string NO_CRYPTO_ALGORITHM = "NO_CRYPTO";

const int DEFAULT_RETRY_CLEAR_CODE_PACKAGER_TIMES = 3;
}  // namespace functionsystem::function_agent
#endif  // FUNCTION_AGENT_CONSTANTS_H
