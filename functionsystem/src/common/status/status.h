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

#ifndef FUNCTIONSYSTEM_STATUS_H
#define FUNCTIONSYSTEM_STATUS_H

#include <async/option.hpp>
#include <cassert>
#include <map>
#include <memory>
#include <string>

#include "common/logs/logging.h"
#include "common/proto/pb/posix_pb.h"

// ASSERT in functionsystem
#ifdef ASSERT_FS
#undef ASSERT_FS
#endif
#define ASSERT_FS(x) \
    do {             \
        assert((x)); \
    } while (false)

#ifdef RETURN_IF_NOT_OK
#undef RETURN_IF_NOT_OK
#endif
#define RETURN_IF_NOT_OK(statement)                \
    do {                                           \
        ::functionsystem::Status rc = (statement); \
        if (rc.IsError()) {                        \
            return rc;                             \
        }                                          \
    } while (false)

#ifdef RETURN_IF_TRUE
#undef RETURN_IF_TRUE
#endif
#define RETURN_IF_TRUE(statement, m) \
    do {                             \
        if (statement) {             \
            YRLOG_ERROR(m);          \
            return;                  \
        }                            \
    } while (false)

#ifdef ASSERT_IF_NULL
#undef ASSERT_IF_NULL
#endif
#define ASSERT_IF_NULL(x)                                         \
    do {                                                          \
        if (!(x)) {                                               \
            YRLOG_ERROR("invalid parameter, pointer is null");    \
            (void)raise(SIGINT);                                  \
        }                                                         \
    } while (false)

#ifdef RETURN_IF_NULL
#undef RETURN_IF_NULL
#endif
#define RETURN_IF_NULL(x)                                      \
    do {                                                       \
        if ((x) == nullptr) {                                  \
            YRLOG_ERROR("invalid parameter, pointer is null"); \
            return;                                            \
        }                                                      \
    } while (false)

#ifdef RETURN_STATUS_IF_NULL
#undef RETURN_STATUS_IF_NULL
#endif
#define RETURN_STATUS_IF_NULL(x, c, m)             \
    do {                                           \
        if ((x) == nullptr) {                      \
            return ::functionsystem::Status(c, m); \
        }                                          \
    } while (false)

#ifdef RETURN_STATUS_IF_TRUE
#undef RETURN_STATUS_IF_TRUE
#endif
#define RETURN_STATUS_IF_TRUE(x, c, m)             \
    do {                                           \
        if ((x)) {                                 \
            YRLOG_ERROR(m);                        \
            return ::functionsystem::Status(c, m); \
        }                                          \
    } while (false)

#ifdef RETURN_NONE_IF_NULL
#undef RETURN_NONE_IF_NULL
#endif
#define RETURN_NONE_IF_NULL(x)      \
    do {                            \
        if ((x) == nullptr) {       \
            return litebus::None(); \
        }                           \
    } while (false)

#ifdef CHECKED_IS_NONE_EXIT
#undef CHECKED_IS_NONE_EXIT
#endif
#define CHECKED_IS_NONE_EXIT(opt)                           \
    do {                                                    \
        if ((opt).IsNone()) {                               \
            YRLOG_ERROR("option object is none,will exit"); \
            BUS_EXIT("Exit for none of Option object.");    \
        }                                                   \
    } while (0)

namespace functionsystem {

enum CompCode : int32_t {
    COMMON = 0,
    POSIX = 1000,
    BUSPROXY = 10000,
    FUNCTION_ACCESSOR = 20000,
    RUNTIME_INSTANCE = 30000,
    GLOBAL_SCHEDULER = 40000,
    LOCAL_SCHEDULER = 50000,
    DOMAIN_SCHEDULER = 60000,
    FUNCTION_AGENT = 70000,
    RUNTIME_MANAGER = 80000,
    IAM_SERVER = 90000,
    META_STORE = 91000,
    END = 100000,
};

enum StatusCode : int32_t {
    FAILED = static_cast<int>(COMMON) - 1,
    SUCCESS = static_cast<int>(COMMON),
    // Error code 1 is conflict with error code in litebus, which should never use.
    RESERVED = static_cast<int>(COMMON) + 1,
    LOG_CONFIG_ERROR,
    PARAMETER_ERROR,
    ENV_CONFIG_ERROR,
    REQUEST_TIME_OUT,
    RESOURCE_NOT_ENOUGH,
    SCHEDULE_CONFLICTED,
    INSTANCE_ALLOCATED,
    FILE_NOT_FOUND,
    JSON_PARSE_ERROR,
    REGISTER_ERROR,
    CONN_ERROR,
    POINTER_IS_NULL,
    STS_DISABLED,

    // Common component RPC error code, range [100, 199]
    SYNC_GRPC_CALL_ERROR = static_cast<int>(COMMON) + 100,
    GRPC_CQ_ERROR,
    GRPC_CALL_OBJ_ERROR,
    GRPC_STREAM_CALL_ERROR,
    GRPC_OK,
    GRPC_CANCELLED,
    GRPC_UNKNOWN,
    GRPC_INVALID_ARGUMENT,
    GRPC_DEADLINE_EXCEEDED,
    GRPC_NOT_FOUND,
    GRPC_ALREADY_EXISTS,
    GRPC_PERMISSION_DENIED,
    GRPC_RESOURCE_EXHAUSTED,
    GRPC_FAILED_PRECONDITION,
    GRPC_ABORTED,
    GRPC_OUT_OF_RANGE,
    GRPC_UNIMPLEMENTED,
    GRPC_INTERNAL,
    GRPC_UNAVAILABLE,
    GRPC_DATA_LOSS,
    GRPC_UNAUTHENTICATED,

    // Common component schedule framework error code, range [200, 999]
    PLUGIN_REGISTER_ERROR = static_cast<int>(COMMON) + 200,
    PLUGIN_UNREGISTER_ERROR,
    FILTER_PLUGIN_ERROR,
    SCORE_PLUGIN_SERROR,
    BING_PLUGIN_ERROR,
    POST_BIND_PLUGIN_ERROR,
    INSTANCE_UNSCHEDULE_ERROR,
    INVALID_RESOURCE_PARAMETER,
    AFFINITY_SCHEDULE_FAILED,
    HETEROGENEOUS_SCHEDULE_FAILED,
    DISK_SCHEDULE_FAILED,

    // instance transaction error coder, range [300, 350)
    // transaction failed because version not consistent
    INSTANCE_TRANSACTION_WRONG_VERSION = static_cast<int>(COMMON) + 300,
    // transaction failed because Get operationType is wrong or value is empty
    INSTANCE_TRANSACTION_GET_INFO_FAILED,
    INSTANCE_TRANSACTION_WRONG_RESPONSE_SIZE,
    INSTANCE_TRANSACTION_DELETE_FAILED,
    INSTANCE_TRANSACTION_WRONG_PARAMETER,

    // Posix request error code, range [1000, 2000)
    ERR_PARAM_INVALID = static_cast<int>(POSIX) + 1,
    ERR_RESOURCE_NOT_ENOUGH,
    ERR_INSTANCE_NOT_FOUND,
    ERR_INSTANCE_DUPLICATED,
    ERR_INVOKE_RATE_LIMITED,
    ERR_RESOURCE_CONFIG_ERROR,
    ERR_INSTANCE_EXITED,
    ERR_EXTENSION_META_ERROR,
    ERR_INSTANCE_SUB_HEALTH,
    ERR_GROUP_SCHEDULE_FAILED,
    ERR_GROUP_EXIT_TOGETHER,
    ERR_CREATE_RATE_LIMITED,
    ERR_INSTANCE_EVICTED,
    ERR_AUTHORIZE_FAILED,
    ERR_FUNCTION_META_NOT_FOUND,
    ERR_INSTANCE_INFO_INVALID,
    ERR_SCHEDULE_CANCELED,
    ERR_SCHEDULE_PLUGIN_CONFIG,
    ERR_SUB_STATE_INVALID,
    ERR_INSTANCE_SUSPEND,
    ERR_INSTANCE_BUSY,

    // Posix user error code, range [2000, 3000)
    ERR_USER_CODE_LOAD = static_cast<int>(POSIX) + 1001,
    ERR_USER_FUNCTION_EXCEPTION,

    // Posix inner system error code, range [3000, 4000)
    ERR_REQUEST_BETWEEN_RUNTIME_BUS = static_cast<int>(POSIX) + 2001,
    ERR_INNER_COMMUNICATION,
    ERR_INNER_SYSTEM_ERROR,
    ERR_DISCONNECT_FRONTEND_BUS,
    ERR_ETCD_OPERATION_ERROR,
    ERR_BUS_DISCONNECTION,
    ERR_REDIS_OPERATION_ERROR,
    ERR_K8S_UNAVAILABLE,
    ERR_FUNCTION_AGENT_OPERATION_ERROR,
    ERR_STATE_MACHINE_ERROR,
    ERR_LOCAL_SCHEDULER_OPERATION_ERROR,
    ERR_RUNTIME_MANAGER_OPERATION_ERROR,
    ERR_INSTANCE_MANAGER_OPERATION_ERROR,
    ERR_LOCAL_SCHEDULER_ABNORMAL,
    ERR_DS_UNAVAILABLE,
    ERR_NPU_FAULT_ERROR,

    // Busproxy error code, range [10000, 20000)
    BP_DATASYSTEM_ERROR = static_cast<int>(BUSPROXY),
    BP_INSTANCE_NOT_FOUND,
    BP_META_STORAGE_PUT_ERROR,
    BP_META_STORAGE_DELETE_ERROR,
    BP_META_STORAGE_REVOKE_ERROR,
    BP_LEASE_ID_NOT_FOUND,
    BP_PROXYACTOR_NULL,
    BP_META_STORAGE_GRANT_ERROR,
    INSTANCE_HEARTBEAT_LOST,
    INSTANCE_HEALTH_CHECK_ERROR,
    INSTANCE_SUB_HEALTH,
    DS_SCALE_DOWN,

    // FunctionAccessor error code, range [20000, 30000)
    FA_HTTP_REGISTER_HANDLER_NULL_ERROR = static_cast<int>(FUNCTION_ACCESSOR),
    FA_HTTP_REGISTER_REPEAT_URL_ERROR,
    FA_REQUIRED_CPU_SIZE_INVALID,
    FA_REQUIRED_MEMORY_SIZE_INVALID,
    FA_CPU_NOT_ENOUGH,
    FA_MEMORY_NOT_ENOUGH,
    FA_CUSTOM_RESOURCE_NOT_ENOUGH,
    FA_FUNCTION_META_NOT_EXISTED,
    FA_FUNCTION_META_EMPTY_CPU,
    FA_FUNCTION_META_EMPTY_MEMORY,

    // Instance error code, [30000, 40000)
    INSTANCE_FAILED_OR_KILLED = static_cast<int>(RUNTIME_INSTANCE),
    RUNTIME_ERROR_FATAL,  // fatal error indicates the instance exits with serious problems and should not be recovered
    RUNTIME_ERROR_NON_FATAL,  // nonfatal error indicates the instance exits accidentally and should be recovered
    INSTANCE_DISK_USAGE_EXCEED_LIMIT,

    // Global scheduler error code, range [40000, 50000)
    GS_GET_FROM_METASTORE_FAILED = static_cast<int>(GLOBAL_SCHEDULER),
    GS_PUT_TO_METASTORE_FAILED,
    GS_SCHED_TOPOLOGY_BROKEN,
    GS_ACTIVATE_DOMAIN_FAILED,
    GS_REGISTER_REQUEST_INVALID,
    GS_REGISTERED_SCHEDULER_TOPOLOGY_IS_NONE,
    GS_START_SCALER_FAILED,
    GS_START_CREATE_DEPLOYMENTS_FAILED,
    GS_START_CREATE_POD_FAILED,

    // Local scheduler error code, [50000, 60000)
    LS_DOMAIN_SCHEDULER_AID_EMPTY = static_cast<int>(LOCAL_SCHEDULER),
    LS_GLOBAL_SCHEDULER_AID_EMPTY,
    LS_INSTANCE_CTRL_IS_NULL,
    LS_REGISTRY_TIMEOUT,
    LS_META_STORE_ACCESSOR_IS_NULL,
    LS_PING_PONG_IS_NULL,
    LS_SYNC_RESCHEDULE_INSTANCE_FAIL,
    LS_SYNC_KILL_INSTANCE_FAIL,
    LS_SYNC_DEPLOY_INSTANCE_FAIL,
    LS_SYNC_INSTANCE_FAIL,
    LS_DEPLOY_INSTANCE_FAILED,
    LS_INIT_RUNTIME_FAILED,
    LS_AGENT_MGR_START_HEART_BEAT_FAIL,
    LS_AGENT_NOT_FOUND,
    LS_RESOURCE_VIEW_IS_NULL,
    LS_SYNC_INSTANCE_COMPLETE,
    LS_META_STORAGE_GET_ERROR,
    LS_REQUEST_NOT_FOUND,
    LS_FORWARD_DOMAIN_TIMEOUT,
    LS_FORWARD_REQUEST_IS_NULL,
    LS_UPDATE_INSTANCE_FAIL,
    LS_FORWARD_INSTANCE_MANAGER_TIMEOUT,
    LS_AGENT_EVICTED,
    LS_DEPLOY_GET_TEMPORARY_ACCESS_KEY_FAIL,

    // Domain error code, range [60000, 70000)
    DOMAIN_SCHEDULER_REGISTER_ERR = static_cast<int>(DOMAIN_SCHEDULER),
    DOMAIN_SCHEDULER_FORWARD_ERR,
    DOMAIN_SCHEDULER_UNAVAILABLE_SCHEDULER,
    DOMAIN_SCHEDULER_RESERVE,
    DOMAIN_SCHEDULER_NO_PREEMPTABLE_INSTANCE,

    // function-agent error code, range [70000, 80000)
    FUNC_AGENT_REQUEST_ID_ILLEGAL_ERROR = static_cast<int>(FUNCTION_AGENT) + 50,
    FUNC_AGENT_REQUEST_ID_REPEAT_ERROR,
    FUNC_AGENT_INVALID_DEPLOYER_ERROR,
    FUNC_AGENT_FAILED_DEPLOY,
    FUNC_AGENT_DEPLOYMENT_CONFIG_NOT_FOUND,
    FUNC_AGENT_REPEATED_DEPLOY_REQUEST_ERROR,
    FUNC_AGENT_SET_NETWORK_ERROR,
    FUNC_AGENT_NETWORK_WORK_ERROR,
    FUNC_AGENT_EXITED,
    FUNC_AGENT_EXITING,
    FUNC_AGENT_INVALID_TOKEN_ERROR,
    FUNC_AGENT_INVALID_ACCESS_KEY_ERROR,
    FUNC_AGENT_INVALID_SECRET_ACCESS_KEY_ERROR,
    FUNC_AGENT_INVALID_WORKING_DIR_FILE,
    FUNC_AGENT_MKDIR_DEST_WORKING_DIR_ERROR,
    FUNC_AGENT_UNSUPPORTED_WORKING_DIR_SCHEMA,
    FUNC_AGENT_NOT_REGISTERED,

    FUNC_AGENT_OBS_INIT_OPTIONS_ERROR = static_cast<int>(FUNCTION_AGENT) + 60,
    FUNC_AGENT_OBS_OPEN_FILE_ERROR,
    FUNC_AGENT_OBS_ADD_BUCKET_ERROR,
    FUNC_AGENT_OBS_DEL_BUCKET_ERROR,
    FUNC_AGENT_OBS_PUT_OBJECT_ERROR,
    FUNC_AGENT_OBS_GET_OBJECT_ERROR,
    FUNC_AGENT_OBS_RENAME_TMP_ERROR,
    FUNC_AGENT_OBS_CONNECTION_ERROR,
    FUNC_AGENT_OBS_ERROR_NEED_RETRY,

    FUNC_AGENT_PING_PONG_IS_NULL = static_cast<int>(FUNCTION_AGENT) + 70,
    FUNC_AGENT_RESOURCE_UNIT_IS_NULL,
    FUNC_AGENT_START_HEARTBEAT_FAILED,
    FUNC_AGENT_INVALID_STORAGE_TYPE,
    FUNC_AGENT_INVALID_DEPLOY_DIRECTORY,
    FUNC_AGENT_START_RUNTIME_FAILED,

    FUNC_AGENT_STATUS_VPC_PROBE_FAILED,
    FUNC_AGENT_REGIS_INFO_SERIALIZED_FAILED,

    FUNC_AGENT_CLEAN_CODE_PACKAGE_TIME_OUT = static_cast<int>(FUNCTION_AGENT) + 80,

    FUNC_AGENT_ILLEGAL_NSP_URL = static_cast<int>(FUNCTION_AGENT) + 90,  // for nsp.
    FUNC_AGENT_NSP_REQUEST_FAILED,
    FUNC_AGENT_NSP_RESPONSE_FAILED,
    FUNC_AGENT_ILLEGAL_OBS_URL,
    FUNC_AGENT_ILLEGAL_OBS_METHOD,
    FUNC_AGENT_ILLEGAL_OBS_HEADERS,
    FUNC_AGENT_OBS_REQUEST_FAILED,
    FUNC_AGENT_OBS_RESPONSE_FAILED,
    FUNC_AGENT_OBS_ILLEGAL_RANGES,
    FUNC_AGENT_NSP_AUTH_PARAMS_ERROR,

    // runtime-manager error code, range [80000, 90000)
    RUNTIME_MANAGER_PORT_UNAVAILABLE = static_cast<int>(RUNTIME_MANAGER),
    RUNTIME_MANAGER_EXEC_PATH_NOT_FOUND,
    RUNTIME_MANAGER_BUILD_ARGS_INVALID,
    RUNTIME_MANAGER_MOUNT_VOLUME_FAILED,
    RUNTIME_MANAGER_CREATE_EXEC_FAILED,
    RUNTIME_MANAGER_EXECUTABLE_PATH_INVALID,
    RUNTIME_MANAGER_RUNTIME_PROCESS_NOT_FOUND,
    RUNTIME_MANAGER_EXEC_RUN_COMMAND_FAILED,
    RUNTIME_MANAGER_EXEC_STOP_RUN_COMMAND_FAILED,
    RUNTIME_MANAGER_EXEC_DAEMON_EXIT,
    RUNTIME_MANAGER_STOP_INSTANCE_FAILED,
    RUNTIME_MANAGER_EXEC_GET_OUTPUT_FAILED,
    RUNTIME_MANAGER_START_INSTANCE_FAILED,
    RUNTIME_MANAGER_PARAMS_INVALID,
    RUNTIME_MANAGER_DISK_USAGE_EXCEED_LIMIT,
    RUNTIME_MANAGER_INSTANCE_HAS_BEEN_DEPLOYED,
    RUNTIME_MANAGER_POST_START_EXEC_FAILED,
    RUNTIME_MANAGER_CLEAN_STATUS_RESPONSE_TIME_OUT,
    RUNTIME_MANAGER_REGISTER_FAILED,
    RUNTIME_MANAGER_UPDATE_TOKEN_FAILED,
    RUNTIME_MANAGER_EXEC_WRITE_PIPE_FAILED,
    RUNTIME_MANAGER_GPU_NOTFOUND,
    RUNTIME_MANAGER_GPU_PARTITION_NOTFOUND,
    RUNTIME_MANAGER_NPU_NOTFOUND,
    RUNTIME_MANAGER_NPU_PARTITION_NOTFOUND,
    RUNTIME_MANAGER_INSTANCE_EXIST,
    RUNTIME_MANAGER_WORKING_DIR_FOR_APP_NOTFOUND,
    RUNTIME_MANAGER_DEPLOY_DIR_IS_EMPTY,
    RUNTIME_MANAGER_DEBUG_SERVER_NOTFOUND,
    RUNTIME_MANAGER_CONDA_PARAMS_INVALID,
    RUNTIME_MANAGER_CONDA_ENV_FILE_WRITE_FAILED,
    RUNTIME_MANAGER_CONDA_ENV_NOT_EXIST,
    RUNTIME_MANAGER_WARMUP_FAILURE,
    RUNTIME_MANAGER_RUNTIME_NOT_FOUND,
    RUNTIME_MANAGER_CHECKPOINT_FAILED,
    RUNTIME_MANAGER_CHECKPOINT_REGISTER_FAILED,

    // Checkpoint errors
    ERR_CHECKPOINT_NOT_FOUND,
    ERR_CHECKPOINT_ALREADY_EXISTS,
    ERR_FILE_OPERATION_FAILED,

    // iam-server error code, range [90000,91000)
    IAM_WAIT_INITIALIZE_COMPLETE = static_cast<int>(IAM_SERVER),

    // meta-store error code, range [91000,92000)
    META_STORE_BACK_UP_ERR = static_cast<int>(META_STORE),
};

class Status {
public:
    Status();
    explicit Status(enum StatusCode statusCode, const std::string &errMsg = "");
    Status(const StatusCode code, int lineOfCode, const char *fileName, const std::string &errMsg = "");

    ~Status() = default;

    static Status OK();

    void AppendMessage(const std::string &errMsg);
    enum StatusCode StatusCode() const;
    std::string ToString() const;
    int GetLineOfCode() const;
    std::string GetMessage() const;
    const std::string &RawMessage() const;
    bool MultipleErr() const;

    bool operator==(const Status &other) const;
    bool operator==(enum StatusCode otherCode) const;
    bool operator!=(const Status &other) const;
    bool operator!=(enum StatusCode otherCode) const;

    bool IsOk() const;
    bool IsError() const;

    explicit operator bool() const;
    explicit operator int() const;

    friend std::ostream &operator<<(std::ostream &os, const Status &s);

    static std::string GetStatusInfo(enum StatusCode code);

    static common::ErrorCode GetPosixErrorCode(enum StatusCode code);

    static common::ErrorCode GetPosixErrorCode(common::ErrorCode code);

    static common::ErrorCode GetPosixErrorCode(int32_t code);

    static enum StatusCode GrpcCode2StatusCode(int grpcErrCode);

    static bool NeedKeepStatusCode(enum StatusCode code);

private:
    struct Data;
    std::shared_ptr<Data> data_;
    static std::map<enum StatusCode, std::string> statusInfoMap_;
    static std::map<enum StatusCode, enum StatusCode> codeToPosix_;
};

}  // namespace functionsystem

#endif  // FUNCTIONSYSTEM_STATUS_H