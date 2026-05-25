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

#include "instance_ctrl_actor.h"

#include <nlohmann/json.hpp>

#include "async/async.hpp"
#include "async/asyncafter.hpp"
#include "async/collect.hpp"
#include "async/defer.hpp"
#include "async/option.hpp"
#include "common/constants/constants.h"
#include "common/constants/signal.h"
#include "common/create_agent_decision/create_agent_decision.h"
#include "common/logs/logging.h"
#include "common/metadata/metadata.h"
#include "common/metrics/metrics_adapter.h"
#include "common/posix_client/control_plane_client/control_interface_posix_client.h"
#include "common/posix_service/posix_service.h"
#include "common/proto/pb/posix/common.pb.h"
#include "common/proto/pb/posix_pb.h"
#include "common/resource_view/resource_type.h"
#include "common/resource_view/scala_resource_tool.h"
#include "common/scheduler_framework/utils/label_affinity_selector.h"
#include "common/state_handler/state_handler.h"
#include "common/types/instance_state.h"
#include "common/utils/aksk_content.h"
#include "common/utils/generate_message.h"
#include "common/utils/random_number.h"
#include "common/utils/struct_transfer.h"
#include "common/trace/trace_manager.h"
#include "instance_ctrl_message.h"
#include "local_scheduler/snap_ctrl/snap_ctrl.h"
#include "local_scheduler/grpc_server/bus_service/bus_service.h"
#include "local_scheduler_service/local_sched_srv.h"
#include "local_scheduler/traefik_registry/traefik_registry.h"

namespace functionsystem::local_scheduler {
using namespace messages;
using namespace std::placeholders;
using schedule_decision::ScheduleResult;

static const uint32_t MAX_INIT_CALL_TIMEOUT_MS = 60 * 60 * 1000;

static const uint32_t CLEAR_RATE_LIMITER_INTERVAL_MS = 6 * 60 * 60 * 1000;

static const int64_t INVALID_INSTANCE_STATE = -1;

static const uint32_t OBSERVER_TIMEOUT_MS = 60000;

static const uint32_t HEARTBEAT_INTERVAL_MS = 3000;

static const uint32_t MILLISECONDS_PRE_SECOND = 1000;
static const uint32_t RETRY_CHECK_CLIENT_CONNECT_TIME = 1000;
static const uint32_t INSTANCE_BUSY_SUSPEND_RETRY_MS = 300;
static uint32_t g_getLocalSchedulerInterval = 10000;
static const std::string KILL_JOB_INS_PREFIX = "job-killer-";
static const std::string DATA_AFFINITY_ENABLED_KEY = "DATA_AFFINITY_ENABLED";
static const uint32_t MAX_LABEL_AFFINITY_COUNT = 10;
static const uint32_t TENANT_ID_MAX_LENGTH = 128;

// INSTANCE_SCHEDULE_FAILED_TIMEOUT = FORWARD_SCHEDULE_MAX_RETRY * FORWARD_SCHEDULE_TIMEOUT

const uint8_t ERROR_MESSAGE_SEPARATE = 2;

const int64_t DRIVER_RECONNECTED_TIMEOUT = 3;

const int64_t CANCEL_TIMEOUT = 5000;
const int64_t CHECK_STATE_TIMEOUT = 1000;
const int64_t CHECK_RETRY_TIMES = 5;

// OS path
const char* OIDC_TOKEN_PATH = "/var/run/secrets/tokens/oidc-token";

const char* HEADER_IDP_KEY = "X-Idp-Id"; // key for identity provider
const char* HEADER_AUTH_TOKEN_KEY = "X-Auth-Token"; // key for iam-token

const char *DEBUG_CONFIG_KEY = "debug_config";

static std::string GenerateJobIDFromTraceID(const std::string &traceID)
{
    static const std::string sep = "-trace-";
    static const uint32_t validSplitLen = 2;
    auto items = litebus::strings::Split(traceID, sep);
    if (items.size() != validSplitLen) {
        return "";
    }
    return items[0];
}

static AddressInfo GenerateAddressInfo(const std::string &instanceID, const std::string &runtimeID,
                                       const std::string &address, bool isDriver)
{
    AddressInfo info{ .instanceID = instanceID, .runtimeID = runtimeID, .address = address, .isDriver = isDriver };
    return info;
}

KillResponse StatusToKillResponse(const Status &status)
{
    KillResponse rsp;
    rsp.set_code(Status::GetPosixErrorCode(status.StatusCode()));
    rsp.set_message(status.RawMessage());
    return rsp;
}

InstanceCtrlActor::InstanceCtrlActor(const std::string &name, const std::string &nodeID,
                                     const InstanceCtrlConfig &config)
    : BasisActor(name),
      nodeID_(nodeID),
      config_(config),
      instanceControlView_(std::make_shared<InstanceControlView>(nodeID, config.isMetaStoreEnabled)),
      fcAccessorHeartbeat_(true)
{
    // make sure client reconnect time is lower than heartbeat lost time
    auto reconnectTimeout = HEARTBEAT_INTERVAL_MS * (config.runtimeConfig.runtimeMaxHeartbeatTimeoutTimes - 1) /
                                config.maxInstanceReconnectTimes -
                            config.reconnectInterval;
    config_.reconnectTimeout = reconnectTimeout / MILLISECONDS_PRE_SECOND == 0
                                   ? config_.reconnectTimeout
                                   : reconnectTimeout / MILLISECONDS_PRE_SECOND;
    config_.connectTimeout = config.connectTimeout;
    checkStateHelper_.SetBackOffStrategy([](int64_t) { return CHECK_STATE_TIMEOUT; }, CHECK_RETRY_TIMES);
}

InstanceCtrlActor::~InstanceCtrlActor()
{
    if (instanceControlView_ && observer_) {
        observer_->Detach(instanceControlView_);
    }
    scheduler_ = nullptr;
    functionAgentMgr_ = nullptr;
    observer_ = nullptr;
    instanceControlView_ = nullptr;
}

void InstanceCtrlActor::Init()
{
    YRLOG_INFO("init InstanceCtrlActor");
    exitHandler_ = [aid(GetAID())](const InstanceInfo &instanceInfo) -> litebus::Future<Status> {
        YRLOG_INFO("{}|execute exit handler, instance({})", instanceInfo.requestid(), instanceInfo.instanceid());
        auto startTimeMillis =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
        litebus::Async(aid, &InstanceCtrlActor::StopHeartbeat, instanceInfo.instanceid());
        if (IsDriver(instanceInfo)) {
            YRLOG_INFO("{}|driver exited, ({}) should be clear.", instanceInfo.requestid(),
                       instanceInfo.instanceid());
            litebus::Async(aid, &InstanceCtrlActor::DeleteDriverClient, instanceInfo.instanceid(),
                           instanceInfo.jobid());
        }
        if (instanceInfo.functionagentid().empty()) {
            YRLOG_INFO("{}|function agent ID of instance({}) is empty, delete instance in control view",
                       instanceInfo.requestid(), instanceInfo.instanceid());
            return litebus::Async(aid, &InstanceCtrlActor::DeleteInstanceInControlView, Status::OK(), instanceInfo);
        }
        return litebus::Async(aid, &InstanceCtrlActor::DeleteInstanceInResourceView, Status::OK(), instanceInfo)
            .Then(litebus::Defer(aid, &InstanceCtrlActor::ShutDownInstance, instanceInfo,
                                 static_cast<uint32_t>(instanceInfo.gracefulshutdowntime())))
            .Then([instanceInfo, aid](const Status &) {
                return litebus::Async(aid, &InstanceCtrlActor::KillRuntime, instanceInfo, false);
            })
            .Then(litebus::Defer(aid, &InstanceCtrlActor::DeleteInstanceInControlView, std::placeholders::_1,
                                 instanceInfo))
            .Then(litebus::Defer(aid, &InstanceCtrlActor::ReportInstanceExitLatency, std::placeholders::_1,
                                 startTimeMillis, instanceInfo));
    };
    InstanceStateMachine::SetExitHandler(exitHandler_);
    InstanceStateMachine::SetExitFailedHandler([aid(GetAID()), nodeID(nodeID_)](const TransitionResult &result) {
        YRLOG_INFO("{}|failed to exit instance({}), try again", result.savedInfo.requestid(),
                   result.savedInfo.instanceid());
        if (result.savedInfo.functionproxyid() != nodeID) {
            litebus::Async(aid, &InstanceCtrlActor::UpdateInstanceInfo, result.savedInfo)
                .Then(litebus::Defer(aid, &InstanceCtrlActor::Kill, result.savedInfo.parentid(),
                                     GenKillRequest(result.savedInfo.instanceid(), SHUT_DOWN_SIGNAL), false));
        } else {
            litebus::Async(aid, &InstanceCtrlActor::UpdateInstanceInfo, result.previousInfo)
                .Then(litebus::Defer(aid, &InstanceCtrlActor::Kill, result.previousInfo.parentid(),
                                     GenKillRequest(result.previousInfo.instanceid(), SHUT_DOWN_SIGNAL), false));
        }
    });

    Receive("ForwardCustomSignalRequest", &InstanceCtrlActor::ForwardCustomSignalRequest);
    Receive("ForwardCustomSignalResponse", &InstanceCtrlActor::ForwardCustomSignalResponse);
    Receive("ForwardCallResultRequest", &InstanceCtrlActor::ForwardCallResultRequest);
    Receive("ForwardCallResultResponse", &InstanceCtrlActor::ForwardCallResultResponse);

    Receive("CheckInstanceState", &InstanceCtrlActor::CheckInstanceState);
    Receive("CheckInstanceStateResponse", &InstanceCtrlActor::CheckInstanceStateResponse);
}


Status InstanceCtrlActor::UpdateInstanceInfo(const resources::InstanceInfo &instanceInfo)
{
    auto stateMachine = instanceControlView_->GetInstance(instanceInfo.instanceid());
    if (stateMachine == nullptr) {
        YRLOG_ERROR("failed to find instance({}) to update instance info", instanceInfo.instanceid());
        return Status(StatusCode::FAILED);
    }
    stateMachine->UpdateInstanceInfo(instanceInfo);
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::ReportInstanceExitLatency(const Status &status, long long startTimeMillis,
                                                                     const InstanceInfo &instanceInfo)
{
    functionsystem::metrics::MetricsAdapter::GetInstance().ReportInstanceExitLatency(status, startTimeMillis,
                                                                                     instanceInfo);
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::DeleteInstanceInResourceView(const Status &status,
                                                                        const InstanceInfo &instanceInfo)
{
    ASSERT_IF_NULL(resourceViewMgr_);
    auto type = resource_view::GetResourceType(instanceInfo);
    return resourceViewMgr_->GetInf(type)->DeleteInstances({ instanceInfo.instanceid() });
}

litebus::Future<Status> InstanceCtrlActor::DeleteInstanceInControlView(const Status &status,
                                                                       const InstanceInfo &instanceInfo)
{
    if (traefikRegistry_) {
        (void)litebus::Async(GetAID(), &InstanceCtrlActor::UnregisterTraefikRoute, instanceInfo.instanceid());
    }
    // Delete the reserved resource information corresponding to the group instance bound to the local node.
    if (!instanceInfo.groupid().empty() && groupInstanceClear_ != nullptr) {
        groupInstanceClear_(instanceInfo);
    }
    YRLOG_INFO("{}|delete instance({}) in control view", instanceInfo.requestid(), instanceInfo.instanceid());
    return instanceControlView_->DelInstance(instanceInfo.instanceid())
        .Then([insCtrlView(instanceControlView_), instanceID(instanceInfo.instanceid()),
               requestID(instanceInfo.requestid())](const Status &status) {
            insCtrlView->OnDelInstance(instanceID, requestID, status.IsOk());
            return Status::OK();
        })
        .Then([instanceID(instanceInfo.instanceid())](const Status &status) {
            if (status.IsOk()) {
                function_proxy::StateHandler::DeleteState(instanceID);
            }
            return Status::OK();
        });
}

litebus::Future<KillResponse> InstanceCtrlActor::KillResourceGroup(const std::string &srcInstanceID,
                                                                    const std::shared_ptr<KillRequest> &killReq)
{
    std::string callerTenant;
    auto callerInstanceMachine = instanceControlView_->GetInstance(srcInstanceID);
    if (callerInstanceMachine != nullptr) {
        callerTenant = callerInstanceMachine->GetInstanceInfo().tenantid();
    }
    ASSERT_IF_NULL(rGroupCtrl_);
    return rGroupCtrl_->Kill(srcInstanceID, callerTenant, killReq);
}

litebus::Future<KillResponse> InstanceCtrlActor::SendNotificationSignal(
    const std::shared_ptr<KillContext> &killCtx, const std::string &srcInstanceID,
    const std::shared_ptr<KillRequest> &killReq, uint32_t cnt)
{
    return SendSignal(killCtx, srcInstanceID, killReq)
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::RetrySendNotificationSignal, _1, killCtx, srcInstanceID,
                             killReq, cnt));
}

litebus::Future<KillResponse> InstanceCtrlActor::RetrySendNotificationSignal(
    const KillResponse &killResponse, const std::shared_ptr<KillContext> &killCtx, const std::string &srcInstanceID,
    const std::shared_ptr<KillRequest> &killReq, uint32_t cnt)
{
    if (killResponse.code() != common::ERR_REQUEST_BETWEEN_RUNTIME_BUS) {
        return killResponse;
    }

    if (cnt >= MAX_NOTIFICATION_SIGNAL_RETRY_TIMES) {
        YRLOG_ERROR("Failed to resend notification signal after {} attempts: src_instance({}), dst_instance({}).",
                    MAX_NOTIFICATION_SIGNAL_RETRY_TIMES, srcInstanceID, killReq->instanceid());
        return killResponse;
    }

    cnt += 1;
    YRLOG_INFO("Resend notification signal, times: {}, src_instance({}), dst_instance({}).",
               cnt, srcInstanceID, killReq->instanceid());
    return SendNotificationSignal(killCtx, srcInstanceID, killReq, cnt);
}

litebus::Future<KillResponse> InstanceCtrlActor::ProcessSubscribeRequest(const std::string &srcInstanceID,
                                                                         const std::shared_ptr<KillRequest> &killReq)
{
    YRLOG_INFO("receive a subscribe request: src_instance({}), dst_instance({}).",
               srcInstanceID, killReq->instanceid());
    ASSERT_IF_NULL(subscriptionMgr_);
    return subscriptionMgr_->Subscribe(srcInstanceID, killReq);
}

litebus::Future<KillResponse> InstanceCtrlActor::ProcessUnsubscribeRequest(const std::string &srcInstanceID,
                                                                           const std::shared_ptr<KillRequest> &killReq)
{
    YRLOG_INFO("receive a unsubscribe request: src_instance({}), dst_instance({}).",
               srcInstanceID, killReq->instanceid());
    ASSERT_IF_NULL(subscriptionMgr_);
    return subscriptionMgr_->Unsubscribe(srcInstanceID, killReq);
}

litebus::Future<KillResponse> InstanceCtrlActor::HandleSnapshotSignal(const std::shared_ptr<KillContext> &killCtx,
    const std::string &srcInstanceID,
    const std::shared_ptr<KillRequest> &killReq)
{
    // 如果 SignalRoute 失败，返回错误
    if (killCtx->killRsp.code() != common::ErrorCode::ERR_NONE) {
        YRLOG_ERROR("{}|failed to route snapshot signal, code: {}",
                    killReq->requestid(), static_cast<uint32_t>(killCtx->killRsp.code()));
        return killCtx->killRsp;
    }

    // 如果实例在其他节点，转发请求
    if (!killCtx->isLocal) {
        YRLOG_INFO("{}|instance({}) is on remote node, forwarding snapshot request",
                   killReq->requestid(), killReq->instanceid());
        return GetLocalSchedulerAID(killCtx->instanceContext->GetInstanceInfo().instanceid())
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SendForwardCustomSignalRequest, _1,
                                 srcInstanceID, killReq,
                                 killCtx->instanceContext->GetInstanceInfo().requestid(), false));
    }

    // 实例在本地，直接处理
    ASSERT_IF_NULL(snapCtrl_);
    return snapCtrl_->HandleSnapshot(killReq->requestid(), killReq->instanceid(), killReq->payload());
}

litebus::Future<KillResponse> InstanceCtrlActor::HandleKill(const std::string &srcInstanceID,
                                                            const std::shared_ptr<KillRequest> &killReq,
                                                            bool isSkipAuth)
{
    const int &signal = killReq->signal();
    switch (signal) {
        case SHUT_DOWN_SIGNAL:
            [[fallthrough]];
        case SHUT_DOWN_SIGNAL_SYNC: {
            return CheckInstanceExist(srcInstanceID, killReq)
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::AuthorizeKill, srcInstanceID, killReq, isSkipAuth))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::CheckKillParam, _1, srcInstanceID, killReq))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::PrepareKillByInstanceState, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::ProcessKillCtxByInstanceState, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SignalRoute, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::Exit, _1, (signal == SHUT_DOWN_SIGNAL_SYNC)));
        }
        case SHUT_DOWN_SIGNAL_ALL: {
            return KillInstancesOfJob(killReq);
        }
        case SHUT_DOWN_SIGNAL_GROUP:
            [[fallthrough]];
        case GROUP_SUSPEND_SIGNAL:
            [[fallthrough]];
        case GROUP_RESUME_SIGNAL: {
            return KillGroup(srcInstanceID, killReq);
        }
        case GROUP_EXIT_SIGNAL:
            [[fallthrough]];
        case FAMILY_EXIT_SIGNAL: {
            return CheckInstanceExist(srcInstanceID, killReq)
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::AuthorizeKill, srcInstanceID, killReq, isSkipAuth))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::CheckKillParam, _1, srcInstanceID, killReq))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::PrepareKillByInstanceState, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::ProcessKillCtxByInstanceState, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SignalRoute, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SetInstanceFatal, _1));
        }
        case APP_STOP_SIGNAL: {
            return CheckInstanceExist(srcInstanceID, killReq)
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::AuthorizeKill, srcInstanceID, killReq, isSkipAuth))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::CheckKillParam, _1, srcInstanceID, killReq))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::PrepareKillByInstanceState, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::ProcessKillCtxByInstanceState, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SignalRoute, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::StopAppDriver, _1));
        }
        case REMOVE_RESOURCE_GROUP: {
            return KillResourceGroup(srcInstanceID, killReq);
        }
        case SUBSCRIBE_SIGNAL: {
            return ProcessSubscribeRequest(srcInstanceID, killReq);
        }
        case NOTIFY_SIGNAL: {
            return CheckInstanceExist(srcInstanceID, killReq)
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::CheckKillParam, _1, srcInstanceID, killReq))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SignalRoute, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SendNotificationSignal, _1, srcInstanceID,
                                     killReq, 0));
        }
        case UNSUBSCRIBE_SIGNAL: {
            return ProcessUnsubscribeRequest(srcInstanceID, killReq);
        }
        case INSTANCE_CHECKPOINT_SIGNAL: {
            return MakeCheckpoint(killReq->instanceid()).Then([](const Status &status) {
                return StatusToKillResponse(status);
            });
        }
        case INSTANCE_TRANS_SUSPEND_SIGNAL: {
            return ToSuspend(killReq->instanceid()).Then([](const Status &status) {
                return StatusToKillResponse(status);
            });
        }
        case INSTANCE_RESUME_SIGNAL: {
            return ToResume(killReq->instanceid()).Then([](const Status &status) {
                return StatusToKillResponse(status);
            });
        }
        case INSTANCE_SNAPSHOT_SIGNAL: {
            return CheckInstanceExist(srcInstanceID, killReq)
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::CheckKillParam, _1, srcInstanceID, killReq))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SignalRoute, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::HandleSnapshotSignal, _1, srcInstanceID, killReq));
        }
        case INSTANCE_SNAPSTART_SIGNAL: {
            // snapstart不需要路由，直接处理
            ASSERT_IF_NULL(snapCtrl_);
            return snapCtrl_->HandleSnapStart(killReq->requestid(), killReq->instanceid(), killReq->payload());
        }
        case MIN_USER_SIGNAL_NUM ... MAX_SIGNAL_NUM: {
            return CheckInstanceExist(srcInstanceID, killReq)
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::AuthorizeKill, srcInstanceID, killReq, isSkipAuth))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::CheckKillParam, _1, srcInstanceID, killReq))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::ProcessKillCtxByInstanceState, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SignalRoute, _1))
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SendSignal, _1, srcInstanceID, killReq));
        }
        default:
            YRLOG_WARN("unexpected signal number: {}", signal);
    }
    return KillResponse{};
}

litebus::Future<ExitResponse> InstanceCtrlActor::HandleExit(const std::string &srcInstanceID,
                                                            const std::shared_ptr<ExitRequest> &exitReq)
{
    ExitResponse response;
    if (exitReq->code() == common::ERR_NONE) {
        auto killReq = GenKillRequest(srcInstanceID, 1);
        Kill(srcInstanceID, killReq, false);
        response.set_code(common::ERR_NONE);
        return response;
    }

    if (exitReq->code() == common::ERR_NPU_FAULT_ERROR) {
        return HandleNpuFault(srcInstanceID);
    }

    YRLOG_WARN("invalid exit code({}) for instance({}), ignore", static_cast<uint32_t>(exitReq->code()), srcInstanceID);
    response.set_code(common::ERR_PARAM_INVALID);
    response.set_message("invalid exit code");
    return response;
}

litebus::Future<ExitResponse> InstanceCtrlActor::HandleNpuFault(const std::string &instanceID)
{
    ExitResponse response;
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr || stateMachine->GetOwner() != nodeID_) {
        YRLOG_ERROR("failed to find instance({}) on this node, instance not found({})", instanceID,
                    std::to_string(stateMachine == nullptr));
        response.set_code(common::ERR_INSTANCE_NOT_FOUND);
        response.set_message("instance not found");
        return response;
    }

    auto state = stateMachine->GetInstanceState();
    if (state == InstanceState::RUNNING) {
        YRLOG_ERROR("instance({}) is sub-health, because of npu fault", instanceID);
        TransInstanceState(stateMachine, TransContext{ InstanceState::SUB_HEALTH, stateMachine->GetVersion(),
                                                       stateMachine->Information() + "sub_health: npu is not healthy",
                                                       true, ERR_NPU_FAULT_ERROR });
        response.set_code(common::ERR_NONE);
        return response;
    }

    if (state == InstanceState::CREATING) {
        YRLOG_WARN("npu fault when instance({}) is CREATING", instanceID);
        stateMachine->AddStateChangeCallback(
            { InstanceState::RUNNING },
            [stateMachine, aid(GetAID())](const resources::InstanceInfo &) {
                YRLOG_ERROR("instance({}) is sub-health, because of npu fault",
                            stateMachine->GetInstanceInfo().instanceid());
                litebus::Async(aid, &InstanceCtrlActor::TransInstanceState, stateMachine,
                               TransContext{ InstanceState::SUB_HEALTH, stateMachine->GetVersion(),
                                             stateMachine->Information() + "sub_health: npu is not healthy", true,
                                             ERR_NPU_FAULT_ERROR });
            },
            "HandleNpuFault");
        response.set_code(common::ERR_NONE);
        return response;
    }

    YRLOG_WARN("failed to exit, invalid instance({}) status({})", instanceID, static_cast<uint32_t>(state));
    response.set_code(common::ERR_INNER_SYSTEM_ERROR);
    response.set_message("invalid status, instance is not running");
    return response;
}

litebus::Future<KillResponse> InstanceCtrlActor::Kill(const std::string &srcInstanceID,
                                                      const std::shared_ptr<KillRequest> &killReq, bool isSkipAuth)
{
    const int &signal = killReq->signal();
    if (signal < MIN_SIGNAL_NUM || signal > MAX_SIGNAL_NUM) {
        YRLOG_ERROR("failed to process kill request, invalid signal({}) of instance({}) from instance({}).", signal,
                    killReq->instanceid(), srcInstanceID);
        KillResponse killRsp;
        killRsp.set_code(common::ErrorCode::ERR_PARAM_INVALID);
        killRsp.set_message("invalid signal num");
        return killRsp;
    }
    // former compatible
    if (killReq->requestid().empty()) {
        killReq->set_requestid(killReq->instanceid() + "-" + std::to_string(signal));
    }
    if (auto iter = killingRequest_.find(killReq->requestid()); iter != killingRequest_.end()) {
        YRLOG_INFO("the signal({}) request({}) of instance({}) is processing", signal, killReq->requestid(),
                   killReq->instanceid());
        return iter->second->GetFuture();
    }
    auto promise = std::make_shared<litebus::Promise<KillResponse>>();
    killingRequest_[killReq->requestid()] = promise;
    HandleKill(srcInstanceID, killReq, isSkipAuth)
        .OnComplete(litebus::Defer(GetAID(), &InstanceCtrlActor::OnKill, killReq, _1));
    return promise->GetFuture();
}

void InstanceCtrlActor::OnKill(const std::shared_ptr<KillRequest> &killReq, const litebus::Future<KillResponse> &rsp)
{
    auto iter = killingRequest_.find(killReq->requestid());
    if (iter == killingRequest_.end()) {
        return;
    }
    const int &signal = killReq->signal();
    auto response = rsp.Get();
    if (response.code() == common::ErrorCode::ERR_INSTANCE_NOT_FOUND) {
        auto errorMsg = response.message();
        switch (signal) {
            case SHUT_DOWN_SIGNAL:
                [[fallthrough]];
            case SHUT_DOWN_SIGNAL_SYNC:
                [[fallthrough]];
            case APP_STOP_SIGNAL: {
                YRLOG_INFO("{}|Kill {} response ERR_INSTANCE_NOT_FOUND, ignore it and return OK",
                    killReq->requestid(), killReq->instanceid());
                iter->second->Associate(GenKillResponse(common::ErrorCode::ERR_NONE, ""));
                break;
            }
            default:
                iter->second->Associate(GenKillResponse(response.code(), response.message()));
        }
    } else {
        iter->second->Associate(rsp);
    }
    killingRequest_.erase(iter);
}

litebus::Future<KillResponse> InstanceCtrlActor::SetInstanceFatal(const std::shared_ptr<KillContext> &killCtx)
{
    // if signalRoute failed or instance is in remote node
    // Note: if scheduling is write to etcd, need to process forward signal
    if (killCtx->killRsp.code() != common::ErrorCode::ERR_NONE) {
        YRLOG_ERROR("failed to set FATAL instance, code({})", static_cast<uint32_t>(killCtx->killRsp.code()));
        return killCtx->killRsp;
    }

    ::common::ErrorCode code = ::common::ErrorCode::ERR_INSTANCE_EXITED;
    if (killCtx->killRequest->signal() == GROUP_EXIT_SIGNAL) {
        code = ::common::ErrorCode::ERR_GROUP_EXIT_TOGETHER;
    }

    std::string msg(killCtx->killRequest->payload());
    (void)litebus::Async(GetAID(), &InstanceCtrlActor::SyncFailedInitResult, killCtx->killRequest->instanceid(), code,
                         msg);

    auto stateMachine = instanceControlView_->GetInstance(killCtx->killRequest->instanceid());
    if (stateMachine == nullptr) {
        killCtx->killRsp.set_code(::common::ErrorCode::ERR_INSTANCE_NOT_FOUND);
        killCtx->killRsp.set_message("instance not found");
        return killCtx->killRsp;
    }

    auto transContext = TransContext{ InstanceState::FATAL, stateMachine->GetVersion(), msg, true, code,
                                      0, static_cast<int32_t>(EXIT_TYPE::KILLED_INFO) };
    transContext.scheduleReq = killCtx->instanceContext->GetScheduleRequest();
    auto instanceInfo = transContext.scheduleReq->instance();
    return litebus::Async(GetAID(), &InstanceCtrlActor::TransInstanceState, stateMachine, transContext)
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::ShutDownInstance, instanceInfo,
                             static_cast<uint32_t>(instanceInfo.gracefulshutdowntime())))
        .Then([aid(GetAID()), instanceId(instanceInfo.instanceid())](const litebus::Future<Status> &future) {
            litebus::Async(aid, &InstanceCtrlActor::StopHeartbeat, instanceId);
            return Status::OK();
        })
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::KillRuntime, instanceInfo, false))
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeleteInstanceInResourceView, std::placeholders::_1,
                             instanceInfo))
        .Then([killCtx, code](const litebus::Future<Status> &status) {
            killCtx->killRsp.set_code(code);
            killCtx->killRsp.set_message(status.Get().GetMessage());
            return killCtx->killRsp;
        });
}

void InstanceCtrlActor::PutFailedInstanceStatusByAgentId(const std::string &funcAgentID)
{
    if (observer_ == nullptr) {
        YRLOG_WARN("failed to put failed instance status by agent id because observer is null pointer");
        return;
    }
    (void)observer_->GetAgentInstanceInfoByID(funcAgentID)
        .Then([funcAgentID, instanceControlView(instanceControlView_), aid(GetAID()),
               runtimeConf(config_.runtimeConfig)](const litebus::Option<function_proxy::InstanceInfoMap> &option) {
            if (option.IsNone()) {
                YRLOG_ERROR("failed to update instance failed status, InstanceInfoMap not found, agentID: {}",
                            funcAgentID);
                return Status(StatusCode::FAILED, "InstanceInfoMap not found");
            }
            auto instanceInfoMap = option.Get();
            for (auto &instance : instanceInfoMap) {
                auto stateMachine = instanceControlView->GetInstance(instance.first);
                if (stateMachine == nullptr) {
                    return Status(StatusCode::ERR_INSTANCE_NOT_FOUND, "instance not found");
                }
                auto instanceInfo = stateMachine->GetInstanceInfo();
                if (IsRuntimeRecoverEnable(instanceInfo, stateMachine->GetCancelFuture())) {
                    (void)litebus::Async(aid, &InstanceCtrlActor::TransInstanceState, stateMachine,
                                         TransContext{ InstanceState::FAILED, stateMachine->GetVersion(),
                                                       "local_scheduler and function_agent heartbeat timeout" })
                        .Then(litebus::Defer(aid, &InstanceCtrlActor::RescheduleWithID, instanceInfo.instanceid()));
                } else {
                    (void)litebus::Async(
                        aid, &InstanceCtrlActor::TransInstanceState, stateMachine,
                        TransContext{ InstanceState::FATAL, stateMachine->GetVersion(),
                                      stateMachine->Information() +
                                          "fatal: local_scheduler and function_agent heartbeat timeout",
                                      true, static_cast<int32_t>(common::ErrorCode::ERR_INSTANCE_EXITED) });
                }
            }
            return Status::OK();
        });
}

litebus::Future<litebus::Option<FunctionMeta>> InstanceCtrlActor::GetFuncMeta(const std::string &funcKey)
{
    if (auto iter = funcMetaMap_.find(funcKey); iter != funcMetaMap_.end()) {
        return iter->second;
    }
    return observer_->GetFuncMeta(funcKey).Then(
        [funcKey, aid(GetAID())](const litebus::Option<FunctionMeta> &option) ->
            litebus::Future<litebus::Option<FunctionMeta>> {
            if (option.IsNone()) {
                return option;
            }
            std::unordered_map<std::string, FunctionMeta> metas;
            metas[funcKey] = option.Get();
            litebus::Async(aid, &InstanceCtrlActor::UpdateFuncMetas, true, metas);
            return option;
        });
}

litebus::Future<std::shared_ptr<KillContext>> InstanceCtrlActor::SignalRoute(
    const std::shared_ptr<KillContext> &killCtx)
{
    if (killCtx->killRsp.code() != common::ErrorCode::ERR_NONE) {
        YRLOG_WARN("(kill)failed to check param, code: {}, message: {}", fmt::underlying(killCtx->killRsp.code()),
                   killCtx->killRsp.message());
        return killCtx;
    }

    auto &instanceInfo = killCtx->instanceContext->GetInstanceInfo();
    if (instanceInfo.functionproxyid() != nodeID_) {
        killCtx->isLocal = false;
    } else {
        killCtx->isLocal = true;
    }
    killCtx->killRsp = GenKillResponse(common::ErrorCode::ERR_NONE, "");

    // instance is not in this node, put instance(status is Kill) to meta store.
    // remote node(the instance located) get kill instance event then kill instance
    YRLOG_DEBUG("(kill)proxyID({}) of instance({}), nodeID({}), is local({})", instanceInfo.functionproxyid(),
                instanceInfo.instanceid(), nodeID_, killCtx->isLocal);
    return killCtx;
}

void InstanceCtrlActor::ForwardCustomSignalRequest(const litebus::AID &from, std::string &&, std::string &&msg)
{
    internal::ForwardKillRequest forwardKillRequest;
    if (msg.empty() || !forwardKillRequest.ParseFromString(msg)) {
        YRLOG_WARN("(custom signal)invalid request body from {}.", from.HashString());
        return;
    }

    auto requestID(forwardKillRequest.requestid());
    auto instanceID(forwardKillRequest.req().instanceid());

    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    // if local is not ready and instance not found, return and wait signal again
    if (!IsReady() && stateMachine == nullptr) {
        YRLOG_ERROR("{}|(custom signal)instance not found and local is not ready, wait signal again", requestID);
        return;
    }

    if (stateMachine != nullptr && forwardKillRequest.instancerequestid() != stateMachine->GetRequestID()) {
        YRLOG_ERROR("{}|(custom signal)instance({}) requestID({}) is changed", requestID, instanceID,
                    forwardKillRequest.instancerequestid());
        SendForwardCustomSignalResponse(GenKillResponse(common::ErrorCode::ERR_INSTANCE_NOT_FOUND,
                                                        "instance not found, the instance may have been killed"),
                                        from, requestID);
        return;
    }

    if (forwardCustomSignalRequestIDs_.find(requestID) != forwardCustomSignalRequestIDs_.end()) {
        YRLOG_WARN("{}|(custom signal) request is being processed.", requestID);
        forwardCustomSignalRequestIDs_[requestID].Then(
            litebus::Defer(GetAID(), &InstanceCtrlActor::SendForwardCustomSignalResponse, _1, from, requestID));
        return;
    }

    YRLOG_INFO("{}|received a custom signal request from {}. instance: {} signal: {}", requestID, from.HashString(),
               instanceID, forwardKillRequest.req().signal());

    auto killRequest = std::make_shared<KillRequest>(std::move(*forwardKillRequest.mutable_req()));
    auto future = Kill(forwardKillRequest.srcinstanceid(), killRequest, true);
    forwardCustomSignalRequestIDs_.emplace(requestID, future);
    // call Kill directly, skip auth
    forwardCustomSignalRequestIDs_[requestID].Then(
        litebus::Defer(GetAID(), &InstanceCtrlActor::SendForwardCustomSignalResponse, _1, from, requestID));
}

litebus::Future<Status> InstanceCtrlActor::SendForwardCustomSignalResponse(const KillResponse &killResponse,
                                                                           const litebus::AID &from,
                                                                           const std::string &requestID)
{
    YRLOG_INFO("{}|(custom signal)send response, aid: {}", requestID, from.HashString());
    auto forwardKillResponse = GenForwardKillResponse(requestID, killResponse.code(), killResponse.message());
    Send(from, "ForwardCustomSignalResponse", forwardKillResponse.SerializeAsString());
    (void)forwardCustomSignalRequestIDs_.erase(requestID);

    return Status::OK();
}

void InstanceCtrlActor::ForwardCustomSignalResponse(const litebus::AID &from, std::string &&, std::string &&msg)
{
    internal::ForwardKillResponse forwardKillResponse;
    if (msg.empty() || !forwardKillResponse.ParseFromString(msg)) {
        YRLOG_WARN("(custom signal)invalid response body from({}).", from.HashString());
        return;
    }

    auto requestID(forwardKillResponse.requestid());
    if (auto iter(forwardCustomSignalNotifyPromise_.find(requestID)); iter == forwardCustomSignalNotifyPromise_.end()) {
        YRLOG_WARN("{}|(custom signal)failed to get response, no request matches result", requestID);
        return;
    }

    KillResponse killResponse;
    killResponse.set_code(forwardKillResponse.code());
    killResponse.set_message(forwardKillResponse.message());
    forwardCustomSignalNotifyPromise_[requestID]->SetValue(killResponse);
    (void)forwardCustomSignalNotifyPromise_.erase(requestID);

    YRLOG_INFO("{}|(custom signal)received forward response, from: {}", requestID, from.HashString());
}

litebus::Future<KillResponse> InstanceCtrlActor::SendForwardCustomSignalRequest(
    const litebus::Option<litebus::AID> &option, const std::string &srcInstanceID,
    const std::shared_ptr<KillRequest> &killRequest, const std::string &dstInstanceRequestID, bool isSynchronized)
{
    if (option.IsNone()) {
        YRLOG_ERROR("{}|(custom signal)instance({}) actor aid is none", killRequest->requestid(),
                    killRequest->instanceid());
        return GenKillResponse(common::ErrorCode::ERR_INSTANCE_NOT_FOUND,
                               "can't find remote instance(" + killRequest->instanceid() + ") proxy aid");
    }

    auto aid = option.Get();
    auto notifyPromise = std::make_shared<KillResponsePromise>();
    auto requestID(killRequest->requestid()+ "-" + std::to_string(killRequest->signal()));
    auto forwardKillRequest = GenForwardKillRequest(requestID, srcInstanceID, std::move(*killRequest));
    forwardKillRequest->set_instancerequestid(dstInstanceRequestID);
    auto emplaceResult = forwardCustomSignalNotifyPromise_.emplace(requestID, notifyPromise);
    if (!emplaceResult.second) {
        YRLOG_INFO("{}|(custom signal)send request repeatedly, instance({})", forwardKillRequest->requestid(),
                   forwardKillRequest->req().instanceid());
        return forwardCustomSignalNotifyPromise_[requestID]->GetFuture();
    }
    YRLOG_INFO("{}|(custom signal)send request to {}, instance({}), signal: {}", requestID, aid.HashString(),
               forwardKillRequest->req().instanceid(), forwardKillRequest->req().signal());
    Send(aid, "ForwardCustomSignalRequest", forwardKillRequest->SerializeAsString());
    litebus::AsyncAfter(isSynchronized ? MAX_FORWARD_KILL_RETRY_CYCLE_SYNC_MS : maxForwardKillRetryCycleMs_, GetAID(),
                        &InstanceCtrlActor::RetrySendForwardCustomSignalRequest, aid, forwardKillRequest, 0,
                        isSynchronized);
    return notifyPromise->GetFuture();
}

void InstanceCtrlActor::RetrySendForwardCustomSignalRequest(
    const litebus::AID &aid, const std::shared_ptr<internal::ForwardKillRequest> forwardKillRequest, uint32_t cnt,
    bool isSynchronized)
{
    auto requestID(forwardKillRequest->requestid());
    if (forwardCustomSignalNotifyPromise_.find(requestID) == forwardCustomSignalNotifyPromise_.end() ||
        forwardCustomSignalNotifyPromise_[requestID]->GetFuture().IsOK()) {
        YRLOG_INFO("{}|(custom signal)response has been received.", requestID);
        return;
    }

    if (cnt < maxForwardKillRetryTimes_) {
        Send(aid, "ForwardCustomSignalRequest", forwardKillRequest->SerializeAsString());
        litebus::AsyncAfter(isSynchronized ? MAX_FORWARD_KILL_RETRY_CYCLE_SYNC_MS : maxForwardKillRetryCycleMs_,
                            GetAID(), &InstanceCtrlActor::RetrySendForwardCustomSignalRequest, aid, forwardKillRequest,
                            cnt + 1, isSynchronized);
        YRLOG_INFO("{}|(custom signal)retry kill({}) request, times: {}.", requestID,
                   forwardKillRequest->req().instanceid(), cnt);
        return;
    }

    auto killResponse =
        GenKillResponse(common::ErrorCode::ERR_INNER_COMMUNICATION, "(custom signal)don't receive response");
    forwardCustomSignalNotifyPromise_[requestID]->SetValue(killResponse);
    (void)forwardCustomSignalNotifyPromise_.erase(requestID);
    YRLOG_WARN("{}|(custom signal) retry more than {}.", requestID, maxForwardKillRetryTimes_);
}

litebus::Future<KillResponse> InstanceCtrlActor::Exit(const std::shared_ptr<KillContext> &killCtx, bool isSynchronized)
{
    // if signalRoute failed or instance is in remote node
    if (killCtx->killRsp.code() != common::ErrorCode::ERR_NONE) {
        YRLOG_ERROR("failed to exit instance, code({})", static_cast<uint32_t>(killCtx->killRsp.code()));
        return killCtx->killRsp;
    }
    auto &instanceInfo = killCtx->instanceContext->GetInstanceInfo();
    if (!killCtx->isLocal) {
        return HandleRemoteInstanceKill(killCtx, isSynchronized);
    }

    YRLOG_INFO("{}|instance({}) is local, exit directly, status code({}).", instanceInfo.requestid(),
               instanceInfo.instanceid(), instanceInfo.instancestatus().code());
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceInfo.instanceid());
    if (stateMachine == nullptr) {
        YRLOG_WARN("{}|failed to get instance({}) info for Exit", instanceInfo.requestid(),
                   instanceInfo.instanceid());
        KillResponse killResp;
        killResp.set_code(static_cast<common::ErrorCode>(StatusCode::ERR_ETCD_OPERATION_ERROR));
        killResp.set_message("failed to get instance info for Exit");
        return killResp;
    }
    // after the driver is marked for exit, subsequent cleanup actions are executed by detecting disconnection.
    if (IsDriver(instanceInfo)) {
        stateMachine->TagStop();
        // get client to check connection is already closed.
        return clientManager_->GetControlInterfacePosixClient(instanceInfo.instanceid())
            .Then([instanceInfo, aid(GetAID())](const std::shared_ptr<ControlInterfacePosixClient> &client) {
                // if connection of client is closed, the client should be removed immediately, avoid garbage collection
                // deferred.
                if (client == nullptr || client->IsDone()) {
                    YRLOG_INFO("connection of {} is already closed, immediately to remove driver info",
                               instanceInfo.instanceid());
                    litebus::Async(aid, &InstanceCtrlActor::DeleteDriverClient, instanceInfo.instanceid(),
                                   instanceInfo.jobid());
                }
                return KillResponse();
            });
    }
    if (auto iter = exiting_.find(instanceInfo.instanceid()); iter != exiting_.end()) {
        YRLOG_INFO("{}|instance({}) is exiting", instanceInfo.requestid(), instanceInfo.instanceid());
        return iter->second.GetFuture();
    }
    exiting_[instanceInfo.instanceid()] = litebus::Promise<KillResponse>();
    return ForceDeleteInstance(instanceInfo.instanceid())
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::OnExitInstance, instanceInfo, _1));
    // return TryExitInstance(stateMachine, killCtx, isSynchronized)
    //     .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::OnExitInstance, instanceInfo, _1));
}

litebus::Future<KillResponse> InstanceCtrlActor::OnExitInstance(const InstanceInfo &instanceInfo, const Status &status)
{
    auto resp = GenKillResponse(common::ErrorCode::ERR_NONE, "");
    if (status.IsError()) {
        YRLOG_ERROR("{}|failed to exit instance({}), msg: {}", instanceInfo.requestid(), instanceInfo.instanceid(),
                    status.GetMessage());
        resp = GenKillResponse(Status::GetPosixErrorCode(status.StatusCode()), status.GetMessage());
    }
    if (auto iter = exiting_.find(instanceInfo.instanceid()); iter != exiting_.end()) {
        iter->second.SetValue(resp);
        exiting_.erase(iter);
    }
    return resp;
}

litebus::Future<KillResponse> InstanceCtrlActor::StopAppDriver(const std::shared_ptr<KillContext> &killCtx)
{
    // if signalRoute failed
    if (killCtx->killRsp.code() != common::ErrorCode::ERR_NONE) {
        YRLOG_ERROR("failed to exit instance, code({})", static_cast<uint32_t>(killCtx->killRsp.code()));
        return killCtx->killRsp;
    }
    // if instance in remote node
    if (!killCtx->isLocal) {
        return HandleRemoteInstanceKill(killCtx, false);
    }

    auto &instanceInfo = killCtx->instanceContext->GetInstanceInfo();
    YRLOG_INFO("{}|instance({}) is local, stop directly, status code({}).", instanceInfo.requestid(),
               instanceInfo.instanceid(), instanceInfo.instancestatus().code());
    return SetInstanceFatal(killCtx);
}

litebus::Future<KillResponse> InstanceCtrlActor::HandleRemoteInstanceKill(const std::shared_ptr<KillContext> &killCtx,
                                                                          bool isSynchronized)
{
    auto &instanceInfo = killCtx->instanceContext->GetInstanceInfo();
    if (instanceInfo.functionproxyid().empty() || instanceInfo.functionproxyid() == INSTANCE_MANAGER_OWNER) {
        auto req = std::make_shared<ForwardKillRequest>();
        req->set_requestid(instanceInfo.requestid());
        req->mutable_instance()->CopyFrom(instanceInfo);
        req->mutable_req()->CopyFrom(*killCtx->killRequest);
        ASSERT_IF_NULL(localSchedSrv_);
        ASSERT_IF_NULL(observer_);
        return localSchedSrv_->ForwardKillToInstanceManager(req).Then(
            [observer(observer_),
             instanceID(instanceInfo.instanceid())](const ForwardKillResponse &response) {
                KillResponse killResp;
                killResp.set_code(Status::GetPosixErrorCode(response.code()));
                killResp.set_message(response.message());
                if (response.code() == SUCCESS) {
                    return observer->DelInstanceEvent(instanceID, -1).Then([killResp]() { return killResp; });
                }
                return litebus::Future<KillResponse>(killResp);
            });
    }
    return GetLocalSchedulerAID(instanceInfo.instanceid())
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SendForwardCustomSignalRequest, _1, killCtx->srcInstanceID,
                             killCtx->killRequest, instanceInfo.requestid(), isSynchronized));
}

litebus::Future<KillResponse> InstanceCtrlActor::ForwardSubscriptionEvent(const std::shared_ptr<KillContext> &ctx)
{
    auto &instanceInfo = ctx->instanceContext->GetInstanceInfo();
    return GetLocalSchedulerAID(instanceInfo.instanceid())
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SendForwardCustomSignalRequest, _1, ctx->srcInstanceID,
                             ctx->killRequest, instanceInfo.requestid(), false));
}

litebus::Future<messages::KillInstanceResponse> InstanceCtrlActor::SendKillRequestToAgent(
    const InstanceInfo &instanceInfo, bool isRecovering, bool forRedeploy)
{
    RemoveInternalTokenReference(instanceInfo.instanceid());
    if (config_.enableServerMode) {
        PosixService::DeleteClient(instanceInfo.instanceid());
    }
    if (concernedInstance_.find(instanceInfo.instanceid()) != concernedInstance_.end()) {
        (void)concernedInstance_.erase(instanceInfo.instanceid());
    }
    const auto &requestID = instanceInfo.requestid();
    auto traceID = "killTrace" + litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    // while isMonopoly is set, the kill would disable the agent to be reuse
    auto isMonopoly = ((instanceInfo.scheduleoption().schedpolicyname() == MONOPOLY_SCHEDULE) && !forRedeploy);
    YRLOG_DEBUG(
        "{}|send kill request, instance({}) with runtimeID({}), storage type({}), for recover({}), for redeploy({}), "
        "agent reuse({})",
        requestID, instanceInfo.instanceid(), instanceInfo.runtimeid(), instanceInfo.storagetype(), isRecovering,
        forRedeploy, !isMonopoly);
    auto killInstanceReq =
        GenKillInstanceRequest(requestID, instanceInfo.instanceid(), traceID, instanceInfo.storagetype(), isMonopoly);
    killInstanceReq->set_runtimeid(instanceInfo.runtimeid());

    ASSERT_IF_NULL(clientManager_);
    ASSERT_IF_NULL(functionAgentMgr_);
    return clientManager_->DeleteClient(instanceInfo.instanceid())
        .Then([instanceInfo, functionAgentMgr(functionAgentMgr_), killInstanceReq,
               isRecovering](const litebus::Future<Status> &status) {
            YRLOG_INFO("{}|{}|start to kill instance({}), runtime({})", killInstanceReq->traceid(),
                       killInstanceReq->requestid(), killInstanceReq->instanceid(), killInstanceReq->runtimeid());
            return functionAgentMgr->KillInstance(killInstanceReq, instanceInfo.functionagentid(), isRecovering);
        });
}

litebus::Future<Status> InstanceCtrlActor::ShutDownInstance(const InstanceInfo &instanceInfo,
                                                            uint32_t shutdownTimeoutSec)
{
    ASSERT_IF_NULL(clientManager_);
    SyncFailedInitResult(instanceInfo.instanceid(), ::common::ErrorCode::ERR_USER_FUNCTION_EXCEPTION,
                         "shutdown instance");
    (void)syncCreateCallResultPromises_.erase(instanceInfo.instanceid());
    return clientManager_->GetControlInterfacePosixClient(instanceInfo.instanceid())
        .Then([instanceInfo, aid(GetAID()), shutdownTimeoutSec](
                  const std::shared_ptr<ControlInterfacePosixClient> &instanceClient) -> litebus::Future<Status> {
            if (instanceClient == nullptr) {
                YRLOG_WARN("{}|failed to get instance client instance({})", instanceInfo.requestid(),
                           instanceInfo.instanceid());
                metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetBillingInstanceEndTime(
                    instanceInfo.instanceid(), std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count());
                return Status::OK();
            }
            runtime::ShutdownRequest shutdownReq;
            shutdownReq.set_graceperiodsecond(shutdownTimeoutSec);
            YRLOG_INFO("{}|graceful shutdown instance({}) timeout: ({}) sec", instanceInfo.requestid(),
                       instanceInfo.instanceid(), shutdownReq.graceperiodsecond());

            return instanceClient->Shutdown(std::move(shutdownReq))
                .After(shutdownTimeoutSec * MILLISECONDS_PRE_SECOND,
                       [](const litebus::Future<runtime::ShutdownResponse> &future)
                           -> litebus::Future<runtime::ShutdownResponse> {
                           litebus::Promise<runtime::ShutdownResponse> promise;
                           if (future.IsOK()) {
                               promise.SetValue(future);
                           } else {
                               runtime::ShutdownResponse shutdownResponse;
                               shutdownResponse.set_code(common::ErrorCode::ERR_INNER_COMMUNICATION);
                               promise.SetValue(std::move(shutdownResponse));
                           }
                           return promise.GetFuture();
                       })
                .Then([instanceInfo](const runtime_service::ShutdownResponse &shutdownRsp) -> litebus::Future<Status> {
                    if (shutdownRsp.code() != common::ErrorCode::ERR_NONE) {
                        YRLOG_WARN("{}|shutdown instance({}), code: {}, message: {}. continue to kill instance.",
                                   instanceInfo.requestid(), instanceInfo.instanceid(),
                                   fmt::underlying(shutdownRsp.code()), shutdownRsp.message());
                    } else {
                        YRLOG_INFO("{}|succeed to shutdown instance({}).", instanceInfo.requestid(),
                                   instanceInfo.instanceid());
                    }

                    metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetBillingInstanceEndTime(
                        instanceInfo.instanceid(), std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count());
                    return Status::OK();
                });
        });
}

litebus::Future<Status> InstanceCtrlActor::KillRuntime(const InstanceInfo &instanceInfo, bool isRecovering)
{
    // stop wait for update status when kill runtime
    auto iter = instanceStatusPromises_.find(instanceInfo.instanceid());
    if (iter != instanceStatusPromises_.end()) {
        iter->second.SetValue(Status::OK());
        (void)instanceStatusPromises_.erase(instanceInfo.instanceid());
    }

    return SendKillRequestToAgent(instanceInfo, isRecovering)
        .Then([instanceInfo](const messages::KillInstanceResponse &rsp) -> litebus::Future<Status> {
            if (rsp.code() != static_cast<int32_t>(StatusCode::SUCCESS)) {
                YRLOG_WARN("{}|kill instance({}), errCode {}", instanceInfo.requestid(), instanceInfo.instanceid(),
                           rsp.code());
            } else {
                YRLOG_INFO("{}|succeed to kill instance({})", instanceInfo.requestid(), instanceInfo.instanceid());
            }
            return Status::OK();
        });
}

litebus::Future<KillResponse> InstanceCtrlActor::SendSignal(const std::shared_ptr<KillContext> &killCtx,
                                                            const std::string &srcInstanceID,
                                                            const std::shared_ptr<KillRequest> &killReq)
{
    // if signalRoute failed or instance is in remote node
    if (killCtx->killRsp.code() != common::ErrorCode::ERR_NONE) {
        return killCtx->killRsp;
    }

    if (killCtx->instanceIsFailed) {
        killCtx->killRsp.set_code(common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
        killCtx->killRsp.set_message("instance already failed, unable to send custom signal");
        return killCtx->killRsp;
    }

    if (!killCtx->isLocal) {
        return GetLocalSchedulerAID(killCtx->instanceContext->GetInstanceInfo().instanceid())
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SendForwardCustomSignalRequest, _1, srcInstanceID,
                                 killReq, killCtx->instanceContext->GetInstanceInfo().requestid(), false));
    }

    auto signalReq = std::make_shared<runtime::SignalRequest>();
    signalReq->set_signal(killReq->signal());
    signalReq->set_payload(killReq->payload());

    auto &instanceInfo = killCtx->instanceContext->GetInstanceInfo();
    // Suspend-state instance handler retrieval only; pending refactor
    static const int32_t GET_INSTANCE = 74;
    static const std::string NAMED_FUNCMETA = "named_funcmeta";
    if (instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::SUSPEND)
        && killReq->signal() == GET_INSTANCE && instanceInfo.args_size() >= 1
        && instanceInfo.createoptions().find(NAMED_FUNCMETA) != instanceInfo.createoptions().end()) {
        killCtx->killRsp.set_code(common::ErrorCode::ERR_NONE);
        killCtx->killRsp.set_message(instanceInfo.createoptions().at(NAMED_FUNCMETA));
        return killCtx->killRsp;
    }
    ASSERT_IF_NULL(clientManager_);
    return clientManager_->GetControlInterfacePosixClient(instanceInfo.instanceid())
        .Then([signalReq, instanceInfo,
               aid(GetAID())](const litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> &future)
                  -> litebus::Future<KillResponse> {
            auto instanceClient = future.Get();
            if (instanceClient == nullptr) {
                YRLOG_ERROR("{}|failed to get instance client instance({})", instanceInfo.requestid(),
                            instanceInfo.instanceid());
                return GenKillResponse(
                    common::ErrorCode::ERR_INSTANCE_NOT_FOUND,
                    "posix connection is not found, the instance is not created or fatal error occurred");
            }
            YRLOG_INFO("{}|send signal {} to instance({})", instanceInfo.requestid(), signalReq->signal(),
                       instanceInfo.instanceid());
            return instanceClient->Signal(std::move(*signalReq))
                .Then([instanceID(instanceInfo.instanceid())](const runtime::SignalResponse &signalRsp) {
                    return GenKillResponse(signalRsp.code(), signalRsp.message(), signalRsp.payload());
                });
        });
}

litebus::Future<KillResponse> InstanceCtrlActor::KillInstancesOfJob(const std::shared_ptr<KillRequest> &killReq)
{
    const auto &jobID = killReq->instanceid();
    YRLOG_INFO("kill instances of jobID: {}", jobID);

    if (killReq->instanceid().empty()) {
        YRLOG_ERROR("invalid param, instance id is empty");
        return GenKillResponse(common::ErrorCode::ERR_PARAM_INVALID, "instance id is empty");
    }

    auto req = std::make_shared<messages::ForwardKillRequest>();
    req->set_requestid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    req->mutable_req()->CopyFrom(*killReq);
    ASSERT_IF_NULL(localSchedSrv_);
    return localSchedSrv_->ForwardKillToInstanceManager(req).Then([](const messages::ForwardKillResponse &response) {
        KillResponse killResp;
        killResp.set_code(Status::GetPosixErrorCode(response.code()));
        killResp.set_message(response.message());
        return killResp;
    });
}

litebus::Future<ScheduleResponse> InstanceCtrlActor::Schedule(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::shared_ptr<litebus::Promise<ScheduleResponse>> &runtimePromise)
{
    if (!scheduleReq->instance().parentid().empty() &&
        scheduleReq->instance().instancestatus().code() == static_cast<int32_t>(InstanceState::NEW)) {
        auto stateMachine = instanceControlView_->GetInstance(scheduleReq->instance().parentid());
        if (stateMachine != nullptr && stateMachine->GetInstanceState() == InstanceState::EXITING) {
            YRLOG_WARN("{}|{}|receive a schedule request from an exiting instance({}) directly return",
                       scheduleReq->traceid(), scheduleReq->requestid(), scheduleReq->instance().parentid());
            runtimePromise->SetValue(
                GenScheduleResponse(StatusCode::ERR_INSTANCE_EXITED,
                                    "you are not allowed to create instance because of you are exiting", *scheduleReq));
            return GenScheduleResponse(StatusCode::ERR_INSTANCE_EXITED,
                                       "you are not allowed to create instance because of you are exiting",
                                       *scheduleReq);
        }
        if (stateMachine != nullptr && IsFrontendFunction(stateMachine->GetInstanceInfo().function())) {
            (*scheduleReq->mutable_instance()->mutable_extensions())[CREATE_SOURCE] = FRONTEND_STR;
        }
    }

    if (!scheduleReq->instance().instanceid().empty()) {
        auto stateMachine = instanceControlView_->GetInstance(scheduleReq->instance().instanceid());
        if (scheduleReq->instance().instancestatus().code() == static_cast<uint32_t>(InstanceState::NEW)
            && stateMachine != nullptr) {
            if (CheckExistInstanceState(static_cast<InstanceState>(stateMachine->GetInstanceState()), runtimePromise,
                                        scheduleReq)) {
                return runtimePromise->GetFuture();
            }
        }
    }

    ASSERT_IF_NULL(observer_);
    YRLOG_INFO("{}|{}|receive a schedule request, instance version({})", scheduleReq->traceid(),
               scheduleReq->requestid(), scheduleReq->instance().version());
    if (isAbnormal_) {
        runtimePromise->SetValue(
            GenScheduleResponse(StatusCode::ERR_LOCAL_SCHEDULER_ABNORMAL, "local is already abnormal", *scheduleReq));
        return GenScheduleResponse(StatusCode::ERR_LOCAL_SCHEDULER_ABNORMAL, "local is already abnormal", *scheduleReq);
    }
    // Check whether the function meta information corresponding to requestID exists.
    return GetFuncMeta(scheduleReq->instance().function())
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DoAuthorizeCreate, _1, scheduleReq, runtimePromise));
}

void InstanceCtrlActor::AddTenantToScheduleAffinity(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
                                                    const std::string &tenantID)
{
    // Create tenant affinity selectors
    auto tenantRequiredAntiAffinity = Selector(false, { { NotIn(TENANT_ID, { tenantID }), Exist(TENANT_ID) } });
    auto tenantPreferredAffinity = Selector(true, { { In(TENANT_ID, { tenantID }) } });

    // Config tenant affinity
    auto &inner = *scheduleReq->mutable_instance()->mutable_scheduleoption()->mutable_affinity()->mutable_inner();
    (*inner.mutable_tenant()->mutable_preferredaffinity()) = std::move(tenantPreferredAffinity);
    (*inner.mutable_tenant()->mutable_requiredantiaffinity()) = std::move(tenantRequiredAntiAffinity);
    int64_t optimalScore = GetAffinityMaxScore(scheduleReq.get());
    (*scheduleReq->mutable_contexts())[LABEL_AFFINITY_PLUGIN].mutable_affinityctx()->set_maxscore(
        optimalScore + MAX_PRIORITY_SCORE);
}

void InstanceCtrlActor::EraseTenantFromScheduleAffinity(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
                                                        const std::string &key)
{
    auto &resourceAffinity =
        *scheduleReq->mutable_instance()->mutable_scheduleoption()->mutable_affinity()->mutable_resource();
    YRLOG_DEBUG("before Inner erase tenant from resource affinity: {}", resourceAffinity.ShortDebugString());
    if (resourceAffinity.has_requiredantiaffinity()) {
        EraseLabelFromSelector(*resourceAffinity.mutable_requiredantiaffinity(), key);
    }
    if (resourceAffinity.has_preferredaffinity()) {
        EraseLabelFromSelector(*resourceAffinity.mutable_preferredaffinity(), key);
    }
    YRLOG_DEBUG("after Inner erase tenant from resource affinity: {}", resourceAffinity.ShortDebugString());

    auto &instanceAffinity =
        *scheduleReq->mutable_instance()->mutable_scheduleoption()->mutable_affinity()->mutable_instance();
    YRLOG_DEBUG("before Inner erase tenant from instance affinity: {}", instanceAffinity.ShortDebugString());
    if (instanceAffinity.has_requiredantiaffinity()) {
        EraseLabelFromSelector(*instanceAffinity.mutable_requiredantiaffinity(), key);
    }
    if (instanceAffinity.has_preferredaffinity()) {
        EraseLabelFromSelector(*instanceAffinity.mutable_preferredaffinity(), key);
    }
    YRLOG_DEBUG("after Inner erase tenant from instance affinity: {}", instanceAffinity.ShortDebugString());
}

void InstanceCtrlActor::SetTenantAffinityOpt(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    auto labels = scheduleReq->mutable_instance()->mutable_labels();
    EraseLabelFromLabels(*labels, TENANT_ID);
    auto tenantID = scheduleReq->instance().tenantid();
    labels->Add(TENANT_ID + ":" + tenantID);
    YRLOG_DEBUG("add labels {}:{}", TENANT_ID, tenantID);

    EraseTenantFromScheduleAffinity(scheduleReq, TENANT_ID);
    AddTenantToScheduleAffinity(scheduleReq, tenantID);
    YRLOG_DEBUG("after AddTenantToScheduleAffinity inner tenant Affinity: {}",
                scheduleReq->instance().scheduleoption().affinity().inner().tenant().ShortDebugString());
}

Status InstanceCtrlActor::VerifyAffinityWithoutTenantKey(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
                                                         const std::string &key)
{
    const std::string traceID = scheduleReq->traceid();
    const std::string requestID = scheduleReq->requestid();

    auto resourceAffinity = scheduleReq->instance().scheduleoption().affinity().resource();
    if (resourceAffinity.has_requiredantiaffinity() &&
        IsSelectorContainsLabel(resourceAffinity.requiredantiaffinity(), key)) {
        YRLOG_ERROR("{}|{}|resource RequiredAntiAffinity contains tenantId, scheduleReq:{}", traceID, requestID,
                    scheduleReq->ShortDebugString());
        return Status(StatusCode::PARAMETER_ERROR, "RequiredAntiAffinity contains label tenantId");
    }
    if (resourceAffinity.has_preferredaffinity() &&
        IsSelectorContainsLabel(resourceAffinity.preferredaffinity(), key)) {
        YRLOG_ERROR("{}|{}|resource preferredaffinity contains tenantId, scheduleReq:{}", traceID, requestID,
                    scheduleReq->ShortDebugString());
        return Status(StatusCode::PARAMETER_ERROR, "PreferredAffinity contains label tenantId");
    }

    auto isntanceAffinity = scheduleReq->instance().scheduleoption().affinity().instance();
    if (isntanceAffinity.has_requiredantiaffinity() &&
        IsSelectorContainsLabel(isntanceAffinity.requiredantiaffinity(), key)) {
        YRLOG_ERROR("{}|{}|instance RequiredAntiAffinity contains tenantId, scheduleReq:{}", traceID, requestID,
                    scheduleReq->ShortDebugString());
        return Status(StatusCode::PARAMETER_ERROR, "RequiredAntiAffinity contains label tenantId");
    }
    if (isntanceAffinity.has_preferredaffinity() &&
        IsSelectorContainsLabel(isntanceAffinity.preferredaffinity(), key)) {
        YRLOG_ERROR("{}|{}|instance preferredaffinity contains tenantId, scheduleReq:{}", traceID, requestID,
                    scheduleReq->ShortDebugString());
        return Status(StatusCode::PARAMETER_ERROR, "PreferredAffinity contains label tenantId");
    }

    return Status(StatusCode::SUCCESS, "Verification passed");
}

Status InstanceCtrlActor::VerifyTenantID(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
                                         const std::string &traceID, const std::string &requestID)
{
    // Verify tenant ID
    auto tenantID = scheduleReq->instance().tenantid();
    if (tenantID.length() > TENANT_ID_MAX_LENGTH) {
        YRLOG_ERROR("{}|{}|invalid tenantid({}) length", traceID, requestID, tenantID);
        return Status(StatusCode::ERR_PARAM_INVALID, "invalid tenantid length");
    }

    // Verify labels without 'tenantId'
    auto labels = scheduleReq->mutable_instance()->labels();
    for (auto label : labels) {
        if (label == TENANT_ID) {
            return Status(StatusCode::ERR_PARAM_INVALID, "labels contains tenantId");
        }
    }

    // Verify scheduleOption.affinity.instance/resource without 'tenantId'
    return VerifyAffinityWithoutTenantKey(scheduleReq, TENANT_ID);
}

messages::ScheduleResponse InstanceCtrlActor::PrepareCreateInstance(
    const Status &authorizeStatus, const litebus::Option<FunctionMeta> &functionMeta,
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise)
{
    const std::string traceID = scheduleReq->traceid();
    const std::string requestID = scheduleReq->requestid();
    const auto &tenantID = scheduleReq->instance().tenantid();
    bool notLimited = DoRateLimit(scheduleReq);
    if (!notLimited) {
        YRLOG_ERROR("{}|{}|tenant({}) create rate limited on local.", traceID, requestID, tenantID);
        runtimePromise->SetValue(
            GenScheduleResponse(StatusCode::ERR_CREATE_RATE_LIMITED, "create rate limited on local", *scheduleReq));
        return GenScheduleResponse(StatusCode::ERR_CREATE_RATE_LIMITED, "create rate limited on local", *scheduleReq);
    }

    auto funcMeta = functionMeta.Get();
    auto isSystemFunc = funcMeta.funcMetaData.isSystemFunc;
    scheduleReq->mutable_instance()->set_storagetype(funcMeta.codeMetaData.storageType);
    LoadDeviceFunctionMetaToScheduleRequest(scheduleReq, funcMeta);

    ASSERT_IF_NULL(observer_);
    if (isSystemFunc) {
        YRLOG_DEBUG("{}|{}|is for system function", traceID, requestID);
        (*scheduleReq->mutable_instance()->mutable_createoptions())[RESOURCE_OWNER_KEY] = SYSTEM_OWNER_VALUE;
        scheduleReq->mutable_instance()->set_issystemfunc(true);
    } else if (scheduleReq->instance().instancestatus().code() == 0) {
        auto status = VerifyTenantID(scheduleReq, traceID, requestID);
        if (status.StatusCode() != StatusCode::SUCCESS) {
            runtimePromise->SetValue(GenScheduleResponse(status.StatusCode(), status.GetMessage(), *scheduleReq));
            return GenScheduleResponse(status.StatusCode(), status.GetMessage(), *scheduleReq);
        }

        if (config_.enableTenantAffinity &&
            scheduleReq->instance().scheduleoption().schedpolicyname() != MONOPOLY_SCHEDULE) {
            // Set tenant affinity scheduling labels after setting the tenant ID.
            SetTenantAffinityOpt(scheduleReq);
            YRLOG_DEBUG("{}|after SetTenantAffinityOpt, scheduleReq:{}", scheduleReq->requestid(),
                        scheduleReq->ShortDebugString());
        }
    }

    auto resourceSelector = scheduleReq->mutable_instance()->mutable_scheduleoption()->mutable_resourceselector();
    if (resourceSelector->find(RESOURCE_OWNER_KEY) == resourceSelector->end()) {
        (*scheduleReq->mutable_instance()->mutable_scheduleoption()
              ->mutable_resourceselector())[RESOURCE_OWNER_KEY] =
            NeedCreateAgent(scheduleReq->instance()) ? litebus::uuid_generator::UUID::GetRandomUUID().ToString()
                                                     : DEFAULT_OWNER_VALUE;
    }
    return GenScheduleResponse(StatusCode::SUCCESS, "", *scheduleReq);
}

litebus::Future<ScheduleResponse> InstanceCtrlActor::DoCreateInstance(
    const Status &authorizeStatus, const litebus::Option<FunctionMeta> &functionMeta,
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::shared_ptr<litebus::Promise<ScheduleResponse>> &runtimePromise)
{
    const std::string traceID = scheduleReq->traceid();
    const std::string requestID = scheduleReq->requestid();
    if (authorizeStatus.IsError()) {
        YRLOG_ERROR("{}|{}|authorize failed.", traceID, requestID);
        runtimePromise->SetValue(
            GenScheduleResponse(StatusCode::ERR_AUTHORIZE_FAILED, "authorize failed", *scheduleReq));
        return GenScheduleResponse(StatusCode::ERR_AUTHORIZE_FAILED, "authorize failed", *scheduleReq);
    }
    if (auto schedResultOpt(instanceControlView_->IsDuplicateRequest(scheduleReq, runtimePromise));
        schedResultOpt.IsSome()) {
        YRLOG_WARN("{}|{}|schedule request already exists.", traceID, requestID);
        RegisterStateChangeCallback(scheduleReq, runtimePromise);
        return schedResultOpt.Get();
    }
    if (functionMeta.IsNone()) {
        YRLOG_ERROR("{}|{}|failed to find function: {} meta for schedule.", traceID, requestID,
                    scheduleReq->instance().function());
        runtimePromise->SetValue(
            GenScheduleResponse(StatusCode::ERR_FUNCTION_META_NOT_FOUND, "invalid function", *scheduleReq));
        return GenScheduleResponse(StatusCode::ERR_FUNCTION_META_NOT_FOUND, "failed to find function meta",
                                   *scheduleReq);
    }

    auto response = PrepareCreateInstance(authorizeStatus, functionMeta, scheduleReq, runtimePromise);
    if (response.code() != static_cast<int32_t>(StatusCode::SUCCESS)) {
        return response;
    }

    if (auto status = CheckSchedRequestValid(scheduleReq); status.IsError()) {
        YRLOG_ERROR("{}|{}|schedule request is invalid.", traceID, requestID);
        auto errorMessage = status.GetMessage();
        runtimePromise->SetValue(GenScheduleResponse(
            status.StatusCode(), errorMessage.substr(1, errorMessage.length() - ERROR_MESSAGE_SEPARATE), *scheduleReq));
        return GenScheduleResponse(StatusCode::FAILED, "resources is invalid", *scheduleReq);
    }
    if (scheduleReq->instance().jobid().empty()) {
        auto jobID = GenerateJobIDFromTraceID(traceID);
        if (jobID.empty()) {
            YRLOG_WARN("{}|{}|jobID is empty", traceID, requestID);
        }
        scheduleReq->mutable_instance()->set_jobid(jobID);
    }
    ASSERT_IF_NULL(instanceControlView_);

    auto schedResult = CheckGeneratedInstanceID(instanceControlView_->TryGenerateNewInstance(scheduleReq), scheduleReq,
                                                runtimePromise);
    // The scheduling result follows the instance life cycle.
    // In the future, the lock mechanism needs to be improved to avoid deduplication of scheduling results.
    instanceControlView_->InsertRequestFuture(requestID, schedResult, runtimePromise);
    return schedResult.Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeleteRequestFuture, std::placeholders::_1,
                                           requestID, scheduleReq));
}

void InstanceCtrlActor::RegisterStateChangeCallback(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::shared_ptr<litebus::Promise<ScheduleResponse>> &runtimePromise)
{
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(scheduleReq->instance().instanceid());
    if (stateMachine == nullptr) {
        runtimePromise->SetValue(GenScheduleResponse(StatusCode::ERR_INSTANCE_EXITED,
                                                     "instance may already have been killed", *scheduleReq));
        return;
    }
    // subsequent instance status change events are subscribed.
    if (stateMachine->GetInstanceState() != InstanceState::SCHEDULING) {
        runtimePromise->SetValue(
            GenScheduleResponse(StatusCode::SUCCESS, "instance has already been scheduling", *scheduleReq));
    } else {
        auto future = instanceControlView_->GetRequestFuture(stateMachine->GetRequestID());
        if (future.IsOK()) {
            runtimePromise->Associate(future);
        } else {
            runtimePromise->SetValue(
                GenScheduleResponse(StatusCode::SUCCESS, "instance has already been scheduling", *scheduleReq));
            return;
        }
    }

    if (scheduleReq->instance().instancestatus().code() != static_cast<int32_t>(InstanceState::NEW)) {
        return;
    }
    auto funcAgentID = scheduleReq->instance().functionagentid();
    stateMachine->AddStateChangeCallback(
        { InstanceState::RUNNING, InstanceState::SCHEDULE_FAILED, InstanceState::EXITING, InstanceState::FATAL },
        [aid(GetAID()), parentID(scheduleReq->instance().parentid()), funcAgentID, functionAgentMgr(functionAgentMgr_),
         requestID(scheduleReq->requestid())](const InstanceInfo &instanceInfo) {
            InstanceInfo info = instanceInfo;
            if (instanceInfo.parentid() != parentID) {
                info.set_parentid(parentID);
                YRLOG_INFO("{} add state change callback for instance {}", info.requestid(), info.instanceid());
            }
            litebus::Async(aid, &InstanceCtrlActor::SubscribeInstanceStatusChanged, info, info.requestid());
        },
        "SubscribeInstanceStatusChanged");
}

litebus::Future<ScheduleResponse> InstanceCtrlActor::CheckGeneratedInstanceID(
    const GeneratedInstanceStates &genStatus, const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::shared_ptr<litebus::Promise<ScheduleResponse>> &runtimePromise)
{
    if (genStatus.instanceID.empty()) {
        YRLOG_ERROR("{}|{}|failed to generate instanceID", scheduleReq->traceid(), scheduleReq->requestid());
        runtimePromise->SetValue(
            GenScheduleResponse(StatusCode::ERR_INSTANCE_INFO_INVALID, "failed to generate instance ID", *scheduleReq));
        return GenScheduleResponse(StatusCode::ERR_INSTANCE_INFO_INVALID, "failed to generate instance ID",
                                   *scheduleReq);
    }
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(genStatus.instanceID);
    if (stateMachine == nullptr) {
        auto rsp =
            GenScheduleResponse(StatusCode::ERR_INSTANCE_EXITED, "instance may already have been killed", *scheduleReq);
        runtimePromise->SetValue(rsp);
        return rsp;
    }
    // Currently, it is considered that new scheduling is triggered when duplicate scheduling requests are received
    // after the failed scheduling.
    if (!genStatus.isDuplicate || genStatus.preState == InstanceState::SCHEDULE_FAILED
        || genStatus.preState == InstanceState::FAILED) {
        int64_t version =
            (genStatus.preState == InstanceState::SCHEDULE_FAILED || genStatus.preState == InstanceState::FAILED)
                ? stateMachine->GetVersion()
                : scheduleReq->instance().version();
        // The instance states may be changed to FATAL or Exiting by InstanceManager and instance never send CallResult
        // back. Need to watch the instance state changed by InstanceManager and send CallResult to caller by self.
        stateMachine->AddStateChangeCallback(
            std::unordered_set<InstanceState>{ InstanceState::EXITING, InstanceState::FATAL },
            [aid(GetAID())](const InstanceInfo &instanceInfo) {
                litebus::Async(aid, &InstanceCtrlActor::SubscribeStateChangedByInstMgr, instanceInfo);
            },
            "SubscribeStateChangedByInstMgr");

        bool persistence = NeedPersistenceState(genStatus.preState);
        auto transContext = TransContext{ InstanceState::SCHEDULING, version, "scheduling", persistence };
        SetGracefulShutdownTime(scheduleReq);
        transContext.scheduleReq = scheduleReq;

        return TransInstanceState(stateMachine, transContext)
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DoDispatchSchedule, scheduleReq, runtimePromise, _1));
    }
    // For repeated requests, the generated instance ID is returned and subsequent instance status change events are
    // subscribed.
    runtimePromise->SetValue(GenScheduleResponse(StatusCode::SUCCESS, "ready to deploy instance", *scheduleReq));
    stateMachine->AddStateChangeCallback(
        { InstanceState::RUNNING, InstanceState::SCHEDULE_FAILED, InstanceState::EXITING, InstanceState::FATAL },
        [aid(GetAID()), scheduleReq](const InstanceInfo &instanceInfo) {
            InstanceInfo info = instanceInfo;
            if (instanceInfo.parentid() != scheduleReq->instance().parentid()) {
                info.set_parentid(scheduleReq->instance().parentid());
                YRLOG_INFO("{}|{} add state change callback for instance {}, parentID is set to: {}",
                           scheduleReq->traceid(), scheduleReq->requestid(), scheduleReq->instance().instanceid(),
                           info.parentid());
            }
            litebus::Async(aid, &InstanceCtrlActor::SubscribeInstanceStatusChanged, info, scheduleReq->requestid());
        },
        "SubscribeInstanceStatusChangedRunning");
    return GenScheduleResponse(StatusCode::SUCCESS, "", *scheduleReq);
}

void InstanceCtrlActor::SubscribeStateChangedByInstMgr(const InstanceInfo &instanceInfo)
{
    if (instanceInfo.functionproxyid() != INSTANCE_MANAGER_OWNER) {
        YRLOG_DEBUG("{}|instance({}) owner is {}, no concern about the state change", instanceInfo.requestid(),
                    instanceInfo.instanceid(), instanceInfo.functionproxyid());
        return;
    }
    auto status = instanceInfo.instancestatus();
    YRLOG_DEBUG(
        "{}|instance({}) owner is {} and change state to {}, send CallResult to caller({}), parent proxy AID({})",
        instanceInfo.requestid(), instanceInfo.instanceid(), instanceInfo.functionproxyid(), status.code(),
        instanceInfo.parentid(), instanceInfo.parentfunctionproxyaid());
    if (static_cast<InstanceState>(status.code()) != InstanceState::FATAL &&
        static_cast<InstanceState>(status.code()) != InstanceState::EXITING) {
        return;
    }
    auto code = (static_cast<InstanceState>(status.code()) == InstanceState::FATAL)
                    ? Status::GetPosixErrorCode(status.errcode())
                    : common::ErrorCode::ERR_INSTANCE_EXITED;
    auto callResult = std::make_shared<functionsystem::CallResult>();
    callResult->set_requestid(instanceInfo.requestid());
    callResult->set_instanceid(instanceInfo.parentid());
    callResult->set_code(code);
    callResult->set_message(status.msg());
    (void)SendCallResult(instanceInfo.instanceid(), instanceInfo.parentid(), instanceInfo.parentfunctionproxyaid(),
                         callResult);
}

void InstanceCtrlActor::SubscribeInstanceStatusChanged(const InstanceInfo &instanceInfo,
                                                       const std::string &currentRequestID)
{
    auto status = instanceInfo.instancestatus();
    auto callResult = std::make_shared<functionsystem::CallResult>();
    callResult->set_requestid(currentRequestID);
    callResult->set_instanceid(instanceInfo.parentid());
    if (static_cast<InstanceState>(status.code()) == InstanceState::SCHEDULE_FAILED
        || static_cast<InstanceState>(status.code()) == InstanceState::FATAL) {
        callResult->set_code(Status::GetPosixErrorCode(status.errcode()));
        callResult->set_message(status.msg());
    }

    if (static_cast<InstanceState>(status.code()) == InstanceState::RUNNING) {
        callResult->set_code(common::ErrorCode::ERR_NONE);
    }
    if (static_cast<InstanceState>(status.code()) == InstanceState::EXITING) {
        callResult->set_code(common::ErrorCode::ERR_INSTANCE_EXITED);
        callResult->set_message(status.msg());
    }
    (void)SendCallResult(instanceInfo.instanceid(), instanceInfo.parentid(), instanceInfo.parentfunctionproxyaid(),
                         callResult);

    instanceControlView_->DeleteRequestFuture(currentRequestID);
}

// When DoDispatchSchedule, it will make the ScheduleDecision, and then start deploy instance process asynchronously,
// the ScheduleDecision may also run asynchronously depends on the instance old prevState,
// * NEW, means the schedule started here (this local), and should reply schedule response before making any decision,
//     just trans to scheduling is enough.
// * SCHEDULING, means the schedule starts from other local scheduler and be forwarded to this local. In this case, we
//     should check local resources and do the schedule decision before we reply the schedule response
litebus::Future<ScheduleResponse> InstanceCtrlActor::DoDispatchSchedule(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::shared_ptr<litebus::Promise<ScheduleResponse>> &runtimePromise, const TransitionResult &result)
{
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(scheduleReq->instance().instanceid());
    if (stateMachine == nullptr) {
        YRLOG_ERROR("{}|{}|failed to find instance({}) to dispatch schedule", scheduleReq->traceid(),
                    scheduleReq->requestid(), scheduleReq->instance().instanceid());
        return GenScheduleResponse(StatusCode::ERR_INSTANCE_NOT_FOUND, "failed to find instance", *scheduleReq);
    }
    if (result.preState.IsNone()) {
        if (result.savedInfo.instanceid().empty()) {
            const std::string msg = "failed to update instance info of " + scheduleReq->instance().instanceid()
                                    + " to metastore, err: " + result.status.GetMessage();
            YRLOG_ERROR("{}|{}|{}", scheduleReq->traceid(), scheduleReq->requestid(), msg);
            runtimePromise->SetValue(GenScheduleResponse(StatusCode::ERR_ETCD_OPERATION_ERROR, msg, *scheduleReq));
            instanceControlView_->OnDelInstance(scheduleReq->instance().instanceid(), scheduleReq->requestid(), true);
            return GenScheduleResponse(StatusCode::ERR_ETCD_OPERATION_ERROR, msg, *scheduleReq);
        } else {
            // failed during Txn, return status according to current state
            if (CheckExistInstanceState(static_cast<InstanceState>(result.savedInfo.instancestatus().code()),
                                        runtimePromise, scheduleReq)
                && scheduleReq->instance().instancestatus().code() == static_cast<uint32_t>(InstanceState::NEW)) {
                return runtimePromise->GetFuture();
            }
            if (static_cast<InstanceState>(result.savedInfo.instancestatus().code()) == InstanceState::RUNNING) {
                YRLOG_INFO("{}|{}|{}|{}", scheduleReq->traceid(), scheduleReq->requestid(),
                           scheduleReq->instance().instanceid(), "instance is running, just return instance state");
                return runtimePromise->GetFuture();
            }
            const std::string msg = "instance has been scheduled on other node";
            YRLOG_WARN("{}|{}|{}", scheduleReq->traceid(), scheduleReq->requestid(),
                       "instance has been scheduled on other node");
            runtimePromise->SetValue(GenScheduleResponse(StatusCode::SUCCESS, msg, *scheduleReq));
            return GenScheduleResponse(StatusCode::SUCCESS, msg, *scheduleReq);
        }
    }
    // This promise is used by the request from runtime.
    runtimePromise->SetValue(GenScheduleResponse(StatusCode::SUCCESS, "ready to deploy instance", *scheduleReq));
    if (config_.isPseudoDataPlane) {
        ScheduleResult schedResult;
        schedResult.code = static_cast<int32_t>(StatusCode::RESOURCE_NOT_ENOUGH);
        return ConfirmScheduleDecisionAndDispatch(scheduleReq, schedResult, result.preState.Get());
    }
    if (result.preState.Get() == InstanceState::NEW || result.preState.Get() == InstanceState::SCHEDULE_FAILED) {
        YRLOG_DEBUG("{}|{}|this local-scheduler is the first local-scheduler of the schedule request, instance: {}",
                    scheduleReq->traceid(), scheduleReq->requestid(), scheduleReq->instance().instanceid());
    }
    ASSERT_IF_NULL(scheduler_);
    // todo(lwy_robb): to use traceID
    trace::TraceManager::GetInstance().StartSpanWithRecord(
        { "LocalSchedule", scheduleReq->requestid(), "",
          scheduleReq->instance().function(), scheduleReq->instance().instanceid() });
    return scheduler_->ScheduleDecision(scheduleReq)
                      .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::ConfirmScheduleDecisionAndDispatch,
                                           scheduleReq, _1, result.preState.Get()));
}

litebus::Future<messages::ScheduleResponse> InstanceCtrlActor::TryDispatchOnLocal(
    const Status &status, const std::shared_ptr<ScheduleRequest> &scheduleReq, const ScheduleResult &result,
    const InstanceState &prevState, const std::shared_ptr<InstanceStateMachine> &stateMachineRef)
{
    if (status.IsError()) {
        YRLOG_WARN("{}|{}|failed to allocated instance({}) on ({}). retry to schedule decision",
                   scheduleReq->traceid(), scheduleReq->requestid(), scheduleReq->instance().instanceid(), result.id);
        auto rsp = std::make_shared<ScheduleResponse>();
        rsp->set_code(static_cast<int32_t>(StatusCode::FUNC_AGENT_FAILED_DEPLOY));
        rsp->set_requestid(scheduleReq->requestid());
        rsp->set_instanceid(scheduleReq->instance().instanceid());
        ASSERT_IF_NULL(scheduler_);
        return scheduler_->ScheduleConfirm(rsp, scheduleReq->instance(), result)
            .Then([scheduler(scheduler_), scheduleReq, aid(GetAID()),
                   prevState, stateMachineRef](const Status &) -> litebus::Future<messages::ScheduleResponse> {
                return scheduler->ScheduleDecision(scheduleReq)
                    .Then(litebus::Defer(aid, &InstanceCtrlActor::ConfirmScheduleDecisionAndDispatch, scheduleReq, _1,
                                         prevState));
            });
    }
    YRLOG_DEBUG("{}|{}|start deploy instance({}) to function agent({})", scheduleReq->traceid(),
                scheduleReq->requestid(), scheduleReq->instance().instanceid(), result.id);
    MergeScheduleResultToRequest(scheduleReq, result);
    scheduleReq->mutable_instance()->set_datasystemhost(config_.cacheStorageHost);
    auto scheduleResp = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto transContext = TransContext{ InstanceState::CREATING, stateMachineRef->GetVersion(), "creating" };
    transContext.scheduleReq = scheduleReq;
    if (scheduleReq->instance().has_snapshotinfo()) {
        TransInstanceState(stateMachineRef, transContext)
        .OnComplete([scheduleResp, scheduleReq, result, snapCtrl(snapCtrl_)](
                        const litebus::Future<TransitionResult> &transResult) {
            ASSERT_FS(transResult.IsOK());
            return snapCtrl->SnapStart(scheduleResp, scheduleReq, result, transResult.Get());
        });
    } else {
        TransInstanceState(stateMachineRef, transContext)
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::OnTryDispatchOnLocal,
                scheduleResp, scheduleReq, result, _1))
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeployInstance, scheduleReq, 0, _1, false))
            .OnComplete(litebus::Defer(GetAID(), &InstanceCtrlActor::ScheduleEnd, _1, scheduleReq));
    }
    return scheduleResp->GetFuture();
}

litebus::Option<TransitionResult> InstanceCtrlActor::OnTryDispatchOnLocal(
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
    const std::shared_ptr<ScheduleRequest> &scheduleReq, const ScheduleResult &result,
    const TransitionResult &transResult)
{
    // todo(lwy_robb): to use traceID
    trace::TraceManager::GetInstance().StopSpan("LocalSchedule", scheduleReq->requestid());
    if (IsLowReliabilityInstance(scheduleReq->instance()) || transResult.version != 0) {
        scheduleResp->SetValue(GenScheduleResponse(result.code, result.reason, *scheduleReq));
        return litebus::None();
    }
    if (transResult.savedInfo.functionproxyid().empty()) {
        YRLOG_ERROR("failed to update state of instance({}), err: {}", transResult.previousInfo.instanceid(),
                    transResult.status.GetMessage());
        scheduleResp->SetValue(GenScheduleResponse(
            StatusCode::ERR_ETCD_OPERATION_ERROR,
            "failed to update instance info, err: " + transResult.status.GetMessage(), *scheduleReq));
        return transResult;
    }
    YRLOG_INFO("failed to update instance info, instance({}) is on local scheduler({})",
               transResult.savedInfo.instanceid(), transResult.savedInfo.functionproxyid());
    // version is incorrect and own by proxy which location is parent, need to reschedule by parent
    if (transResult.status.StatusCode() == StatusCode::INSTANCE_TRANSACTION_WRONG_VERSION &&
        (scheduleReq->instance().parentfunctionproxyaid().empty() ||
         ExtractProxyIDFromProxyAID(scheduleReq->instance().parentfunctionproxyaid()) ==
         transResult.savedInfo.functionproxyid())) {
        YRLOG_ERROR("{}|failed to update state of instance({}), parent({}), err: {}", scheduleReq->requestid(),
                    scheduleReq->instance().instanceid(), scheduleReq->instance().parentfunctionproxyaid(),
                    transResult.status.GetMessage());
        scheduleResp->SetValue(GenScheduleResponse(StatusCode::INSTANCE_TRANSACTION_WRONG_VERSION,
                                                   "version is incorrect and own by driver", *scheduleReq));
        return transResult;
    }
    scheduleResp->SetValue(
        GenScheduleResponse(StatusCode::SUCCESS, "instance is scheduled to another node", *scheduleReq));
    return transResult;
}

litebus::Future<messages::DeployInstanceResponse> InstanceCtrlActor::DeploySnapStartInstance(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    const auto &instanceID = scheduleReq->instance().instanceid();
    const auto &requestID = scheduleReq->requestid();

    YRLOG_INFO("{}|{}|DeploySnapStartInstance: start deploying snapstart instance", requestID, instanceID);

    // Check if function meta exists
    if (funcMetaMap_.find(scheduleReq->instance().function()) == funcMetaMap_.end()) {
        YRLOG_ERROR("{}|{}|failed to deploy instance, function meta not found", requestID, instanceID);
        return GenDeployInstanceResponse(StatusCode::ERR_FUNCTION_META_NOT_FOUND,
            "function meta not found", requestID);
    }

    // Prepare DeployInstanceRequest
    auto deployInstanceRequest = GetDeployInstanceReq(funcMetaMap_.at(scheduleReq->instance().function()), scheduleReq);
    auto funcAgentID = scheduleReq->instance().functionagentid();

    AddDsAuthToDeployInstanceReq(scheduleReq, deployInstanceRequest);

    // Call functionAgentMgr->DeployInstance
    ASSERT_IF_NULL(functionAgentMgr_);
    return AddCredToDeployInstanceReq(scheduleReq->instance().tenantid(), deployInstanceRequest)
        .Then([functionAgentMgr(functionAgentMgr_), funcAgentID, deployInstanceRequest, instanceID, requestID](
                  const Status &status) -> litebus::Future<messages::DeployInstanceResponse> {
            if (status.IsError()) {
                YRLOG_ERROR("{}|{}|AddCredToDeployInstanceReq failed", requestID, instanceID);
                return GenDeployInstanceResponse(status.StatusCode(), "require token failed",
                    deployInstanceRequest->requestid());
            }

            YRLOG_INFO("{}|{}|calling functionAgentMgr->DeployInstance", requestID, instanceID);
            return functionAgentMgr->DeployInstance(deployInstanceRequest, funcAgentID);
        });
}

litebus::Future<messages::ScheduleResponse> InstanceCtrlActor::ConfirmScheduleDecisionAndDispatch(
    const std::shared_ptr<ScheduleRequest> &scheduleReq, const ScheduleResult &result, const InstanceState &prevState)
{
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachineRef = instanceControlView_->GetInstance(scheduleReq->instance().instanceid());
    if (stateMachineRef == nullptr) {
        return GenScheduleResponse(StatusCode::ERR_INSTANCE_NOT_FOUND, "instance not found", *scheduleReq);
    }
    if (result.code == static_cast<int>(StatusCode::SUCCESS) ||
        result.code == static_cast<int>(StatusCode::INSTANCE_ALLOCATED)) {
        if (result.allocatedPromise != nullptr && result.allocatedPromise->GetFuture().IsInit()) {
            return result.allocatedPromise->GetFuture().Then(litebus::Defer(
                GetAID(), &InstanceCtrlActor::TryDispatchOnLocal, _1, scheduleReq, result, prevState, stateMachineRef));
        }
        auto status = Status::OK();
        if (result.allocatedPromise != nullptr && !result.allocatedPromise->GetFuture().IsInit()) {
            status = result.allocatedPromise->GetFuture().Get();
        }
        return TryDispatchOnLocal(status, scheduleReq, result, prevState, stateMachineRef);
    }
    stateMachineRef->ReleaseOwner();
    auto code = (result.code == static_cast<int32_t>(StatusCode::INVALID_RESOURCE_PARAMETER))
                    ? static_cast<int32_t>(StatusCode::RESOURCE_NOT_ENOUGH)
                    : result.code;
    YRLOG_DEBUG("{}|{}|now determine whether to forward schedule of instance({}), code({}), prevState({})",
                scheduleReq->traceid(), scheduleReq->requestid(), scheduleReq->instance().instanceid(), code,
                fmt::underlying(prevState));

    // schedule request will be modified in local_sched_srv, need to GenScheduleResponse before RetryForwardSchedule
    auto resp = GenScheduleResponse(code == static_cast<int32_t>(StatusCode::AFFINITY_SCHEDULE_FAILED)
                                        ? static_cast<int32_t>(StatusCode::SCHEDULE_CONFLICTED)
                                        : code,
                                    result.reason, *scheduleReq);

    // if this is first scheduled by this local and the resource of this local is not enough, forward the
    // schedule request to domain
    if ((prevState == InstanceState::NEW || prevState == InstanceState::SCHEDULE_FAILED)
        && (code == static_cast<int32_t>(StatusCode::RESOURCE_NOT_ENOUGH)
            || code == static_cast<int32_t>(StatusCode::AFFINITY_SCHEDULE_FAILED))) {
        (void)RetryForwardSchedule(scheduleReq, GenScheduleResponse(result.code, result.reason, *scheduleReq), 0,
                                   stateMachineRef)
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::HandleForwardResponseAndNotifyCreator, scheduleReq, _1));
    } else if (code != static_cast<int32_t>(StatusCode::SUCCESS)) {
        // partial watch schedule from domain failed need to clear the state machine cache, because of failed schedule
        // would not watch the instance which caused state machine leak.
        if (ExtractProxyIDFromProxyAID(scheduleReq->instance().parentfunctionproxyaid()) != nodeID_) {
            TryClearStateMachineCache(scheduleReq);
        }
    }
    return resp;
}

void InstanceCtrlActor::TryClearStateMachineCache(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    if (!config_.isPartialWatchInstances) {
        return;
    }
    ASSERT_IF_NULL(instanceControlView_);
    instanceControlView_->OnDelInstance(scheduleReq->instance().instanceid(), scheduleReq->requestid(), true);
}

litebus::Future<messages::ScheduleResponse> InstanceCtrlActor::RetryForwardSchedule(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq, const messages::ScheduleResponse &resp,
    uint32_t retryTimes, const std::shared_ptr<InstanceStateMachine> &stateMachine)
{
    // todo(lwy_robb): to use traceID
    trace::TraceManager::GetInstance().StartSpanWithRecord(
        { "ForwardSchedule", scheduleReq->requestid(), "",
          scheduleReq->instance().function(), scheduleReq->instance().instanceid() });
    if (auto cancel = stateMachine->GetCancelFuture(); cancel.IsOK()) {
        YRLOG_WARN("{}|{}|instance canceled before forward schedule, reason({})", scheduleReq->requestid(),
                   scheduleReq->instance().instanceid(), cancel.Get());
        std::string msg = "instance canceled before forward schedule, reason: " + cancel.Get();
        return GenScheduleResponse(StatusCode::ERR_SCHEDULE_CANCELED, msg, *scheduleReq);
    }
    if (IsStaticFunctionInstance(scheduleReq->instance()) && resp.code() != static_cast<int>(StatusCode::SUCCESS)) {
        return resp;
    }
    if (retryTimes < maxForwardScheduleRetryTimes_) {
        ASSERT_IF_NULL(localSchedSrv_);
        return localSchedSrv_->ForwardSchedule(scheduleReq)
            .Then([aid(GetAID()), retryTimes, scheduleReq, instanceControlView(instanceControlView_)](
                      const ScheduleResponse &resp) -> litebus::Future<messages::ScheduleResponse> {
                if (resp.code() == static_cast<int32_t>(StatusCode::INSTANCE_TRANSACTION_WRONG_VERSION)) {
                    auto stateMachineRef = instanceControlView->GetInstance(scheduleReq->instance().instanceid());
                    if (stateMachineRef == nullptr) {
                        YRLOG_DEBUG("{}|{} failed to get state machine", scheduleReq->requestid(),
                                    scheduleReq->instance().instanceid());
                        return resp;
                    }
                    // reschedule instance only when state is scheduling
                    if (stateMachineRef->GetInstanceState() != InstanceState::SCHEDULING) {
                        YRLOG_DEBUG("{}|{} instance state is not scheduling", scheduleReq->requestid(),
                                    scheduleReq->instance().instanceid());
                        return resp;
                    }
                    scheduleReq->mutable_instance()->set_version(stateMachineRef->GetVersion());
                    YRLOG_INFO("{}|{} forward schedule version is wrong, update version to ({}) and retry",
                               scheduleReq->requestid(), scheduleReq->instance().instanceid(),
                               scheduleReq->instance().version());
                    return litebus::Async(aid, &InstanceCtrlActor::RetryForwardSchedule, scheduleReq, resp,
                                          retryTimes + 1, stateMachineRef);
                }
                return resp;
            });
    }
    return resp;
}

void InstanceCtrlActor::SetGracefulShutdownTime(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    if (scheduleReq->mutable_instance()->gracefulshutdowntime() == -1) {
        YRLOG_DEBUG("set graceful shutdown time to {}", config_.runtimeConfig.runtimeShutdownTimeoutSeconds);
        scheduleReq->mutable_instance()->set_gracefulshutdowntime(config_.runtimeConfig.runtimeShutdownTimeoutSeconds);
    }
}

litebus::Future<ScheduleResponse> InstanceCtrlActor::HandleForwardResponseAndNotifyCreator(
    const std::shared_ptr<ScheduleRequest> &scheduleReq, const ScheduleResponse &resp)
{
    // todo(lwy_robb): to use traceID
    trace::TraceManager::GetInstance().StopSpan("ForwardSchedule", scheduleReq->requestid());
    ASSERT_IF_NULL(instanceControlView_);
    // If the forwarded scheduling request fails, the notify interface is invoked to notify the instance
    // creator of the scheduling failure, and this local scheduler, as the owner scheduling starting point
    // at local level (means not including domain level), should set the instance as failed, and wait for
    // the creator(runtime/driver/function-accessor) to clear the failed record.
    if (resp.code() != static_cast<int32_t>(StatusCode::SUCCESS)) {
        auto stateMachineRef = instanceControlView_->GetInstance(scheduleReq->instance().instanceid());
        if (stateMachineRef == nullptr) {
            YRLOG_WARN("{}|{}|failed to find instance({}) when notify creator the scheduling failure",
                       scheduleReq->traceid(), scheduleReq->requestid(), scheduleReq->instance().instanceid());
            return resp;
        }
        auto callResult = std::make_shared<functionsystem::CallResult>();
        callResult->set_requestid(resp.requestid());
        callResult->set_instanceid(scheduleReq->instance().parentid());
        callResult->set_code(Status::GetPosixErrorCode(resp.code()));
        callResult->set_message(resp.message());

        // schedule failed
        if (stateMachineRef->GetInstanceState() != InstanceState::SCHEDULING) {
            SendCallResult(scheduleReq->instance().instanceid(), scheduleReq->instance().parentid(),
                           scheduleReq->instance().parentfunctionproxyaid(), callResult);
            YRLOG_INFO("{}|{}|forward schedule doesn't succeed, notify creator the failure.", scheduleReq->traceid(),
                       scheduleReq->requestid(), scheduleReq->instance().instanceid());
            return resp;
        }

        (void)instanceControlView_->SetOwner(scheduleReq->instance().instanceid());
        auto transContext = TransContext{ InstanceState::SCHEDULE_FAILED, stateMachineRef->GetVersion(), resp.message(),
                                          true, resp.code() };
        transContext.scheduleReq = scheduleReq;
        TransInstanceState(stateMachineRef, transContext)
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SendCallResult, scheduleReq->instance().instanceid(),
                                 scheduleReq->instance().parentid(), scheduleReq->instance().parentfunctionproxyaid(),
                                 callResult));
        YRLOG_INFO(
            "{}|{}|forward schedule doesn't succeed, set instance({}) FAILED and notify creator the "
            "failure.",
            scheduleReq->traceid(), scheduleReq->requestid(), scheduleReq->instance().instanceid());
    }
    return resp;
}

Status InstanceCtrlActor::CheckSchedRequestValid(const std::shared_ptr<ScheduleRequest> &scheduleReq)
{
    const auto &traceID = scheduleReq->traceid();
    const auto &requestID = scheduleReq->requestid();
    if (funcMetaMap_.find(scheduleReq->instance().function()) == funcMetaMap_.end()) {
        YRLOG_ERROR("{}|{}|failed to find function meta.", traceID, requestID);
        return Status(StatusCode::ERR_FUNCTION_META_NOT_FOUND, "failed to find function meta");
    }
    if (IsHeterogeneousRequest(scheduleReq)) {
        return CheckHeteroResourceValid(scheduleReq);
    }
    if (resource_view::HasDiskResource(scheduleReq->instance()) && !CheckDiskResourceValid(scheduleReq)) {
        YRLOG_ERROR("{}|{}|Disk resource must be greater than 0 in schedule request", traceID, requestID);
        std::string errorMessage = "Invalid disk resource: positive value required";
        return Status(StatusCode::ERR_RESOURCE_CONFIG_ERROR, errorMessage);
    }
    auto metaResources = funcMetaMap_[scheduleReq->instance().function()].resources.resources();
    if (metaResources.find(CPU_RESOURCE_NAME) == metaResources.end() ||
        metaResources.find(MEMORY_RESOURCE_NAME) == metaResources.end()) {
        YRLOG_ERROR("{}|{}|resources in function meta is invalid.", traceID, requestID);
        return Status(StatusCode::ERR_FUNCTION_META_NOT_FOUND, "invalid function meta");
    }

    auto resourcesMap = scheduleReq->instance().resources().resources();
    if (resourcesMap.find(CPU_RESOURCE_NAME) == resourcesMap.end()) {
        scheduleReq->mutable_instance()->mutable_resources()->mutable_resources()->operator[](CPU_RESOURCE_NAME) =
            metaResources.at(CPU_RESOURCE_NAME);
    }
    if (resourcesMap.find(MEMORY_RESOURCE_NAME) == resourcesMap.end()) {
        scheduleReq->mutable_instance()->mutable_resources()->mutable_resources()->operator[](MEMORY_RESOURCE_NAME) =
            metaResources.at(MEMORY_RESOURCE_NAME);
    }

    // Check the CPU and memory ranges.
    if (auto it(resourcesMap.find(CPU_RESOURCE_NAME)); it != resourcesMap.end()) {
        auto cpuValue = it->second.scalar().value();
        if (cpuValue < config_.limitResource.minCpu || cpuValue > config_.limitResource.maxCpu) {
            YRLOG_ERROR("{}|{}|cpu resource ({}) millicores is not in valid range", traceID, requestID, cpuValue);
            std::string errorMessage = "Required CPU resource size " + std::to_string(static_cast<int64_t>(cpuValue)) +
                                       " millicores is invalid. Valid value range is [" +
                                       std::to_string(config_.limitResource.minCpu) + "," +
                                       std::to_string(config_.limitResource.maxCpu) + "] millicores";
            return Status(StatusCode::ERR_RESOURCE_CONFIG_ERROR, errorMessage);
        }
    }

    if (auto it(resourcesMap.find(MEMORY_RESOURCE_NAME)); it != resourcesMap.end()) {
        auto memValue = it->second.scalar().value();
        if (memValue < config_.limitResource.minMemory || memValue > config_.limitResource.maxMemory) {
            YRLOG_ERROR("{}|{}|memory resource ({}) MB is not in valid range", traceID, requestID, memValue);
            std::string errorMessage =
                "Required memory resource size " + std::to_string(static_cast<int64_t>(memValue)) +
                " MB is invalid. Valid value range is [" + std::to_string(config_.limitResource.minMemory) + "," +
                std::to_string(config_.limitResource.maxMemory) + "] MB";
            return Status(StatusCode::ERR_RESOURCE_CONFIG_ERROR, errorMessage);
        }
    }
    if (scheduleReq->instance().lowreliability()) {
        if (GetRuntimeRecoverTimes(scheduleReq->instance()) > 0) {
            return Status(StatusCode::ERR_PARAM_INVALID, "low reliability instance does not support recovery");
        }
        if (scheduleReq->instance().createoptions().find("DELEGATE_POD_LABELS")
            != scheduleReq->instance().createoptions().end()) {
            return Status(StatusCode::ERR_PARAM_INVALID, "low reliability instance does not support pod labels");
        }
    }
    return Status::OK();
}

bool InstanceCtrlActor::CheckDiskResourceValid(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    auto diskReq = scheduleReq->instance().resources().resources().at(resource_view::DISK_RESOURCE_NAME);
    return !resource_view::ScalaValueIsEmpty(diskReq) && resource_view::ScalaValueValidate(diskReq);
}

Status InstanceCtrlActor::CheckHeteroResourceValid(const std::shared_ptr<ScheduleRequest> &scheduleReq)
{
    const auto &traceID = scheduleReq->traceid();
    const auto &requestID = scheduleReq->requestid();
    std::unordered_set<std::string> requiredResources = { HETEROGENEOUS_MEM_KEY, HETEROGENEOUS_LATENCY_KEY,
                                                         HETEROGENEOUS_STREAM_KEY };
    bool countExists = false;
    auto resourcesMap = scheduleReq->instance().resources().resources();
    for (auto &res : resourcesMap) {
        auto resourceNameFields = litebus::strings::Split(res.first, "/");
        // heterogeneous resource name is like: NPU/310/count
        if (resourceNameFields.size() != HETERO_RESOURCE_FIELD_NUM) {
            // Skip if the resource name format is incorrect
            continue;
        }
        auto cardType = resourceNameFields[VENDOR_IDX] + "/" + resourceNameFields[PRODUCT_INDEX];
        if (!IsHeteroProductRegexValid(cardType)) {
            YRLOG_ERROR("{}|{}|Heterogeneous product regex syntax error: {}.", traceID, requestID, cardType);
            std::string errorMessage = "Heterogeneous product regex syntax error: " + cardType;
            return Status(StatusCode::ERR_RESOURCE_CONFIG_ERROR, errorMessage);
        }
        if (res.second.scalar().value() <= 0) {
            YRLOG_ERROR("{}|{}|Heterogeneous resource ({}) must be greater than 0 in schedule request",
                        traceID, requestID, res.first);
            std::string errorMessage = "Heterogeneous resources " + res.first +
                                       " being 0 is invalid in schedule request, non-zero required.";
            return Status(StatusCode::ERR_RESOURCE_CONFIG_ERROR, errorMessage);
        }
        auto resourceType = resourceNameFields[RESOURCE_IDX];
        if (resourceType == HETEROGENEOUS_CARDNUM_KEY) {
            countExists = true;
        } else {
            requiredResources.erase(resourceType);
        }
    }
    // If cardnum_key(count) is specified, there should be no other heterogeneous resources.
    if (countExists && requiredResources.size() != HETEROGENEOUS_RESOURCE_REQUIRED_COUNT) {
        std::string errorMessage = "Heterogeneous resources count being non-zero and other heterogeneous "
                                   "resources exist is invalid.";
        return Status(StatusCode::ERR_RESOURCE_CONFIG_ERROR, errorMessage);
    }
    // If cardnum_key(count) is not specified, then HBM, latency, and stream must all be present.
    if (!countExists && !requiredResources.empty()) {
        std::string errorMessage = "Heterogeneous resources (exclude count) must be 3, but now have " +
                                   std::to_string(requiredResources.size()) + " missing.";
        return Status(StatusCode::ERR_RESOURCE_CONFIG_ERROR, errorMessage);
    }

    return Status(SUCCESS);
}

litebus::Future<Status> InstanceCtrlActor::DispatchSchedule(const std::shared_ptr<ScheduleRequest> &request)
{
    return litebus::Async(GetAID(), &InstanceCtrlActor::DeployInstance, request, 0, litebus::None(), false)
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::ScheduleConfirmed, _1, request));
}

litebus::Future<Status> InstanceCtrlActor::DeployInstance(const std::shared_ptr<ScheduleRequest> &request,
                                                          uint32_t retriedTimes,
                                                          const litebus::Option<TransitionResult> &result,
                                                          bool isRecovering)
{
    auto requestID = request->requestid();
    // todo(lwy_robb): to use traceID
    trace::TraceManager::GetInstance().StartSpanWithRecord(
        { "DeployInstance", requestID, "", request->instance().function(), request->instance().instanceid() });
    if (result.IsSome()) {
        YRLOG_DEBUG("{}|{}|failed to deploy instance({}) because failed to update instance info", request->traceid(),
                    requestID, request->instance().instanceid());
        return Status(result.Get().status.StatusCode(), "failed to update instance info");
    }
    // For creating Pod(createoptions contain SchedulingTarget-Pod), local scheduler doesn't need to deploy instance
    // local schdeduler just needs to reploy notifyresult with schedule result(instanceid@targetnode)
    if (auto iter = request->instance().createoptions().find("SchedulingTarget");
        iter != request->instance().createoptions().end() && iter->second == "Pod") {
        YRLOG_INFO("Find pod schedule in InstanceCtrlActor. response now. pod {} on node {}",
                   request->instance().instanceid(), nodeID_);
        return Status(StatusCode::SUCCESS, "deploy pod instance finish");
    }
    if (retriedTimes > 0 && retriedTimes <= config_.maxInstanceRedeployTimes) {
        YRLOG_WARN("{}|{}|retry to deploy instance({}) {} times", request->traceid(), requestID,
                   request->instance().instanceid(), retriedTimes);
    }
    if (retriedTimes > 0 && IsStaticFunctionInstance(request->instance())) {
        YRLOG_ERROR("{}|{}|static function deploy instance({}) failed", request->traceid(), requestID,
                    request->instance().instanceid());
        return Status(StatusCode::LS_DEPLOY_INSTANCE_FAILED, "static function instance deployment failed");
    }
    if (retriedTimes > config_.maxInstanceRedeployTimes) {
        YRLOG_ERROR("{}|{}|retry to deploy instance({}) exceed limit {} times", request->traceid(), requestID,
                    request->instance().instanceid(), retriedTimes);
        return Status(StatusCode::LS_DEPLOY_INSTANCE_FAILED,
                      "instance deployment failed because the number of retries exceeded");
    }
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(request->instance().instanceid());
    if (stateMachine == nullptr) {
        YRLOG_ERROR("{}|{}|failed to deploy instance({}), state machine not found.", request->traceid(), requestID,
                    request->instance().instanceid());
        return Status(StatusCode::ERR_INSTANCE_EXITED, "instance exited");
    }
    if (funcMetaMap_.find(request->instance().function()) == funcMetaMap_.end()) {
        YRLOG_ERROR("{}|{}|failed to deploy instance({}), function meta not found.", request->traceid(), requestID,
                    request->instance().instanceid());
        return Status(StatusCode::ERR_FUNCTION_META_NOT_FOUND, "function meta not found");
    }
    auto deployInstanceRequest = GetDeployInstanceReq(funcMetaMap_.at(request->instance().function()), request);
    auto funcAgentID = request->instance().functionagentid();

    AddDsAuthToDeployInstanceReq(request, deployInstanceRequest);

    ASSERT_IF_NULL(functionAgentMgr_);
    return AddCredToDeployInstanceReq(request->instance().tenantid(), deployInstanceRequest)
        .Then([functionAgentMgr(functionAgentMgr_), funcAgentID, stateMachine, request,
               deployInstanceRequest](const Status &status) -> litebus::Future<messages::DeployInstanceResponse> {
            if (status.IsError()) {
                return GenDeployInstanceResponse(status.StatusCode(), "require token failed",
                                                 deployInstanceRequest->requestid());
            }
            if (auto cancel = stateMachine->GetCancelFuture(); cancel.IsOK()) {
                YRLOG_WARN("{}|{}|instance({}) canceled before deploy instance, reason({})", request->traceid(),
                           request->requestid(), request->instance().instanceid(), cancel.Get());
                std::string msg = "instance canceled before deploy instance, reason: " + cancel.Get();
                return GenDeployInstanceResponse(StatusCode::ERR_SCHEDULE_CANCELED, msg,
                                                 deployInstanceRequest->requestid());
            }
            if (IsStaticFunctionInstance(request->instance())) {  // only static function need to notify
                stateMachine->AddStateChangeCallback(
                    { InstanceState::RUNNING, InstanceState::FAILED, InstanceState::EXITING, InstanceState::FATAL,
                      InstanceState::SUB_HEALTH },
                    [functionAgentMgr(functionAgentMgr), funcAgentID(funcAgentID)](const InstanceInfo &info) {
                        auto request = GetStaticFunctionChangeRequest(info, info.instancestatus().code());
                        YRLOG_INFO("{} try to notify static instance {} status({})", info.requestid(),
                                   info.instanceid(), request->status());
                        functionAgentMgr->NotifyFunctionStatusChange(request, funcAgentID);
                    },
                    "StaticFunctionStatusChanged");
            }

            YRLOG_INFO("{}|{}|start to deploy instance({})", deployInstanceRequest->traceid(),
                       deployInstanceRequest->requestid(), deployInstanceRequest->instanceid());
            return functionAgentMgr->DeployInstance(deployInstanceRequest, funcAgentID);
        })
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::UpdateInstance, _1, request, retriedTimes, isRecovering));
}

void InstanceCtrlActor::AsyncDeployInstance(const std::shared_ptr<litebus::Promise<Status>> &promise,
                                            const std::shared_ptr<messages::ScheduleRequest> &request,
                                            uint32_t retriedTimes, bool isRecovering)
{
    promise->Associate(DeployInstance(request, retriedTimes, litebus::None(), isRecovering));
}

litebus::Future<Status> InstanceCtrlActor::UpdateInstance(const DeployInstanceResponse &response,
                                                          const std::shared_ptr<ScheduleRequest> &request,
                                                          uint32_t retriedTimes, bool isRecovering)
{
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(request->instance().instanceid());
    if (stateMachine == nullptr) {
        YRLOG_ERROR("{}|failed to update instance, failed to find state machine of instance({})", request->requestid(),
                    request->instance().instanceid());
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "failed to update instance");
    }
    if (response.code() != 0) {
        YRLOG_ERROR("{}|{}|failed to deploy instance({}), code: {}, message: {}", request->traceid(),
                    request->requestid(), request->instance().instanceid(), response.code(), response.message());
        std::string message = response.message().empty() ? "failed to deploy instance" : response.message();
        StatusCode errCode = static_cast<StatusCode>(response.code());
        auto instanceInfo = stateMachine->GetInstanceInfo();
        auto status = IsRuntimeRecoverEnable(instanceInfo, stateMachine->GetCancelFuture()) ? InstanceState::FAILED
                                                                                            : InstanceState::FATAL;
        // monopoly need to send kill to avoid pod reused
        if (instanceInfo.scheduleoption().schedpolicyname() == MONOPOLY_SCHEDULE) {
            KillRuntime(instanceInfo, false);
        }
        // do not need to update by scheduleReq, because stateMachine was updated while transiting to creating
        return TransInstanceState(stateMachine, TransContext{ status, stateMachine->GetVersion(), message, true,
                                                              Status::GetPosixErrorCode(response.code()) })
            .Then([errCode, message]() { return Status(errCode, message); });
    }
    YRLOG_DEBUG("{}|{}|success to deploy instance({}) with runtimeID({}), runtimeAddress({}), startTime({}), pid({}), "
                "containerID({})",
                request->traceid(), request->requestid(), request->instance().instanceid(), response.runtimeid(),
                response.address(), response.timeinfo(), response.pid(), response.containerid());
    request->mutable_instance()->set_runtimeid(response.runtimeid());
    request->mutable_instance()->set_starttime(response.timeinfo());
    request->mutable_instance()->set_runtimeaddress(response.address());
    (*request->mutable_instance()->mutable_extensions())[PID] = std::to_string(response.pid());
    request->mutable_instance()->set_containerid(response.containerid());
    if (!response.portmappings().empty()) {
        (*request->mutable_instance()->mutable_extensions())[PORT_FORWARD_KEY] = response.portmappings();
    }
    // Set proxy gRPC address
    request->mutable_instance()->set_proxygrpcaddress(config_.proxyGrpcAddress);
    // Port mappings are stored in extensions["portForward"] as JSON string
    // This will be parsed in RegisterTraefikRoute when registering to Traefik
    SetBillingMetrics(request, response);

    // when instance is an app driver, no connection built from proxy to app driver
    if (IsAppDriver(request->instance().createoptions())) {
        return OnAppDriverDeployed(request);
    }
    litebus::Promise<Status> instanceStatusPromise;
    instanceStatusPromises_[request->instance().instanceid()] = instanceStatusPromise;
    // todo(lwy_robb): to use traceID
    trace::TraceManager::GetInstance().StartSpanWithRecord({ "WaitConnection", request->requestid(), "",
        request->instance().function(), request->instance().instanceid() });
    return CreateInstanceClient(request->instance().instanceid(), response.runtimeid(), response.address())
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::CheckReadiness, _1, request, retriedTimes, isRecovering))
        .Then([aid(GetAID()), request, isRecovering](const Status &status) -> litebus::Future<Status> {
            if (status.IsError()) {
                YRLOG_ERROR("{}|{}|failed to create instance({}), recycle it. error: {}", request->traceid(),
                            request->requestid(), request->instance().instanceid(), status.GetMessage());
                (void)litebus::Async(aid, &InstanceCtrlActor::KillRuntime, request->instance(), isRecovering);
            }
            return status;
        });
}

litebus::Future<Status> InstanceCtrlActor::OnAppDriverDeployed(
    const std::shared_ptr<messages::ScheduleRequest> &request)
{
    (void)concernedInstance_.insert(request->instance().instanceid());
    auto callResult = std::make_shared<functionsystem::CallResult>();
    callResult->set_code(common::ErrorCode::ERR_NONE);
    callResult->set_instanceid(request->instance().instanceid());
    callResult->set_requestid(request->requestid());

    auto callback = RegisterCreateCallResultCallback(request);
    return callback(callResult)
        .Then([=](const CallResultAck ack) {
            return Status(static_cast<StatusCode>(ack.code()), ack.message());
        });
}

void InstanceCtrlActor::SetBillingMetrics(const std::shared_ptr<ScheduleRequest> &request,
                                          const DeployInstanceResponse &response)
{
    YRLOG_INFO("set billing cpu type: {} of function: {}", response.cputype(), request->instance().function());
    metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetBillingCpuType(request->instance().instanceid(),
                                                                                 response.cputype());
    std::vector<std::string> labels;
    for (auto &l : request->instance().labels()) {
        labels.push_back(l);
    }
    metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetBillingPoolLabels(request->instance().instanceid(),
                                                                                    labels);
    // add extension to metrics context
    std::map<std::string, std::string> schedulingExtensions;
    for (auto it = request->instance().scheduleoption().extension().begin();
         it != request->instance().scheduleoption().extension().end(); ++it) {
        schedulingExtensions.insert(*it);
    }
    metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetBillingSchedulingExtensions(
        schedulingExtensions, request->instance().instanceid());
}

litebus::Future<Status> InstanceCtrlActor::HandleCheckReadinessFailure(const std::shared_ptr<ScheduleRequest> &request,
                                                                       uint32_t retriedTimes, const std::string &errMsg,
                                                                       bool isRecovering)
{
    if (retriedTimes < config_.maxInstanceRedeployTimes) {
        return SendKillRequestToAgent(request->instance(), isRecovering, true)
            .Then([aid(GetAID()), request, retriedTimes, minDeployInterval(config_.minDeployIntervalMs), isRecovering,
                   maxDeployInterval(config_.maxDeployIntervalMs)](const messages::KillInstanceResponse &rsp) {
                YRLOG_INFO("{}|{}|killed not ready runtime({}) of instance({}). rsp code ({}){}. try to redeploy",
                           request->traceid(), request->requestid(), request->instance().runtimeid(),
                           request->instance().instanceid(), rsp.code(), rsp.message());
                auto promise = std::make_shared<litebus::Promise<Status>>();
                (void)litebus::AsyncAfter(GenerateRandomNumber<uint64_t>(minDeployInterval, maxDeployInterval), aid,
                                          &InstanceCtrlActor::AsyncDeployInstance, promise, request, retriedTimes + 1,
                                          isRecovering);
                return promise->GetFuture();
            });
    }
    if (instanceStatusPromises_.find(request->instance().instanceid()) == instanceStatusPromises_.end()) {
        YRLOG_ERROR("failed to handle readiness failure because failed to find corresponding instance({})'s promise.",
                    request->instance().instanceid());
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "failed to check readiness.");
    }
    litebus::Promise<Status> instanceStatusPromise = instanceStatusPromises_[request->instance().instanceid()];
    return instanceStatusPromises_[request->instance().instanceid()]
        .GetFuture()
        .After(config_.waitStatusCodeUpdateMs,
               [instanceStatusPromise, errMsg](const litebus::Future<Status> &future) {
                   instanceStatusPromise.SetValue(
                       Status(StatusCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS,
                              "unable to init runtime, because " + errMsg + " and not received exit info of runtime"));
                   return instanceStatusPromise.GetFuture();
               })
        .OnComplete([aid(GetAID()), request, isRecovering](const litebus::Future<Status> &future) {
            (void)litebus::Async(aid, &InstanceCtrlActor::KillRuntime, request->instance(), isRecovering);
            return future;
        })
        .OnComplete(litebus::Defer(GetAID(), &InstanceCtrlActor::DeleteInstanceStatusPromise, _1,
                                   request->instance().instanceid()));
}

litebus::Future<Status> InstanceCtrlActor::CheckReadiness(
    const std::shared_ptr<ControlInterfacePosixClient> &instanceClient, const std::shared_ptr<ScheduleRequest> &request,
    uint32_t retriedTimes, bool isRecovering)
{
    // todo(lwy_robb): to use traceID
    trace::TraceManager::GetInstance().StopSpan("WaitConnection", request->requestid());
    auto stateMachine = instanceControlView_->GetInstance(request->instance().instanceid());
    if (stateMachine == nullptr) {
        YRLOG_ERROR("{}|{}|instance({}) stateMachine is nullptr", request->traceid(), request->requestid(),
                    request->instance().instanceid());
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "failed to update instance");
    }
    if (auto cancel = stateMachine->GetCancelFuture(); cancel.IsOK()) {
        YRLOG_WARN("{}|{}|instance({}) canceled before readiness, reason({})", request->traceid(),
                   request->requestid(), request->instance().instanceid(), cancel.Get());
        std::string msg = "instance canceled before readiness, reason: " + cancel.Get();
        return Status(StatusCode::ERR_SCHEDULE_CANCELED, msg);
    }
    if (instanceClient == nullptr) {
        YRLOG_ERROR("{}|{}|failed to create client for instance({})", request->traceid(), request->requestid(),
                    request->instance().instanceid());
        return litebus::Async(GetAID(), &InstanceCtrlActor::HandleCheckReadinessFailure, request, retriedTimes,
                              "connect runtime failed", isRecovering);
    }
    auto promise = std::make_shared<litebus::Promise<Status>>();
    YRLOG_INFO("{}|{}|check instance({}) readiness.", request->traceid(), request->requestid(),
               request->instance().instanceid());
    litebus::Promise<Status> instanceStatusPromise;
    instanceStatusPromises_[request->instance().instanceid()] = instanceStatusPromise;

    (void)instanceClient->Readiness().OnComplete([request, aid(GetAID()), instanceClient, retriedTimes, promise,
                                                  isRecovering](const litebus::Future<Status> &status) {
        if (status.IsError()) {
            YRLOG_WARN("{}|{}|readiness future is error, kill instance({}).", request->traceid(), request->requestid(),
                       request->instance().instanceid());
            auto future = litebus::Async(aid, &InstanceCtrlActor::HandleCheckReadinessFailure, request, retriedTimes,
                                         "check readiness failed", isRecovering);
            promise->Associate(future);
            return;
        }
        YRLOG_INFO("{}|{}|readiness is valid, init instance({}) runtime.", request->traceid(), request->requestid(),
                   request->instance().instanceid());
        auto future = litebus::Async(aid, &InstanceCtrlActor::SendInitRuntime, instanceClient, request);
        promise->Associate(future);
    });
    return promise->GetFuture();
}

litebus::Future<Status> InstanceCtrlActor::HandleCallResultTimeout(
    const std::shared_ptr<messages::ScheduleRequest> &request)
{
    const std::string &traceID = request->traceid();
    const std::string &requestID = request->requestid();
    const std::string &instanceID = request->instance().instanceid();
    YRLOG_DEBUG("{}|{}|begin to handler call result timeout, instance({})", traceID, requestID, instanceID);

    if (!CheckIsCreateRequestExist(instanceID)) {
        YRLOG_ERROR("{}|{}|call result request is not existed, instance({})", traceID, requestID, instanceID);
        return Status::OK();
    }
    auto callResultPromise = syncCreateCallResultPromises_[instanceID];
    if (callResultPromise->GetFuture().IsOK()) {
        YRLOG_INFO("{}|{}|call result request had been received, instance({})", traceID, requestID, instanceID);
        return Status::OK();
    }
    YRLOG_ERROR("{}|{}|failed to receive call result, reason(timeout), instance({})", traceID, requestID, instanceID);

    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_ERROR("{}|{}|instance's({}) stateMachine is nullptr", traceID, requestID, instanceID);
        return Status(StatusCode::FAILED);
    }

    auto callResult = std::make_shared<functionsystem::CallResult>();
    callResult->set_code(common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
    callResult->set_message("failed to receive call result, reason(timeout)");
    auto transContext = TransContext{ InstanceState::FATAL, stateMachine->GetVersion(), callResult->message(), true,
                                      StatusCode::ERR_USER_FUNCTION_EXCEPTION };
    transContext.scheduleReq = request;
    (void)TransInstanceState(stateMachine, transContext);
    callResultPromise->SetValue(callResult);
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::SendRecoverReq(const std::shared_ptr<InstanceStateMachine> &stateMachine,
                                                          const std::shared_ptr<ScheduleRequest> &request)
{
    auto instanceInfo = stateMachine->GetInstanceInfo();
    return Recover(instanceInfo)
        .Then([instanceID(request->instance().instanceid()), stateMachine, aid(GetAID()),
               request](const litebus::Future<Status> &future) -> litebus::Future<Status> {
            if (future.Get().StatusCode() != StatusCode::SUCCESS) {
                YRLOG_INFO("instance({}) recover failed", instanceID);
                return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "failed to recover instance");
            }
            YRLOG_INFO("instance({}) recover successfully", instanceID);
            auto transContext = TransContext{ InstanceState::RUNNING, stateMachine->GetVersion(), "running" };
            transContext.scheduleReq = request;
            return litebus::Async(aid, &InstanceCtrlActor::TransInstanceState, stateMachine, transContext)
                .Then([](const TransitionResult &result) -> litebus::Future<Status> {
                    if (result.preState.IsNone()) {
                        return Status(
                            StatusCode::ERR_ETCD_OPERATION_ERROR,
                            "failed to update instance info for meta store, err: " + result.status.GetMessage());
                    }
                    return Status::OK();
                });
        });
}

litebus::Future<Status> InstanceCtrlActor::SendCheckpointReq(const std::shared_ptr<ScheduleRequest> &request)
{
    auto instanceInfo = request->instance();
    if (!IsRuntimeRecoverEnable(instanceInfo)) {
        return Status::OK();
    }
    return Checkpoint(instanceInfo.instanceid())
        .Then([request, instanceInfo](const litebus::Future<Status> &status) {
            if (status.IsError()) {
                YRLOG_ERROR("{}|instance({}) checkpoint failed", instanceInfo.requestid(), instanceInfo.instanceid());
                return status;
            }
            request->mutable_instance()->set_ischeckpointed(true);
            return status;
        });
}

bool IsDebugInstance(const std::shared_ptr<ScheduleRequest> &request)
{
    const auto &createOptions = request->instance().createoptions();
    // debug config key not found
    if (createOptions.find(std::string(DEBUG_CONFIG_KEY)) == createOptions.end()) {
        return false;
    }
    return true;
}

litebus::Future<Status> InstanceCtrlActor::SendInitRuntime(
    const std::shared_ptr<ControlInterfacePosixClient> &instanceClient, const std::shared_ptr<ScheduleRequest> &request)
{
    YRLOG_INFO("{}|{}|begin init call of instance({})", request->traceid(), request->requestid(),
               request->instance().instanceid());
    litebus::Promise<Status> instanceStatusPromise;
    instanceStatusPromises_[request->instance().instanceid()] = instanceStatusPromise;
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(request->instance().instanceid());
    if (stateMachine == nullptr) {
        return Status(StatusCode::LS_INIT_RUNTIME_FAILED, "failed to get stateMachine from instanceControlView");
    }
    if (auto cancel = stateMachine->GetCancelFuture(); cancel.IsOK()) {
        YRLOG_WARN("{}|{}|instance({}) canceled before send init call, reason({})", request->traceid(),
                   request->requestid(), request->instance().instanceid(), cancel.Get());
        std::string msg = "instance canceled before send init call, reason: " + cancel.Get();
        return Status(StatusCode::ERR_SCHEDULE_CANCELED, msg);
    }
    // not a debug instance, should maintain heartbeat
    if (!IsDebugInstance(request)) {
        // Send heartbeat to runtime after the connection between LocalScheduler and Runtime is established.
        YRLOG_INFO("{}|{}|begin heartbeat of instance({})", request->traceid(), request->requestid(),
                   request->instance().instanceid());
        (void)litebus::Async(GetAID(), &InstanceCtrlActor::StartHeartbeat, request->instance().instanceid(), 0,
                             request->instance().runtimeid(), StatusCode::SUCCESS);
    }

    (void)concernedInstance_.insert(request->instance().instanceid());
    if (request->instance().ischeckpointed()) {
        return SendRecoverReq(stateMachine, request);
    }
    // Init runtime
    auto callRequest = std::make_shared<runtime_service::CallRequest>();
    if (request->initrequest().empty()) {
        callRequest->set_requestid(request->requestid());
        callRequest->set_traceid(request->traceid());
        callRequest->set_function(request->instance().function());
        callRequest->set_iscreate(true);
        callRequest->set_senderid(request->instance().parentid());
        callRequest->mutable_args()->CopyFrom(request->instance().args());
        *(callRequest->mutable_createoptions()) = *(request->mutable_instance()->mutable_createoptions());
    } else if (!callRequest->ParseFromString(request->initrequest())) {
        YRLOG_ERROR("{}|{}|failed to parse CallRequest.", request->traceid(), request->requestid());
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "failed to parse CallRequest");
    }
    return SendInitCall(instanceClient, request, stateMachine, callRequest);
}

litebus::Future<Status> InstanceCtrlActor::SendInitCall(
    const std::shared_ptr<ControlInterfacePosixClient> &instanceClient, const std::shared_ptr<ScheduleRequest> &request,
    const std::shared_ptr<InstanceStateMachine> &stateMachine,
    const std::shared_ptr<runtime_service::CallRequest> &callRequest)
{
    if (syncCreateCallResultPromises_.find(request->instance().instanceid()) == syncCreateCallResultPromises_.end()) {
        auto createCallResultPromise =
            std::make_shared<litebus::Promise<std::shared_ptr<functionsystem::CallResult>>>();
        syncCreateCallResultPromises_[request->instance().instanceid()] = createCallResultPromise;
    }
    auto createCallResultPromise = syncCreateCallResultPromises_[request->instance().instanceid()];
    auto promise = std::make_shared<litebus::Promise<runtime::CallResponse>>();

    (void)RegisterCreateCallResultCallback(request);

    YRLOG_INFO("{}|{}|send init call to instance({}) runtime({})", request->traceid(), request->requestid(),
               request->instance().instanceid(), request->instance().runtimeid());
    SetInstanceBillingContext(request->instance());
    (void)instanceClient->InitCall(callRequest, config_.runtimeConfig.runtimeInitCallTimeoutMS)
        .OnComplete([promise](const litebus::Future<runtime::CallResponse> &callFuture) {
            if (callFuture.IsError()) {
                runtime::CallResponse callRsp;
                callRsp.set_code(static_cast<common::ErrorCode>(callFuture.GetErrorCode()));
                callRsp.set_message("failed to send init call");
                promise->SetValue(callRsp);
                return;
            }
            promise->SetValue(callFuture.Get());
        });
    return promise->GetFuture().Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SyncCreateResult, _1,
                                                    createCallResultPromise->GetFuture(), request));
}

litebus::Future<Status> InstanceCtrlActor::SyncCreateResult(
    const litebus::Future<runtime::CallResponse> &callFuture,
    const litebus::Future<std::shared_ptr<functionsystem::CallResult>> &resultFuture,
    const std::shared_ptr<ScheduleRequest> &request)
{
    auto callResponse = callFuture.Get();
    if (callResponse.code() != common::ERR_NONE) {
        YRLOG_ERROR("{}|{}|failed to init runtime, code({}), message({})", request->traceid(), request->requestid(),
                    fmt::underlying(callResponse.code()), callResponse.message());
        return Status(static_cast<StatusCode>(callResponse.code()), callResponse.message());
    }
    auto aid = GetAID();
    uint32_t timeout = (uint32_t)request->instance().scheduleoption().initcalltimeout() * 1000;
    if (timeout == 0 || timeout > MAX_INIT_CALL_TIMEOUT_MS) {
        timeout = config_.runtimeConfig.runtimeInitCallTimeoutMS;
    }
    YRLOG_INFO("{}|wait init call result of instance({}), timeout interval: {}ms", request->requestid(),
               request->instance().instanceid(), timeout);
    return resultFuture
        .After(timeout,
               [aid, request](const litebus::Future<std::shared_ptr<functionsystem::CallResult>> &resultFuture) {
                   (void)litebus::Async(aid, &InstanceCtrlActor::HandleCallResultTimeout, request);
                   return resultFuture;
               })
        .Then([request](const std::shared_ptr<functionsystem::CallResult> &callResult) {
            if (callResult->code() != 0) {
                YRLOG_ERROR("{}|{}|failed to init runtime call result with code: {}, message: {}", request->traceid(),
                            request->requestid(), fmt::underlying(callResult->code()), callResult->message());
                return Status(static_cast<StatusCode>(callResult->code()), callResult->message());
            }
            return Status(StatusCode::SUCCESS, "succeed to init runtime");
        });
}

litebus::Future<CallResultAck> InstanceCtrlActor::CallResult(
    const std::string &from, const std::shared_ptr<functionsystem::CallResult> &callResult)
{
    const std::string requestID = callResult->requestid();
    CallResultAck ack;
    auto instanceID(from);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine != nullptr && stateMachine->GetInstanceState() == InstanceState::RUNNING) {
        YRLOG_WARN("{}|instance ({}) is already running, directly pass init call result to caller", requestID,
                   instanceID);
        const auto &instanceInfo = stateMachine->GetInstanceInfo();
        return SendCallResult(instanceID, instanceInfo.parentid(), instanceInfo.parentfunctionproxyaid(), callResult);
    }

    if (stateMachine != nullptr
        && (stateMachine->GetInstanceState() == InstanceState::EXITING
            || stateMachine->GetInstanceState() == InstanceState::EVICTING)) {
        YRLOG_WARN("{}|instance ({}) is going to exiting, ignore callresult, return err", requestID, instanceID);
        ack.set_code(static_cast<common::ErrorCode>(StatusCode::ERR_INSTANCE_EVICTED));
        ack.set_message(
            fmt::format("you are {}, failed to send init callresult",
                        stateMachine->GetInstanceState() == InstanceState::EXITING ? "exiting" : "evicting"));
        return ack;
    }

    if (!CheckIsCreateRequestExist(from)) {
        YRLOG_DEBUG("{}|call result request is not existed.", requestID);
        ack.set_code(static_cast<common::ErrorCode>(StatusCode::LS_REQUEST_NOT_FOUND));
        return ack;
    }
    if (syncCreateCallResultPromises_.find(from) != syncCreateCallResultPromises_.end() &&
        syncCreateCallResultPromises_[from]->GetFuture().IsInit()) {
        syncCreateCallResultPromises_[from]->SetValue(callResult);
    }
    auto callBackIter = createCallResultCallback_.find(from);
    if (callBackIter == createCallResultCallback_.end()) {
        YRLOG_ERROR("{}|{} can not find instance callback, state transition failed", callResult->requestid(), from);
        ack.set_code(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
        return ack;
    }
    YRLOG_DEBUG("{}|{}|receive callResult and start to execute callback", callResult->requestid(), from);
    auto callback = callBackIter->second;
    return callback(callResult)
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::ClearCreateCallResultPromises, std::placeholders::_1, from));
}

litebus::Future<CallResultAck> InstanceCtrlActor::ClearCreateCallResultPromises(
    const litebus::Future<CallResultAck> &future, const std::string &from)
{
    if (future.IsError() || future.Get().code() != 0) {
        YRLOG_ERROR("failed to send NotifyResult for {}, don't clear map, wait for retry", from);
        return future;
    }

    (void)createCallResultCallback_.erase(from);
    (void)syncCreateCallResultPromises_.erase(from);
    return future;
}

litebus::Future<CallResultAck> InstanceCtrlActor::SendCallResult(
    const std::string &srcInstance, const std::string &dstInstance, const std::string &dstProxyID,
    const std::shared_ptr<functionsystem::CallResult> &callResult)
{
    auto requestID(callResult->requestid());
    if (dstInstance.empty()) {
        YRLOG_INFO("{}|instance({}) was created by function master. no need to notify.", callResult->requestid(),
                   srcInstance);
        return CallResultAck();
    }
    if (dstProxyID == GetAID()) {
        if (auto iter = instanceRegisteredReadyCallback_.find(callResult->requestid());
            iter != instanceRegisteredReadyCallback_.end() && iter->second != nullptr) {
            YRLOG_INFO("{}| the instance was concerned by group ctrl. callback is performed. code:{} msg:{}",
                       callResult->requestid(), fmt::underlying(callResult->code()), callResult->message());
            iter->second(Status(static_cast<StatusCode>(callResult->code()), callResult->message()));
            return CallResultAck();
        }
        ASSERT_IF_NULL(clientManager_);
        // todo(lwy_robb): to use traceID
        trace::TraceManager::GetInstance().StopSpan("Create", requestID, {{"instance.id", srcInstance}});
        auto clientFuture = clientManager_->GetControlInterfacePosixClient(dstInstance);
        return clientFuture.Then(
            litebus::Defer(GetAID(), &InstanceCtrlActor::SendNotifyResult, _1, dstInstance, requestID, callResult));
    }
    // forward
    auto forwardCallResultRequest = std::make_shared<internal::ForwardCallResultRequest>();
    forwardCallResultRequest->mutable_req()->CopyFrom(*callResult);
    forwardCallResultRequest->set_instanceid(dstInstance);
    forwardCallResultRequest->set_functionproxyid(dstProxyID);
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(srcInstance);
    if (stateMachine != nullptr) {
        forwardCallResultRequest->mutable_readyinstance()->CopyFrom(stateMachine->GetInstanceInfo());
        (*forwardCallResultRequest->mutable_readyinstance()->mutable_extensions())[INSTANCE_MOD_REVISION] =
            std::to_string(stateMachine->GetModRevision());
    }
    ASSERT_IF_NULL(observer_);
    litebus::AID proxyAID(dstProxyID);
    return SendForwardCallResultRequest(proxyAID, forwardCallResultRequest)
        .Then([](const internal::ForwardCallResultResponse &response) {
            CallResultAck ack;
            ack.set_code(Status::GetPosixErrorCode(response.code()));
            ack.set_message(response.message());
            return ack;
        });
}

litebus::Future<bool> InstanceCtrlActor::WaitClientConnected(const std::string &dstInstance)
{
    auto promise = std::make_shared<litebus::Promise<bool>>();
    (void)litebus::AsyncAfter(RETRY_CHECK_CLIENT_CONNECT_TIME, GetAID(), &InstanceCtrlActor::CheckClientConnected,
                              dstInstance, promise);
    return promise->GetFuture();
}

void InstanceCtrlActor::ClearRateLimiterRegularly()
{
    for (auto it = rateLimiterMap_.begin(); it != rateLimiterMap_.end();) {
        auto cost = GetDuration(it->second->lastRefillTime_);
        if (cost > CLEAR_RATE_LIMITER_INTERVAL_MS) {
            YRLOG_DEBUG("clear tenant({}) TokenBucketLimiter", it->first);
            it = rateLimiterMap_.erase(it);
        } else {
            ++it;
        }
    }
    (void)litebus::AsyncAfter(CLEAR_RATE_LIMITER_INTERVAL_MS, GetAID(), &InstanceCtrlActor::ClearRateLimiterRegularly);
}

void InstanceCtrlActor::CheckClientConnected(const std::string &dstInstance,
                                             const std::shared_ptr<litebus::Promise<bool>> &promise)
{
    auto stateMachine = instanceControlView_->GetInstance(dstInstance);
    if (stateMachine == nullptr) {
        YRLOG_ERROR("failed to find creator {} info", dstInstance);
        promise->SetValue(false);
        return;
    }
    auto owner = stateMachine->GetOwner();
    if (owner != nodeID_) {
        YRLOG_WARN("instance({}) owner is {}", dstInstance, owner);
        promise->SetValue(false);
        return;
    }
    clientManager_->GetControlInterfacePosixClient(dstInstance)
        .Then([dstInstance, promise, aid(GetAID())](const std::shared_ptr<ControlInterfacePosixClient> &client) {
            if (client == nullptr) {
                (void)litebus::AsyncAfter(RETRY_CHECK_CLIENT_CONNECT_TIME, aid,
                                          &InstanceCtrlActor::CheckClientConnected, dstInstance, promise);
                return false;
            }
            promise->SetValue(true);
            return true;
        });
}

litebus::Future<CallResultAck> InstanceCtrlActor::SendNotifyResult(
    const std::shared_ptr<ControlInterfacePosixClient> &instanceClient, const std::string &instanceID,
    const std::string &requestID, const std::shared_ptr<functionsystem::CallResult> &callResult)
{
    CallResultAck ack;
    if (instanceClient == nullptr) {
        bool instanceNotFound = IsReady() && instanceControlView_->GetInstance(instanceID) == nullptr;
        YRLOG_ERROR("{}|failed to notify create result for instance({}), instance not found({})", requestID, instanceID,
                    instanceNotFound);
        ack.set_code(instanceNotFound ? common::ERR_INSTANCE_NOT_FOUND : common::ERR_INNER_COMMUNICATION);
        return ack;
    }
    runtime_service::NotifyRequest notifyRequest;
    notifyRequest.set_requestid(requestID);
    notifyRequest.set_code(Status::GetPosixErrorCode(callResult->code()));
    notifyRequest.set_message(callResult->message());
    notifyRequest.mutable_smallobjects()->Swap(callResult->mutable_smallobjects());
    notifyRequest.mutable_stacktraceinfos()->Swap(callResult->mutable_stacktraceinfos());
    if (callResult->has_runtimeinfo()) {
        notifyRequest.mutable_runtimeinfo()->Swap(callResult->mutable_runtimeinfo());
    }
    auto promise = std::make_shared<litebus::Promise<CallResultAck>>();
    YRLOG_INFO("{}|ready to notify create result to instance({})", requestID, instanceID);
    (void)instanceClient->NotifyResult(std::move(notifyRequest))
        .OnComplete([promise, instanceID, requestID](const litebus::Future<runtime_service::NotifyResponse> &future) {
            CallResultAck ack;
            if (future.IsError()) {
                YRLOG_ERROR("{}|failed to notify result to instance({})", requestID, instanceID);
                ack.set_code(common::ERR_INNER_COMMUNICATION);
            } else {
                YRLOG_INFO("{}|succeed to notify create result to instance({})", requestID, instanceID);
                ack.set_code(common::ERR_NONE);
            }
            promise->SetValue(ack);
        });
    return promise->GetFuture();
}

litebus::Future<internal::ForwardCallResultResponse> InstanceCtrlActor::SendForwardCallResultRequest(
    const litebus::AID &proxyAID, const std::shared_ptr<internal::ForwardCallResultRequest> &forwardCallResultRequest)
{
    auto notifyPromise = std::make_shared<ForwardCallResultPromise>();
    const std::string &requestID = forwardCallResultRequest->mutable_req()->requestid();
    auto emplaceResult = forwardCallResultPromise_.emplace(requestID, notifyPromise);
    if (!emplaceResult.second) {
        YRLOG_INFO("{}|(call result)send forward call result repeatedly", requestID);
        Send(proxyAID, "ForwardCallResultRequest", forwardCallResultRequest->SerializeAsString());
        return forwardCallResultPromise_[requestID]->GetFuture();
    }

    YRLOG_INFO("{}|send forward CallResult request to {}", requestID, std::string(proxyAID));
    Send(proxyAID, "ForwardCallResultRequest", forwardCallResultRequest->SerializeAsString());
    return notifyPromise->GetFuture();
}

litebus::Future<Status> InstanceCtrlActor::SendForwardCallResultResponse(const CallResultAck &ack,
                                                                         const litebus::AID &from,
                                                                         const std::string &requestID,
                                                                         const std::string &instanceID)
{
    internal::ForwardCallResultResponse response;
    response.set_code(ack.code());
    response.set_message(ack.message());
    response.set_requestid(requestID);
    response.set_instanceid(instanceID);
    YRLOG_DEBUG("{}|send forward CallResult response to {}", requestID, from.HashString());
    Send(from, "ForwardCallResultResponse", response.SerializeAsString());

    return Status::OK();
}

void InstanceCtrlActor::ForwardCallResultRequest(const litebus::AID &from, std::string &&, std::string &&msg)
{
    internal::ForwardCallResultRequest forwardCallResultRequest;
    if (msg.empty() || !forwardCallResultRequest.ParseFromString(msg)) {
        YRLOG_WARN("(custom signal)invalid request body from {}", from.HashString());
        return;
    }

    auto requestID(forwardCallResultRequest.req().requestid());
    YRLOG_INFO("{}|received CallResult from {}.", requestID, from.HashString());

    std::string srcInstanceID;
    if (forwardCallResultRequest.has_readyinstance() && forwardCallResultRequest.readyinstance().lowreliability()) {
        srcInstanceID = forwardCallResultRequest.readyinstance().instanceid();
        auto stateMachine = instanceControlView_->GetInstance(srcInstanceID);
        if ((stateMachine == nullptr) || (stateMachine != nullptr && stateMachine->GetUpdateByRouteInfo())) {
            YRLOG_INFO("{}|instance {} is unbelievable, need to kill", requestID, srcInstanceID);
            CallResultAck ack;
            ack.set_code(static_cast<common::ErrorCode>(StatusCode::ERR_INSTANCE_EXITED));
            (void)SendForwardCallResultResponse(ack, from, requestID, srcInstanceID);
            return;
        }
    }

    // for update instance ready fast
    if (forwardCallResultRequest.has_readyinstance()) {
        srcInstanceID = forwardCallResultRequest.readyinstance().instanceid();
        auto instanceInfo = forwardCallResultRequest.readyinstance();
        if (instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::RUNNING)) {
            ASSERT_IF_NULL(observer_);
            YRLOG_INFO("{}|received instance({}) is created, to be fast published", requestID, srcInstanceID);
            observer_->FastPutRemoteInstanceEvent(instanceInfo, false, GetModRevisionFromInstanceInfo(instanceInfo));
        }
    }
    auto callResult = std::make_shared<core_service::CallResult>(std::move(*forwardCallResultRequest.mutable_req()));
    SendCallResult(srcInstanceID, forwardCallResultRequest.instanceid(), forwardCallResultRequest.functionproxyid(),
                   callResult)
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SendForwardCallResultResponse, _1, from, requestID,
                             srcInstanceID));
}

void InstanceCtrlActor::ForwardCallResultResponse(const litebus::AID &from, std::string &&, std::string &&msg)
{
    internal::ForwardCallResultResponse response;
    if (msg.empty() || !response.ParseFromString(msg)) {
        YRLOG_WARN("(call result)invalid response body from({}).", from.HashString());
        return;
    }

    auto requestID(response.requestid());
    if (auto iter(forwardCallResultPromise_.find(requestID)); iter == forwardCallResultPromise_.end()) {
        YRLOG_WARN("(call result)no requestID({}) matches result, failed to get response", requestID);
        return;
    }
    forwardCallResultPromise_[requestID]->SetValue(response);
    (void)forwardCallResultPromise_.erase(requestID);

    YRLOG_INFO("{}|(call result)received forward call result response, from: {}", requestID, from.HashString());

    if (response.code() == static_cast<common::ErrorCode>(StatusCode::ERR_INSTANCE_EXITED)) {
        auto instanceID(response.instanceid());
        YRLOG_WARN("{}|instance {} is low reliability and instance info cannot find in {}, need to be killed",
                   requestID, instanceID, from.HashString());
        auto stateMachine = instanceControlView_->GetInstance(instanceID);
        if (stateMachine == nullptr) {
            return;
        }
        auto instanceInfo = stateMachine->GetInstanceInfo();
        exitHandler_(instanceInfo);
    }
}

litebus::Future<Status> InstanceCtrlActor::ScheduleConfirmed(const Status &status,
                                                             const std::shared_ptr<ScheduleRequest> &request)
{
    // todo(lwy_robb): to use traceID
    trace::TraceManager::GetInstance().StopSpan("DeployInstance", request->requestid());
    auto rsp = std::make_shared<ScheduleResponse>();
    rsp->set_code(static_cast<int32_t>(status.StatusCode()));
    rsp->set_requestid(request->requestid());
    rsp->set_instanceid(request->instance().instanceid());
    rsp->set_message(status.GetMessage());
    ASSERT_IF_NULL(scheduler_);
    (void)scheduler_->ScheduleConfirm(rsp, request->instance(), ScheduleResult{});

    return status;
}

litebus::Future<Status> InstanceCtrlActor::HandleFailedInstance(const std::string &instanceID,
                                                                const std::string &runtimeID, const std::string &errMsg)
{
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND, "instance not found");
    }
    if (stateMachine->GetOwner() != nodeID_) {
        YRLOG_WARN("instance({}) owner is {}", instanceID, stateMachine->GetOwner());
        return Status::OK();
    }
    auto instanceInfo = stateMachine->GetInstanceInfo();
    if (instanceInfo.runtimeid() != runtimeID) {
        YRLOG_WARN("instance({}) runtimeID({}) changed", instanceID, instanceInfo.runtimeid());
        return Status::OK();
    }
    return TryRecover(instanceID, runtimeID, errMsg, stateMachine, instanceInfo);
}

litebus::Future<Status> InstanceCtrlActor::TryRecover(const std::string &instanceID, const std::string &runtimeID,
                                                      const std::string &errMsg,
                                                      std::shared_ptr<InstanceStateMachine> &stateMachine,
                                                      InstanceInfo &instanceInfo)
{
    auto promise = std::make_shared<litebus::Promise<Status>>();
    functionAgentMgr_->QueryInstanceStatusInfo(instanceInfo.functionagentid(), instanceID, runtimeID)
        .OnComplete(litebus::Defer(GetAID(), &InstanceCtrlActor::OnQueryInstanceStatusInfo, std::placeholders::_1,
                                   stateMachine, std::make_pair(errMsg, runtimeID),
                                   IsRuntimeRecoverEnable(instanceInfo, stateMachine->GetCancelFuture()), promise));
    return promise->GetFuture();
}

void InstanceCtrlActor::OnQueryInstanceStatusInfo(const litebus::Future<messages::InstanceStatusInfo> &future,
                                                  const std::shared_ptr<InstanceStateMachine> &stateMachine,
                                                  const std::pair<std::string, std::string> &runtimeErrMsg,
                                                  bool isRuntimeRecoverEnable,
                                                  const std::shared_ptr<litebus::Promise<Status>> promise)
{
    auto instanceInfo = stateMachine->GetInstanceInfo();
    if (instanceInfo.runtimeid() != runtimeErrMsg.second || stateMachine->GetOwner() != nodeID_) {
        YRLOG_WARN("instance({}) runtimeID({}) changed to {}", instanceInfo.instanceid(), runtimeErrMsg.second,
                   instanceInfo.runtimeid());
        promise->SetValue(Status::OK());
        return;
    }

    auto msg = runtimeErrMsg.first;
    auto errCode = common::ERR_INSTANCE_EXITED;
    if (future.IsError()) {
        YRLOG_WARN("query instance({}), runtime({}) abnormal information failed.", instanceInfo.instanceid(),
                   instanceInfo.runtimeid());
        msg = msg + " reason: unknown err because of failed to query instance information";
    } else {
        errCode = common::ERR_USER_FUNCTION_EXCEPTION;
        auto instanceStatusInfo = future.Get();
        msg = runtimeErrMsg.first + " reason: " + instanceStatusInfo.instancemsg();
        if (instanceStatusInfo.type() == static_cast<int32_t>(RETURN)
            || instanceStatusInfo.type() == static_cast<int32_t>(NONE_EXIT)) {
            errCode = common::ERR_INSTANCE_EXITED;
        }
    }
    if (isRuntimeRecoverEnable) {
        if (redeployTimesMap_.find(instanceInfo.instanceid()) == redeployTimesMap_.end()) {
            (void)litebus::Async(GetAID(), &InstanceCtrlActor::TransInstanceState, stateMachine,
                                 TransContext{ InstanceState::FAILED, stateMachine->GetVersion(), msg, true, errCode })
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::RescheduleWithID, instanceInfo.instanceid()));
        } else {
            YRLOG_WARN("the reschedule instance({}) was discarded because it already exists",
                       instanceInfo.instanceid());
        }
    } else {
        (void)litebus::Async(GetAID(), &InstanceCtrlActor::SyncFailedInitResult, instanceInfo.instanceid(), errCode,
                             msg);
        (void)litebus::Async(GetAID(), &InstanceCtrlActor::TransInstanceState, stateMachine,
                             TransContext{ InstanceState::FATAL, stateMachine->GetVersion(), msg, true, errCode })
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::KillRuntime, instanceInfo, false))
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeleteInstanceInResourceView, _1, instanceInfo));
    }
    promise->SetValue(Status::OK());
}

litebus::Future<Status> InstanceCtrlActor::UpdateInstanceStatusPromise(const std::string &instanceID,
                                                                       const std::string &errMsg)
{
    if (instanceStatusPromises_.find(instanceID) != instanceStatusPromises_.end()) {
        YRLOG_DEBUG("update instance({}) status promise. uploaded msg: {}", instanceID, errMsg);
        instanceStatusPromises_[instanceID].SetValue(Status(StatusCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS, errMsg));
    }
    SyncFailedInitResult(instanceID, common::ErrorCode::ERR_USER_FUNCTION_EXCEPTION, errMsg);
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::UpdateInstanceStatus(const std::shared_ptr<InstanceExitStatus> &info)
{
    YRLOG_INFO("update instance({}), errCode({}), exitCode({}), msg({}), errCode({}) with info uploaded by runtime "
        "manager", info->instanceID, info->errCode, info->exitCode, info->statusMsg, info->errCode);

    auto stateMachine = instanceControlView_->GetInstance(info->instanceID);
    if (stateMachine == nullptr) {
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND, "instance not found");
    }
    auto state = stateMachine->GetInstanceState();
    if (state == InstanceState::SCHEDULING || state == InstanceState::FAILED || state == InstanceState::EVICTED
        || state == InstanceState::SCHEDULE_FAILED || state == InstanceState::FATAL) {
        YRLOG_WARN("instance {} with state({}) is not concerned updated status", info->instanceID,
                   fmt::underlying(state));
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "invalid instance state to change");
    }
    if (concernedInstance_.find(info->instanceID) == concernedInstance_.end()) {
        YRLOG_WARN("instance {} status is not concerned", info->instanceID);
        return UpdateInstanceStatusPromise(info->instanceID, info->statusMsg);
    }
    auto instanceInfo = stateMachine->GetInstanceInfo();
    if (stateMachine->GetOwner() != nodeID_) {
        YRLOG_WARN("instance {} is on node({}), not on current node({})", info->instanceID, stateMachine->GetOwner(),
                   nodeID_);
        return UpdateInstanceStatusPromise(info->instanceID, "instance isn't own by this node");
    }
    if (!IsRuntimeRecoverEnable(instanceInfo, stateMachine->GetCancelFuture())) {
        YRLOG_WARN("instance({}) exit, transition it to fatal", info->instanceID);
        return TransInstanceState(stateMachine, TransContext{ InstanceState::FATAL, stateMachine->GetVersion(),
                                                              stateMachine->Information() + info->statusMsg, true,
                                                              info->errCode, info->exitCode, info->exitType })
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::KillRuntime, stateMachine->GetInstanceInfo(), false))
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeleteInstanceInResourceView, std::placeholders::_1,
                                 stateMachine->GetInstanceInfo()))
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::UpdateInstanceStatusPromise, info->instanceID,
                                 info->statusMsg));
    } else if (redeployTimesMap_.find(info->instanceID) == redeployTimesMap_.end()) {
        return TransInstanceState(
                   stateMachine,
                   TransContext{ InstanceState::FAILED, stateMachine->GetVersion(), info->statusMsg, true,
                                 common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS, info->exitCode, info->exitType })
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::UpdateInstanceStatusPromise, info->instanceID,
                                 info->statusMsg))
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::RescheduleWithID, info->instanceID));
    } else {
        YRLOG_WARN("the reschedule instance({}) was discarded because it already exists", info->instanceID);
        return Status::OK();
    }
}

void InstanceCtrlActor::CollectInstanceResources(const InstanceInfo &instance)
{
    if (instance.resources().resources().find(resource_view::CPU_RESOURCE_NAME) !=
        instance.resources().resources().end()) {
        struct functionsystem::metrics::MeterTitle cpuTitle {
            instance.instanceid() + "_cpu_limit", "limit CPU of instance", "m"
        };
        struct functionsystem::metrics::MeterData data {
            instance.resources().resources().at(resource_view::CPU_RESOURCE_NAME).scalar().value(),
            {
            }
        };
        functionsystem::metrics::MetricsAdapter::GetInstance().ReportGauge(cpuTitle, data);
    }

    if (instance.resources().resources().find(resource_view::MEMORY_RESOURCE_NAME) !=
        instance.resources().resources().end()) {
        struct functionsystem::metrics::MeterTitle memTitle {
            instance.instanceid() + "_memory_limit", "limit memory of instance", "Byte"
        };

        struct functionsystem::metrics::MeterData data {
            instance.resources().resources().at(resource_view::MEMORY_RESOURCE_NAME).scalar().value(),
            {
            }
        };
        functionsystem::metrics::MetricsAdapter::GetInstance().ReportGauge(memTitle, data);
    }
}

void InstanceCtrlActor::ScheduleEnd(const litebus::Future<Status> &future,
                                    const std::shared_ptr<ScheduleRequest> &request)
{
    // todo(lwy_robb): to use traceID
    trace::TraceManager::GetInstance().StopSpan("DeployInstance", request->requestid());
    Status status;
    if (future.IsError()) {
        status = Status(static_cast<StatusCode>(future.GetErrorCode()), "failed to create instance");
    } else {
        status = future.Get();
    }
    auto rsp = std::make_shared<ScheduleResponse>();
    rsp->set_code(static_cast<int32_t>(status.StatusCode()));
    rsp->set_requestid(request->requestid());
    rsp->set_instanceid(request->instance().instanceid());
    rsp->set_message(status.GetMessage());
    ASSERT_IF_NULL(scheduler_);
    (void)scheduler_->ScheduleConfirm(rsp, request->instance(), ScheduleResult{});

    auto statusCode = status.StatusCode();
    if (statusCode != StatusCode::SUCCESS && statusCode != StatusCode::INSTANCE_TRANSACTION_WRONG_VERSION) {
        auto instanceID = request->instance().instanceid();
        const std::string &parent = request->instance().parentid();
        const std::string &parentProxy = request->instance().parentfunctionproxyaid();
        YRLOG_ERROR(
            "{}|{}|failed to create instance({}), "
            "statusCode: {}, msg: {}, notify call result to parent instance({}) and kill instance",
            request->traceid(), request->requestid(), request->instance().instanceid(), fmt::underlying(statusCode),
            status.GetMessage(), parent);
        auto callResult = std::make_shared<functionsystem::CallResult>();
        callResult->set_instanceid(parent);
        callResult->set_requestid(request->requestid());
        callResult->set_code(Status::GetPosixErrorCode(statusCode));
        callResult->set_message(fmt::format("{} on {}",
            status.MultipleErr() ? status.GetMessage() : status.RawMessage(), nodeID_));
        auto stateMachine = instanceControlView_->GetInstance(instanceID);
        if (stateMachine == nullptr) {
            (void)SendCallResult(instanceID, parent, parentProxy, callResult);
            return;
        }

        auto instanceInfo = request->instance();
        if (IsRuntimeRecoverEnable(instanceInfo, stateMachine->GetCancelFuture())) {
            (void)litebus::Async(GetAID(), &InstanceCtrlActor::TransInstanceState, stateMachine,
                                 TransContext{ InstanceState::FAILED, stateMachine->GetVersion(), status.GetMessage(),
                                               true, statusCode })
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::RescheduleWithID, instanceInfo.instanceid()));
        } else {
            (void)SendCallResult(instanceID, parent, parentProxy, callResult);
            // need to update stateMachine by scheduleReq, because scheduleReq was already updated
            auto transContext =
                TransContext{ InstanceState::FATAL, stateMachine->GetVersion(),
                              stateMachine->Information() + "fatal: Failed to create caused by " + status.GetMessage(),
                              true, static_cast<int32_t>(statusCode) };
            transContext.scheduleReq = request;
            (void)TransInstanceState(stateMachine, transContext);
        }
    }
}

void InstanceCtrlActor::SendHeartbeat(const std::string &instanceID, uint32_t timeoutTimes,
                                      const std::string &runtimeID, const StatusCode &prevStatus)
{
    if (auto pos(instanceID.find("functionaccessor"));
        (pos != std::string::npos && !fcAccessorHeartbeat_) ||
        (pos == std::string::npos && config_.runtimeConfig.runtimeHeartbeatEnable == "false")) {
        return;
    }

    ASSERT_IF_NULL(clientManager_);
    (void)clientManager_->GetControlInterfacePosixClient(instanceID)
        .Then([instanceID, timeoutTimes, runtimeID, runtimeConf(config_.runtimeConfig), aid(GetAID()),
               prevStatus](const std::shared_ptr<ControlInterfacePosixClient> &client) {
            // future of GetControlInterfacePosixClient would not return error
            if (client == nullptr) {
                YRLOG_WARN("failed to send heartbeat to instance({}), not found posix stream client", instanceID);
                litebus::Async(aid, &InstanceCtrlActor::HandleRuntimeHeartbeatLost, instanceID, runtimeID);
                return Status(StatusCode::FAILED);
            }
            (void)client->Heartbeat(runtimeConf.runtimeHeartbeatTimeoutMS * (timeoutTimes + 1))
                .OnComplete(litebus::Defer(aid, &InstanceCtrlActor::SendHeartbeatCallback, instanceID, timeoutTimes,
                                           prevStatus, runtimeID, std::placeholders::_1));
            return Status::OK();
        });
}

void InstanceCtrlActor::SendHeartbeatCallback(const std::string &instanceID, uint32_t timeoutTimes,
                                              const StatusCode &prevStatus, const std::string &runtimeID,
                                              const litebus::Future<Status> &status)
{
    if (!CheckHeartbeatExist(instanceID)) {
        return;
    }

    auto timeout = timeoutTimes;
    if (status.IsError()) {
        if (status.GetErrorCode() == static_cast<int32_t>(StatusCode::INSTANCE_HEALTH_CHECK_ERROR)) {
            YRLOG_ERROR("heartbeat of instance({}) failed to health check", instanceID);
            litebus::Async(GetAID(), &InstanceCtrlActor::HandleRuntimeHeartbeatLost, instanceID, runtimeID);
            return;
        }

        timeout++;
        YRLOG_ERROR("heartbeat of instance({}) is timeout, timeout times: {}", instanceID, timeout);
        if (timeout >= config_.runtimeConfig.runtimeMaxHeartbeatTimeoutTimes) {
            litebus::Async(GetAID(), &InstanceCtrlActor::HandleRuntimeHeartbeatLost, instanceID, runtimeID);
            return;
        }
        litebus::TimerTools::Cancel(runtimeHeartbeatTimers_[instanceID]);
        runtimeHeartbeatTimers_[instanceID] =
            litebus::AsyncAfter(HEARTBEAT_INTERVAL_MS, GetAID(), &InstanceCtrlActor::SendHeartbeat, instanceID, timeout,
                                runtimeID, prevStatus);
        return;
    }

    if (timeout != 0) {
        timeout = 0;
    }

    if (prevStatus != status.Get().StatusCode()) {
        YRLOG_INFO("instance({}) health status({}) changes to {}", instanceID, fmt::underlying(prevStatus),
                   fmt::underlying(status.Get().StatusCode()));
        litebus::Async(GetAID(), &InstanceCtrlActor::HandleInstanceHealthChange, instanceID, status.Get().StatusCode());
    }
    runtimeHeartbeatTimers_[instanceID] =
        litebus::AsyncAfter(HEARTBEAT_INTERVAL_MS, GetAID(), &InstanceCtrlActor::SendHeartbeat, instanceID, timeout,
                            runtimeID, status.Get().StatusCode());
}

void InstanceCtrlActor::StartHeartbeat(const std::string &instanceID, uint32_t timeoutTimes,
                                       const std::string &runtimeID, const functionsystem::StatusCode &prevStatus)
{
    if (runtimeHeartbeatTimers_.find(instanceID) != runtimeHeartbeatTimers_.end()) {
        (void)litebus::TimerTools::Cancel(runtimeHeartbeatTimers_[instanceID]);
        runtimeHeartbeatTimers_.erase(instanceID);
        YRLOG_WARN("cancel previous heartbeat of instance({})", instanceID);
    }
    litebus::Timer timer;
    runtimeHeartbeatTimers_[instanceID] = timer;
    SendHeartbeat(instanceID, timeoutTimes, runtimeID, prevStatus);
}

bool InstanceCtrlActor::CheckHeartbeatExist(const std::string &instanceID)
{
    if (runtimeHeartbeatTimers_.find(instanceID) == runtimeHeartbeatTimers_.end()) {
        YRLOG_WARN("heartbeat of instance({}) does not exist", instanceID);
        return false;
    }
    return true;
}

void InstanceCtrlActor::StopHeartbeat(const std::string &instanceID)
{
    if (runtimeHeartbeatTimers_.find(instanceID) == runtimeHeartbeatTimers_.end()) {
        YRLOG_WARN("heartbeat of instance({}) doesn't exist", instanceID);
        return;
    }
    YRLOG_WARN("stop heartbeat of instance({}) successfully", instanceID);
    (void)litebus::TimerTools::Cancel(runtimeHeartbeatTimers_[instanceID]);
    runtimeHeartbeatTimers_.erase(instanceID);
}

void InstanceCtrlActor::SyncFailedInitResult(const std::string &instanceID, const common::ErrorCode &errCode,
                                             const std::string &msg)
{
    if (syncCreateCallResultPromises_.find(instanceID) != syncCreateCallResultPromises_.end() &&
        syncCreateCallResultPromises_.at(instanceID)->GetFuture().IsInit()) {
        // If no CallResult message is returned after the heartbeat lost, need to set value to the
        // syncCreateCallResultPromise, otherwise Schedule request will not return.
        YRLOG_ERROR("instance({}) occurs error {} and haven't send CallResult message", instanceID, msg);
        auto callResult = std::make_shared<functionsystem::CallResult>();
        callResult->set_instanceid(instanceID);
        callResult->set_code(errCode);
        callResult->set_message(msg);
        syncCreateCallResultPromises_.at(instanceID)->SetValue(callResult);
    }
}

void InstanceCtrlActor::HandleRuntimeHeartbeatLost(const std::string &instanceID, const std::string &runtimeID)
{
    YRLOG_ERROR("heartbeat of instance({}) is lost, set it to failed.", instanceID);
    ASSERT_IF_NULL(clientManager_);

    (void)clientManager_->GetControlInterfacePosixClient(instanceID)
        .Then([instanceID](const std::shared_ptr<ControlInterfacePosixClient> &client) {
            YRLOG_ERROR("heartbeat of instance({}) is lost, close client.", instanceID);
            if (client == nullptr || client->IsDone()) {
                YRLOG_WARN("failed to close client to instance({}), posix stream client is {}", instanceID,
                           client == nullptr ? "not found." : "done.");
                return Status::OK();
            }
            client->Close();
            return Status::OK();
        });

    if (instanceID.find("functionaccessor") != std::string::npos) {
        return;
    }

    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_ERROR("state machine of instance: {} is null", instanceID);
        return;
    }

    if (!CheckHeartbeatExist(instanceID)) {
        return;
    }

    auto instanceInfo = stateMachine->GetInstanceInfo();
    if (IsDriver(instanceInfo)) {
        YRLOG_WARN("heartbeat of driver {} lost, try to delete client.", instanceID);
        (void)DeleteDriverClient(instanceID, instanceInfo.jobid());
        return;
    }
    auto errMsg = stateMachine->Information() + "heartbeat lost between local scheduler and instance";
    if (instanceStatusPromises_.find(instanceID) == instanceStatusPromises_.end()) {
        (void)litebus::Async(GetAID(), &InstanceCtrlActor::HandleFailedInstance, instanceID, runtimeID, errMsg);
    } else {
        (void)instanceStatusPromises_[instanceID].GetFuture().After(
            config_.waitStatusCodeUpdateMs,
            litebus::Defer(GetAID(), &InstanceCtrlActor::HandleFailedInstance, instanceID, runtimeID, errMsg));
    }
}

void InstanceCtrlActor::HandleInstanceHealthChange(const std::string &instanceID, const StatusCode &code)
{
    if (instanceID.find("functionaccessor") != std::string::npos) {
        return;
    }

    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_ERROR("state machine of instance({}) is null", instanceID);
        return;
    }

    if (!CheckHeartbeatExist(instanceID)) {
        return;
    }

    if (code == StatusCode::SUCCESS) {
        (void)TransInstanceState(stateMachine, TransContext{ InstanceState::RUNNING, stateMachine->GetVersion(),
                                                             "running", true, StatusCode::SUCCESS });
        return;
    }

    if (code != StatusCode::INSTANCE_SUB_HEALTH) {
        return;
    }
    (void)TransInstanceState(stateMachine, TransContext{ InstanceState::SUB_HEALTH, stateMachine->GetVersion(),
                                                         "subHealth", true, StatusCode::ERR_INSTANCE_SUB_HEALTH });
}

litebus::Future<Status> InstanceCtrlActor::DoSync(const litebus::Option<function_proxy::InstanceInfoMap> &instanceInfo,
                                                  const std::string &funcAgentID)
{
    YRLOG_DEBUG("(sync)get instance info of agent({})", funcAgentID);
    if (instanceInfo.IsNone()) {
        YRLOG_INFO("agent({}) don't have any instance.", funcAgentID);
        funcAgentMap_[funcAgentID] = std::make_shared<function_proxy::InstanceInfoMap>();
    } else {
        funcAgentMap_[funcAgentID] = std::make_shared<function_proxy::InstanceInfoMap>(instanceInfo.Get());
    }

    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::CheckSyncInstance(const litebus::Future<Status> &status,
                                                             const std::string &funcAgentID)
{
    (void)funcAgentMap_.erase(funcAgentID);
    (void)syncKillPromises_.erase(funcAgentID);
    (void)syncRecoverPromises_.erase(funcAgentID);
    (void)syncDeployPromises_.erase(funcAgentID);

    litebus::Promise<Status> promiseRet;
    if (status.IsError()) {
        YRLOG_ERROR("failed to sync agent({}), code: {}", funcAgentID, status.GetErrorCode());
        promiseRet.SetFailed(static_cast<int32_t>(StatusCode::LS_SYNC_INSTANCE_FAIL));
        return promiseRet.GetFuture();
    }

    YRLOG_INFO("sync instance successfully.");
    return status.Get();
}

litebus::Future<Status> InstanceCtrlActor::RecoverAgentInstance(const Status &status,
                                                                const std::shared_ptr<ResourceUnit> &resourceUnit)
{
    std::vector<std::string> needRecoverInstances;
    const auto &funcAgentID = resourceUnit->id();
    if (funcAgentMap_.find(funcAgentID) == funcAgentMap_.end()) {
        YRLOG_ERROR("failed to find function agent({}) to recover instance", funcAgentID);
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR);
    }
    auto instanceInfoMapPtr = funcAgentMap_[funcAgentID];
    if (instanceInfoMapPtr == nullptr) {
        YRLOG_ERROR("function agent({}) instance info map is null, failed to find to recover instance", funcAgentID);
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR);
    }
    for (const auto &instance : (*instanceInfoMapPtr)) {
        auto instanceStatus = instance.second.instancestatus().code();
        // agent only running(3)/creating(2)/evicting(9) instance need to recover.
        if (instanceStatus == static_cast<int32_t>(InstanceState::RUNNING) ||
            instanceStatus == static_cast<int32_t>(InstanceState::EVICTING) ||
            instanceStatus == static_cast<int32_t>(InstanceState::SUB_HEALTH)) {
            (void)needRecoverInstances.emplace_back(instance.first);
        }
        if (instanceStatus == static_cast<int32_t>(InstanceState::CREATING) && instance.second.args_size() != 0) {
            YRLOG_WARN("creating instance({}), which needs to be recovered", instance.first);
            (void)needRecoverInstances.emplace_back(instance.first);
        }
    }
    auto promiseRet = std::make_shared<litebus::Promise<Status>>();

    if (needRecoverInstances.empty()) {
        YRLOG_INFO("agent({}) don't need recover any instance.", funcAgentID);
        return status;
    }

    YRLOG_INFO("agent({}) need recover {} instances.", funcAgentID, needRecoverInstances.size());

    auto emplaceResult =
        syncRecoverPromises_.emplace(funcAgentID, std::make_pair(promiseRet, needRecoverInstances.size()));
    if (!emplaceResult.second) {
        YRLOG_INFO("repeat sync request, funcAgentID: {}", funcAgentID);
        return syncRecoverPromises_[funcAgentID].first->GetFuture();
    }

    for (const auto &instanceID : needRecoverInstances) {
        if ((*instanceInfoMapPtr).find(instanceID) == (*instanceInfoMapPtr).end()) {
            YRLOG_ERROR("instance({}) is not in instance map, failed to recover", instanceID);
            return Status(StatusCode::ERR_INNER_SYSTEM_ERROR);
        }
        const auto &instance = (*instanceInfoMapPtr).at(instanceID);
        YRLOG_INFO("begin recover instance({}). agent: {}", instance.instanceid(), funcAgentID);
        (void)litebus::Async(GetAID(), &InstanceCtrlActor::RecoverInstance, instance.instanceid())
            .OnComplete(litebus::Defer(GetAID(), &InstanceCtrlActor::CheckSyncRecoverInstance, _1, funcAgentID,
                                       instance.instanceid(), instance.tenantid()));
    }

    return syncRecoverPromises_[funcAgentID].first->GetFuture();
}

litebus::Future<Status> InstanceCtrlActor::CheckSyncRecoverInstance(const litebus::Future<Status> &future,
                                                                    const std::string &funcAgentID,
                                                                    const std::string &instanceID,
                                                                    const std::string &tenantID)
{
    if (syncRecoverPromises_.find(funcAgentID) == syncRecoverPromises_.end()) {
        YRLOG_ERROR("agent({}) sync failed when recover instance. agent don't exist or process completed", funcAgentID);
        return Status(StatusCode::LS_AGENT_NOT_FOUND, "failed to find agent");
    }
    if (future.IsError()) {
        YRLOG_ERROR("agent({}) sync failed when recover instance({}), code: {}", funcAgentID, instanceID,
                    future.GetErrorCode());
        syncRecoverPromises_[funcAgentID].first->SetFailed(
            static_cast<int32_t>(StatusCode::LS_SYNC_RESCHEDULE_INSTANCE_FAIL));
        return syncRecoverPromises_[funcAgentID].first->GetFuture();
    }
    if (future.Get().IsError()) {
        YRLOG_ERROR("agent({}) sync failed when recover instance({}), message: {}", funcAgentID, instanceID,
                    future.Get().GetMessage());
        syncRecoverPromises_[funcAgentID].first->SetFailed(
            static_cast<int32_t>(StatusCode::LS_SYNC_RESCHEDULE_INSTANCE_FAIL));
        return syncRecoverPromises_[funcAgentID].first->GetFuture();
    }
    YRLOG_INFO("recover instance({}) of agent({}) successfully.", instanceID, funcAgentID);
    if (--syncRecoverPromises_[funcAgentID].second == 0) {
        YRLOG_INFO("all inconsistent instances have been recovered.");
        syncRecoverPromises_[funcAgentID].first->SetValue(Status(StatusCode::SUCCESS));
    }
    if (internalIAM_ != nullptr && internalIAM_->IsIAMEnabled()) {
        ASSERT_IF_NULL(instanceControlView_);
        auto stateMachine = instanceControlView_->GetInstance(instanceID);
        if (stateMachine == nullptr) {
            YRLOG_WARN("{}|state machine is null", instanceID);
            return Status::OK();
        }
        if (internalIAM_->GetCredType() == function_proxy::IAMCredType::TOKEN) {
            internalIAM_->RequireEncryptToken(tenantID).Then(
                [aid(GetAID()), tenantID, funcAgentID,
                 runtimeID(stateMachine->GetInstanceInfo().runtimeid())](const std::shared_ptr<TokenSalt> &tokenSalt) {
                    if (tokenSalt->status.IsError()) {
                        YRLOG_ERROR("{}|failed to require token for recover", runtimeID);
                        return Status::OK();
                    }
                    litebus::Async(aid, &InstanceCtrlActor::UpdateInstanceToken, runtimeID, funcAgentID,
                                   tokenSalt->token, tokenSalt->salt);
                    return Status::OK();
                });
        } else if (internalIAM_->GetCredType() == function_proxy::IAMCredType::AK_SK) {
            internalIAM_->RequireCredentialByTenantID(tenantID).Then(
                [aid(GetAID()), tenantID, funcAgentID, runtimeID(stateMachine->GetInstanceInfo().runtimeid())](
                    const std::shared_ptr<AKSKContent> &akskContent) {
                    if (akskContent->status.IsError()) {
                        YRLOG_ERROR("{}|failed to require ak sk for recover", runtimeID);
                        return Status::OK();
                    }
                    litebus::Async(aid, &InstanceCtrlActor::UpdateInstanceAkSk, funcAgentID, akskContent);
                    return Status::OK();
                });
        }
    }
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::SyncInstance(
    const std::shared_ptr<resource_view::ResourceUnit> &resourceUnit)
{
    const auto &funcAgentID = resourceUnit->id();
    YRLOG_INFO("start to sync instance of agent({}).", funcAgentID);
    ASSERT_IF_NULL(observer_);
    return observer_->GetAgentInstanceInfoByID(funcAgentID)
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DoSync, _1, funcAgentID))
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::KillAgentInstance, _1, resourceUnit))
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::RecoverAgentInstance, _1, resourceUnit))
        .OnComplete(litebus::Defer(GetAID(), &InstanceCtrlActor::CheckSyncInstance, _1, funcAgentID));
}

litebus::Future<Status> InstanceCtrlActor::SyncAgent(
    const std::unordered_map<std::string, messages::FuncAgentRegisInfo> &agentMap)
{
    observer_->GetLocalInstanceInfo().Then(
        litebus::Defer(GetAID(), &InstanceCtrlActor::SyncFailedAgentInstance, agentMap, _1));
    return Status::OK();
}

void InstanceCtrlActor::TryRecoverExistedInstanceWithoutAgent(const InstanceInfo &info)
{
    auto stateMachine = instanceControlView_->GetInstance(info.instanceid());
    if (stateMachine == nullptr) {
        YRLOG_ERROR("state machine of instance({}) is null", info.instanceid());
        return;
    }
    if (!IsRuntimeRecoverEnable(info, stateMachine->GetCancelFuture())) {
        YRLOG_WARN("instance({})'s agent has exited, trans to FATAL", info.instanceid());
        (void)TransInstanceState(stateMachine, TransContext{ InstanceState::FATAL, stateMachine->GetVersion(),
                                                             info.functionagentid() + " agent has exited", true,
                                                             StatusCode::ERR_INSTANCE_EXITED })
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeleteInstanceInResourceView, Status::OK(), info));
        return;
    }
    auto request = stateMachine->GetScheduleRequest();
    auto context = TransContext{ InstanceState::FAILED, stateMachine->GetVersion(), "instance Failed" };
    context.scheduleReq = request;
    YRLOG_INFO("instance({})'s agent has exited, try to reschedule", info.instanceid());
    (void)TransInstanceState(stateMachine, context)
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::Reschedule, Status(StatusCode::FAILED), request));
}

litebus::Future<Status> InstanceCtrlActor::SyncFailedAgentInstance(
    const std::unordered_map<std::string, messages::FuncAgentRegisInfo> &agentMap,
    const litebus::Option<function_proxy::InstanceInfoMap> &instanceInfoMap)
{
    if (instanceInfoMap.IsNone()) {
        YRLOG_WARN("failed to sync failed agent instance, failed to get local instance");
        return Status(StatusCode::FAILED);
    }

    for (auto infoIter : instanceInfoMap.Get()) {
        auto info = infoIter.second;
        auto agentIter = agentMap.find(info.functionagentid());
        // agent of instance is empty
        // or agent is not existed
        // or agent is evicted or failed
        if (!info.functionagentid().empty() && agentIter != agentMap.end() &&
            agentIter->second.statuscode() != static_cast<int32_t>(FunctionAgentMgrActor::RegisStatus::EVICTED) &&
            agentIter->second.statuscode() != static_cast<int32_t>(FunctionAgentMgrActor::RegisStatus::FAILED)) {
            continue;
        }
        if (IsDriver(info)) {
            continue;
        }
        YRLOG_INFO("instance({}) on invalid agent({}) with status({})", info.instanceid(),
                   info.functionagentid().empty() ? "nil" : info.functionagentid(), info.instancestatus().code());
        auto stateMachine = instanceControlView_->GetInstance(info.instanceid());
        if (stateMachine == nullptr) {
            YRLOG_ERROR("state machine of instance({}) is null", info.instanceid());
            continue;
        }
        // running/creating/failed/sub-health while empty agent or agent not existing
        // if restart is configured, the instance will be rescheduled.
        // otherwise, the instance is set to fatal
        if (info.instancestatus().code() == static_cast<int32_t>(InstanceState::RUNNING) ||
            info.instancestatus().code() == static_cast<int32_t>(InstanceState::CREATING) ||
            info.instancestatus().code() == static_cast<int32_t>(InstanceState::FAILED) ||
            info.instancestatus().code() == static_cast<int32_t>(InstanceState::SUB_HEALTH)) {
            TryRecoverExistedInstanceWithoutAgent(info);
            continue;
        }

        // exiting while empty agent or agent not existing, directly delete it
        if (info.instancestatus().code() == static_cast<int32_t>(InstanceState::EXITING)) {
            exitHandler_(info);
            continue;
        }

        // evicting while empty agent or agent not existing, directly set it to be evicted
        if (info.instancestatus().code() == static_cast<int32_t>(InstanceState::EVICTING)) {
            (void)TransInstanceState(stateMachine, TransContext{ InstanceState::EVICTED, stateMachine->GetVersion(),
                                                                 info.functionagentid() + " function-agent has exited",
                                                                 true, StatusCode::ERR_INSTANCE_EVICTED });
            continue;
        }

        // scheduling while empty agent or agent not existing, try to reschedule
        if (info.instancestatus().code() == static_cast<int32_t>(InstanceState::SCHEDULING)) {
            (void)RecoverSchedulingInstance(stateMachine->GetScheduleRequest());
            continue;
        }

        // schedule_failed while empty agent or agent not existing, should to notify result(avoid caller blocked),
        // because of schedule_failed is put before notify result
        if (info.instancestatus().code() == static_cast<int32_t>(InstanceState::SCHEDULE_FAILED)) {
            YRLOG_WARN("{}|instance({}) status is schedule-failed, Resend the result", info.requestid(),
                       info.instanceid());
            auto callResult = std::make_shared<functionsystem::CallResult>();
            callResult->set_requestid(info.requestid());
            callResult->set_instanceid(info.parentid());
            callResult->set_code(Status::GetPosixErrorCode(info.instancestatus().errcode()));
            callResult->set_message(fmt::format("{} on {}", info.instancestatus().msg(), nodeID_));
            (void)WaitClientConnected(info.parentid())
                .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SendCallResult, info.instanceid(),
                                     info.parentid(), info.parentfunctionproxyaid(), callResult));
            continue;
        }
        // EVICTED/FATAL nothing to do
    }
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::CheckSyncKillInstance(
    const litebus::Future<Status> &future, const std::shared_ptr<litebus::Promise<Status>> &killPromise,
    const std::string &funcAgentID)
{
    litebus::Promise<Status> promiseRet;
    if (future.IsError()) {
        YRLOG_ERROR("agent({}) sync failed when killing instance. code: {}", funcAgentID, future.GetErrorCode());
        killPromise->SetFailed(static_cast<int32_t>(StatusCode::LS_SYNC_INSTANCE_FAIL));
        promiseRet.SetFailed(future.GetErrorCode());
        return promiseRet.GetFuture();
    }
    YRLOG_ERROR("kill instance successfully when agent({}) sync.", funcAgentID);
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::KillAgentInstance(const Status &status,
                                                             const std::shared_ptr<ResourceUnit> &resourceUnit)
{
    std::set<std::string> needKillInstances;
    const auto actualInstances = resourceUnit->instances();
    const auto &funcAgentID = resourceUnit->id();
    if (funcAgentMap_.find(funcAgentID) == funcAgentMap_.end()) {
        YRLOG_ERROR("failed to find agent({}), failed to kill instances of agent", funcAgentID);
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR);
    }
    for (const auto &instance : actualInstances) {
        (void)concernedInstance_.insert(instance.first);
        if (funcAgentMap_[funcAgentID]->find(instance.first) == funcAgentMap_[funcAgentID]->end()
            || funcAgentMap_[funcAgentID]->find(instance.first)->second.functionproxyid() == INSTANCE_MANAGER_OWNER
            || funcAgentMap_[funcAgentID]->find(instance.first)->second.instancestatus().code()
                   == static_cast<int32_t>(InstanceState::SUSPEND)) {
            (void)needKillInstances.insert(instance.first);
        }
    }
    for (auto ins : needKillInstances) {
        YRLOG_DEBUG("clear instance({}) which needs to be killed", ins);
        (void)resourceUnit->mutable_instances()->erase(ins);
    }

    for (auto iter = funcAgentMap_[funcAgentID]->cbegin(); iter != funcAgentMap_[funcAgentID]->cend(); ++iter) {
        if ((iter->second).instancestatus().code() == static_cast<int32_t>(InstanceState::EXITING)) {
            (void)needKillInstances.insert(iter->first);
        }
        // we can't recover the creating instance without init args
        if ((iter->second).instancestatus().code() == static_cast<int32_t>(InstanceState::CREATING) &&
            (iter->second).args_size() == 0) {
            YRLOG_WARN("instance({}) without init args, which needs to be killed", iter->first);
            (void)needKillInstances.insert(iter->first);
        }
    }

    auto promiseRet = std::make_shared<litebus::Promise<Status>>();
    auto needKillInstancesNum = needKillInstances.size();
    if (needKillInstancesNum == 0) {
        YRLOG_INFO("agent({}) don't need kill any instance.", funcAgentID);
        promiseRet->SetValue(Status::OK());
        return promiseRet->GetFuture();
    }
    YRLOG_INFO("agent({}) need kill {} instances.", funcAgentID, needKillInstancesNum);
    auto emplaceResult = syncKillPromises_.emplace(funcAgentID, std::make_pair(promiseRet, needKillInstancesNum));
    if (!emplaceResult.second) {
        return syncKillPromises_[funcAgentID].first->GetFuture();
    }
    for (const auto &instanceID : needKillInstances) {
        InstanceInfo instance;
        if (actualInstances.find(instanceID) != actualInstances.end()) {
            YRLOG_DEBUG("add instance({}) in actual instances to sync kill", instanceID);
            instance = actualInstances.at(instanceID);
        } else if (funcAgentMap_[funcAgentID]->find(instanceID) != funcAgentMap_[funcAgentID]->end()) {
            YRLOG_DEBUG("add instance({}) in function agent map to sync kill", instanceID);
            instance = funcAgentMap_[funcAgentID]->at(instanceID);
        }
        instance.set_functionagentid(funcAgentID);
        SendKillRequestToAgent(instance, true, false)
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::CheckSyncKill, _1, funcAgentID, instance.instanceid()))
            .OnComplete(
                litebus::Defer(GetAID(), &InstanceCtrlActor::CheckSyncKillInstance, _1, promiseRet, funcAgentID))
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeleteInstanceInResourceView, std::placeholders::_1,
                                 instance))
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeleteInstanceInControlView, std::placeholders::_1,
                                 instance));
    }

    return promiseRet->GetFuture();
}

litebus::Future<Status> InstanceCtrlActor::CheckSyncKill(const messages::KillInstanceResponse &killStatus,
                                                         const std::string &funcAgentID, const std::string &instanceID)
{
    litebus::Promise<Status> promiseRet;
    if (syncKillPromises_.find(funcAgentID) == syncKillPromises_.end()) {
        YRLOG_INFO("agent({}) sync failed when killing instance. agent don't exist or process completed", funcAgentID);
        promiseRet.SetFailed(static_cast<int32_t>(StatusCode::LS_SYNC_KILL_INSTANCE_FAIL));
        return promiseRet.GetFuture();
    }

    if (killStatus.code() != static_cast<int32_t>(StatusCode::SUCCESS) &&
        killStatus.code() != static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_RUNTIME_PROCESS_NOT_FOUND)) {
        YRLOG_ERROR("{}|agent({}) sync failed when killing instance({}). code: {}, msg: {}, instanceID({})",
                    killStatus.requestid(), funcAgentID, instanceID, killStatus.code(), killStatus.message(),
                    killStatus.instanceid());
        syncKillPromises_[funcAgentID].first->SetFailed(static_cast<int32_t>(StatusCode::LS_SYNC_INSTANCE_FAIL));
        promiseRet.SetFailed(static_cast<int32_t>(StatusCode::LS_SYNC_KILL_INSTANCE_FAIL));
        return promiseRet.GetFuture();
    }

    YRLOG_INFO("succeed to kill instance({}) of agent({})", instanceID, funcAgentID);
    if (--syncKillPromises_[funcAgentID].second == 0) {
        YRLOG_INFO("all inconsistent instances have been killed.");
        syncKillPromises_[funcAgentID].first->SetValue(Status(StatusCode::SUCCESS));
    }

    return Status::OK();
}

litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> InstanceCtrlActor::CreateInstanceClient(
    const std::string &instanceID, const std::string &runtimeID, const std::string &address,
    const std::function<void()> &customCb, bool isDriver)
{
    auto promise = std::make_shared<CtrlClientPromise>();
    auto info = GenerateAddressInfo(instanceID, runtimeID, address, isDriver);
    CreateClientWithRetry(info, promise, 0, isDriver ? DRIVER_RECONNECTED_TIMEOUT : config_.connectTimeout, customCb);
    return promise->GetFuture();
}

void InstanceCtrlActor::CreateClientWithRetry(const AddressInfo &info,
                                              const std::shared_ptr<CtrlClientPromise> &promise, uint32_t alreadyFailed,
                                              uint64_t timeoutSec, const std::function<void()> &customCb)
{
    // driver or accessor need to retry creating client
    auto stateMachine = instanceControlView_->GetInstance(info.instanceID);
    if (!info.isDriver && (stateMachine == nullptr || stateMachine->GetInstanceState() == InstanceState::FATAL ||
                                stateMachine->GetInstanceState() == InstanceState::EXITING)) {
        // if instance status is fetal, no need to reconnect
        YRLOG_WARN("no need to create client, instance({}) info not exist", info.instanceID);
        std::shared_ptr<ControlInterfacePosixClient> client;
        promise->SetValue(client);
        return;
    }
    // when the "stopped" flag of the driver is marked, disconnection directly triggers the driver's cleanup to avoid
    // entering an invalid retry connection process.
    if (info.isDriver && stateMachine != nullptr && stateMachine->IsStopped()) {
        YRLOG_INFO("{} is tagged stopped which is driver, directly delete it", info.instanceID);
        return DeleteDriverClient(info.instanceID, stateMachine->GetInstanceInfo().jobid());
    }
    auto retry = [aid(GetAID()), config(config_), info, promise, customCb]() {
        YRLOG_WARN("instance({}) runtime({}) address({}) client has disconnected. start to reconnect. timeout({}s)",
                   info.instanceID, info.runtimeID, info.address,
                   info.isDriver ? DRIVER_RECONNECTED_TIMEOUT : config.reconnectTimeout);
        litebus::Async(aid, &InstanceCtrlActor::CreateClientWithRetry, info, promise, 0,
                       info.isDriver ? DRIVER_RECONNECTED_TIMEOUT : config.reconnectTimeout, customCb);
    };
    ASSERT_IF_NULL(clientManager_);
    (void)clientManager_
        ->NewControlInterfacePosixClient(info.instanceID, info.runtimeID, info.address, retry, timeoutSec,
                                         config_.maxGrpcSize)
        .Then([aid(GetAID()), config(config_), info, promise, alreadyFailed,
               customCb](const std::shared_ptr<ControlInterfacePosixClient> &client) {
            if (client != nullptr) {
                promise->SetValue(client);
                return Status::OK();
            }
            auto failed = alreadyFailed + 1;
            YRLOG_WARN("failed to connect instance({}) runtime({}) address({}) client for {} times.", info.instanceID,
                       info.runtimeID, info.address, failed);
            if (failed < config.maxInstanceReconnectTimes) {
                (void)litebus::AsyncAfter(config.reconnectInterval, aid, &InstanceCtrlActor::CreateClientWithRetry,
                                          info, promise, failed, config.reconnectTimeout, customCb);
            } else {
                YRLOG_ERROR("reconnecting instance({}) runtime({}) address({}) client has reached limitation {} times.",
                            info.instanceID, info.runtimeID, info.address, failed);
                if (customCb != nullptr) {
                    customCb();
                }
                promise->SetValue(client);
            }
            return Status::OK();
        });
}

litebus::Future<Status> InstanceCtrlActor::Checkpoint(const std::string &instanceID)
{
    auto promise = std::make_shared<litebus::Promise<Status>>();
    ASSERT_IF_NULL(clientManager_);
    clientManager_->GetControlInterfacePosixClient(instanceID)
        .Then([instanceID](const litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> &future)
                  -> litebus::Future<runtime::CheckpointResponse> {
            auto instanceClient = future.Get();
            if (instanceClient == nullptr) {
                YRLOG_ERROR("failed to get instance client instance({})", instanceID);
                return GenCheckpointResponse(common::ErrorCode::ERR_LOCAL_SCHEDULER_OPERATION_ERROR,
                                             "failed to get instance client");
            }
            runtime::CheckpointRequest req{};
            req.set_checkpointid(instanceID);
            YRLOG_INFO("send checkpoint to instance({})", instanceID);
            return instanceClient->Checkpoint(std::move(req));
        })
        .Then([instanceID](const litebus::Future<runtime::CheckpointResponse> &rsp)
                  -> litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> {
            auto checkpointRsp = rsp.Get();
            if (checkpointRsp.code() != common::ErrorCode::ERR_NONE) {
                YRLOG_ERROR("failed to get checkpoint state from instance({})", instanceID);
                return GenStateSaveRspStreamMessage(checkpointRsp.code(), checkpointRsp.message());
            }
            if (checkpointRsp.state().empty()) {
                YRLOG_WARN("checkpoint with empty state from ({})", instanceID);
                return GenStateSaveRspStreamMessage(common::ErrorCode::ERR_NONE, "");
            }
            StateSaveRequest req{};
            req.set_state(checkpointRsp.state());
            auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
            *msg->mutable_savereq() = std::move(req);
            return function_proxy::StateHandler::SaveState(instanceID, msg);
        })
        .OnComplete([promise](const litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> &rsp) {
            StateSaveResponse saveRsp = std::move(rsp.Get()->saversp());
            if (saveRsp.code() != common::ErrorCode::ERR_NONE) {
                YRLOG_ERROR("failed to save checkpoint state, error code: {}, msg: {}", fmt::underlying(saveRsp.code()),
                            saveRsp.message());
                promise->SetValue(Status(static_cast<StatusCode>(saveRsp.code()), saveRsp.message()));
                return;
            }
            promise->SetValue(Status::OK());
        });
    return promise->GetFuture();
}

litebus::Future<Status> InstanceCtrlActor::Recover(const resource_view::InstanceInfo &instance)
{
    auto promise = std::make_shared<litebus::Promise<Status>>();
    ASSERT_IF_NULL(clientManager_);
    clientManager_->GetControlInterfacePosixClient(instance.instanceid())
        .Then([aid(GetAID()), instance, promise](
            const litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> &future)
                  -> litebus::Future<runtime::RecoverResponse> {
            auto instanceClient = future.Get();
            if (instanceClient == nullptr) {
                YRLOG_ERROR("failed to get instance({}) client", instance.instanceid());
                return GenRecoverResponse(
                    common::ErrorCode::ERR_LOCAL_SCHEDULER_OPERATION_ERROR, "failed to get instance client");
            }
            StateLoadRequest req{};
            req.set_checkpointid(instance.instanceid());
            auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
            *msg->mutable_loadreq() = std::move(req);
            return function_proxy::StateHandler::LoadState(instance.instanceid(), msg)
                .Then([instance, aid,
                       instanceClient](const litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> &rsp)
                          -> litebus::Future<runtime::RecoverResponse> {
                    StateLoadResponse loadRsp = std::move(rsp.Get()->loadrsp());
                    if (loadRsp.code() != common::ErrorCode::ERR_NONE) {
                        return GenRecoverResponse(loadRsp.code(), loadRsp.message());
                    }

                    runtime::RecoverRequest req{};
                    req.set_state(loadRsp.state());
                    auto createOptions = instance.createoptions();
                    *req.mutable_createoptions() = createOptions;

                    YRLOG_INFO("send recover to instance({})", instance.instanceid());
                    litebus::Async(aid, &InstanceCtrlActor::SetInstanceBillingContext, instance);
                    return instanceClient->Recover(std::move(req), GetRuntimeRecoverTimeout(instance));
                });
        })
        .OnComplete([promise](const litebus::Future<runtime::RecoverResponse> &rsp) {
            auto recoverRsp = rsp.Get();
            if (recoverRsp.code() != common::ErrorCode::ERR_NONE) {
                YRLOG_ERROR("failed to recover, error code: {}, msg: {}", fmt::underlying(recoverRsp.code()),
                            recoverRsp.message());
                promise->SetValue(Status(StatusCode(recoverRsp.code())));
                return;
            }
            promise->SetValue(Status::OK());
        });
    return promise->GetFuture();
}

inline bool InstanceCtrlActor::IsValidKillParam(
    const Status &status, std::shared_ptr<KillContext> &killCtx, const std::shared_ptr<KillRequest> &killReq,
    std::shared_ptr<InstanceStateMachine> &stateMachine)
{
    auto &killRsp = killCtx->killRsp;
    const auto &instanceID = killReq->instanceid();
    if (status.IsError()) {
        if (status.StatusCode() == StatusCode::ERR_INSTANCE_NOT_FOUND) {
            YRLOG_WARN("failed to kill instance, instance({}) is not found.", instanceID);
            killRsp = GenKillResponse(common::ErrorCode::ERR_INSTANCE_NOT_FOUND,
                                      "instance not found, the instance may have been killed");
        } else {
            YRLOG_ERROR("failed to kill instance, authorize status is error.");
            killRsp = GenKillResponse(common::ErrorCode::ERR_PARAM_INVALID, "authorize error");
        }
        return false;
    }
    if (instanceID.empty()) {
        YRLOG_ERROR("failed to kill instance, instanceID is empty.");
        killRsp = GenKillResponse(common::ErrorCode::ERR_PARAM_INVALID, "instanceID is empty");
        return false;
    }
    if (stateMachine == nullptr) {
        YRLOG_WARN("failed to kill instance, instance({}) is not found.", instanceID);
        killRsp = GenKillResponse(common::ErrorCode::ERR_INSTANCE_NOT_FOUND,
                                  "instance not found, the instance may have been killed");
        return false;
    }
    return true;
}

litebus::Future<Status> InstanceCtrlActor::CheckInstanceExist(const std::string &srcInstanceID,
                                                              const std::shared_ptr<KillRequest> &killReq)
{
    if (!config_.isPartialWatchInstances) {
        return Status::OK();
    }

    litebus::Promise<Status> instanceExistStatus;
    observer_->GetOrWatchInstance(killReq->instanceid())
        .OnComplete([instanceExistStatus,
                     instanceID(killReq->instanceid())](const litebus::Future<resource_view::InstanceInfo> &future) {
            // make sure instance is already updated in instance control view
            instanceExistStatus.SetValue(Status::OK());
        });
    return instanceExistStatus.GetFuture();
}

litebus::Future<std::shared_ptr<KillContext>> InstanceCtrlActor::ProcessKillCtxByInstanceState(
    const std::shared_ptr<KillContext> &killCtx)
{
    if (killCtx->killRsp.code() != common::ErrorCode::ERR_NONE) {
        YRLOG_WARN("(kill)failed to check param, code: {}, message: {}", fmt::underlying(killCtx->killRsp.code()),
                   killCtx->killRsp.message());
        return killCtx;
    }

    const auto &instanceID = killCtx->killRequest->instanceid();
    auto &killRsp = killCtx->killRsp;
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_WARN("failed to kill instance, instance({}) is not found.", instanceID);
        killRsp = GenKillResponse(common::ErrorCode::ERR_INSTANCE_NOT_FOUND,
                                  "instance not found, the instance may have been killed");
        return killCtx;
    }

    auto state = killCtx->instanceContext->GetState();
    YRLOG_INFO("{}|kill instance({})), current status ({})", killCtx->instanceContext->GetRequestID(), instanceID,
               fmt::underlying(state));
    if (state == InstanceState::NEW || state == InstanceState::SCHEDULING || state == InstanceState::CREATING
        || state == InstanceState::EVICTING) {
        YRLOG_WARN("instance({}) state({}) is not ready, register callback for state change.", instanceID,
                   fmt::underlying(state));
        auto promise = std::make_shared<litebus::Promise<std::shared_ptr<KillContext>>>();
        std::string eventKey = "CheckKillParam-signal-" + std::to_string(killCtx->killRequest->signal()) + "-" +
            litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        stateMachine->AddStateChangeCallback(
            { InstanceState::RUNNING, InstanceState::FAILED, InstanceState::FATAL, InstanceState::EVICTED,
              InstanceState::SCHEDULE_FAILED, InstanceState::SUB_HEALTH },
            [promise, killCtx](const resources::InstanceInfo &instanceInfo) {
                auto states = instanceInfo.instancestatus().code();
                if (static_cast<InstanceState>(states) == InstanceState::FAILED) {
                    killCtx->instanceIsFailed = true;
                }
                killCtx->instanceContext->UpdateInstanceInfo(instanceInfo);
                promise->SetValue(killCtx);
            }, eventKey);
        return promise->GetFuture();
    }
    if ((state == InstanceState::RUNNING || state == InstanceState::SUB_HEALTH) &&
        killCtx->instanceContext->GetInstanceInfo().functionproxyid() == nodeID_) {
        // when a running instance (RUNNING/SUB_HEALTH) receives a kill request, ensure that the agent is not in the
        // recover process to prevent concurrent modification of the instance state machine.
        ASSERT_IF_NULL(functionAgentMgr_);
        return functionAgentMgr_->IsFuncAgentRecovering(killCtx->instanceContext->GetInstanceInfo().functionagentid())
            .Then([killCtx, stateMachine, instanceID](const bool &) -> litebus::Future<std::shared_ptr<KillContext>> {
                YRLOG_INFO("{} is already recovered, continue to kill instance({})",
                           killCtx->instanceContext->GetInstanceInfo().functionagentid(), instanceID);
                // update context to avoid using outdated instance state
                killCtx->instanceContext = stateMachine->GetInstanceContextCopy();
                return killCtx;
            });
    }
    return killCtx;
}

litebus::Future<std::shared_ptr<KillContext>> InstanceCtrlActor::PrepareKillByInstanceState(
    const std::shared_ptr<KillContext> &killCtx)
{
    if (killCtx->killRsp.code() != common::ErrorCode::ERR_NONE) {
        YRLOG_WARN("(kill)failed to check param, code: {}, message: {}", fmt::underlying(killCtx->killRsp.code()),
                   killCtx->killRsp.message());
        return killCtx;
    }

    const auto &instanceID = killCtx->killRequest->instanceid();
    auto &killRsp = killCtx->killRsp;
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_WARN("failed to kill instance, instance({}) is not found.", instanceID);
        killRsp = GenKillResponse(common::ErrorCode::ERR_INSTANCE_NOT_FOUND,
                                  "instance not found, the instance may have been killed");
        return killCtx;
    }
    auto state = killCtx->instanceContext->GetState();
    YRLOG_INFO("{}|kill instance({})), current status ({})", killCtx->instanceContext->GetRequestID(), instanceID,
               fmt::underlying(state));
    if (stateMachine->GetCancelFuture().IsInit()) {
        auto msg = fmt::format("receive stop signal {} to kill instance {}",
                               std::to_string(killCtx->killRequest->signal()), killCtx->killRequest->instanceid());
        stateMachine->SetCancel(msg);
    }
    if (state == InstanceState::SCHEDULING) {
        auto reason = fmt::format("{}|instance({}) canceled", killCtx->instanceContext->GetRequestID(), instanceID);
        auto cancelRequest = GenCancelSchedule(killCtx->instanceContext->GetRequestID(), CancelType::REQUEST, reason);
        ASSERT_IF_NULL(localSchedSrv_);
        (void)localSchedSrv_->TryCancelSchedule(cancelRequest);
        auto promise = std::make_shared<litebus::Promise<std::shared_ptr<KillContext>>>();
        std::string eventKey = "CheckKillParam-signal-" + std::to_string(killCtx->killRequest->signal()) + "-"
                               + litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        stateMachine->AddStateChangeCallback(
            { InstanceState::SCHEDULE_FAILED, InstanceState::CREATING, InstanceState::RUNNING, InstanceState::FATAL },
            [promise, killCtx](const resources::InstanceInfo &instanceInfo) {
                auto states = instanceInfo.instancestatus().code();
                if (static_cast<InstanceState>(states) == InstanceState::FAILED) {
                    killCtx->instanceIsFailed = true;
                }
                killCtx->instanceContext->UpdateInstanceInfo(instanceInfo);
                promise->SetValue(killCtx);
            },
            eventKey);
        return promise->GetFuture();
    }
    return killCtx;
}

litebus::Future<std::shared_ptr<KillContext>> InstanceCtrlActor::CheckKillParam(
    const Status &status, const std::string &srcInstanceID, const std::shared_ptr<KillRequest> &killReq)
{
    auto killCtx = std::make_shared<KillContext>();
    killCtx->srcInstanceID = srcInstanceID;
    const auto &instanceID = killReq->instanceid();
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    // check instanceID validation
    if (!IsValidKillParam(status, killCtx, killReq, stateMachine)) {
        return killCtx;
    }
    killCtx->instanceContext = stateMachine->GetInstanceContextCopy();
    killCtx->killRequest = killReq;
    return killCtx;
}

litebus::Future<Status> InstanceCtrlActor::RescheduleWithID(const std::string &instanceID)
{
    metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetBillingInstanceEndTime(
        instanceID, std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    RETURN_STATUS_IF_TRUE(isAbnormal_, StatusCode::ERR_INNER_SYSTEM_ERROR, "abnormal local scheduler " + nodeID_);
    auto request = GetScheReqByInstID(instanceID);
    if (request.IsNone()) {
        YRLOG_ERROR("failed to reschedule, request of instance({}) cache empty", instanceID);
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "request cache empty");
    }
    return Reschedule(Status(StatusCode::FAILED), request.Get());
}

litebus::Future<Status> InstanceCtrlActor::RescheduleAfterJudgeRecoverable(const std::string &instanceID,
                                                                           const std::string &funcAgentID)
{
    YRLOG_INFO("{}|RuntimeManager retry register failed, instance should be killed or rescheduled", instanceID);
    RETURN_STATUS_IF_TRUE(isAbnormal_, StatusCode::ERR_INNER_SYSTEM_ERROR, "abnormal local scheduler " + nodeID_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_ERROR("failed to find instance({}) state machine", instanceID);
        return Status(StatusCode::FAILED);
    }

    if (stateMachine->GetOwner() != nodeID_) {
        YRLOG_WARN("instance {} is on node({}), not on current node({})", instanceID, stateMachine->GetOwner(),
                   nodeID_);
        return Status(StatusCode::FAILED, "instance not on current node");
    }

    auto instanceInfo = stateMachine->GetInstanceInfo();
    std::string msg = stateMachine->Information() +
                      "fatal: the instance is faulty because the function-agent or runtime-manager exits.";
    if (redeployTimesMap_.find(instanceID) != redeployTimesMap_.end()) {
        YRLOG_WARN("the reschedule instance({}) was discarded because it already exists", instanceID);
        return Status::OK();
    }

    if (IsRuntimeRecoverEnable(instanceInfo, stateMachine->GetCancelFuture())) {
        return TransInstanceState(stateMachine, TransContext{ InstanceState::FAILED, stateMachine->GetVersion(), msg })
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::RescheduleWithID, instanceID));
    }

    bool needToSendCallResult = (stateMachine->GetInstanceState() == InstanceState::CREATING);
    if (instanceInfo.functionagentid() != funcAgentID) {
        YRLOG_ERROR("failed to trans instance({}) to FETAL, invalid agent({}), instance should be in {}", instanceID,
                    funcAgentID, instanceInfo.functionagentid());
        return Status(StatusCode::FAILED);
    }

    (void)TransInstanceState(stateMachine, TransContext{ InstanceState::FATAL, stateMachine->GetVersion(), msg, true,
                                                         StatusCode::ERR_INSTANCE_EXITED })
        .Then([aid(GetAID()), nodeID(nodeID_), needToSendCallResult, stateMachine](const TransitionResult &result) {
            if (!needToSendCallResult) {
                return result;
            }
            auto callResult = std::make_shared<functionsystem::CallResult>();
            auto sche = stateMachine->GetScheduleRequest();
            callResult->set_instanceid(sche->instance().parentid());
            callResult->set_requestid(sche->instance().requestid());
            callResult->set_code(Status::GetPosixErrorCode(sche->instance().instancestatus().errcode()));
            callResult->set_message(fmt::format("{} on {}", sche->instance().instancestatus().msg(), nodeID));
            litebus::Async(aid, &InstanceCtrlActor::SendCallResult, sche->instance().instanceid(),
                           sche->instance().parentid(), sche->instance().parentfunctionproxyaid(), callResult);
            return result;
        })
        .Then([aid(GetAID()), instanceInfo](const TransitionResult &result) {
            (void)litebus::Async(aid, &InstanceCtrlActor::SendKillRequestToAgent, instanceInfo, false, false);
            return result;
        });
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::RecoverInstance(const std::string &instanceID)
{
    RETURN_STATUS_IF_TRUE(isAbnormal_, StatusCode::ERR_INNER_SYSTEM_ERROR, "abnormal local scheduler " + nodeID_);
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_WARN("failed to recover instance, instance({}) is not found.", instanceID);
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND, "instance is not found");
    }
    auto request = stateMachine->GetScheduleRequest(); // a copy of ScheduleReq
    if (request == nullptr) {
        YRLOG_ERROR("failed to get scheduleRequest from stateMachine");
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND, "instance is not found");
    }

    auto state = static_cast<InstanceState>(request->instance().instancestatus().code());
    if (state == InstanceState::FATAL) {
        YRLOG_ERROR("{}|{}|instance({}) status is ({}), reschedule failed", request->traceid(), request->requestid(),
                    request->instance().instanceid(), fmt::underlying(state));
        return Status(StatusCode::FAILED, "instance state is FATAL, failed to reschedule");
    }

    YRLOG_DEBUG("{}|{}|recover instance({}) state({}) function({}) in functionAgentID({})", request->traceid(),
                request->requestid(), request->instance().instanceid(), fmt::underlying(state),
                request->instance().function(), request->instance().functionagentid());
    (void)AddTokenReference(std::make_pair(stateMachine->GetInstanceInfo().tenantid(), instanceID));
    if (state == InstanceState::RUNNING || state == InstanceState::EVICTING || state == InstanceState::SUB_HEALTH) {
        // The connection needs to be restored for the instance being evicted or in sub-health.
        return RecoverRunningInstance(request, stateMachine);
    }

    if (state == InstanceState::CREATING) {
        return RecoverCreatingInstance(request, stateMachine);
    }

    if (state == InstanceState::EXITING) {
        return exitHandler_(stateMachine->GetInstanceInfo());
    }

    return RecoverSchedulingInstance(request);
}

litebus::Future<messages::ScheduleResponse> InstanceCtrlActor::DoAuthorizeCreate(
    const litebus::Option<FunctionMeta> &functionMeta, const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise)
{
    return AuthorizeCreate(functionMeta, scheduleReq, runtimePromise)
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::GetAffinity, _1, scheduleReq))
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DoCreateInstance, _1, functionMeta, scheduleReq,
                             runtimePromise));
}

Status InstanceCtrlActor::NormalizeCreateTenantID(
    const litebus::Option<FunctionMeta> &functionMeta, const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    // 1. Verify the function meta information.
    if (functionMeta.IsNone() ||
        scheduleReq->instance().instancestatus().code() != static_cast<int32_t>(InstanceState::NEW)) {
        return Status::OK();
    }
    // 2. Determine tenantId for create request. This is part of normal create semantics and
    // should not depend on whether IAM authorization is enabled.
    auto parentInstanceID = scheduleReq->instance().parentid();
    if (parentInstanceID.empty() && IsStaticFunctionInstance(scheduleReq->instance())) {
        auto createOptions = scheduleReq->instance().createoptions();
        if (createOptions.find(TENANT_ID) == createOptions.end() || createOptions[TENANT_ID].empty()) {
            YRLOG_ERROR("{}|static function instance({}), tenantId is empty", scheduleReq->requestid(),
                        scheduleReq->instance().instanceid());
            return Status(StatusCode::FAILED);
        }
        // if static function, use tenant id from create options.
        scheduleReq->mutable_instance()->set_tenantid(createOptions[TENANT_ID]);
        scheduleReq->mutable_instance()->set_parentid(STATIC_FUNCTION_OWNER_VALUE);
        YRLOG_INFO("{}|static function instance({}), no need to authorize, tenantId={}", scheduleReq->requestid(),
                   scheduleReq->instance().instanceid(), scheduleReq->instance().tenantid());
        return Status::OK();
    }
    if (parentInstanceID.empty()) {
        auto createOptions = scheduleReq->instance().createoptions();
        auto createTenantIter = createOptions.find(TENANT_ID);
        const auto &tenantID = (createTenantIter != createOptions.end() && !createTenantIter->second.empty()) ?
            createTenantIter->second : functionMeta.Get().funcMetaData.tenantId;
        scheduleReq->mutable_instance()->set_tenantid(tenantID);
        YRLOG_INFO("{}|system function instance({}), tenantId is set to {}", scheduleReq->requestid(),
                   scheduleReq->instance().instanceid(), scheduleReq->instance().tenantid());
        return Status::OK();
    }
    // 3. Verify the parent instance:
    // 3.1. FaaS-Controller create FaaS-Schedule during Init. Because not running, the state has not be sync to other
    // nodes. Therefore, all states of system function instances must be persistent in MetaStore.
    // 3.2. The parent and child instances are not on the same node. After the parent instance is created, the child
    // instance is created immediately. The parent instance's 'router' is not synchronized in time. As a result, the
    // parent instance's 'router' cannot be found when the child instance is created.
    auto parentStateMachine = instanceControlView_->GetInstance(parentInstanceID);
    if (parentStateMachine == nullptr) {
        YRLOG_ERROR("{}|not found parent instance({}) when authorize Create", scheduleReq->requestid(),
                    parentInstanceID);
        return Status(StatusCode::FAILED);
    }

    if (functionMeta.Get().funcMetaData.isSystemFunc) {
        scheduleReq->mutable_instance()->set_tenantid(functionMeta.Get().funcMetaData.tenantId);
    } else {
        if (parentStateMachine->GetInstanceInfo().issystemfunc()) {
            auto createOptions = scheduleReq->instance().createoptions();
            if (createOptions.find(TENANT_ID) != createOptions.end() && !createOptions[TENANT_ID].empty()) {
                scheduleReq->mutable_instance()->set_tenantid(createOptions[TENANT_ID]);
            } else {
                scheduleReq->mutable_instance()->set_tenantid(functionMeta.Get().funcMetaData.tenantId);
            }
        } else {
            scheduleReq->mutable_instance()->set_tenantid(parentStateMachine->GetInstanceInfo().tenantid());
        }
    }
    YRLOG_INFO("{}|tenantId is set to {}", scheduleReq->requestid(), scheduleReq->instance().tenantid());
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::AuthorizeCreate(
    const litebus::Option<FunctionMeta> &functionMeta, const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise)
{
    auto status = NormalizeCreateTenantID(functionMeta, scheduleReq);
    if (status.IsError()) {
        return status;
    }
    // todo(lwy): should be jwt or other auth info in create options, and should not be required for system function.
    // 1. Verify the IAM function's switch.
    if (internalIAM_ == nullptr || !internalIAM_->IsIAMEnabled()) {
        return Status::OK();
    }
    // 2. Verify the function meta information.
    if (functionMeta.IsNone() ||
        scheduleReq->instance().instancestatus().code() != static_cast<int32_t>(InstanceState::NEW)) {
        // funcMeta is none, schedule will return error, no need to do authorize
        // if request is from domain, skip authorize
        return Status::OK();
    }
    // 3. The head functions are created by bootstrap. Bootstrap is not instance, no InstanceID.
    auto parentInstanceID = scheduleReq->instance().parentid();
    if (parentInstanceID.empty()) {
        return Status::OK();
    }
    // 4. Verify the parent instance.
    auto parentStateMachine = instanceControlView_->GetInstance(parentInstanceID);
    if (parentStateMachine == nullptr) {
        YRLOG_ERROR("{}|not found parent instance({}) when authorize Create", scheduleReq->requestid(),
                    parentInstanceID);
        return Status(StatusCode::FAILED);
    }
    const auto &calleeTenantID = functionMeta.Get().funcMetaData.tenantId;
    function_proxy::AuthorizeParam authorizeParam{
        .callerTenantID = parentStateMachine->GetInstanceInfo().tenantid(),
        .calleeTenantID = calleeTenantID,
        .callMethod = function_proxy::CALL_METHOD_CREATE,
        .funcName = parentStateMachine->GetInstanceInfo().function(),
    };
    return internalIAM_->Authorize(authorizeParam);
}

litebus::Future<Status> InstanceCtrlActor::AuthorizeKill(const std::string &callerInstanceID,
                                                         const std::shared_ptr<KillRequest> &killReq, bool isSkipAuth)
{
    if (internalIAM_ == nullptr || !internalIAM_->IsIAMEnabled() || callerInstanceID.empty()) {
        return Status::OK();
    }
    // forward kill have already been authorized, skip
    if (litebus::strings::StartsWithPrefix(callerInstanceID, KILL_JOB_INS_PREFIX) || isSkipAuth) {
        return Status::OK();
    }
    if (callerInstanceID == GROUP_MANAGER_OWNER) {
        return Status::OK();
    }
    auto callerInstanceMachine = instanceControlView_->GetInstance(callerInstanceID);
    if (callerInstanceMachine == nullptr) {
        YRLOG_ERROR("failed to find source instance({}) when authorize.", callerInstanceID);
        return Status(StatusCode::FAILED);
    }
    // when environment upgrade from 'without iam' to 'enable iam', tenantID maybe empty
    if (callerInstanceMachine->GetInstanceInfo().tenantid().empty()) {
        YRLOG_INFO("caller instance({}) tenantId is empty, skip authorize", callerInstanceID);
        return Status::OK();
    }
    auto calleeInstance = instanceControlView_->GetInstance(killReq->instanceid());
    if (calleeInstance == nullptr) {
        YRLOG_DEBUG("failed to find instance({}) in authorizing as it may be killed.", killReq->instanceid());
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND);
    }
    // when environment upgrade from 'without iam' to 'enable iam', tenantID maybe empty
    if (calleeInstance->GetInstanceInfo().tenantid().empty()) {
        YRLOG_INFO("callee instance({}) tenantId is empty, skip authorize", killReq->instanceid());
        return Status::OK();
    }
    function_proxy::AuthorizeParam authorizeParam{
        .callerTenantID = callerInstanceMachine->GetInstanceInfo().tenantid(),
        .calleeTenantID = calleeInstance->GetInstanceInfo().tenantid(),
        .callMethod = function_proxy::CALL_METHOD_KILL,
        .funcName = calleeInstance->GetInstanceInfo().function(),
    };
    return internalIAM_->Authorize(authorizeParam);
}

litebus::Future<Status> InstanceCtrlActor::RecoverRunningInstance(
    const std::shared_ptr<messages::ScheduleRequest> &request,
    const std::shared_ptr<InstanceStateMachine> &stateMachine)
{
    RETURN_STATUS_IF_TRUE(isAbnormal_, StatusCode::ERR_INNER_SYSTEM_ERROR, "abnormal local scheduler " + nodeID_);
    YRLOG_INFO("{}|{}|instance({}) status is running, only need to create client", request->traceid(),
               request->requestid(), request->instance().instanceid());
    auto promise = std::make_shared<litebus::Promise<Status>>();
    auto instanceInfo = stateMachine->GetInstanceInfo();
    (void)CreateInstanceClient(request->instance().instanceid(), request->instance().runtimeid(),
                               request->instance().runtimeaddress())
        .OnComplete([promise, aid(GetAID()), request, stateMachine, resourceViewMgr(resourceViewMgr_),
                     recoverRuntime(IsRuntimeRecoverEnable(instanceInfo, stateMachine->GetCancelFuture()))](
                        const litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> &client) {
            if (client.IsError() || client.Get() == nullptr) {
                YRLOG_ERROR("{}|{}|failed to recover running instance({}) which only need creating client",
                            request->traceid(), request->requestid(), request->instance().instanceid());
                if (stateMachine == nullptr) {
                    YRLOG_ERROR("{}|{}|failed to find instance({}) to recover", request->traceid(),
                                request->requestid(), request->instance().instanceid());
                    promise->SetValue(Status(StatusCode::INSTANCE_FAILED_OR_KILLED, "instance not found"));
                    return;
                }
                if (recoverRuntime) {
                    promise->Associate(litebus::Async(aid, &InstanceCtrlActor::TransInstanceState, stateMachine,
                                                      TransContext{ InstanceState::FAILED, stateMachine->GetVersion(),
                                                                    "instance Failed" })
                                           .Then(litebus::Defer(aid, &InstanceCtrlActor::Reschedule,
                                                                Status(StatusCode::FAILED), request)));
                } else {
                    promise->Associate(
                        litebus::Async(aid, &InstanceCtrlActor::TransInstanceState, stateMachine,
                                       TransContext{ InstanceState::FATAL, stateMachine->GetVersion(),
                                                     stateMachine->Information() + "fatal: failed to recover instance",
                                                     true, StatusCode::ERR_INSTANCE_EXITED })
                            .Then([](const TransitionResult &state) {
                                if (state.preState.IsNone()) {
                                    return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "failed to transition to fatal");
                                } else {
                                    return Status::OK();
                                }
                            }));
                }
                return;
            }
            promise->Associate(
                resourceViewMgr->GetInf(resource_view::GetResourceType(request->instance()))
                    ->AddInstances({ { request->instance().instanceid(), { request->instance(), nullptr } } }));

            YRLOG_INFO("start heartbeat for instance({}) during recover, status({})", request->instance().instanceid(),
                       fmt::underlying(stateMachine->GetInstanceState()));
            // RUNNING and EVICTING instances had a healthy heartbeat
            (void)litebus::Async(aid, &InstanceCtrlActor::StartHeartbeat, request->instance().instanceid(), 0,
                                 stateMachine->GetInstanceInfo().runtimeid(),
                                 stateMachine->GetInstanceState() == InstanceState::SUB_HEALTH
                                     ? StatusCode::INSTANCE_SUB_HEALTH
                                     : StatusCode::SUCCESS);
        });
    return promise->GetFuture();
}

litebus::Future<Status> InstanceCtrlActor::RecoverCreatingInstance(
    const std::shared_ptr<messages::ScheduleRequest> &request,
    const std::shared_ptr<InstanceStateMachine> &stateMachine)
{
    RETURN_STATUS_IF_TRUE(isAbnormal_, StatusCode::ERR_INNER_SYSTEM_ERROR, "abnormal local scheduler " + nodeID_);
    YRLOG_INFO("{}|{}|instance({}) status is creating, need to be redeployed", request->traceid(), request->requestid(),
               request->instance().instanceid());
    auto promise = std::make_shared<litebus::Promise<Status>>();
    ASSERT_IF_NULL(observer_);
    // if recover failed, state will be changed and notify parent
    auto funcAgentID = request->instance().functionagentid();
    stateMachine->AddStateChangeCallback(
        { InstanceState::RUNNING, InstanceState::SCHEDULE_FAILED, InstanceState::EXITING, InstanceState::FATAL },
        [aid(GetAID()), parentID(request->instance().parentid()), funcAgentID,
         functionAgentMgr(functionAgentMgr_)](const InstanceInfo &instanceInfo) {
            InstanceInfo info = instanceInfo;
            if (instanceInfo.parentid() != parentID) {
                info.set_parentid(parentID);
                YRLOG_INFO("{} add state change callback for instance {}", info.requestid(), info.instanceid());
            }
            litebus::Async(aid, &InstanceCtrlActor::SubscribeInstanceStatusChanged, info, info.requestid());
        },
        "SubscribeInstanceStatusChanged");
    auto createCallResultPromise = std::make_shared<litebus::Promise<std::shared_ptr<functionsystem::CallResult>>>();
    syncCreateCallResultPromises_[request->instance().instanceid()] = createCallResultPromise;
    GetFuncMeta(request->instance().function())
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::CheckFuncMeta, _1, request))
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeployInstance, request, 0, litebus::None(), true))
        .OnComplete([promise, aid(GetAID()), request, stateMachine,
                     resourceViewMgr(resourceViewMgr_)](const litebus::Future<Status> &status) {
            if (status.IsError() || status.Get().IsError()) {
                YRLOG_ERROR("{}|{}|recover instance({}) which redeploy failed, start rescheduling", request->traceid(),
                            request->requestid(), request->instance().instanceid());
                if (stateMachine == nullptr) {
                    YRLOG_ERROR("{}|{}|failed to find instance({}) to reschedule", request->traceid(),
                                request->requestid(), request->instance().instanceid());
                    promise->SetValue(Status(StatusCode::INSTANCE_FAILED_OR_KILLED, "instance not found"));
                    return;
                }
                if (IsStaticFunctionInstance(request->instance())) {
                    litebus::Async(aid, &InstanceCtrlActor::TransInstanceState, stateMachine,
                                   TransContext{ InstanceState::FATAL, stateMachine->GetVersion(),
                                                 "static function instance Failed" });
                    promise->SetValue(Status(StatusCode::SUCCESS, "instance not found"));
                    return;
                }
                auto future =
                    litebus::Async(aid, &InstanceCtrlActor::TransInstanceState, stateMachine,
                                   TransContext{ InstanceState::FAILED, stateMachine->GetVersion(), "instance Failed" })
                        .Then(litebus::Defer(aid, &InstanceCtrlActor::Reschedule, Status(StatusCode::FAILED), request));
                promise->Associate(future);
                return;
            }
            promise->Associate(
                resourceViewMgr->GetInf(resource_view::GetResourceType(request->instance()))
                    ->AddInstances({ { request->instance().instanceid(), { request->instance(), nullptr } } }));
        });
    return promise->GetFuture();
}

litebus::Future<Status> InstanceCtrlActor::RecoverSchedulingInstance(
    const std::shared_ptr<messages::ScheduleRequest> &request)
{
    RETURN_STATUS_IF_TRUE(isAbnormal_, StatusCode::ERR_INNER_SYSTEM_ERROR, "abnormal local scheduler " + nodeID_);
    // Currently, instances in the scheduling state are not recovered.
    YRLOG_INFO("{}|{}|instance({}) status is scheduling, try to reschedule", request->traceid(), request->requestid(),
               request->instance().instanceid());
    auto ignorePro = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    TransitionResult result{};
    result.preState = InstanceState::NEW;
    (void)DoDispatchSchedule(request, ignorePro, result)
        .Then([request](const messages::ScheduleResponse &resp) {
            if (resp.code() != static_cast<int32_t>(StatusCode::SUCCESS)) {
                YRLOG_ERROR("{}|{}|failed to recover scheduling instance({}), code:{} err:{}", request->traceid(),
                            request->requestid(), request->instance().instanceid(), resp.code(), resp.message());
                return Status::OK();
            }
            YRLOG_INFO("{}|{}|successful to recover scheduling instance({})", request->traceid(), request->requestid(),
                       request->instance().instanceid());
            return Status::OK();
        });
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::Reschedule(const Status &status,
                                                      const std::shared_ptr<messages::ScheduleRequest> &request)
{
    RETURN_STATUS_IF_TRUE(isAbnormal_, StatusCode::ERR_INNER_SYSTEM_ERROR, "abnormal local scheduler " + nodeID_);
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(request->instance().instanceid());
    RETURN_STATUS_IF_NULL(stateMachine, StatusCode::ERR_INSTANCE_EXITED,
                          "instance already have been exited. " + request->instance().instanceid());
    auto states = request->instance().instancestatus().code();
    if (states == static_cast<int32_t>(InstanceState::FATAL)) {
        YRLOG_ERROR("{}|{}|instance({}) status is ({}), reschedule failed", request->traceid(), request->requestid(),
                    request->instance().instanceid(), states);
        return Status(StatusCode::FAILED, "instance state is FATAL, failed to reschedule");
    }

    litebus::Async(GetAID(), &InstanceCtrlActor::StopHeartbeat, request->instance().instanceid());
    YRLOG_INFO("{}|{}|rescheduler begin to reschedule instance({}), deployTimes {}, scheduleTimes {}",
               request->traceid(), request->requestid(), request->instance().instanceid(),
               stateMachine->GetDeployTimes(), request->instance().scheduletimes());
    if (redeployTimesMap_.find(request->instance().instanceid()) != redeployTimesMap_.end()) {
        YRLOG_ERROR("{}|{}|the reschedule request was discarded because it already exists.", request->traceid(),
                    request->requestid());
        return Status(StatusCode::FAILED);
    }
    redeployTimesMap_[request->instance().instanceid()] = stateMachine->GetDeployTimes();
    return RedeployDecision(status, request);
}

litebus::Future<Status> InstanceCtrlActor::RedeployDecision(const Status &status,
                                                            const std::shared_ptr<ScheduleRequest> &request)
{
    RETURN_STATUS_IF_TRUE(isAbnormal_, StatusCode::ERR_INNER_SYSTEM_ERROR, "abnormal local scheduler " + nodeID_);
    if (status.IsOk()) {
        YRLOG_DEBUG("status of instance({}) ok", request->instance().instanceid());
        (void)redeployTimesMap_.erase(request->instance().instanceid());
        return Status::OK();
    }
    auto stateMachine = instanceControlView_->GetInstance(request->instance().instanceid());
    if (stateMachine == nullptr) {
        YRLOG_WARN("instance({}) not found when redeploy", request->instance().instanceid());
        (void)redeployTimesMap_.erase(request->instance().instanceid());
        return Status(StatusCode::INSTANCE_FAILED_OR_KILLED, "instance not found when redeploy");
    }
    auto state = stateMachine->GetInstanceState();
    if (state != InstanceState::FAILED) {
        YRLOG_INFO("{}|current instance state is {}, transit to FAILED", request->requestid(), fmt::underlying(state));
        (void)TransInstanceState(stateMachine,
                                 TransContext{ InstanceState::FAILED, stateMachine->GetVersion(), "instance Failed" });
    }
    YRLOG_DEBUG("reschedule begin to kill and clean instance({}) before redeploy", request->instance().instanceid());
    return KillRuntime(request->instance(), false)
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::Redeploy, _1, request));
}

litebus::Future<Status> InstanceCtrlActor::Redeploy(const Status &status,
                                                    const std::shared_ptr<::messages::ScheduleRequest> &request)
{
    RETURN_STATUS_IF_TRUE(isAbnormal_, StatusCode::ERR_INNER_SYSTEM_ERROR, "abnormal local scheduler " + nodeID_);
    auto instanceID = request->instance().instanceid();
    if (!status.IsOk()) {
        YRLOG_ERROR("{}|{}|failed to kill or clean instance({}) before redeploy", request->traceid(),
                    request->requestid(), instanceID);
        (void)redeployTimesMap_.erase(request->instance().instanceid());
        return Status(StatusCode::FAILED);
    }
    YRLOG_DEBUG("instance ({}) killed and cleaned", instanceID);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_WARN("failed to recover instance, instance({}) is not found.", request->instance().instanceid());
        (void)redeployTimesMap_.erase(request->instance().instanceid());
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND, "instance is not found");
    }

    if (redeployTimesMap_.find(instanceID) == redeployTimesMap_.end()) {
        YRLOG_ERROR("failed to find instance({}) redeploy times in redeploy times map", instanceID);
        return Status(StatusCode::FAILED);
    }
    if (redeployTimesMap_[instanceID] <= 0 || request->instance().scheduletimes() <= 0) {
        YRLOG_WARN("{}|instance({}) deployTimes exceeded, clean resource view", request->requestid(),
                   request->instance().instanceid());
        (void)redeployTimesMap_.erase(instanceID);
        return litebus::Async(GetAID(), &InstanceCtrlActor::DoReschedule, request, status.StatusCode(),
                              status.GetMessage());
    }
    return TransInstanceState(stateMachine, TransContext{ InstanceState::SCHEDULING, stateMachine->GetVersion(),
                                                          "Rescheduling", false })
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::TransInstanceState, stateMachine,
                             TransContext{ InstanceState::CREATING, stateMachine->GetVersion(), "Creating" }))
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::GetFuncMeta, request->instance().function()))
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::CheckFuncMeta, _1, request))
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DoLocalRedeploy, std::placeholders::_1, request,
                             stateMachine));
}

litebus::Future<Status> InstanceCtrlActor::DoLocalRedeploy(const Status &status,
                                                           const std::shared_ptr<ScheduleRequest> &request,
                                                           const std::shared_ptr<InstanceStateMachine> &stateMachine)
{
    RETURN_STATUS_IF_TRUE(isAbnormal_, StatusCode::ERR_INNER_SYSTEM_ERROR, "abnormal local scheduler " + nodeID_);
    if (redeployTimesMap_.find(request->instance().instanceid()) == redeployTimesMap_.end()) {
        YRLOG_ERROR("failed to find instance({}) redeploy times in redeploy times map",
                    request->instance().instanceid());
        return Status(StatusCode::FAILED);
    }
    auto times = redeployTimesMap_[request->instance().instanceid()];
    redeployTimesMap_[request->instance().instanceid()] = (times <= 1) ? 0 : (times - 1);
    // redeploy consider as one reschedule
    request->mutable_instance()->set_scheduletimes(request->instance().scheduletimes() - 1);
    if (status.IsOk()) {
        return DispatchSchedule(request).Then(
            litebus::Defer(GetAID(), &InstanceCtrlActor::RedeployDecision, std::placeholders::_1, request));
    }
    (void)redeployTimesMap_.erase(request->instance().instanceid());
    return Status(StatusCode::FAILED);
}

litebus::Future<Status> InstanceCtrlActor::RescheduleConfirm(const messages::ScheduleResponse &response,
                                                             const std::shared_ptr<messages::ScheduleRequest> &request)
{
    RETURN_STATUS_IF_TRUE(isAbnormal_, StatusCode::ERR_INNER_SYSTEM_ERROR, "abnormal local scheduler " + nodeID_);
    if (response.code() == static_cast<int32_t>(StatusCode::SUCCESS)) {
        return Status::OK();
    } else {
        return DoReschedule(request, static_cast<uint32_t>(response.code()), response.message());
    }
}

litebus::Future<Status> InstanceCtrlActor::DoReschedule(const std::shared_ptr<messages::ScheduleRequest> &request,
                                                        uint32_t code, const std::string &msg)
{
    RETURN_STATUS_IF_TRUE(isAbnormal_, StatusCode::ERR_INNER_SYSTEM_ERROR, "abnormal local scheduler " + nodeID_);
    auto instanceID = request->instance().instanceid();
    auto requestID = request->requestid();
    ASSERT_IF_NULL(resourceViewMgr_);
    auto type = resource_view::GetResourceType(request->instance());
    (void)resourceViewMgr_->GetInf(type)->DeleteInstances({ instanceID })
        .OnComplete([instanceID, requestID](const litebus::Future<Status> &status) {
            if (status.IsError()) {
                YRLOG_ERROR("{}|failed to delete instance({}) in resource view, future err", requestID, instanceID);
            } else if (status.Get().IsError()) {
                YRLOG_ERROR("{}|failed to delete instance({}) in resource view, err {}", requestID, instanceID,
                            status.Get().ToString());
            }
        });
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_WARN("{}|failed to get instance({}) info for reschedule", requestID, instanceID);
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "failed to get instance info for reschedule");
    }
    int scheduleTimes = request->instance().scheduletimes();
    if (scheduleTimes <= 0) {
        YRLOG_ERROR("{}|instance({}) scheduleTimes exceeded", requestID, request->instance().instanceid());
        (void)TransInstanceState(
            stateMachine, TransContext{ InstanceState::FATAL, stateMachine->GetVersion(), "failed to recover", true,
                                        StatusCode::ERR_USER_FUNCTION_EXCEPTION });
        return Status(code != 0 ? static_cast<StatusCode>(code) : StatusCode::ERR_INNER_SYSTEM_ERROR, msg);
    }
    request->mutable_instance()->set_scheduletimes(scheduleTimes - 1);

    auto context = TransContext{ InstanceState::SCHEDULING, stateMachine->GetVersion(), "Rescheduling" };
    context.scheduleReq = request;
    context.scheduleReq->mutable_instance()->set_functionagentid("");
    return TransInstanceState(stateMachine, context)
        .Then([stateMachine, request, aid(GetAID()),
               localSchedSrv(localSchedSrv_)](const TransitionResult &result) -> litebus::Future<Status> {
            if (result.preState.IsNone()) {
                YRLOG_ERROR("{}|failed to transition instance({}) to SCHEDULING, can not forward schedule request",
                            request->requestid(), request->instance().instanceid());
                return Status(StatusCode::FAILED);
            }
            stateMachine->ReleaseOwner();
            request->mutable_instance()->set_functionproxyid("");
            request->mutable_instance()->clear_schedulerchain();
            schedule_framework::ClearContext(*request->mutable_contexts());
            YRLOG_INFO("{}|forward schedule instance({})", request->requestid(), request->instance().instanceid());
            ASSERT_IF_NULL(localSchedSrv);
            return localSchedSrv->ForwardSchedule(request).Then(
                litebus::Defer(aid, &InstanceCtrlActor::RescheduleConfirm, _1, request));
        });
}

litebus::Future<Status> InstanceCtrlActor::EvictInstanceOnAgent(const std::shared_ptr<messages::EvictAgentRequest> &req)
{
    YRLOG_INFO("start to evict instance on agent({})", req->agentid());
    ASSERT_IF_NULL(observer_);
    return observer_->GetAgentInstanceInfoByID(req->agentid())
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DoEvictInstances, _1, req));
}

litebus::Future<Status> InstanceCtrlActor::DoEvictInstances(
    const litebus::Option<function_proxy::InstanceInfoMap> &instanceInfoMapOpt,
    const std::shared_ptr<messages::EvictAgentRequest> &req)
{
    if (instanceInfoMapOpt.IsNone() || instanceInfoMapOpt.Get().empty()) {
        YRLOG_INFO("{}|no instance found in agent({})", req->requestid(), req->agentid());
        return Status::OK();
    }
    std::unordered_set<std::string> instanceSet;
    auto &instanceInfoMap = instanceInfoMapOpt.Get();
    for (auto &iter : instanceInfoMap) {
        instanceSet.insert(iter.first);
    }
    return EvictInstances(instanceSet, req, false);
}

litebus::Future<Status> InstanceCtrlActor::EvictInstances(const std::unordered_set<std::string> &instanceSet,
                                                          const std::shared_ptr<messages::EvictAgentRequest> &req,
                                                          bool isEvictForReuse)
{
    if (instanceSet.empty()) {
        return Status::OK();
    }
    std::list<litebus::Future<Status>> futures;
    for (auto instanceID : instanceSet) {
        YRLOG_INFO("{}|start evict instance({}) on agent({})", req->requestid(), instanceID, req->agentid());
        futures.push_back(EvictInstance(instanceID, req, isEvictForReuse));
    }
    auto promise = std::make_shared<litebus::Promise<Status>>();
    (void)litebus::Collect<Status>(futures).OnComplete(
        [nodeID(nodeID_), promise](const litebus::Future<std::list<Status>> &future) {
            if (future.IsError()) {
                promise->SetValue(Status(StatusCode::ERR_INNER_SYSTEM_ERROR,
                                         "evict instance may occur error, please check log of " + nodeID));
                return;
            }
            bool isError = false;
            auto result = Status::OK();
            for (auto status : future.Get()) {
                if (status.IsOk()) {
                    continue;
                }
                isError = true;
                result.AppendMessage(status.ToString());
            }
            if (isError) {
                promise->SetValue(Status(StatusCode::ERR_INNER_SYSTEM_ERROR, result.GetMessage()));
                return;
            }
            promise->SetValue(result);
        });
    return promise->GetFuture();
}

litebus::Future<Status> InstanceCtrlActor::EvictInstance(const std::string &instanceID,
                                                         const std::shared_ptr<messages::EvictAgentRequest> &req,
                                                         bool isEvictForReuse)
{
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_WARN("{}|instance({}) is not found, which should be evicted on agent({}).", req->requestid(), instanceID,
                   req->agentid());
        return Status::OK();
    }
    // Only evict running/creating/exiting/sub-health instance.
    // exiting: instance is going to be exited.
    // fatal: instance is exited.
    // sub-health: instance is sub-health or npu fault
    // the instance on other status, agent allocated is not confirmed.
    auto state = stateMachine->GetInstanceState();
    if (state == InstanceState::RUNNING || state == InstanceState::EVICTING || state == InstanceState::SUB_HEALTH) {
        /* If an instance is in the EXITING state, should retry to evict */
        return DoEvictInstance(stateMachine, instanceID, req, isEvictForReuse);
    }
    if (state == InstanceState::CREATING || state == InstanceState::EXITING) {
        auto promise = std::make_shared<litebus::Promise<Status>>();
        YRLOG_WARN(
            "instance({}) is not ready, which should be evicted on agent({}). waiting it to be running/evicted/fatal",
            instanceID, req->agentid());
        stateMachine->AddStateChangeCallback(
            { InstanceState::FATAL, InstanceState::RUNNING, InstanceState::EXITED, InstanceState::EVICTED },
            [aid(GetAID()), promise, stateMachine, instanceID, req,
             isEvictForReuse](const resources::InstanceInfo &instanceInfo) {
                auto status = instanceInfo.instancestatus().code();
                if (status == static_cast<uint32_t>(InstanceState::FATAL) ||
                    status == static_cast<uint32_t>(InstanceState::EXITED) ||
                    status == static_cast<uint32_t>(InstanceState::EVICTED)) {
                    promise->SetValue(Status::OK());
                    return;
                }
                promise->Associate(litebus::Async(aid, &InstanceCtrlActor::DoEvictInstance, stateMachine, instanceID,
                                                  req, isEvictForReuse));
            },
            "DoEvictInstance");
        return promise->GetFuture();
    }
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::DoEvictInstance(const std::shared_ptr<InstanceStateMachine> &stateMachine,
                                                           const std::string &instanceID,
                                                           const std::shared_ptr<messages::EvictAgentRequest> &req,
                                                           bool isEvictForReuse)
{
    uint32_t timeout = req->timeoutsec() == UINT32_MAX ? stateMachine->GetGracefulShutdownTime() : req->timeoutsec();
    auto future =
        stateMachine->GetInstanceState() == InstanceState::EVICTING
            ? litebus::Future<TransitionResult>(TransitionResult{})
            : TransInstanceState(stateMachine, TransContext{ InstanceState::EVICTING, stateMachine->GetVersion(),
                                                             "WARN: instance is going to be evicted", true,
                                                             StatusCode::ERR_INSTANCE_EVICTED });
    return future
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::ShutDownInstance, stateMachine->GetInstanceInfo(), timeout))
        .Then([aid(GetAID()), instanceId(instanceID)](const litebus::Future<Status> &future) {
            litebus::Async(aid, &InstanceCtrlActor::StopHeartbeat, instanceId);
            return Status::OK();
        })
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::KillRuntime, stateMachine->GetInstanceInfo(), false))
        .Then([aid(GetAID()), stateMachine, isEvictForReuse](const Status &status) -> litebus::Future<Status> {
            if (!isEvictForReuse) {
                return status;
            }
            return litebus::Async(aid, &InstanceCtrlActor::DeleteInstanceInResourceView, status,
                                  stateMachine->GetInstanceInfo());
        })
        .Then([aid(GetAID()), stateMachine](const Status &) {
            return litebus::Async(aid, &InstanceCtrlActor::TransInstanceState, stateMachine,
                                  TransContext{ InstanceState::EVICTED, stateMachine->GetVersion(),
                                                "WARN: instance is evicted", true, StatusCode::ERR_INSTANCE_EVICTED });
        })
        .Then([instanceID, req](const TransitionResult &result) -> litebus::Future<Status> {
            if (result.preState.IsNone()) {
                YRLOG_WARN("failed to transfer instance({}) on agent({}) to evicted.", instanceID, req->agentid());
            }
            return Status::OK();
        });
}

litebus::Option<std::shared_ptr<messages::ScheduleRequest>> InstanceCtrlActor::GetScheReqByInstID(
    const std::string instanceID)
{
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_ERROR("failed to get instance from instance control view");
        return {};
    }
    auto scheduleRequest = stateMachine->GetScheduleRequest();
    if (scheduleRequest == nullptr) {
        YRLOG_ERROR("failed to get scheduleRequest from stateMachine");
        return {};
    }
    return scheduleRequest;
}

litebus::Future<Status> InstanceCtrlActor::CheckFuncMeta(const litebus::Option<FunctionMeta> &funcMeta,
                                                         const std::shared_ptr<messages::ScheduleRequest> &request)
{
    if (funcMeta.IsSome()) {
        return Status::OK();
    }
    YRLOG_ERROR("{}|failed to get function meta of instance ({}) while rescheduling", request->requestid(),
                request->instance().instanceid());
    return Status(StatusCode::FAILED);
}

void InstanceCtrlActor::OnDriverEvent(const resource_view::InstanceInfo &instanceInfo)
{
    const std::string instanceID = instanceInfo.instanceid();
    const std::string jobID = instanceInfo.jobid();
    if (connectingDriver_.find(instanceID) != connectingDriver_.end()) {
        YRLOG_DEBUG("driver instance({}) of job({}) is connecting, ignore", instanceID, jobID);
        return;
    }
    connectingDriver_.insert(instanceID);
    YRLOG_DEBUG(
        "execute driver event callback function, create client "
        "for instance({}), job({})",
        instanceID, jobID);
    (void)AddTokenReference(std::make_pair(instanceInfo.tenantid(), instanceID));
    auto driverExitCb = [aid(GetAID()), instanceID, jobID]() {
        litebus::Async(aid, &InstanceCtrlActor::DeleteDriverClient, instanceID, jobID);
    };
    (void)CreateInstanceClient(instanceID, instanceInfo.runtimeid(), instanceInfo.runtimeaddress(), driverExitCb, true)
        .OnComplete(litebus::Defer(GetAID(), &InstanceCtrlActor::OnDriverConnected, _1, instanceInfo));
}

void InstanceCtrlActor::OnDriverConnected(
    const litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> &instanceClient,
    const resource_view::InstanceInfo &instanceInfo)
{
    (void)connectingDriver_.erase(instanceInfo.instanceid());
    if (instanceClient.IsOK() && instanceClient.Get() != nullptr) {
        YRLOG_INFO("succeed to create client for instance({}). {}", instanceInfo.instanceid(),
                   config_.enableServerMode ? "build heartbeat for client " : "driver client is connected ");
        if (config_.enableServerMode) {
            (void)StartHeartbeat(instanceInfo.instanceid(), 0, instanceInfo.runtimeid(), StatusCode::SUCCESS);
        }
        connectedDriver_[instanceInfo.instanceid()] = instanceInfo.jobid();
        return;
    }
    if (config_.enableServerMode) {
        YRLOG_INFO("failed to create client for instance({})", instanceInfo.instanceid());
        (void)DeleteDriverClient(instanceInfo.instanceid(), instanceInfo.jobid());
    }
}

void InstanceCtrlActor::BindObserver(const std::shared_ptr<function_proxy::ControlPlaneObserver> &observer)
{
    auto cancelHeartbeat = [aid(GetAID())]() {
        YRLOG_ERROR("failed to reconnect, stop sending heartbeat to functionAccessor.");
        (void)litebus::Async(aid, &InstanceCtrlActor::FcAccessorHeartbeatEnable, false);
    };

    ASSERT_IF_NULL(observer);
    observer->SetDriverEventCbFunc(
        [serverMode(config_.enableServerMode), aid(GetAID())](const resource_view::InstanceInfo &instanceInfo) {
            return litebus::Async(aid, &InstanceCtrlActor::OnDriverEvent, instanceInfo);
        });

    observer->SetInstanceInfoSyncerCbFunc([aid(GetAID())](const resource_view::RouteInfo &routeInfo) {
        YRLOG_DEBUG("{}|{}|execute instance info sync callback function", routeInfo.requestid(),
                    routeInfo.instanceid());
        return litebus::Async(aid, &InstanceCtrlActor::InstanceRouteInfoSyncer, routeInfo);
    });

    observer->SetUpdateFuncMetasFunc(
        [aid(GetAID())](bool isAdd, const std::unordered_map<std::string, FunctionMeta> &funcMetas) {
            YRLOG_DEBUG("update function meta, isAdd: {}, size: {}", isAdd, funcMetas.size());
            litebus::Async(aid, &InstanceCtrlActor::UpdateFuncMetas, isAdd, funcMetas);
        });

    observer->SetTrafficReportCbFunc([aid(GetAID())](const std::string &instanceID, const size_t &processingNum) {
        litebus::Async(aid, &InstanceCtrlActor::TrafficReport, instanceID, processingNum);
    });

    observer_ = observer;
    observer_->Attach(instanceControlView_);
}

void InstanceCtrlActor::DeleteDriverClient(const std::string &instanceID, const std::string &jobID)
{
    YRLOG_INFO("delete driver({}) client and job({})", instanceID, jobID);
    ASSERT_IF_NULL(observer_);
    ASSERT_IF_NULL(clientManager_);
    connectedDriver_.erase(instanceID);
    RemoveInternalTokenReference(instanceID);
    // Unregister from Traefik (async, non-blocking)
    if (traefikRegistry_) {
        (void)litebus::Async(GetAID(), &InstanceCtrlActor::UnregisterTraefikRoute, instanceID);
    }
    (void)observer_->DelInstance(instanceID)
        .After(OBSERVER_TIMEOUT_MS,
               [instanceID](litebus::Future<Status>) -> litebus::Future<Status> {
                   YRLOG_ERROR("timeout to delete driver instance({})", instanceID);
                   std::string errorMessage = "timeout to delete driver instance " + instanceID;
                   return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, errorMessage);
               })
        .Then([jobID, instanceID, aid(GetAID()),
               clientManager(clientManager_)](const Status &status) -> litebus::Future<KillResponse> {
            if (status.IsError()) {
                YRLOG_ERROR("failed to delete driver instance({}), error: {}", instanceID, status.ToString());
            }
            (void)clientManager->DeleteClient(instanceID);
            std::shared_ptr<KillRequest> killReq = GenKillRequest(jobID, SHUT_DOWN_SIGNAL_ALL);
            return litebus::Async(aid, &InstanceCtrlActor::KillInstancesOfJob, killReq)
                .OnComplete([jobID, instanceID](const litebus::Future<KillResponse> &future) {
                    if (future.IsError()) {
                        YRLOG_ERROR("failed to kill instances of instance({}) with job({}), errcode({})", instanceID,
                                    jobID, future.GetErrorCode());
                        return;
                    }
                    YRLOG_INFO("kill instances of instance({}) with job({}), response code({})", instanceID, jobID,
                               fmt::underlying(future.Get().code()));
                });
        });
}

litebus::Future<TransitionResult> InstanceCtrlActor::TransInstanceState(
    const std::shared_ptr<InstanceStateMachine> machine, const TransContext &context)
{
    if (machine->IsSaving()) {
        return machine->GetSavingFuture().Then(
            litebus::Defer(GetAID(), &InstanceCtrlActor::TransInstanceState, machine, context));
    }
    if (auto instance = machine->GetInstanceInfo();
        context.newState == InstanceState::FATAL && IsStaticFunctionInstance(instance)) {
        YRLOG_INFO("{}|static function instance is fatal, delete: {}", instance.requestid(), instance.instanceid());
        instance.mutable_instancestatus()->set_code(static_cast<int32_t>(context.newState));
        instance.mutable_instancestatus()->set_errcode(context.errCode);
        instance.mutable_instancestatus()->set_msg(context.msg);
        return observer_->PutInstanceEvent(instance, false, GetModRevisionFromInstanceInfo(instance))
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeleteInstanceForStaticFunction, instance))
            .Then([machine](const Status &) -> TransitionResult {
                return TransitionResult{ machine->GetInstanceState(), {}, {}, 0, Status::OK() };
            });
    }
    return machine->TransitionTo(context).Then(
        [machine, nodeID(nodeID_), context, isTraefikEnable(traefikRegistry_ != nullptr),
         aid(GetAID())](const TransitionResult &result) -> litebus::Future<TransitionResult> {
            // transition successful
            if (result.status.IsOk()) {
                // if successfully, need to update state for observer and execute callback
                machine->ExecuteStateChangeCallback(machine->GetRequestID(), context.newState);
                // Register to Traefik when instance enters RUNNING state (async, non-blocking)
                if (context.newState == InstanceState::RUNNING && isTraefikEnable) {
                    const auto &instanceInfo = machine->GetInstanceInfo();
                    (void)litebus::Async(aid, &InstanceCtrlActor::RegisterTraefikRoute, instanceInfo);
                }
                return result;
            }
            // transition failed but local state is changed which need to roll back
            if (machine->GetVersion() == result.previousInfo.version() + 1) {
                machine->UpdateInstanceInfo(result.previousInfo);
            }
            // txn failed but get response
            if (!result.savedInfo.functionproxyid().empty()) {
                machine->UpdateInstanceInfo(result.savedInfo);
                // owner has changed to another node
                if (result.savedInfo.functionproxyid() != nodeID) {
                    machine->SetVersion(0);
                }
                // the status of info from metastore owned current node same as we wanted, return ok.
                if (result.savedInfo.functionproxyid() == nodeID
                    && result.savedInfo.instancestatus().code() == static_cast<int32_t>(context.newState)) {
                    auto ret = result;
                    ret.status = Status::OK();
                    machine->ExecuteStateChangeCallback(machine->GetRequestID(), context.newState);
                    if (context.newState == InstanceState::RUNNING && isTraefikEnable) {
                        const auto& instanceInfo = machine->GetInstanceInfo();
                        YRLOG_INFO("TransInstanceState: triggering Traefik register (path=txn_recovery), instanceID={}",
                                   instanceInfo.instanceid());
                        (void)litebus::Async(aid, &InstanceCtrlActor::RegisterTraefikRoute, instanceInfo);
                    }
                    return ret;
                }
            }
            return result;
        });
}

litebus::Future<Status> InstanceCtrlActor::TryExitInstance(const std::shared_ptr<InstanceStateMachine> stateMachine,
                                                           const std::shared_ptr<KillContext> &killCtx,
                                                           bool isSynchronized)
{
    if (stateMachine->IsSaving()) {
        return stateMachine->GetSavingFuture().Then(
            litebus::Defer(GetAID(), &InstanceCtrlActor::TryExitInstance, stateMachine, killCtx, isSynchronized));
    }

    auto promise = std::make_shared<litebus::Promise<Status>>();
    (void)stateMachine->TryExitInstance(promise, killCtx, isSynchronized)
        .Then([machine(stateMachine)](litebus::Future<Status> statusFuture) {
            if (statusFuture.IsOK()) {
                machine->ExecuteStateChangeCallback(machine->GetRequestID(), InstanceState::EXITING);
            }
            return statusFuture;
        });
    return promise->GetFuture();
}

void InstanceCtrlActor::SetAbnormal()
{
    isAbnormal_ = true;
    instanceControlView_->SetLocalAbnormal();
}

void InstanceCtrlActor::BindInternalIAM(const std::shared_ptr<function_proxy::InternalIAM> &internalIAM)
{
    ASSERT_IF_NULL(internalIAM);
    internalIAM_ = internalIAM;
    if (!internalIAM_->IsIAMEnabled()) {
        return;
    }
    internalIAM_->RegisterUpdateTokenCallback(
        [aid(GetAID())](const std::string &tenantID, const std::string &token, const std::string &salt) {
            litebus::Async(aid, &InstanceCtrlActor::UpdateInternalToken, tenantID, token, salt);
        });
    internalIAM_->RegisterUpdateCredentialCallback(
        [aid(GetAID())](const std::string &tenantID, const std::shared_ptr<AKSKContent> &akSk) {
            litebus::Async(aid, &InstanceCtrlActor::UpdateInternalAkSk, tenantID, akSk);
        });
}

litebus::Future<Status> InstanceCtrlActor::AddCredToDeployInstanceReq(
    const std::string &tenantID, const std::shared_ptr<messages::DeployInstanceRequest> &deployInstanceReq)
{
    deployInstanceReq->set_enableservermode(config_.enableServerMode);
    deployInstanceReq->set_posixport(config_.posixPort);
    deployInstanceReq->set_dposixudspath(config_.udsPath);
    deployInstanceReq->set_tenantid(tenantID);
    ASSERT_IF_NULL(internalIAM_);
    if (!internalIAM_->IsIAMEnabled()) {
        deployInstanceReq->set_enableauthservercert(config_.enableSSL);
        deployInstanceReq->set_serverauthtoken("");
        deployInstanceReq->set_serverrootcertdata(config_.serverRootCert);
        deployInstanceReq->set_servernameoverride(config_.serverNameOverride);
        (void)AddTokenReference(std::make_pair(tenantID, deployInstanceReq->instanceid()));
        return Status::OK();
    }

    auto credType = internalIAM_->GetCredType();
    if (credType == function_proxy::IAMCredType::TOKEN) {
        return internalIAM_->RequireEncryptToken(tenantID)
            .Then([config(config_), tenantID, deployInstanceReq](
                      const std::shared_ptr<TokenSalt> &tokenSalt) -> std::pair<std::string, std::string> {
                if (tokenSalt->status.IsError()) {
                    return std::make_pair("", "");
                }

                deployInstanceReq->set_enableauthservercert(config.enableSSL);
                deployInstanceReq->set_serverauthtoken(tokenSalt->token);
                deployInstanceReq->set_salt(tokenSalt->salt);
                deployInstanceReq->set_serverrootcertdata(config.serverRootCert);
                deployInstanceReq->set_servernameoverride(config.serverNameOverride);
                return std::make_pair(tenantID, deployInstanceReq->instanceid());
            })
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::AddTokenReference, std::placeholders::_1));
    } else if (credType == function_proxy::IAMCredType::AK_SK) {
        return internalIAM_->RequireCredentialByTenantID(tenantID)
            .Then([tenantID, deployInstanceReq](
                      const std::shared_ptr<AKSKContent> &akskContent) -> std::pair<std::string, std::string> {
                if (akskContent == nullptr || !akskContent->IsValid()) {
                    YRLOG_ERROR("{}|instance({}) gets invalid ak/sk content", deployInstanceReq->requestid(),
                                deployInstanceReq->instanceid());
                    return std::make_pair("", "");
                }

                auto tenantCred = deployInstanceReq->mutable_tenantcredentials();
                tenantCred->set_accesskey(akskContent->accessKey);
                tenantCred->set_secretkey(
                    std::string(akskContent->secretKey.GetData(), akskContent->secretKey.GetSize()));
                tenantCred->set_datakey(
                    std::string(akskContent->dataKey.GetData(), akskContent->dataKey.GetSize()));
                tenantCred->set_iscredential(true);
                return std::make_pair(tenantID, deployInstanceReq->instanceid());
            })
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::AddTokenReference, std::placeholders::_1));
    }
    YRLOG_ERROR("{}|instance({}) has invalid cred type ({})", deployInstanceReq->requestid(),
                deployInstanceReq->instanceid(), static_cast<uint32_t>(credType));
    return Status(StatusCode::FAILED, "invalid cred type");
}

void InstanceCtrlActor::AddDsAuthToDeployInstanceReq(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleRequest,
    const std::shared_ptr<messages::DeployInstanceRequest> &deployInstanceReq)
{
    deployInstanceReq->set_runtimedsauthenable(config_.runtimeConfig.runtimeDsAuthEnable);
    deployInstanceReq->set_runtimedsencryptenable(config_.runtimeConfig.runtimeDsEncryptEnable);
    if (scheduleRequest->instance().issystemfunc()) {
        deployInstanceReq->set_accesskey(config_.runtimeConfig.dataSystemAccessKey);
        deployInstanceReq->set_securitykey(std::string(config_.runtimeConfig.dataSystemSecurityKey.GetData(),
                                                       config_.runtimeConfig.dataSystemSecurityKey.GetSize()));
    }
    deployInstanceReq->set_runtimedsclientpublickey(
        std::string(config_.runtimeConfig.runtimeDsClientPublicKey.GetData(),
                    config_.runtimeConfig.runtimeDsClientPublicKey.GetSize()));
    deployInstanceReq->set_runtimedsserverpublickey(
        std::string(config_.runtimeConfig.runtimeDsServerPublicKey.GetData(),
                    config_.runtimeConfig.runtimeDsServerPublicKey.GetSize()));
    deployInstanceReq->set_runtimedsclientprivatekey(
        std::string(config_.runtimeConfig.runtimeDsClientPrivateKey.GetData(),
                    config_.runtimeConfig.runtimeDsClientPrivateKey.GetSize()));
}

void InstanceCtrlActor::RemoveInternalTokenReference(const std::string &instanceID)
{
    ASSERT_IF_NULL(internalIAM_);
    internalIAM_->Delete(instanceID);
    if (!internalIAM_->IsIAMEnabled()) {
        return;
    }
    std::string tenantID;
    for (auto ite : internalCredReferenceMap_) {
        if (ite.second.find(instanceID) != ite.second.end()) {
            tenantID = ite.first;
            break;
        }
    }
    if (tenantID.empty()) {
        return;
    }
    if (internalCredReferenceMap_.find(tenantID) != internalCredReferenceMap_.end()) {
        (void)internalCredReferenceMap_[tenantID].erase(instanceID);
        if (internalCredReferenceMap_[tenantID].empty()) {
            YRLOG_INFO("{}|gonna remove internal token", tenantID);
            internalIAM_->AbandonTokenByTenantID(tenantID);
            internalIAM_->AbandonCredentialByTenantID(tenantID);
            (void)internalCredReferenceMap_.erase(tenantID);
        }
    }
}

void InstanceCtrlActor::UpdateInternalToken(const std::string &tenantID, const std::string &token,
                                            const std::string &salt)
{
    ASSERT_IF_NULL(internalIAM_);
    if (!internalIAM_->IsIAMEnabled()) {
        return;
    }
    if (internalCredReferenceMap_.find(tenantID) == internalCredReferenceMap_.end()) {
        YRLOG_WARN("{}|do not have reference of token", tenantID);
        return;
    }
    for (auto instanceIte : internalCredReferenceMap_[tenantID]) {
        auto stateMachine = instanceControlView_->GetInstance(instanceIte.data());
        if (stateMachine == nullptr) {
            YRLOG_ERROR("{}|{}|StateMachine is null", tenantID, instanceIte.data());
            continue;
        }
        auto instanceInfo = stateMachine->GetInstanceInfo();
        UpdateInstanceToken(instanceInfo.runtimeid(), instanceInfo.functionagentid(), token, salt);
    }
}

void InstanceCtrlActor::UpdateInternalAkSk(const std::string &tenantID, const std::shared_ptr<AKSKContent> &akskContent)
{
    ASSERT_IF_NULL(internalIAM_);
    if (!internalIAM_->IsIAMEnabled()) {
        return;
    }
    if (internalCredReferenceMap_.find(tenantID) == internalCredReferenceMap_.end()) {
        YRLOG_WARN("do not have reference of aksk");
        return;
    }

    for (auto instanceIte : internalCredReferenceMap_[tenantID]) {
        auto stateMachine = instanceControlView_->GetInstance(instanceIte.data());
        if (stateMachine == nullptr) {
            YRLOG_ERROR("{}|stateMachine is null", instanceIte.data());
            continue;
        }
        auto instanceInfo = stateMachine->GetInstanceInfo();
        UpdateInstanceAkSk(instanceInfo.functionagentid(), akskContent);
    }
}

void InstanceCtrlActor::UpdateInstanceToken(const std::string &runtimeID, const std::string &agentID,
                                            const std::string &token, const std::string &salt)
{
    auto request = std::make_shared<messages::UpdateCredRequest>();
    auto requestID = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    request->set_requestid(requestID);
    request->set_runtimeid(runtimeID);
    request->set_token(token);
    request->set_salt(salt);
    (void)functionAgentMgr_->UpdateCred(agentID, request)
        .OnComplete([requestID](const litebus::Future<messages::UpdateCredResponse> &response) {
            if (response.IsError()) {
                YRLOG_ERROR("{}|update token failed, err: {}", requestID, response.GetErrorCode());
                return;
            }
            auto code = response.Get().code();
            auto msg = response.Get().message();
            if (code != static_cast<int>(StatusCode::SUCCESS)) {
                YRLOG_ERROR("{}|update token failed, errcode: {}, errmsg: {}", requestID, code, msg);
                return;
            }
            YRLOG_INFO("{}|update token success!", requestID);
        });
}

void InstanceCtrlActor::UpdateInstanceAkSk(const std::string &agentID, const std::shared_ptr<AKSKContent> &akskContent)
{
    auto request = std::make_shared<messages::UpdateCredRequest>();
    auto requestID = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    request->set_requestid(requestID);
    request->mutable_tenantcredentials()->set_accesskey(akskContent->accessKey);
    request->mutable_tenantcredentials()->set_secretkey(
        std::string(akskContent->secretKey.GetData(), akskContent->secretKey.GetSize()));
    request->mutable_tenantcredentials()->set_datakey(
        std::string(akskContent->dataKey.GetData(), akskContent->dataKey.GetSize()));

    (void)functionAgentMgr_->UpdateCred(agentID, request)
        .OnComplete([requestID](const litebus::Future<messages::UpdateCredResponse> &response) {
            if (response.IsError()) {
                YRLOG_ERROR("{}|update ak sk failed, err: {}", requestID, response.GetErrorCode());
                return;
            }
            auto code = response.Get().code();
            auto msg = response.Get().message();
            if (code != static_cast<int>(StatusCode::SUCCESS)) {
                YRLOG_ERROR("{}|update ak sk failed, errcode: {}, errmsg: {}", requestID, code, msg);
                return;
            }
            YRLOG_INFO("{}|update ak sk success!", requestID);
        });
}

litebus::Future<Status> InstanceCtrlActor::AddTokenReference(const std::pair<std::string, std::string> &tokenReferPair)
{
    if (internalIAM_ == nullptr) {
        return Status::OK();
    }
    // for posix service to verify instance exist, if iam is not enabled, only add instance to iam
    if (!internalIAM_->IsIAMEnabled()) {
        internalIAM_->Insert(tokenReferPair.second);
        return Status::OK();
    }
    if (!tokenReferPair.first.empty()) {
        (void)internalCredReferenceMap_[tokenReferPair.first].emplace(tokenReferPair.second);
        internalIAM_->Insert(tokenReferPair.second);
        return Status::OK();
    }
    return Status(StatusCode::ERR_AUTHORIZE_FAILED, "require token error");
}

void InstanceCtrlActor::NotifyDsHealthy(bool healthy)
{
    if (healthy) {
        return;
    }
    YRLOG_WARN("ready to set instance fatal because of dsworker unhealthy");
    // we should support reschedule instance to another node in the future
    auto doEvict = [aid(GetAID())](const std::shared_ptr<InstanceStateMachine> &stateMachine,
                                   const resources::InstanceInfo &instanceInfo) {
        // Static functions are evicted by the platform, such as ers.
        if (IsStaticFunctionInstance(stateMachine->GetInstanceInfo())) {
            // 1. If static function and used stream, do evict.
            if (IsDataSystemFeatureUsedStream(stateMachine->GetInstanceInfo())) {
                (void)litebus::Async(aid, &InstanceCtrlActor::TransInstanceState, stateMachine,
                                     TransContext{ InstanceState::SUB_HEALTH, stateMachine->GetVersion(),
                                                   stateMachine->Information() + "sub_health: ds worker is not healthy",
                                                   true, ERR_DS_UNAVAILABLE });
            }
            // 2. If static function and not used stream, do not evict.
        } else {
            (void)litebus::Async(aid, &InstanceCtrlActor::TransInstanceState, stateMachine,
                                 TransContext{ InstanceState::FATAL, stateMachine->GetVersion(),
                                               stateMachine->Information() + "fatal: ds worker is not healthy", true,
                                               ERR_INSTANCE_EVICTED })
                .Then(litebus::Defer(aid, &InstanceCtrlActor::KillRuntime, instanceInfo, false))
                .Then(litebus::Defer(aid, &InstanceCtrlActor::DeleteInstanceInResourceView, std::placeholders::_1,
                                     instanceInfo));
        }
    };

    auto doingInstances = instanceControlView_->GetInstancesWithStatus(InstanceState::SCHEDULING);
    auto creatingInstances = instanceControlView_->GetInstancesWithStatus(InstanceState::CREATING);
    for (auto &[instanceID, instanceInfo] : creatingInstances) {
        doingInstances[instanceID] = instanceInfo;
    }
    for (auto &instance : doingInstances) {
        auto instanceID = instance.first;
        auto stateMachine = instanceControlView_->GetInstance(instanceID);
        if (!stateMachine) {
            continue;
        }
        stateMachine->AddStateChangeCallback(
            { InstanceState::RUNNING },
            [nodeID(nodeID_), stateMachine, doEvict](const resources::InstanceInfo &instanceInfo) {
                if (instanceInfo.functionproxyid() != nodeID) {
                    return;
                }
                doEvict(stateMachine, instanceInfo);
            }, "SetFatal");
    }

    auto runningInstances = instanceControlView_->GetInstancesWithStatus(InstanceState::RUNNING);
    for (auto &[instanceID, instanceInfo] : runningInstances) {
        auto stateMachine = instanceControlView_->GetInstance(instanceID);
        if (!stateMachine) {
            continue;
        }
        if (IsDriver(stateMachine->GetInstanceInfo())) {
            YRLOG_DEBUG("{}|skip to evict running driver instance({})", stateMachine->GetRequestID(),
                        stateMachine->GetInstanceInfo().instanceid());
            continue;
        }
        (void)doEvict(stateMachine, instanceInfo);
    }
}

litebus::Future<litebus::Option<litebus::AID>> InstanceCtrlActor::GetLocalSchedulerAID(const std::string &instanceID)
{
    auto promise = std::make_shared<litebus::Promise<litebus::Option<litebus::AID>>>();
    RetryGetLocalSchedulerAID(instanceID, promise, 0);
    return promise->GetFuture();
}

void InstanceCtrlActor::SetGetLocalInterval(uint64_t interval)
{
    g_getLocalSchedulerInterval = interval;
}

void InstanceCtrlActor::RetryGetLocalSchedulerAID(
    const std::string &instanceID, const std::shared_ptr<litebus::Promise<litebus::Option<litebus::AID>>> &promise,
    const uint32_t retryTimes)
{
    if (retryTimes > config_.maxGetLocalAidTimes) {
        YRLOG_ERROR("failed to get AID of instance({}) after try {} times", instanceID, config_.maxGetLocalAidTimes);
        promise->SetValue(litebus::Option<litebus::AID>());
        return;
    }

    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        (void)litebus::AsyncAfter(g_getLocalSchedulerInterval, GetAID(), &InstanceCtrlActor::RetryGetLocalSchedulerAID,
                                  instanceID, promise, retryTimes + 1);
        return;
    }

    std::string proxyID = stateMachine->GetOwner();
    RETURN_IF_NULL(observer_);
    (void)observer_->GetLocalSchedulerAID(proxyID).Then([aid(GetAID()), promise, proxyID, instanceID,
                                                         retryTimes](const litebus::Option<litebus::AID> &localAid) {
        if (localAid.IsNone()) {
            YRLOG_INFO("failed to get local scheduler({}) AID, retrying...", proxyID);
            (void)litebus::AsyncAfter(g_getLocalSchedulerInterval, aid, &InstanceCtrlActor::RetryGetLocalSchedulerAID,
                                      instanceID, promise, retryTimes + 1);
        } else {
            promise->SetValue(localAid);
        }
        return Status::OK();
    });
}

bool InstanceCtrlActor::IsInstanceRunning(const std::string &instanceID)
{
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_DEBUG("failed to find instance({})", instanceID);
        return false;
    }
    return stateMachine->GetInstanceState() == InstanceState::RUNNING;
}

void InstanceCtrlActor::SetNodeLabelsToMetricsContext(const std::string &functionAgentID,
                                                      std::map<std::string, resources::Value::Counter> nodeLabels)
{
    if (nodeLabels.empty()) {
        return;
    }

    ASSERT_IF_NULL(observer_);
    (void)observer_->GetAgentInstanceInfoByID(functionAgentID)
        .OnComplete([functionAgentID, nodeLabels](
                const litebus::Future<litebus::Option<function_proxy::InstanceInfoMap>> &future) {
            ASSERT_FS(future.IsOK());
            auto opt = future.Get();
            if (opt.IsNone()) {
                return;
            }
            for (auto &instance : opt.Get()) {
                auto agentID = instance.second.functionagentid();
                if (functionAgentID == agentID) {
                    functionsystem::metrics::NodeLabelsType nodeLabelsMap;
                    for (const auto &[key, value] : nodeLabels) {
                        std::map<std::string, uint64_t> itemsMap;
                        for (const auto &ite : value.items()) {
                            itemsMap[ite.first] = ite.second;
                        }
                        nodeLabelsMap[key] = itemsMap;
                    }
                    functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetBillingNodeLabels(
                        instance.second.instanceid(), nodeLabelsMap);
                }
                return;
            }
        });
}
litebus::Future<KillResponse> InstanceCtrlActor::KillGroup(const std::string &srcInstanceID,
                                                           const std::shared_ptr<KillRequest> &killReq)
{
    auto killGroup = std::make_shared<messages::KillGroup>();
    killGroup->set_groupid(killReq->instanceid());
    killGroup->set_srcinstanceid(srcInstanceID);
    killGroup->set_signal(killReq->signal());
    ASSERT_IF_NULL(localSchedSrv_);
    return localSchedSrv_->KillGroup(killGroup).Then([](const Status &status) {
        KillResponse response;
        response.set_code(Status::GetPosixErrorCode(status.StatusCode()));
        response.set_message(status.GetMessage());
        return response;
    });
}

void InstanceCtrlActor::PrepareParam(const FunctionMeta &funcMeta,
                                     const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    const std::string traceID = scheduleReq->traceid();
    const std::string requestID = scheduleReq->requestid();
    auto isSystemFunc = funcMeta.funcMetaData.isSystemFunc;
    scheduleReq->mutable_instance()->set_storagetype(funcMeta.codeMetaData.storageType);

    if (isSystemFunc) {
        YRLOG_DEBUG("{}|{}|Add require args for system function", traceID, requestID);
        (*scheduleReq->mutable_instance()->mutable_createoptions())[RESOURCE_OWNER_KEY] = SYSTEM_OWNER_VALUE;
        scheduleReq->mutable_instance()->set_issystemfunc(true);
    }

    auto resourceSelector = scheduleReq->mutable_instance()->mutable_scheduleoption()->mutable_resourceselector();
    if (resourceSelector->find(RESOURCE_OWNER_KEY) == resourceSelector->end()) {
        (*scheduleReq->mutable_instance()->mutable_scheduleoption()->mutable_resourceselector())[RESOURCE_OWNER_KEY] =
            NeedCreateAgent(scheduleReq->instance()) ? litebus::uuid_generator::UUID::GetRandomUUID().ToString()
                                                     : DEFAULT_OWNER_VALUE;
    }
}

Status InstanceCtrlActor::CheckParam(const Status &authorizeStatus,
    const litebus::Option<FunctionMeta> &functionMeta, const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    const std::string traceID = scheduleReq->traceid();
    const std::string requestID = scheduleReq->requestid();
    if (authorizeStatus.IsError()) {
        YRLOG_ERROR("{}|{}|authorize failed.", traceID, requestID);
        return Status(StatusCode::ERR_AUTHORIZE_FAILED, "authorize failed");
    }
    if (functionMeta.IsNone()) {
        YRLOG_ERROR("{}|{}|failed to find function meta for schedule.", traceID, requestID);
        return Status(StatusCode::ERR_FUNCTION_META_NOT_FOUND, "failed to find function meta");
    }
    auto funcMeta = functionMeta.Get();
    if (!funcMeta.funcMetaData.isSystemFunc) {
        auto status = VerifyTenantID(scheduleReq, traceID, requestID);
        if (status.StatusCode() != StatusCode::SUCCESS) {
            return status;
        }
        if (config_.enableTenantAffinity &&
            scheduleReq->instance().scheduleoption().schedpolicyname() != MONOPOLY_SCHEDULE) {
            // Set tenant affinity scheduling labels after setting the tenant ID.
            SetTenantAffinityOpt(scheduleReq);
            YRLOG_DEBUG("{}|after SetTenantAffinityOpt, scheduleReq:{}", scheduleReq->requestid(),
                        scheduleReq->ShortDebugString());
        }
    }
    PrepareParam(funcMeta, scheduleReq);

    if (auto status = CheckSchedRequestValid(scheduleReq); status.IsError()) {
        YRLOG_ERROR("{}|{}|schedule request is invalid.", traceID, requestID);
        auto errorMessage = status.GetMessage();
        return Status(status.StatusCode(), errorMessage.substr(1, errorMessage.length() - ERROR_MESSAGE_SEPARATE));
    }

    if (scheduleReq->instance().jobid().empty()) {
        auto jobID = GenerateJobIDFromTraceID(traceID);
        if (jobID.empty()) {
            YRLOG_WARN("{}|{}|jobID is empty", traceID, requestID);
        }
        scheduleReq->mutable_instance()->set_jobid(jobID);
    }
    ASSERT_IF_NULL(instanceControlView_);
    auto genStatus = instanceControlView_->TryGenerateNewInstance(scheduleReq);
    if (genStatus.instanceID.empty()) {
        YRLOG_ERROR("{}|{}|failed to generate instanceID", scheduleReq->traceid(), scheduleReq->requestid());
        return Status(StatusCode::ERR_INSTANCE_INFO_INVALID, "failed to generate instance ID");
    }
    if (genStatus.isDuplicate) {
        return Status(StatusCode::ERR_INSTANCE_DUPLICATED,
                      "you are not allowed to create instance with the same instance id, please kill first " +
                          scheduleReq->instance().instanceid());
    }
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::TransScheduling(
    const Status &authorizeStatus, const litebus::Option<FunctionMeta> &functionMeta,
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    if (auto status = CheckParam(authorizeStatus, functionMeta, scheduleReq); status.IsError()) {
        return status;
    }
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(scheduleReq->instance().instanceid());
    if (stateMachine == nullptr) {
        return Status(StatusCode::ERR_INSTANCE_EXITED, "instance may already have been killed");
    }
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(
        static_cast<int32_t>(InstanceState::SCHEDULING));
    // we can not change stateMachine directly while scheduling,
    // all change in scheduleReq and stateMachine update by scheduleReq
    stateMachine->UpdateScheduleReq(std::make_shared<messages::ScheduleRequest>(*scheduleReq));
    ASSERT_IF_NULL(observer_);
    observer_->PutInstanceEvent(stateMachine->GetInstanceInfo(), false, 0);
    // Range schedule should clear owner, The owner must be clear to prevent subsequent state machine update failures.
    stateMachine->ReleaseOwner();
    // should not to be persisted, but considered as normal, need to be watched
    observer_->WatchInstance(scheduleReq->instance().instanceid());
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::ToScheduling(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    if (!scheduleReq->instance().parentid().empty()) {
        auto stateMachine = instanceControlView_->GetInstance(scheduleReq->instance().parentid());
        if (stateMachine != nullptr && stateMachine->GetInstanceState() == InstanceState::EXITING) {
            return Status(StatusCode::ERR_INSTANCE_EXITED,
                          "you are not allowed to create instance because of you are exiting");
        }
        if (stateMachine != nullptr && IsFrontendFunction(stateMachine->GetInstanceInfo().function())) {
            (*scheduleReq->mutable_instance()->mutable_extensions())[CREATE_SOURCE] = FRONTEND_STR;
        }
    }
    if (!scheduleReq->instance().instanceid().empty()) {
        auto stateMachine = instanceControlView_->GetInstance(scheduleReq->instance().instanceid());
        if (scheduleReq->instance().instancestatus().code() == static_cast<uint32_t>(InstanceState::NEW) &&
            stateMachine != nullptr) {
            return Status(StatusCode::ERR_INSTANCE_DUPLICATED,
                          "you are not allowed to create instance with the same instance id, please kill first " +
                              scheduleReq->instance().instanceid());
        }
    }
    ASSERT_IF_NULL(observer_);
    YRLOG_INFO("{}|{}|ready to scheduling instance, instance version({})", scheduleReq->traceid(),
               scheduleReq->requestid(), scheduleReq->instance().version());
    if (isAbnormal_) {
        return Status(StatusCode::ERR_LOCAL_SCHEDULER_ABNORMAL, "local is already abnormal");
    }
    scheduleReq->mutable_instance()->set_parentfunctionproxyaid(GetAID());
    // Check whether the function meta information corresponding to requestID exists.
    // runtimePromise for compatibility
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    return GetFuncMeta(scheduleReq->instance().function())
        .Then([aid(GetAID()), scheduleReq, runtimePromise](const litebus::Option<FunctionMeta> &functionMeta) {
            return litebus::Async(aid, &InstanceCtrlActor::AuthorizeCreate, functionMeta, scheduleReq, runtimePromise)
                .Then(litebus::Defer(aid, &InstanceCtrlActor::GetAffinity, _1, scheduleReq))
                .Then(litebus::Defer(aid, &InstanceCtrlActor::TransScheduling, _1, functionMeta, scheduleReq));
        });
}

// for compatibility
Status InstanceCtrlActor::FetchedFunctionMeta(const litebus::Option<FunctionMeta> &functionMeta,
                                              const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    if (functionMeta.IsNone()) {
        return Status(StatusCode::ERR_FUNCTION_META_NOT_FOUND, "function meta not found");
    }
    return Status::OK();
}

litebus::Future<TransitionResult> InstanceCtrlActor::ToTransCreating(
    const std::shared_ptr<InstanceStateMachine> &stateMachineRef,
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    auto transContext = TransContext{ InstanceState::CREATING, stateMachineRef->GetVersion(), "creating" };
    transContext.scheduleReq = scheduleReq;
    if (funcMetaMap_.find(scheduleReq->instance().function()) == funcMetaMap_.end()) {
        YRLOG_WARN("{}|{}|instance({}) function meta not found. need to fetch meta.", scheduleReq->traceid(),
                   scheduleReq->requestid(), scheduleReq->instance().instanceid());
        ASSERT_IF_NULL(observer_);
        return GetFuncMeta(scheduleReq->instance().function())
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::FetchedFunctionMeta, _1, scheduleReq))
            .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::TransInstanceState, stateMachineRef, transContext));
    }
    return TransInstanceState(stateMachineRef, transContext);
}

litebus::Future<Status> InstanceCtrlActor::ToCreating(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
                                                      const ScheduleResult &result)
{
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachineRef = instanceControlView_->GetInstance(scheduleReq->instance().instanceid());
    if (stateMachineRef == nullptr) {
        YRLOG_DEBUG("{}|{} failed to get stateMachine, instanceID({}), requestID({})", scheduleReq->traceid(),
                    scheduleReq->requestid(), scheduleReq->instance().instanceid(),
                    scheduleReq->instance().requestid());
        instanceControlView_->GenerateStateMachine(scheduleReq->instance().instanceid(), scheduleReq->instance());
        stateMachineRef = instanceControlView_->GetInstance(scheduleReq->instance().instanceid());
    }
    if (stateMachineRef->GetInstanceState() == InstanceState::CREATING ||
        stateMachineRef->GetInstanceState() == InstanceState::RUNNING) {
        YRLOG_INFO("{}|{}|instance({}) already been created or running", scheduleReq->traceid(),
                   scheduleReq->requestid(), scheduleReq->instance().instanceid());
        return Status::OK();
    }
    YRLOG_DEBUG("{}|{}|start deploy instance({}) to function agent({})", scheduleReq->traceid(),
                scheduleReq->requestid(), scheduleReq->instance().instanceid(), result.id);
    MergeScheduleResultToRequest(scheduleReq, result);
    scheduleReq->mutable_instance()->set_datasystemhost(config_.cacheStorageHost);
    scheduleReq->mutable_instance()->set_functionproxyid(nodeID_);
    SetGracefulShutdownTime(scheduleReq);
    auto status = std::make_shared<litebus::Promise<Status>>();
    ToTransCreating(stateMachineRef, scheduleReq)
        .Then([status, result,
               stateMachineRef](const TransitionResult &transResult) -> litebus::Option<TransitionResult> {
            if (transResult.version != 0) {
                status->SetValue(Status(static_cast<StatusCode>(result.code), result.reason));
                return litebus::None();
            }
            if (transResult.savedInfo.functionproxyid().empty()) {
                YRLOG_ERROR("failed to update state of instance({}), err: {}", transResult.previousInfo.instanceid(),
                            transResult.status.GetMessage());
                status->SetValue(
                    Status(StatusCode::ERR_ETCD_OPERATION_ERROR,
                           "failed to update instance info to creating, err: " + transResult.status.GetMessage()));
                return transResult;
            }
            YRLOG_INFO("failed to update instance info, instance({}) is on local scheduler({})",
                       transResult.savedInfo.instanceid(), transResult.savedInfo.functionproxyid());
            // status is error, instance will be deleted forced
            status->SetValue(Status(StatusCode::ERR_INSTANCE_DUPLICATED, "instance is scheduled to another node"));
            return transResult;
        })
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeployInstance, scheduleReq, 0, _1, false))
        .OnComplete(litebus::Defer(GetAID(), &InstanceCtrlActor::ScheduleEnd, _1, scheduleReq));
    return status->GetFuture();
}

void InstanceCtrlActor::RegisterReadyCallback(const std::string &instanceID,
                                              const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
                                              InstanceReadyCallBack callback)
{
    ASSERT_IF_NULL(instanceControlView_);
    ASSERT_IF_NULL(callback);
    YRLOG_INFO("{}|{}|register callback for instance({})", scheduleReq->traceid(), scheduleReq->requestid(),
               instanceID);
    instanceRegisteredReadyCallback_[scheduleReq->requestid()] = callback;
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        instanceControlView_->GenerateStateMachine(instanceID, scheduleReq->instance());
        stateMachine = instanceControlView_->GetInstance(instanceID);
    }
    if (stateMachine->GetInstanceState() == InstanceState::RUNNING) {
        callback(Status::OK());
        return;
    }
    if (stateMachine->GetInstanceState() == InstanceState::FATAL) {
        auto instance = stateMachine->GetInstanceInfo();
        callback(Status(static_cast<StatusCode>(instance.instancestatus().code()), instance.instancestatus().msg()));
        return;
    }
    stateMachine->AddStateChangeCallback(
        { InstanceState::RUNNING, InstanceState::FATAL },
        [callback](const resources::InstanceInfo &instance) {
            if (instance.instancestatus().code() == static_cast<int32_t>(InstanceState::RUNNING)) {
                callback(Status::OK());
            }
            if (instance.instancestatus().code() == static_cast<int32_t>(InstanceState::FATAL)) {
                callback(
                    Status(static_cast<StatusCode>(instance.instancestatus().code()), instance.instancestatus().msg()));
            }
            return;
        },
        "ReadyCallback");
}

litebus::Future<Status> InstanceCtrlActor::DeleteSchedulingInstance(const std::string &instanceID,
                                                                    const std::string &requestID)
{
    (void)instanceRegisteredReadyCallback_.erase(requestID);
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_WARN("failed to get instance machine {}", instanceID);
        return Status::OK();
    }
    auto instanceInfo = stateMachine->GetInstanceInfo();
    if (instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::SCHEDULING)) {
        ASSERT_IF_NULL(observer_);
        observer_->DelInstanceEvent(instanceID, stateMachine->GetModRevision());
    }
    // Unregister from Traefik (async, non-blocking)
    if (traefikRegistry_) {
        (void)litebus::Async(GetAID(), &InstanceCtrlActor::UnregisterTraefikRoute, instanceID);
    }
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::ForceDeleteInstance(const std::string &instanceID)
{
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        return Status::OK();
    }
    auto instanceInfo = stateMachine->GetInstanceInfo();
    (void)instanceRegisteredReadyCallback_.erase(instanceInfo.requestid());
    if (instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::CREATING) ||
        instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::SCHEDULING) ||
        instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::EXITING) ||
        instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::EVICTING) ||
        instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::FAILED) ||
        instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::NEW)) {
        auto promise = std::make_shared<litebus::Promise<Status>>();
        (void)stateMachine->AddStateChangeCallback(
            { InstanceState::RUNNING, InstanceState::FATAL, InstanceState::EXITED },
            [exitHandler(exitHandler_), promise](const InstanceInfo &info) {
                if (info.instancestatus().code() == static_cast<int32_t>(InstanceState::EXITED)) {
                    promise->SetValue(Status::OK());
                    return;
                }
                promise->Associate(exitHandler(info));
            });
        return promise->GetFuture();
    }
    return exitHandler_(instanceInfo);
}

bool InstanceCtrlActor::DoRateLimit(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    if (!config_.createLimitationEnable) {
        return true;
    }

    // except rescheduled request
    if (instanceControlView_->IsRescheduledRequest(scheduleReq)) {
        YRLOG_INFO("{}|{}|schedule request is rescheduled, don't limit create request rate",
            scheduleReq->traceid(), scheduleReq->requestid());
        return true;
    }

    // except system tenant
    const auto &tenantID = scheduleReq->instance().tenantid();
    if (tenantID == "0") {
        return true;
    }

    std::shared_ptr<TokenBucketRateLimiter> limiter;
    if (rateLimiterMap_.find(tenantID) == rateLimiterMap_.end()) {
        limiter = std::make_shared<TokenBucketRateLimiter>(static_cast<uint64_t>(config_.tokenBucketCapacity),
            static_cast<float>(config_.tokenBucketCapacity));
        rateLimiterMap_.emplace(tenantID, limiter);
        YRLOG_DEBUG("{}|{}|tenant({}) new rate limiter", scheduleReq->traceid(), scheduleReq->requestid(), tenantID);
    } else {
        limiter = rateLimiterMap_[tenantID];
    }
    if (limiter->TryAcquire()) {
        return true;
    }

    YRLOG_WARN("{}|{}|tenant({})|instance({}) create rate limited on local({})",
        scheduleReq->traceid(), scheduleReq->requestid(), tenantID, scheduleReq->instance().instanceid(), nodeID_);
    return false;
}

litebus::Future<Status> InstanceCtrlActor::GetAffinity(
    const Status &authorizeStatus, const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    if (authorizeStatus.IsError()) {
        return authorizeStatus;
    }

    // PreemptionAffinity
    if (config_.maxPriority != 0 && config_.enablePreemption &&
        scheduleReq->instance().instancestatus().code() == static_cast<int32_t>(InstanceState::NEW)) {
        SetPreemptionAffinity(scheduleReq);
    }

    // DataAffinity
    return SetDataAffinity(scheduleReq);
}

litebus::Future<Status> InstanceCtrlActor::SetDataAffinity(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    if (auto iter = scheduleReq->instance().createoptions().find(DATA_AFFINITY_ENABLED_KEY);
        iter == scheduleReq->instance().createoptions().end() || iter->second != "true" || dataObjClient_ == nullptr) {
        YRLOG_DEBUG("{}|{}|instance({}) data affinity is disabled, skipped", scheduleReq->traceid(),
                    scheduleReq->requestid(), scheduleReq->instance().instanceid());
        return Status::OK();
    }

    std::set<std::string> objIDSet;
    for (const auto &arg : scheduleReq->instance().args()) {
        if (arg.type() == common::Arg_ArgType_OBJECT_REF && !arg.value().empty()) {
            objIDSet.insert(arg.value());
        }

        for (const auto &objID : arg.nested_refs()) {
            if (!objID.empty()) {
                objIDSet.insert(objID);
            }
        }
    }

    std::vector<std::string> objIDList(objIDSet.begin(), objIDSet.end());
    if (objIDList.empty()) {
        YRLOG_DEBUG("{}|{}|instance({}) empty objIDs, data affinity skipped", scheduleReq->traceid(),
                    scheduleReq->requestid(), scheduleReq->instance().instanceid());
        return Status::OK();
    }

    litebus::Promise<Status> promise;
    dataObjClient_->GetObjNodeList(scheduleReq->instance().tenantid(), objIDList)
        .OnComplete(litebus::Defer(GetAID(), &InstanceCtrlActor::AddDataAffinity, _1, scheduleReq, promise));
    return promise.GetFuture();
}

void InstanceCtrlActor::AddDataAffinity(const litebus::Future<std::vector<std::string>> &nodeListFut,
                                        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
                                        const litebus::Promise<Status> &promise)
{
    if (nodeListFut.IsError() || nodeListFut.Get().empty()) {
        YRLOG_WARN("{}|{}|failed to get data affinity for instance({})", scheduleReq->traceid(),
                   scheduleReq->requestid(), scheduleReq->instance().instanceid());
        // failed to get data affinity, don't affect scheduling, still return OK
        promise.SetValue(Status::OK());
        return;
    }

    auto nodeList = nodeListFut.Get();
    affinity::Selector preferredDataSelector;
    preferredDataSelector.mutable_condition()->set_orderpriority(true);

    for (uint32_t i = 0; i < nodeList.size() && i < MAX_LABEL_AFFINITY_COUNT; ++i) {
        auto expressions = preferredDataSelector.mutable_condition()->add_subconditions()->add_expressions();
        expressions->set_key(HOST_IP_LABEL);
        expressions->mutable_op()->mutable_in()->add_values(nodeList[i]);
    }

    int64_t optimalScore = GetAffinityMaxScore(scheduleReq.get());
    SetAffinityWeight(preferredDataSelector, optimalScore);  // add weight and calculate score
    (*scheduleReq->mutable_instance()
          ->mutable_scheduleoption()
          ->mutable_affinity()
          ->mutable_inner()
          ->mutable_data()
          ->mutable_preferredaffinity()) = preferredDataSelector;
    (*scheduleReq->mutable_contexts())[LABEL_AFFINITY_PLUGIN].mutable_affinityctx()->set_maxscore(
        optimalScore);

    YRLOG_INFO("{}|{}|add data affinity for instance({})", scheduleReq->traceid(), scheduleReq->requestid(),
               scheduleReq->instance().instanceid());

    // clear DATA_AFFINITY_ENABLED, avoids set data affinity repeatedly
    (*scheduleReq->mutable_instance()->mutable_createoptions())[DATA_AFFINITY_ENABLED_KEY] = "";
    promise.SetValue(Status::OK());
}

void InstanceCtrlActor::OnHealthyStatus(const Status &status)
{
    if (!status.IsOk()) {
        return;
    }

    ASSERT_IF_NULL(instanceControlView_);
    observer_->GetAllInstanceInfos().OnComplete(
        litebus::Defer(GetAID(), &InstanceCtrlActor::CheckInstanceFailedState, std::placeholders::_1));
}

void InstanceCtrlActor::CheckInstanceFailedState(const litebus::Future<function_proxy::InstanceInfoMap> &instanceMap)
{
    if (instanceMap.IsError()) {
        YRLOG_ERROR("failed to get all instance from meta-store");
        return;
    }

    for (auto iter : instanceMap.Get()) {
        const auto &info = iter.second;
        if (IsDriver(info)) {
            YRLOG_INFO("{}|{} skip driver", info.requestid(), info.instanceid());
            continue;
        }

        if (info.functionproxyid() != nodeID_) {
            continue;
        }

        auto stateMachine = instanceControlView_->GetInstance(info.instanceid());
        if (stateMachine == nullptr) {
            YRLOG_INFO("{}|{} failed to find instance, delete meta-store", info.requestid(), info.instanceid());
            auto instancePath = GenInstanceKey(info.function(), info.instanceid(), info.requestid());
            auto routePath = GenInstanceRouteKey(info.instanceid());
            if (instancePath.IsSome()) {
                std::shared_ptr<StoreInfo> routePutInfo = std::make_shared<StoreInfo>(routePath, "");
                std::shared_ptr<StoreInfo> instancePutInfo = std::make_shared<StoreInfo>(instancePath.Get(), "");
                instanceOpt_->ForceDelete(instancePutInfo, routePutInfo, nullptr, false);
            }
            continue;
        }

        auto failedState = stateMachine->GetLastSaveFailedState();
        if (failedState == INVALID_LAST_SAVE_FAILED_STATE) {
            continue;
        }

        YRLOG_WARN("failed to save instance({}) status to meta store, need to trans state", info.instanceid());
        TransFailedInstanceState(info, stateMachine, static_cast<InstanceState>(failedState),
                                 stateMachine->GetLastSaveFailedErrCode());
    }
}

litebus::Future<Status> InstanceCtrlActor::InstanceRouteInfoSyncer(const resource_view::RouteInfo &routeInfo)
{
    InstanceInfo info;
    TransToInstanceInfoFromRouteInfo(routeInfo, info);
    if (IsDriver(info)) {
        YRLOG_INFO("{}|{} skip driver", routeInfo.requestid(), routeInfo.instanceid());
        return Status::OK();
    }

    auto stateMachine = instanceControlView_->GetInstance(routeInfo.instanceid());
    if (stateMachine == nullptr) {
        YRLOG_INFO("{}|{} failed to find instance, delete meta-store", routeInfo.requestid(), routeInfo.instanceid());
        auto instancePath = GenInstanceKey(routeInfo.function(), routeInfo.instanceid(), routeInfo.requestid());
        auto routePath = GenInstanceRouteKey(routeInfo.instanceid());
        if (instancePath.IsSome()) {
            std::shared_ptr<StoreInfo> routePutInfo = std::make_shared<StoreInfo>(routePath, "");
            std::shared_ptr<StoreInfo> instancePutInfo = std::make_shared<StoreInfo>(instancePath.Get(), "");
            instanceOpt_->ForceDelete(instancePutInfo, routePutInfo, nullptr, false);
        }
        return Status(StatusCode::FAILED);
    }

    if (stateMachine->GetOwner() != nodeID_) {
        return Status::OK();
    }

    auto failedState = stateMachine->GetLastSaveFailedState();
    if (failedState == INVALID_LAST_SAVE_FAILED_STATE
        && routeInfo.instancestatus().code() != static_cast<int32_t>(stateMachine->GetInstanceState())
        && routeInfo.version() < stateMachine->GetVersion()) {
        TransInstanceState(stateMachine, TransContext{ stateMachine->GetInstanceState(), stateMachine->GetVersion(),
                                                       "success", true });
        return Status::OK();
    }

    if (failedState == INVALID_LAST_SAVE_FAILED_STATE) {
        return Status::OK();
    }

    YRLOG_WARN("failed to save instance({}) status to meta store, need to trans state", routeInfo.instanceid());
    auto instanceInfo = stateMachine->GetInstanceInfo();
    TransToInstanceInfoFromRouteInfo(routeInfo, instanceInfo);
    TransFailedInstanceState(instanceInfo, stateMachine, static_cast<InstanceState>(failedState),
                             stateMachine->GetLastSaveFailedErrCode());
    return Status::OK();
}

litebus::Future<resources::InstanceInfo> InstanceCtrlActor::TransFailedInstanceState(
    const resources::InstanceInfo &info, const std::shared_ptr<InstanceStateMachine> &stateMachine,
    const InstanceState &failedInstanceState, int32_t errCode)
{
    stateMachine->ResetLastSaveFailedState();
    if (failedInstanceState == InstanceState::EXITED) {  // force delete to clear instance info in etcd
        YRLOG_INFO("{}|instance({}) failed state is exited, need to delete from etcd", info.requestid(),
                   info.instanceid());
        stateMachine->ForceDelInstance().Then(
            [stateMachine, instanceID(info.instanceid())](const Status &status) -> Status {
                if (status.IsOk()) {
                    stateMachine->PublishDeleteToLocalObserver(instanceID);
                }
                return status;
            });
        return info;
    }

    stateMachine->UpdateInstanceInfo(info);
    if (stateMachine->GetOwner() != nodeID_) {
        YRLOG_WARN("instance({}) move to node({}), don't trans state", info.instanceid(), stateMachine->GetOwner());
        return info;
    }
    auto currentState = stateMachine->GetInstanceState();
    if (currentState == failedInstanceState) {
        YRLOG_INFO("instance({}) state({}) in meta store, is same as failed save state({}), skip", info.instanceid(),
                   fmt::underlying(currentState), fmt::underlying(failedInstanceState));
        return info;
    }

    // only consider state change between RUNNING and SUB_HEALTH is non-fatal
    if ((currentState == InstanceState::RUNNING && failedInstanceState == InstanceState::SUB_HEALTH)
        || (currentState == InstanceState::SUB_HEALTH && failedInstanceState == InstanceState::RUNNING)) {
        TransInstanceState(
            stateMachine,
            TransContext{ static_cast<InstanceState>(failedInstanceState), stateMachine->GetVersion(),
                          failedInstanceState == InstanceState::RUNNING ? "running" : "subHealth", true,
                          failedInstanceState == InstanceState::RUNNING ? StatusCode::SUCCESS : errCode });
        return info;
    }

    TransInstanceState(stateMachine, TransContext{ InstanceState::FATAL, stateMachine->GetVersion(),
                                                   "failed to save instance status to meta store", true,
                                                   StatusCode::ERR_ETCD_OPERATION_ERROR });
    return info;
}

CreateCallResultCallBack InstanceCtrlActor::RegisterCreateCallResultCallback(
    const std::shared_ptr<ScheduleRequest> &request)
{
    auto callback =
        [request, instanceControlView(instanceControlView_), aid(GetAID()), nodeID(nodeID_)](
            const std::shared_ptr<functionsystem::CallResult> &callResult) -> litebus::Future<CallResultAck> {
        CallResultAck ack;
        ASSERT_IF_NULL(instanceControlView);
        auto instanceID(request->instance().instanceid());
        auto stateMachine = instanceControlView->GetInstance(instanceID);
        if (stateMachine == nullptr) {
            YRLOG_ERROR("{}|{} info not existed to find creator", callResult->requestid(), instanceID);
            ack.set_code(common::ERR_INSTANCE_NOT_FOUND);
            return ack;
        }
        auto instanceInfo = request->instance();
        if (instanceInfo.lowreliability()) {
            callResult->mutable_runtimeinfo()->set_route(aid.Url());
        }
        if (callResult->code() == common::ErrorCode::ERR_NONE && stateMachine != nullptr &&
            stateMachine->GetInstanceState() != InstanceState::RUNNING) {
            auto transContext = TransContext{ InstanceState::RUNNING, stateMachine->GetVersion(), "running" };
            transContext.scheduleReq = request;
            return litebus::Async(aid, &InstanceCtrlActor::SendCheckpointReq, request)
                .Then(litebus::Defer(aid, &InstanceCtrlActor::TransInstanceState, stateMachine, transContext))
                .Then([requestID(callResult->requestid())](const TransitionResult &result) -> litebus::Future<Status> {
                    if (result.preState.IsNone()) {
                        YRLOG_ERROR("{}|failed to update instance info for meta store", requestID);
                        return Status(StatusCode::ERR_ETCD_OPERATION_ERROR,
                                      "failed to update instance info for meta store");
                    }
                    return Status::OK();
                })
                .Then([aid, nodeID, instanceID(instanceInfo.instanceid()), dstInstanceID(instanceInfo.parentid()),
                       dstProxyID(instanceInfo.parentfunctionproxyaid()), callResult](const Status &status) {
                    if (status.IsError()) {
                        callResult->set_code(common::ErrorCode::ERR_ETCD_OPERATION_ERROR);
                        callResult->set_message(fmt::format(
                            "failed to transition to running, err: {} on {}", status.GetMessage(), nodeID));
                    }
                    return litebus::Async(aid, &InstanceCtrlActor::SendCallResult, instanceID, dstInstanceID,
                                          dstProxyID, callResult);
                });
        }
        return litebus::Async(aid, &InstanceCtrlActor::SendCallResult, instanceInfo.instanceid(),
                              instanceInfo.parentid(), instanceInfo.parentfunctionproxyaid(), callResult);
    };
    YRLOG_DEBUG("{}|{}|Register callResult callback for instance({})", request->traceid(), request->requestid(),
                request->instance().instanceid());
    createCallResultCallback_[request->instance().instanceid()] = callback;
    return callback;
}

void InstanceCtrlActor::SetInstanceBillingContext(const resource_view::InstanceInfo &instance)
{
    auto customMetricsOption = metrics::MetricsAdapter::GetInstance().GetMetricsContext().
            GetCustomMetricsOption(instance);
    metrics::MetricsAdapter::GetInstance().GetMetricsContext().InitBillingInstance(instance, customMetricsOption);
    metrics::MetricsAdapter::GetInstance().RegisterBillingInstanceRunningDuration();
}

void InstanceCtrlActor::UpdateFuncMetas(bool isAdd, const std::unordered_map<std::string, FunctionMeta> &funcMetas)
{
    if (isAdd) {
        for (const auto &funcMeta : funcMetas) {
            YRLOG_DEBUG("update function({}) meta", funcMeta.first);
            if (funcMetaMap_.find(funcMeta.first) != funcMetaMap_.end()
                && GetFunctionHashTag(funcMetaMap_[funcMeta.first]) != GetFunctionHashTag(funcMeta.second)) {
                YRLOG_INFO("function({}) meta is updated, old should be removed", funcMeta.first);
                FunctionDelete(funcMeta.first, funcMetaMap_[funcMeta.first]);
            }
            funcMetaMap_[funcMeta.first] = funcMeta.second;
            FunctionWarmUp(funcMeta.first, funcMeta.second);
        }
        return;
    }

    for (const auto &funcMeta : funcMetas) {
        YRLOG_DEBUG("delete function({}) meta", funcMeta.first);
        FunctionDelete(funcMeta.first, funcMetaMap_[funcMeta.first]);
        funcMetaMap_.erase(funcMeta.first);
    }
}

void InstanceCtrlActor::TriggerToWarmUpFunction(const std::string &agentID)
{
    for (auto [funcKey, funcMeta] : funcMetaMap_) {
        FunctionWarmUp(funcKey, funcMeta, agentID);
    }
    return;
}

bool InstanceCtrlActor::CheckExistInstanceState(
    const InstanceState &state, const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise,
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq, bool isStateFromRemote)
{
    // we don't care about the state(new,scheduling) in scheduling, only care about the state after scheduling
    // running or creating instance return ERR_INSTANCE_DUPLICATED
    if (state == InstanceState::RUNNING) {
        return CheckRunningInstanceStateFromRemote(scheduleReq, runtimePromise, isStateFromRemote);
    }

    // stable failed states(EXITING/FATAL/SCHEDULE_FAILED/EVICTED) return ERR_INSTANCE_EXITED
    if (state == InstanceState::EXITING || state == InstanceState::FATAL || state == InstanceState::EVICTING
        || state == InstanceState::EVICTED) {
        YRLOG_WARN("{}|{}|receive a schedule request for a failed instance({}) directly return", scheduleReq->traceid(),
                   scheduleReq->requestid(), scheduleReq->instance().instanceid());
        runtimePromise->SetValue(GenScheduleResponse(StatusCode::ERR_INSTANCE_EXITED,
                                                     "you are not allowed to create instance with the same "
                                                     "instance id of an failed instance, please kill first",
                                                     *scheduleReq));
        return true;
    }

    if (state == InstanceState::CREATING || state == InstanceState::SCHEDULING) {
        YRLOG_WARN("{}|{}|receive a schedule request for a instance({}) of state({}), wait state change",
                   scheduleReq->traceid(), scheduleReq->requestid(), scheduleReq->instance().instanceid(),
                   static_cast<int32_t>(state));
        RegisterStateChangeCallback(scheduleReq, runtimePromise);
        return true;
    }
    return false;
}

bool InstanceCtrlActor::CheckRunningInstanceStateFromRemote(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise, bool isStateFromRemote)
{
    auto stateMachine = instanceControlView_->GetInstance(scheduleReq->instance().instanceid());
    if (stateMachine == nullptr) {
        // continue scheduling
        return false;
    }

    if (stateMachine->GetOwner() == nodeID_ || isStateFromRemote) {
        YRLOG_WARN("{}|{}|receive a schedule request for a existing instance({}) directly return",
                   scheduleReq->traceid(), scheduleReq->requestid(), scheduleReq->instance().instanceid());
        runtimePromise->SetValue(GenScheduleResponse(StatusCode::ERR_INSTANCE_DUPLICATED,
                                                     "you are not allowed to create instance with the same instance id",
                                                     *scheduleReq));
        return true;
    }

    GetLocalSchedulerAID(scheduleReq->instance().instanceid())
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::SendCheckStateRequest, std::placeholders::_1, scheduleReq,
                             stateMachine->GetOwner(), runtimePromise));
    // return true, stop scheduling, decide which error code to use, base on remote state
    return true;
}

bool InstanceCtrlActor::SendCheckStateRequest(
    const litebus::Option<litebus::AID> &option, const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::string &remote, const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise)
{
    if (option.IsNone()) {
        YRLOG_WARN("failed to send check instance({}) state to remote({}), use local state",
                   scheduleReq->instance().instanceid(), remote);
        runtimePromise->SetValue(GenScheduleResponse(StatusCode::ERR_INNER_COMMUNICATION,
                                                     "you are not allowed to create instance with the same instance id",
                                                     *scheduleReq));
        return false;
    }

    messages::CheckInstanceStateRequest request;
    request.set_requestid(scheduleReq->requestid());
    request.set_instanceid(scheduleReq->instance().instanceid());
    YRLOG_DEBUG("{}|send check instance({}) request to {}:{}", request.requestid(), request.instanceid(), remote,
                option.Get().HashString());
    (void)checkStateHelper_
        .Begin(scheduleReq->requestid(), std::make_shared<litebus::AID>(option.Get()), "CheckInstanceState",
               request.SerializeAsString())
        .Then([runtimePromise, scheduleReq, aid(GetAID())](const litebus::Option<InstanceState> &state) {
            if (state.IsNone()) {
                YRLOG_WARN("{}|failed to get state from remote", scheduleReq->requestid());
                runtimePromise->SetValue(GenScheduleResponse(
                    StatusCode::ERR_INNER_COMMUNICATION,
                    "you are not allowed to create instance with the same instance id", *scheduleReq));
                return false;
            }
            // check state with remote state
            litebus::Async(aid, &InstanceCtrlActor::CheckExistInstanceState, state.Get(), runtimePromise, scheduleReq,
                           true);
            return true;
        });
    return true;
}

void InstanceCtrlActor::CheckInstanceState(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::CheckInstanceStateRequest request;
    RETURN_IF_TRUE(!request.ParseFromString(msg), "failed to parse check instance state request");
    YRLOG_INFO("{}|receive check instance({}) state request from {}", request.requestid(), request.instanceid(),
               from.HashString());
    messages::CheckInstanceStateResponse response;
    response.set_requestid(request.requestid());
    auto stateMachine = instanceControlView_->GetInstance(request.instanceid());
    if (stateMachine == nullptr || stateMachine->GetOwner() != nodeID_) {
        YRLOG_WARN("{}|failed to check instance({}) state, failed to find instance on this node", request.requestid(),
                   request.instanceid());
        response.set_code(INVALID_INSTANCE_STATE);
    } else {
        response.set_code(static_cast<int64_t>(stateMachine->GetInstanceState()));
    }
    Send(from, "CheckInstanceStateResponse", response.SerializeAsString());
}

void InstanceCtrlActor::CheckInstanceStateResponse(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::CheckInstanceStateResponse response;
    RETURN_IF_TRUE(!response.ParseFromString(msg), "failed to parse check instance state response");
    YRLOG_INFO("{}|receive check instance state({}) response from {}", response.requestid(), response.code(),
               from.HashString());
    if (response.code() == INVALID_INSTANCE_STATE) {
        checkStateHelper_.End(response.requestid(), litebus::None());
        return;
    }
    checkStateHelper_.End(response.requestid(), static_cast<InstanceState>(response.code()));
}

litebus::Future<ScheduleResponse> InstanceCtrlActor::DeleteRequestFuture(
    const litebus::Future<ScheduleResponse> &scheduleResponse, const std::string &requestID,
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    instanceControlView_->DeleteRequestFuture(requestID);

    // release owner after failed forward schedule from domain
    auto stateMachine = instanceControlView_->GetInstance(scheduleReq->instance().instanceid());
    if (stateMachine != nullptr && stateMachine->GetInstanceState() == InstanceState::SCHEDULING
        && (scheduleResponse.IsError() || scheduleResponse.Get().code() != 0)) {
        stateMachine->ReleaseOwner();
    }
    return scheduleResponse;
}

litebus::Future<bool> InstanceCtrlActor::IsSystemTenant(const std::string &tenantID)
{
    return internalIAM_->IsSystemTenant(tenantID);
}

litebus::Future<Status> InstanceCtrlActor::DeleteInstanceForStaticFunction(const InstanceInfo &instanceInfo)
{
    YRLOG_INFO("{}|execute static function instance exit handler, instance({})", instanceInfo.requestid(),
               instanceInfo.instanceid());
    (void)instanceRegisteredReadyCallback_.erase(instanceInfo.requestid());
    return litebus::Async(GetAID(), &InstanceCtrlActor::DeleteInstanceInResourceView, Status::OK(), instanceInfo)
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeleteInstanceInControlView, std::placeholders::_1,
                             instanceInfo));
}

litebus::Future<Status> InstanceCtrlActor::GracefulShutdown()
{
    ClearLocalDriver();
    return Status::OK();
}
void InstanceCtrlActor::ClearLocalDriver()
{
    auto connected = connectedDriver_;
    for (auto [instanceID, jobID] : connected) {
        DeleteDriverClient(instanceID, jobID);
    }
    connectedDriver_.clear();
}

litebus::Future<Status> InstanceCtrlActor::ToResume(const std::string &instanceID)
{
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND,
                      fmt::format("instance({}) not found for resume", instanceID));
    }
    auto state = stateMachine->GetInstanceState();
    RETURN_STATUS_IF_TRUE(state != InstanceState::SUSPEND && state != InstanceState::RUNNING,
                          StatusCode::ERR_STATE_MACHINE_ERROR,
                          fmt::format("instance({}) is state in ({}), which is not allow to resume", instanceID,
                                      fmt::underlying(state)));
    RETURN_STATUS_IF_TRUE(state == InstanceState::RUNNING, StatusCode::SUCCESS, "");

    if (config_.enableFakeSuspendResume) {
        YRLOG_INFO("fake resume enabled, resume instance({}) on current owner proxy", instanceID);
        return DoLocalResumeInstance(instanceID);
    }

    auto request = stateMachine->GetScheduleRequest();
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    return Schedule(request, runtimePromise).Then([](const ScheduleResponse &resp) {
        return Status(static_cast<StatusCode>(resp.code()), resp.message());
    });
}

Status SuspendStateCheck(const InstanceState &state, const std::string &instanceID)
{
    if (state == InstanceState::SUSPEND) {
        YRLOG_INFO("InstanceID:{} is already suspended", instanceID);
        return Status::OK();
    }
    if (state != InstanceState::RUNNING) {
        auto msg = fmt::format("suspend failed: InstanceID {} is not in running state, current state: {}", instanceID,
                               fmt::underlying(state));
        YRLOG_ERROR("{}", msg);
        return Status(StatusCode::ERR_STATE_MACHINE_ERROR, msg);
    }
    return Status::OK();
}

litebus::Future<Status> InstanceCtrlActor::ToSuspend(const std::string &instanceID)
{
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND,
                      fmt::format("instance({}) not found for suspend", instanceID));
    }
    auto state = stateMachine->GetInstanceState();
    if (state == InstanceState::SUSPEND) {
        YRLOG_INFO("InstanceID:{} is already suspended", instanceID);
        return Status::OK();
    }
    RETURN_STATUS_IF_TRUE(state != InstanceState::RUNNING, StatusCode::ERR_STATE_MACHINE_ERROR,
                          fmt::format("suspend failed: InstanceID {} is not in running state, current state: {}",
                                      instanceID, fmt::underlying(state)));
    auto future = TransInstanceState(
                      stateMachine,
                      TransContext{ InstanceState::SUSPEND, stateMachine->GetVersion(),
                                    "WARN: instance is already SUSPEND, please resume instance before you invoke it",
                                    true, StatusCode::ERR_INSTANCE_SUSPEND });

    if (config_.enableFakeSuspendResume) {
        return future.Then(litebus::Defer(GetAID(), &InstanceCtrlActor::HandleFakeSuspendTransition, stateMachine,
                                          instanceID, std::placeholders::_1));
    }

    return future.Then(litebus::Defer(GetAID(), &InstanceCtrlActor::HandleNormalSuspendTransition, stateMachine,
                                      instanceID, std::placeholders::_1));
}

litebus::Future<Status> InstanceCtrlActor::HandleFakeSuspendTransition(
    const std::shared_ptr<InstanceStateMachine> &stateMachine, const std::string &instanceID,
    const TransitionResult &result)
{
    if (result.status.IsError()) {
        return result.status;
    }
    StopHeartbeat(instanceID);
    YRLOG_INFO("fake suspend instance({}): remove from resource view without killing runtime", instanceID);
    auto info = stateMachine->GetInstanceInfo();
    return DeleteInstanceInResourceView(Status::OK(), info);
}

litebus::Future<Status> InstanceCtrlActor::HandleNormalSuspendTransition(
    const std::shared_ptr<InstanceStateMachine> &stateMachine, const std::string &instanceID,
    const TransitionResult &result)
{
    if (result.status.IsError()) {
        return result.status;
    }
    StopHeartbeat(instanceID);
    YRLOG_INFO("ready to recycle runtime of instance({})", instanceID);
    auto info = stateMachine->GetInstanceInfo();
    return KillRuntime(info, false)
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeleteInstanceInResourceView, std::placeholders::_1, info))
        .Then([instanceInfo(info), groupInsClear(groupInstanceClear_)](
                  const Status &status) -> litebus::Future<Status> {
            if (status.IsError()) {
                return status;
            }
            // Delete the reserved resource information corresponding to the group instance
            // bound to the local node when instance suspend.
            if (!instanceInfo.groupid().empty() && groupInsClear != nullptr) {
                groupInsClear(instanceInfo);
            }
            return Status::OK();
        });
}

litebus::Future<Status> InstanceCtrlActor::MakeCheckpoint(const std::string &instanceID)
{
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        auto msg = fmt::format("instance({}) not found to checkpoint", instanceID);
        YRLOG_ERROR("{}", msg);
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND, msg);
    }
    auto state = stateMachine->GetInstanceState();
    if (state == InstanceState::SUSPEND) {
        YRLOG_INFO("InstanceID:{} is already suspended", instanceID);
        return Status::OK();
    }
    RETURN_STATUS_IF_TRUE(state != InstanceState::RUNNING, StatusCode::ERR_STATE_MACHINE_ERROR,
                          fmt::format("checkpoint failed: InstanceID {} is not in running state, current state: {}",
                                      instanceID, fmt::underlying(state)));
    litebus::Promise<Status> promise;
    DoCheckpoint(instanceID, promise);
    return promise.GetFuture();
}

void InstanceCtrlActor::DoCheckpoint(const std::string &instanceID, const litebus::Promise<Status> &promise,
    uint32_t retryTimes)
{
    (void)litebus::Async(GetAID(), &InstanceCtrlActor::Checkpoint, instanceID).Then(
        [aid(GetAID()), retryTimes, instanceID, promise](const Status &status) -> litebus::Future<Status> {
            if (status.IsOk() || status.StatusCode() != StatusCode::ERR_INSTANCE_BUSY || retryTimes <= 1) {
                promise.SetValue(status);
                return Status::OK();
            }
            litebus::AsyncAfter(INSTANCE_BUSY_SUSPEND_RETRY_MS, aid, &InstanceCtrlActor::DoCheckpoint,
                instanceID, promise, retryTimes - 1);
            return Status::OK();
        });
}
void InstanceCtrlActor::FunctionWarmUp(const std::string &funcKey, const FunctionMeta &funcMeta,
    const litebus::Option<std::string> &agentID)
{
    if (config_.isPseudoDataPlane || funcMeta.warmup == WarmupType::NONE || funcMeta.warmup == WarmupType::INVALID) {
        return;
    }
    YRLOG_INFO("start to warm up {}, type:{}", funcKey, fmt::underlying(funcMeta.warmup));
    auto warm = std::make_shared<ScheduleRequest>();
    warm->set_requestid(fmt::format("FunctionWarmUp-{}", funcKey));
    warm->set_traceid(fmt::format("FunctionWarmUp-{}", funcKey));
    auto instance = warm->mutable_instance();
    instance->set_instanceid(GetFunctionHashTag(funcMeta));
    // todo : may change another field in future
    auto deployInstanceRequest = GetDeployInstanceReq(funcMeta, warm);
    deployInstanceRequest->set_warmuptype(static_cast<int32_t>(funcMeta.warmup));
    (void)functionAgentMgr_->RegisterToWarmUp(deployInstanceRequest, agentID).OnComplete(
        litebus::Defer(GetAID(), &InstanceCtrlActor::OnFunctionWarmUp, funcKey, std::placeholders::_1, agentID));
}

void InstanceCtrlActor::OnFunctionWarmUp(const std::string &funcKey, const litebus::Future<Status> &future,
                                         const litebus::Option<std::string> &agentID)
{
    if (!future.IsError() && future.Get().IsOk()) {
        YRLOG_INFO("function({}) warm up succeeded", funcKey);
        return;
    }
    YRLOG_WARN("function({}) warm up failed: {}, defer to retry", funcKey,
               future.IsError() ? std::to_string(future.GetErrorCode()) : future.Get().GetMessage());
    auto it = funcMetaMap_.find(funcKey);
    if (it == funcMetaMap_.end()) {
        YRLOG_INFO("function({}) is already deleted. stop to warmup", funcKey);
        return;
    }
    static const int64_t warmUpRetryInterval = 10000;
    litebus::AsyncAfter(warmUpRetryInterval, GetAID(), &InstanceCtrlActor::FunctionWarmUp, funcKey, it->second,
                        agentID);
}

void InstanceCtrlActor::FunctionDelete(const std::string &funcKey, const FunctionMeta &funcMeta)
{
    if (funcMeta.warmup == WarmupType::NONE || funcMeta.warmup == WarmupType::INVALID) {
        return;
    }
    auto killInstanceReq = GenKillInstanceRequest(funcKey, fmt::format("FunctionWarmUp-Instance-{}", funcKey), funcKey,
                                                  funcMeta.codeMetaData.storageType, false);
    killInstanceReq->set_runtimeid(GetFunctionHashTag(funcMeta));
    (void)functionAgentMgr_->UnRegisterWarmUp(killInstanceReq)
        .OnComplete(litebus::Defer(GetAID(), &InstanceCtrlActor::OnFunctionDelete, funcKey, std::placeholders::_1));
}

void InstanceCtrlActor::OnFunctionDelete(
    const std::string &funcKey, const litebus::Future<Status> &future)
{
    if (future.IsError()) {
        YRLOG_WARN("function({}) delete failed: {}", funcKey, future.GetErrorCode());
        return;
    }
    auto status = future.Get();
    if (!status.IsOk()) {
        YRLOG_WARN("function({}) delete failed: {}", funcKey, status.GetMessage());
        return;
    }
    YRLOG_INFO("function({}) delete succeeded", funcKey);
}

void InstanceCtrlActor::HandleIdleTimeout(const std::string &instanceID)
{
    idleTimers_.erase(instanceID);
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        return;
    }

    // Double-check: ensure no active sessions before evicting
    auto it = instanceActiveSessions_.find(instanceID);
    if (it != instanceActiveSessions_.end() && it->second) {
        YRLOG_INFO("{}|instance({}) idle timeout cancelled due to active sessions",
                   stateMachine->GetInstanceInfo().requestid(), instanceID);
        return;
    }

    const auto &instanceInfo = stateMachine->GetInstanceInfo();
    YRLOG_INFO("{}|instance({}) idle timeout, ready to evict", instanceInfo.requestid(), instanceID);
    std::string msg =
        fmt::format("instance was evicted after idle for more than {} seconds", GetIdleTimeout(instanceInfo));
    (void)TransInstanceState(stateMachine, TransContext{ InstanceState::EVICTING, stateMachine->GetVersion(), msg,
                                                          true, StatusCode::ERR_INSTANCE_EVICTED })
        .Then([aid(GetAID()), instanceId(instanceID)]() {
            litebus::Async(aid, &InstanceCtrlActor::StopHeartbeat, instanceId);
            return Status::OK();
        })
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::KillRuntime, instanceInfo, false))
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::DeleteInstanceInResourceView,
            std::placeholders::_1, instanceInfo))
        .Then(litebus::Defer(GetAID(), &InstanceCtrlActor::TransInstanceState, stateMachine,
            TransContext{ InstanceState::EVICTED, stateMachine->GetVersion(),
                          msg, true, StatusCode::ERR_INSTANCE_EVICTED }))
        .Then([instanceID](const TransitionResult &result) -> litebus::Future<Status> {
            if (result.preState.IsNone()) {
                YRLOG_WARN("failed to transfer instance({}) to evicted by idle timeout.", instanceID);
            }
            return Status::OK();
        });
}

void InstanceCtrlActor::StartIdleTimer(const std::string &instanceID)
{
    if (idleTimers_.find(instanceID) != idleTimers_.end()) {
        return;
    }
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        return;
    }
    const auto &instanceInfo = stateMachine->GetInstanceInfo();
    if (instanceInfo.functionproxyid() != nodeID_ ||
        instanceInfo.instancestatus().code() != static_cast<int32_t>(InstanceState::RUNNING)) {
        return;
    }

    // Additional check: don't start timer if there are active sessions
    auto it = instanceActiveSessions_.find(instanceID);
    if (it != instanceActiveSessions_.end() && it->second) {
        YRLOG_INFO("skip starting idle timer for instance({}) due to active sessions", instanceID);
        return;
    }

    int64_t idleTimeout = GetIdleTimeout(instanceInfo);
    if (idleTimeout <= 0) {
        return;
    }
    YRLOG_INFO("start idle timer for instance({}) with timeout {} seconds", instanceID, idleTimeout);
    constexpr int64_t msPerSecond = 1000;
    idleTimers_[instanceID] = litebus::AsyncAfter(
        idleTimeout * msPerSecond, GetAID(), &InstanceCtrlActor::HandleIdleTimeout, instanceID);
}

void InstanceCtrlActor::CancelIdleTimer(const std::string &instanceID)
{
    auto iter = idleTimers_.find(instanceID);
    if (iter == idleTimers_.end()) {
        return;
    }
    YRLOG_INFO("cancel idle timer for instance({})", instanceID);
    litebus::TimerTools::Cancel(iter->second);
    idleTimers_.erase(iter);
}

void InstanceCtrlActor::TrafficReport(const std::string &instanceID, const size_t &processingNum)
{
    YRLOG_DEBUG("instance({}) processing num: {}", instanceID, processingNum);
    bool isIdle = (processingNum == 0);
    ASSERT_IF_NULL(instanceControlView_);
    if (!isIdle) {
        instanceTrafficIdle_.erase(instanceID);
        CancelIdleTimer(instanceID);
        return;
    }

    instanceTrafficIdle_[instanceID] = true;

    // Only start idle timer if both traffic idle AND no active sessions
    bool hasActiveSessions = false;
    auto it = instanceActiveSessions_.find(instanceID);
    if (it != instanceActiveSessions_.end()) {
        hasActiveSessions = it->second;
    }

    if (!hasActiveSessions) {
        StartIdleTimer(instanceID);
    } else {
        YRLOG_DEBUG("instance({}) is idle but has active exec sessions, skip idle timer", instanceID);
    }
}

void InstanceCtrlActor::SessionCountDelta(const std::string &instanceID, int delta)
{
    if (instanceID.empty() || delta == 0) {
        return;
    }

    auto &count = instanceSessionCounts_[instanceID];
    size_t oldCount = count;

    if (delta > 0) {
        count += static_cast<size_t>(delta);
    } else if (delta < 0 && count > 0) {
        size_t dec = static_cast<size_t>(-delta);
        count = (dec >= count) ? 0 : (count - dec);
    }

    size_t newCount = count;
    if (newCount == 0) {
        instanceSessionCounts_.erase(instanceID);
    }

    // Edge detection: 0->N or N->0
    if ((oldCount == 0 && newCount > 0) || (oldCount > 0 && newCount == 0)) {
        bool hasActiveSessions = (newCount > 0);
        YRLOG_INFO("instance({}) session count edge: {} sessions, hasActiveSessions={}",
                   instanceID, newCount, hasActiveSessions);
        SessionAlive(instanceID, hasActiveSessions);
    }
}

void InstanceCtrlActor::SessionAlive(const std::string &instanceID, bool hasActiveSessions)
{
    YRLOG_INFO("instance({}) session alive status changed: hasActiveSessions={}", instanceID, hasActiveSessions);

    // Update session status
    if (hasActiveSessions) {
        instanceActiveSessions_[instanceID] = true;
        // Cancel idle timer when sessions become active
        CancelIdleTimer(instanceID);
    } else {
        instanceActiveSessions_.erase(instanceID);
        // When sessions become inactive, check traffic idle status before starting timer
        bool trafficIdle = false;
        auto trafficIt = instanceTrafficIdle_.find(instanceID);
        if (trafficIt != instanceTrafficIdle_.end()) {
            trafficIdle = trafficIt->second;
        }
        ASSERT_IF_NULL(instanceControlView_);
        auto stateMachine = instanceControlView_->GetInstance(instanceID);
        if (trafficIdle && stateMachine != nullptr) {
            const auto &instanceInfo = stateMachine->GetInstanceInfo();
            if (instanceInfo.functionproxyid() == nodeID_ &&
                instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::RUNNING)) {
                StartIdleTimer(instanceID);
            }
        }
    }
}

void InstanceCtrlActor::BindSnapCtrl(const std::shared_ptr<SnapCtrl> &snapCtrl)
{
    snapCtrl_ = snapCtrl;
    snapCtrl_->BindInstanceControlView(instanceControlView_);
}


litebus::Future<Status> InstanceCtrlActor::RegisterTraefikRoute(const InstanceInfo& instanceInfo)
{
    if (!traefikRegistry_) {
        return Status::OK();
    }

    // Check if instance has port mappings in extensions
    auto it = instanceInfo.extensions().find(PORT_FORWARD_KEY);
    if (it == instanceInfo.extensions().end() || it->second.empty()) {
        YRLOG_DEBUG("Instance {} has no port mappings, skip Traefik registration", instanceInfo.instanceid());
        return Status::OK();
    }

    const std::string& portMappingsJson = it->second;

    // Helper to parse IP from address (ip:port format)
    auto parseIP = [](const std::string& addr) -> std::string {
        auto pos = addr.find(':');
        return pos != std::string::npos ? addr.substr(0, pos) : "";
    };

    // Use proxy's external IP from config for Traefik routing
    // This ensures external clients can reach the instance through Traefik
    std::string ip = parseIP(config_.proxyGrpcAddress);
    if (ip.empty()) {
        YRLOG_WARN("skip traefik registration for instance({}): invalid proxy address({})",
                   instanceInfo.instanceid(), config_.proxyGrpcAddress);
        return Status::OK();
    }

    // Parse port mappings from JSON: ["https:40001:8080", "tcp:40002:443"] or legacy format ["40001:8080"]
    std::vector<TraefikRegistry::PortMapping> portMappings;
    try {
        nlohmann::json portJson = nlohmann::json::parse(portMappingsJson);
        if (portJson.is_array()) {
            for (const auto& entry : portJson) {
                if (entry.is_string()) {
                    std::string mapping = entry.get<std::string>();
                    // Parse "protocol:hostPort:containerPort" or legacy "hostPort:containerPort"
                    std::vector<std::string> parts;
                    std::stringstream ss(mapping);
                    std::string part;
                    while (std::getline(ss, part, ':')) {
                        parts.push_back(part);
                    }
                    // New format: "protocol:hostPort:containerPort" (3 parts)
                    constexpr size_t newFormatParts = 3;
                    constexpr size_t legacyFormatParts = 2;
                    if (parts.size() == newFormatParts) {
                        try {
                            std::string protocol = parts[0];
                            int hostPort = std::stoi(parts[1]);
                            int sandboxPort = std::stoi(parts[2]);
                            portMappings.push_back({sandboxPort, hostPort, protocol});
                            YRLOG_DEBUG("Parsed port mapping: protocol={}, sandbox_port={}, host_port={}",
                                        protocol, sandboxPort, hostPort);
                        } catch (const std::exception& e) {
                            YRLOG_WARN("Failed to parse port mapping from '{}': {}", mapping, e.what());
                        }
                    } else if (parts.size() == legacyFormatParts) {
                        // Legacy format: "hostPort:containerPort" (2 parts, no protocol)
                        // Default to "http" for backward compatibility (HTTP backend)
                        try {
                            int hostPort = std::stoi(parts[0]);
                            int sandboxPort = std::stoi(parts[1]);
                            portMappings.push_back({sandboxPort, hostPort, "http"});
                            YRLOG_DEBUG("Parsed legacy port mapping (no protocol): sandbox_port={}, "
                                        "host_port={}, defaulting to 'http'",
                                        sandboxPort, hostPort);
                        } catch (const std::exception& e) {
                            YRLOG_WARN("Failed to parse legacy port mapping from '{}': {}", mapping, e.what());
                        }
                    }
                }
            }
        }
    } catch (const nlohmann::json::exception& e) {
        YRLOG_WARN("Failed to parse port mappings JSON '{}': {}", portMappingsJson, e.what());
        return Status::OK();
    }

    if (portMappings.empty()) {
        YRLOG_DEBUG("No valid port mappings found for instance {}", instanceInfo.instanceid());
        return Status::OK();
    }

    YRLOG_INFO("Registering instance to Traefik: instanceID={}, hostIP={}, ports={}",
               instanceInfo.instanceid(), ip, portMappings.size());

    return traefikRegistry_->RegisterInstance(instanceInfo.instanceid(), ip, portMappings)
        .Then([instanceID = instanceInfo.instanceid()](const Status& status) -> Status {
            if (!status.IsOk()) {
                YRLOG_ERROR("failed to register traefik route for instance({}): {}",
                            instanceID, status.GetMessage());
                return status;
            }
            return Status::OK();
        });
}

litebus::Future<Status> InstanceCtrlActor::UnregisterTraefikRoute(const std::string& instanceID)
{
    if (!traefikRegistry_) {
        return Status::OK();
    }
    YRLOG_INFO("Unregistering instance from Traefik: instanceID={}", instanceID);
    return traefikRegistry_->UnregisterInstance(instanceID)
        .Then([instanceID](const Status& status) -> Status {
            if (!status.IsOk()) {
                YRLOG_WARN("failed to unregister traefik route for instance({}): {}",
                           instanceID, status.GetMessage());
                // lease will expire automatically, so don't block main flow
                return status;
            }
            return Status::OK();
        });
}
litebus::Future<Status> InstanceCtrlActor::DoLocalResumeInstance(const std::string &instanceID)
{
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND,
                      fmt::format("instance({}) not found for local resume", instanceID));
    }
    auto state = stateMachine->GetInstanceState();
    if (state != InstanceState::SUSPEND) {
        return Status(StatusCode::ERR_STATE_MACHINE_ERROR,
                      fmt::format("instance({}) is not in SUSPEND state, current state: {}", instanceID,
                                  fmt::underlying(state)));
    }

    auto transContext = TransContext{ InstanceState::RUNNING, stateMachine->GetVersion(), "running" };
    auto instanceInfo = stateMachine->GetInstanceInfo();
    ASSERT_IF_NULL(resourceViewMgr_);
    auto type = resource_view::GetResourceType(instanceInfo);
    return TransInstanceState(stateMachine, transContext)
        .Then([aid(GetAID()), stateMachine, instanceID, instanceInfo, type, resourceViewMgr(resourceViewMgr_)]
                  (const TransitionResult &result) -> litebus::Future<Status> {
            if (result.status.IsError()) {
                YRLOG_ERROR("failed to transition instance({}) to RUNNING: {}", instanceID, result.status.ToString());
                return result.status;
            }
            return resourceViewMgr->GetInf(type)->AddInstances({ { instanceID, { instanceInfo, nullptr } } })
                .Then([aid, stateMachine, instanceID, instanceInfo]
                          (const Status &status) -> litebus::Future<Status> {
                    if (status.IsError()) {
                        YRLOG_ERROR("failed to add instance({}) back to resource view: {}", instanceID,
                                    status.ToString());
                        auto rollbackContext =
                            TransContext{ InstanceState::SUSPEND, stateMachine->GetVersion(), "rollback to suspend" };
                        return litebus::Async(aid, &InstanceCtrlActor::TransInstanceState, stateMachine,
                                              rollbackContext)
                            .Then([instanceID, status](const TransitionResult &rollbackResult)
                                      -> litebus::Future<Status> {
                                if (rollbackResult.status.IsError()) {
                                    YRLOG_ERROR("failed to rollback instance({}) to SUSPEND: {}", instanceID,
                                                rollbackResult.status.ToString());
                                    return rollbackResult.status;
                                }
                                return status;
                            });
                    }
                    litebus::Async(aid, &InstanceCtrlActor::StartHeartbeat, instanceID, 0, instanceInfo.runtimeid(),
                                   StatusCode::SUCCESS);
                    YRLOG_INFO("instance({}) resumed successfully, heartbeat restarted", instanceID);
                    return Status::OK();
                });
        });
}

}  // namespace functionsystem::local_scheduler
