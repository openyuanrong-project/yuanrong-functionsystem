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
#include "request_dispatcher.h"

#include "common/metrics/metrics_adapter.h"
#include "function_proxy/busproxy/invocation_handler/invocation_handler.h"

namespace functionsystem::busproxy {

SharedStreamMsg CreateCallResponse(const common::ErrorCode &code, const std::string &message,
                                   const std::string &messageID)
{
    auto response = std::make_shared<runtime_rpc::StreamingMessage>();
    response->set_messageid(messageID);
    auto callResponse = response->mutable_callrsp();
    callResponse->set_code(Status::GetPosixErrorCode(code));
    callResponse->set_message(message);
    return response;
}

SharedStreamMsg CreateCallResultAck(const common::ErrorCode &code, const std::string &message,
                                    const std::string &messageID)
{
    auto response = std::make_shared<runtime_rpc::StreamingMessage>();
    response->set_messageid(messageID);
    auto callResultAck = response->mutable_callresultack();
    callResultAck->set_code(Status::GetPosixErrorCode(code));
    callResultAck->set_message(message);
    return response;
}

runtime::NotifyRequest CallResultToNotifyRequest(const SharedStreamMsg &request)
{
    auto callresult = request->mutable_callresultreq();
    runtime::NotifyRequest notifyRequest;
    *notifyRequest.mutable_message() = std::move(*callresult->mutable_message());
    notifyRequest.set_code(Status::GetPosixErrorCode(callresult->code()));
    notifyRequest.set_requestid(callresult->requestid());
    notifyRequest.mutable_smallobjects()->Swap(callresult->mutable_smallobjects());
    notifyRequest.mutable_stacktraceinfos()->Swap(callresult->mutable_stacktraceinfos());
    if (callresult->has_runtimeinfo()) {
        notifyRequest.mutable_runtimeinfo()->Swap(callresult->mutable_runtimeinfo());
    }
    return notifyRequest;
}

common::ErrorCode TransFatalCode(const StatusCode &fatalCode)
{
    switch (fatalCode) {
        // while instance is fatal, the ret code should not be Retryable code
        case ERR_REQUEST_BETWEEN_RUNTIME_BUS:
        case ERR_INNER_COMMUNICATION: {
            return common::ERR_INSTANCE_EXITED;
        }
        default:
            return static_cast<common::ErrorCode>(fatalCode);
    }
}

litebus::Future<SharedStreamMsg> RequestDispatcher::Call(const SharedStreamMsg &request,
                                                         const CallerInfo &callerInfo)
{
    ASSERT_FS(request->has_callreq());
    auto callReq = request->callreq();
    // 已退出，不再具备消息处理能力
    if (isFatal_) {
        YRLOG_ERROR("{}|{}|instance({}) is fatal, failed to call", callReq.traceid(), callReq.requestid(), instanceID_);
        // When runtime Invoke a stateless function, determine whether to re-create an instance
        // based on the error code 1003.
        auto err = fmt::format("instance occurs fatal error, cause by: ({})",
                               fatalMsg_.empty() ? "unknown reason" : fatalMsg_);
        return CreateCallResponse(TransFatalCode(fatalCode_), err, request->messageid());
    }
    // 驱逐和亚健康状态，只处理强制调用消息
    if (isReject_ && !callReq.createoptions().contains("ENABLE_FORCE_INVOKE")) {
        YRLOG_ERROR("{}|{}|instance({}) is rejected to handler request, {}|{}", callReq.traceid(), callReq.requestid(),
                    instanceID_, fmt::underlying(fatalCode_), fatalMsg_);
        return CreateCallResponse(static_cast<common::ErrorCode>(fatalCode_), fatalMsg_, request->messageid());
    }
    ASSERT_IF_NULL(callCache_);
    if (auto context(callCache_->FindCallRequestContext(callReq.requestid())); context != nullptr) {
        // If the instance is ready, will resend it; If the instance is not ready, do nothing, which means multiple
        // requests coming before instance ready will only send once.
        if (!isReady_) {
            YRLOG_INFO("{}|call request already in cache, won't resend it due to the instance is still not ready",
                       callReq.requestid());
            return context->callResponse.GetFuture();
        }
        TriggerCall(callReq.requestid());
        return context->callResponse.GetFuture();
    }
    litebus::Promise<SharedStreamMsg> callResponse;
    auto callRequestContext = std::make_shared<CallRequestContext>();
    callRequestContext->from = callReq.senderid();
    callRequestContext->requestID = callReq.requestid();
    callRequestContext->traceID = callReq.traceid();
    callRequestContext->callRequest = request;
    callRequestContext->callResponse = callResponse;
    callRequestContext->callerTenantID = callerInfo.tenantID;
    callCache_->Push(callRequestContext);
    if (isReady_) {
        TriggerCall(callRequestContext->requestID);
    } else if (callReq.createoptions().contains("ENABLE_FORCE_INVOKE")) {
        // 1. used instance belongs to graceful shutdown scenario, forward force invoke directly
        // 2. not used remote instance, if aid is not empty, forward force invoke
        if (used_ || (!local_ && !remoteAid_.Name().empty())) {
            YRLOG_INFO("{}|forward force invoke when instance exiting.", callReq.requestid());
            TriggerCall(callRequestContext->requestID);
        }
    }
    return callResponse.GetFuture();
}

void RecordInvokeMetrics(const SharedStreamMsg &request, const std::string &instanceID)
{
    std::map<std::string, std::string> invokeOptMap;
    auto callRequest = request->callreq();
    for (const auto &ite : callRequest.createoptions()) {
        invokeOptMap[ite.first] = ite.second;
    }
    functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetBillingInvokeOptions(
        callRequest.requestid(), invokeOptMap, callRequest.function(), instanceID);
}

void RequestDispatcher::TriggerCall(const std::string &requestID)
{
    ASSERT_IF_NULL(callCache_);
    auto context = callCache_->FindCallRequestContext(requestID);
    if (context == nullptr) {
        YRLOG_ERROR("{}|invoke request context is null.", requestID);
        return;
    }
    auto request = context->callRequest;
    auto associate = [promise(context->callResponse), request](const litebus::Future<SharedStreamMsg> &future) {
        SharedStreamMsg rsp;
        if (future.IsError()) {
            rsp = CreateCallResponse(common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS,
                                     "connection with runtime may be interrupted, please retry.", request->messageid());
        } else {
            rsp = future.Get();
            rsp->set_messageid(request->messageid());
        }
        promise.SetValue(rsp);
    };
    // If local_ is true, means the InstanceActor and the Runtime on the same node.
    if (local_) {
        if (!AuthorizeInvoke(context->callerTenantID).IsOk()) {
            YRLOG_ERROR("{}|failed to authorize invoke", request->messageid());
            SharedStreamMsg rsp =
                CreateCallResponse(common::ErrorCode::ERR_AUTHORIZE_FAILED, "authorize failed", request->messageid());
            context->callResponse.SetValue(rsp);
            return;
        }
        ++callTimes_;
        ASSERT_IF_NULL(dataInterfaceClient_);
        // Send request to Runtime.
        YRLOG_INFO("{}|{}|send Call instance({}) request to local({}).", context->traceID, requestID, instanceID_,
                   runtimeID_);
        perf_->RecordSendCall(requestID);
        localStartCallTimeMap_[requestID] = std::chrono::system_clock::now();
        RecordInvokeMetrics(request, instanceID_);
        (void)dataInterfaceClient_->Call(request).OnComplete(associate);
        callCache_->MoveToOnResp(requestID);
        return;
    }
    // If InstanceActor is not local, forward request to another busproxy.
    auto forward = remoteClient_.lock();
    if (forward == nullptr) {
        return;
    }
    YRLOG_INFO("{}|{}|send Call instance({}) request to remote({}).", context->traceID, requestID, instanceID_,
               proxyID_);
    perf_->RecordSendCall(requestID);
    (void)forward->SendForwardCall(remoteAid_, context->callerTenantID, request).OnComplete(associate);
    callCache_->MoveToOnResp(requestID);
    return;
}

Status RequestDispatcher::AuthorizeInvoke(const std::string &callerTenantID)
{
    if (internalIam_ == nullptr || !internalIam_->IsIAMEnabled() ||
        verifiedCallerTenantIDs_.find(callerTenantID) != verifiedCallerTenantIDs_.end()) {
        return Status::OK();
    }
    function_proxy::AuthorizeParam authorizeParam{ .callerTenantID = callerTenantID,
                                                   .calleeTenantID = tenantID_,
                                                   .callMethod = function_proxy::CALL_METHOD_INVOKE,
                                                   .funcName = function_ };
    auto status = internalIam_->Authorize(authorizeParam);
    if (status.IsOk()) {
        verifiedCallerTenantIDs_.emplace(callerTenantID);
    }
    return status;
}

litebus::Future<SharedStreamMsg> RequestDispatcher::CallResult(const SharedStreamMsg &request)
{
    if (isFatal_) {
        YRLOG_ERROR("failed to send call result, target instance({}) is fatal", instanceID_);
        return CreateCallResultAck(static_cast<common::ErrorCode>(fatalCode_), fatalMsg_, request->messageid());
    }
    ASSERT_FS(request->has_callresultreq());
    auto callresult = request->callresultreq();
    const auto &requestID = callresult.requestid();

    if (!local_) {
        auto forward = remoteClient_.lock();
        if (forward == nullptr) {
            return CreateCallResultAck(::common::ERR_INNER_COMMUNICATION, "no route to instance", request->messageid());
        }
        YRLOG_DEBUG("{}|forward CallResult to remote({}) instance ({}).", requestID, proxyID_, instanceID_);
        perf_->RecordSendCallResult(requestID);
        return forward->SendForwardCallResult(remoteAid_, request);
    }
    // Send request to Runtime.
    YRLOG_INFO("{}|send CallResult to local({}) instance from instance({}).", requestID, runtimeID_, instanceID_);
    auto promise = std::make_shared<litebus::Promise<SharedStreamMsg>>();
    auto associate = [promise, request](const litebus::Future<runtime::NotifyResponse> &future) {
        if (future.IsError()) {
            auto rsp =
                CreateCallResultAck(common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS,
                                    "connection with runtime may be interrupted, please retry.", request->messageid());
            promise->SetValue(rsp);
            return;
        }
        auto rsp = CreateCallResultAck(common::ErrorCode::ERR_NONE, "success", request->messageid());
        promise->SetValue(rsp);
    };
    // while init calling, caller invoke another instance would be failed because of its status is not ready.
    // sending notify request to unready caller instance would be failure, we can try to obtain the corresponding
    // client sent result from the client manager while bus-proxy and local-scheduler is deployed as one process.
    if (dataInterfaceClient_ == nullptr || dataInterfaceClient_->IsDone()) {
        ASSERT_FS(clientManager_);
        return clientManager_->GetDataInterfacePosixClient(instanceID_)
            .Then([associate, request, promise, perfCtx(perf_->GetPerfContext(requestID))](
                      const std::shared_ptr<DataInterfacePosixClient> &client) -> litebus::Future<SharedStreamMsg> {
                if (client == nullptr) {
                    return CreateCallResultAck(::common::ERR_REQUEST_BETWEEN_RUNTIME_BUS, "no route to instance",
                                               request->messageid());
                }
                if (perfCtx) {
                    perfCtx->proxySendCallResultTime =
                        std::make_shared<TimePoint>(std::chrono::high_resolution_clock::now());
                }
                (void)client->NotifyResult(std::move(CallResultToNotifyRequest(request))).OnComplete(associate);
                return promise->GetFuture();
            });
    }
    perf_->RecordSendCallResult(requestID);
    dataInterfaceClient_->NotifyResult(std::move(CallResultToNotifyRequest(request))).OnComplete(associate);
    return promise->GetFuture();
}

void RequestDispatcher::OnCall(const SharedStreamMsg &callRsp, const std::string &traceID, const std::string &requestID)
{
    ASSERT_FS(callRsp->has_callrsp());
    const auto &response = callRsp->mutable_callrsp();
    YRLOG_INFO("{}|{}|receive Call response from instance({}).", traceID, requestID, instanceID_);
    if (response->code() == common::ERR_NONE) {
        callCache_->MoveToInProgress(requestID);
    } else {
        if (local_) {
            ++failedCallTimes_;
            ReportCallLatency(requestID, response->code());
        }
        callCache_->DeleteReqNew(requestID);
    }
}

void RequestDispatcher::OnCallResult(const SharedStreamMsg &callResultAck, const std::string &requestID,
                                     common::ErrorCode callResultCode)
{
    ASSERT_FS(callResultAck->has_callresultack());
    auto ack = callResultAck->callresultack();

    if (local_) {
        ReportCallLatency(requestID, callResultCode);
    }

    if (ack.code() != common::ErrorCode::ERR_NONE) {
        if (local_) {
            ++failedCallTimes_;
        }
        return;
    }
    ASSERT_IF_NULL(callCache_);
    callCache_->DeleteReqInProgress(requestID);
}

void RequestDispatcher::UpdateInfo(const std::shared_ptr<InstanceRouterInfo> &info)
{
    local_ = info->isLocal;
    if (info->localClient != nullptr) {
        dataInterfaceClient_ = info->localClient;
    }
    bool isReady = info->isReady;
    if (info->isLocal && dataInterfaceClient_ == nullptr) {
        isReady = false;
    }
    if (!info->remote.Name().empty()) {
        remoteAid_ = info->remote;
    }
    // avoid to forward request when remote is empty
    if (!info->isLocal && remoteAid_.Name().empty()) {
        isReady = false;
    }
    proxyID_ = info->proxyID;
    isFatal_ = false;
    isReject_ = false;
    runtimeID_ = info->runtimeID;
    tenantID_ = info->tenantID;
    function_ = info->function;
    // Update traffic report type only when the instance is running and ready
    if (local_ && isReady) {
        trafficReportType_ = info->trafficReportType;
        if (trafficReportType_ == resources::TrafficReportType::None) {
            // disable traffic report
            callCache_->SetTrafficReport(nullptr);
        } else {
            // enable traffic report
            // same lifecycle with RequestDispatcher which can be safe to capture this pointer
            ASSERT_IF_NULL(callCache_);
            YRLOG_INFO("enable traffic report for instance({}) report type({})",
                       instanceID_, fmt::underlying(trafficReportType_));
            callCache_->SetTrafficReport([this](bool idle, const size_t &size) { ReportTrafficMetrics(idle, size); });
            ReportTrafficMetrics(true, 0);
        }
    }
    if (!local_ && isReady_) {
        // if instance is in remote node, subscribed event may be late, ignored unready event
        return;
    }
    if (isReady_ == isReady) {
        // 3. not ready remote instance and aid is not empty, forward force invoke
        if (!isReady_ && !local_ && !remoteAid_.Name().empty()) {
            const auto requests = callCache_->GetNewReqs();
            for (auto &request : requests) {
                auto context = callCache_->FindCallRequestContext(request);
                if (context != nullptr && context->callRequest->has_callreq() &&
                    context->callRequest->callreq().createoptions().contains("ENABLE_FORCE_INVOKE")) {
                    TriggerCall(request);
                }
            }
        }
        return;
    }
    isReady_ = isReady;
    if (isReady_) {
        used_ = true;
        callCache_->MoveAllToNew();
        auto reqNew = callCache_->GetNewReqs();
        for (auto &req : reqNew) {
            TriggerCall(req);
        }
    }
}

void RequestDispatcher::Fatal(const std::string &message, const StatusCode &code)
{
    fatalMsg_ = message;
    // code should never be Success while instance fatal
    fatalCode_ = code == StatusCode::SUCCESS ? StatusCode::ERR_INSTANCE_EXITED : code;
    isFatal_ = true;
    ResponseAllMessage();
    ReportCallTimesMetrics();
}

void RequestDispatcher::ResponseAllMessage()
{
    YRLOG_INFO("reply all message for instance: {}", instanceID_);
    ASSERT_IF_NULL(callCache_);
    auto reqNew = callCache_->GetNewReqs();
    for (auto &req : reqNew) {
        auto context = callCache_->FindCallRequestContext(req);
        if (context == nullptr) {
            YRLOG_ERROR("{}|not find call request for call response to gracefully shutdown.", req);
            continue;
        }
            auto callResponse = CreateCallResponse(Status::GetPosixErrorCode(fatalCode_), fatalMsg_, req);
            context->callResponse.SetValue(callResponse);
            callCache_->DeleteReqNew(req);
    }

    auto reqOnResp = callCache_->GetOnResp();
    for (auto &req : reqOnResp) {
        auto context = callCache_->FindCallRequestContext(req);
        if (context == nullptr) {
            YRLOG_ERROR("{}|not find call request for call response to gracefully shutdown.", req);
            continue;
        }
        auto callResponse = CreateCallResponse(Status::GetPosixErrorCode(fatalCode_), fatalMsg_, req);
        context->callResponse.SetValue(callResponse);
        SendNotify(req, context);
        callCache_->DeleteReqOnResp(req);
    }

    auto reqInProgress = callCache_->GetInProgressReqs();
    for (auto &req : reqInProgress) {
        auto context = callCache_->FindCallRequestContext(req);
        if (context == nullptr) {
            YRLOG_ERROR("{}|not find call request for call result to gracefully shutdown.", req);
            continue;
        }
        SendNotify(req, context);
        callCache_->DeleteReqInProgress(req);
    }
}

void RequestDispatcher::SendNotify(const std::basic_string<char> &req,
                                   const std::shared_ptr<CallRequestContext> &context) const
{
    auto response = std::make_shared<runtime_rpc::StreamingMessage>();
    response->set_messageid(req);
    auto callResult = response->mutable_callresultreq();
    callResult->set_code(Status::GetPosixErrorCode(fatalCode_));
    callResult->set_message(fatalMsg_);
    callResult->set_instanceid(context->from);
    callResult->set_requestid(req);
    (void)InvocationHandler::CallResultAdapter(instanceID_, response);
}

std::list<litebus::Future<SharedStreamMsg>> RequestDispatcher::GetOnRespFuture()
{
    ASSERT_IF_NULL(callCache_);
    return callCache_->GetOnRespFuture();
}

void RequestDispatcher::ReportCallTimesMetrics()
{
    if (callTimes_ == 0) {
        return;
    }

    functionsystem::metrics::LabelType labels = {
        { "instance_id", instanceID_ },
        { "failed_times", std::to_string(failedCallTimes_) },
    };
    struct functionsystem::metrics::MeterData data {
        static_cast<double>(callTimes_), labels
    };
    functionsystem::metrics::MeterTitle totalTitle{ "yr_app_instance_invoke_times", "instance invoke total times",
                                                    "num" };
    functionsystem::metrics::MetricsAdapter::GetInstance().ReportGauge(totalTitle, data);
}

void RequestDispatcher::ReportCallLatency(const std::string &requestID, common::ErrorCode errCode)
{
    if (localStartCallTimeMap_.find(requestID) == localStartCallTimeMap_.end()) {
        return;
    }
    auto endTime = std::chrono::system_clock::now();
    auto startTime = localStartCallTimeMap_[requestID];
    auto startTimeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(startTime.time_since_epoch()).count();
    auto endTimeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(endTime.time_since_epoch()).count();

    functionsystem::metrics::MetricsAdapter::GetInstance().ReportBillingInvokeLatency(
        requestID, static_cast<uint32_t>(errCode), startTimeMillis, endTimeMillis);

    localStartCallTimeMap_.erase(requestID);
}

void RequestDispatcher::Reject(const std::string &message, const StatusCode &code)
{
    fatalMsg_ = message;
    fatalCode_ = code;
    isReject_ = true;
}

void RequestDispatcher::ReportTrafficMetrics(bool idle, const size_t &size)
{
    if (trafficReportType_ == resources::TrafficReportType::None) {
        return;
    }
    // TODO(Lwy_Robb): resources::TrafficReportType::Periodic metric reporting
    YRLOG_INFO("instance({}) report traffic idle({}) size({}) current({})",
               instanceID_, idle, size, currentTrafficIdleState_);
    // only report when idle state changed
    if (currentTrafficIdleState_ != idle) {
        observer_->ReportTraffic(instanceID_, size);
        currentTrafficIdleState_ = idle;
    }
}
}  // namespace functionsystem::busproxy