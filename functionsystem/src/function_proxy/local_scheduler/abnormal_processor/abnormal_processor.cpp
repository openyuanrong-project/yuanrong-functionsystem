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
#include "abnormal_processor.h"

#include <functional>

#include "async/defer.hpp"
#include "timer/timertools.hpp"

namespace functionsystem::local_scheduler {
const std::string ABNORMAL_PREFIX = "/yr/abnormal/localscheduler/";
const int64_t QUERY_LOCAL_INTERVAL = 3000;

AbnormalProcessor::~AbnormalProcessor()
{
    if (isStarted_ && actor_ != nullptr) {
        litebus::Terminate(actor_->GetAID());
        litebus::Await(actor_->GetAID());
    }
}

void AbnormalProcessor::BindObserver(const std::shared_ptr<function_proxy::ControlPlaneObserver> &observer)
{
    actor_->BindObserver(observer);
}

void AbnormalProcessor::BindInstanceCtrl(const std::shared_ptr<InstanceCtrl> &instanceCtrl)
{
    actor_->BindInstanceCtrl(instanceCtrl);
}

void AbnormalProcessor::BindRaiseWrapper(const std::shared_ptr<RaiseWrapper> &raiseWrapper)
{
    actor_->BindRaiseWrapper(raiseWrapper);
}

void AbnormalProcessor::BindMetaStoreClient(const std::shared_ptr<MetaStoreClient> &metaStoreClient)
{
    actor_->BindMetaStoreClient(metaStoreClient);
}

void AbnormalProcessor::Start()
{
    (void)litebus::Spawn(actor_);
    isStarted_ = true;
}

litebus::Future<Status> AbnormalProcessor::Recover()
{
    return litebus::Async(actor_->GetAID(), &AbnormalProcessorActor::CheckLocalSchedulerIsLegal)
        .Then([](const bool &isLegal) -> litebus::Future<Status> {
            if (!isLegal) {
                return Status(StatusCode::FAILED, "local scheduler is abnormal.");
            }
            return Status::OK();
        });
}

void AbnormalProcessor::BindFunctionAgentMgr(const std::shared_ptr<FunctionAgentMgr> &functionAgentMgr)
{
    actor_->BindFunctionAgentMgr(functionAgentMgr);
}

AbnormalProcessorActor::AbnormalProcessorActor(const std::string &id)
    : BasisActor(ABNORMAL_ACTOR), id_(id), queryIntervalMs_(QUERY_LOCAL_INTERVAL)
{
}

void AbnormalProcessorActor::Finalize()
{
    (void)litebus::TimerTools::Cancel(abnormalWatchTimer_);
}

void AbnormalProcessorActor::BindObserver(const std::shared_ptr<function_proxy::ControlPlaneObserver> &observer)
{
    ASSERT_IF_NULL(observer);
    observer_ = observer;
}

void AbnormalProcessorActor::BindInstanceCtrl(const std::shared_ptr<InstanceCtrl> &instanceCtrl)
{
    ASSERT_IF_NULL(instanceCtrl);
    instanceCtrl_ = instanceCtrl;
}

void AbnormalProcessorActor::BindFunctionAgentMgr(const std::shared_ptr<FunctionAgentMgr> &functionAgentMgr)
{
    ASSERT_IF_NULL(functionAgentMgr);
    functionAgentMgr_ = functionAgentMgr;
}

void AbnormalProcessorActor::BindRaiseWrapper(const std::shared_ptr<RaiseWrapper> &raiseWrapper)
{
    ASSERT_IF_NULL(raiseWrapper);
    raiseWrapper_ = raiseWrapper;
}

void AbnormalProcessorActor::BindMetaStoreClient(const std::shared_ptr<MetaStoreClient> &metaStoreClient)
{
    ASSERT_IF_NULL(metaStoreClient);
    metaStoreClient_ = metaStoreClient;
}

void AbnormalProcessorActor::SchedulerAbnormalWatcher(const std::vector<WatchEvent> &events)
{
    for (const auto &event : events) {
        auto eventKey = TrimKeyPrefix(event.kv.key(), metaStoreClient_->GetTablePrefix());
        YRLOG_WARN("receive self local abnormal, type: {}, key: {}", fmt::underlying(event.eventType), eventKey);
        ASSERT_IF_NULL(instanceCtrl_);
        ASSERT_IF_NULL(functionAgentMgr_);
        if (event.eventType == EventType::EVENT_TYPE_PUT) {
            instanceCtrl_->SetAbnormal();
            functionAgentMgr_->SetAbnormal();
            SchedulerAbnormaled(std::make_shared<litebus::Promise<bool>>());
        }
    }
}

litebus::Future<bool> AbnormalProcessorActor::CheckLocalSchedulerIsLegal()
{
    auto selfKey = ABNORMAL_PREFIX + id_;
    ASSERT_IF_NULL(metaStoreClient_);
    ASSERT_IF_NULL(instanceCtrl_);
    ASSERT_IF_NULL(functionAgentMgr_);
    return metaStoreClient_->Get(selfKey, { .prefix = false })
        .Then([instanceCtrl(instanceCtrl_), functionAgentMgr(functionAgentMgr_), aid(GetAID()),
               selfKey](const std::shared_ptr<GetResponse> &response) -> litebus::Future<bool> {
            if (response->status.IsError() || response->kvs.empty()) {
                litebus::Async(aid, &AbnormalProcessorActor::WatchAbnormal, selfKey);
                return true;
            }

            if (nlohmann::json::accept(response->kvs.front().value())) {
                nlohmann::json getRespKvs = nlohmann::json::parse(response->kvs.front().value());
                if (getRespKvs.contains("instanceManagerActorAid")) {
                    YRLOG_WARN("current local is abnormal set by {}, process will be killed by self",
                               getRespKvs.at("instanceManagerActorAid").dump());
                }
            } else {
                YRLOG_WARN("current local is abnormal, process will be killed by self");
            }

            instanceCtrl->SetAbnormal();
            functionAgentMgr->SetAbnormal();
            auto abnormaled = std::make_shared<litebus::Promise<bool>>();
            litebus::Async(aid, &AbnormalProcessorActor::SchedulerAbnormaled, abnormaled);
            return abnormaled->GetFuture();
        });
}

litebus::Future<bool> AbnormalProcessorActor::WatchAbnormal(const std::string &selfKey)
{
    auto watchOpt = WatchOption{ false, true };
    YRLOG_INFO("Register abnormal watch with key: {}", selfKey);
    ASSERT_IF_NULL(metaStoreClient_);
    auto syncer = [aid(GetAID())](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<SyncResult> {
        return litebus::Async(aid, &AbnormalProcessorActor::AbnormalSyncer, getResponse);
    };
    return metaStoreClient_
        ->Watch(selfKey, watchOpt,
                [aid(GetAID())](const std::vector<WatchEvent> &events, bool) {
                    auto respCopy = events;
                    litebus::Async(aid, &AbnormalProcessorActor::SchedulerAbnormalWatcher, respCopy);
                    return true;
                }, syncer)
        .Then([selfKey](const std::shared_ptr<Watcher> &watcher) {
            if (watcher == nullptr) {
                YRLOG_ERROR("failed to register abnormal watch with key: {}", selfKey);
                return false;
            }
            return true;
        });
}

litebus::Future<SyncResult> AbnormalProcessorActor::AbnormalSyncer(const std::shared_ptr<GetResponse> &getResponse)
{
    auto prefixKey = ABNORMAL_PREFIX + id_;
    if (getResponse->status.IsError()) {
        YRLOG_INFO("failed to get key({}) from meta storage", prefixKey);
        return SyncResult{ getResponse->status };
    }

    if (getResponse->kvs.empty()) {
        YRLOG_INFO("get no result with key({}) from meta storage, revision is {}", prefixKey,
                   getResponse->header.revision);
        return SyncResult{ Status::OK() };
    }
    std::vector<WatchEvent> events;
    for (auto &kv : getResponse->kvs) {
        WatchEvent event{ .eventType = EVENT_TYPE_PUT, .kv = kv, .prevKv = {} };
        (void)events.emplace_back(event);
    }
    SchedulerAbnormalWatcher(events);
    return SyncResult{ Status::OK() };
}

void AbnormalProcessorActor::SchedulerAbnormaled(const std::shared_ptr<litebus::Promise<bool>> abnormaled)
{
    ASSERT_IF_NULL(observer_);
    // Polling to check whether local instances still exist
    (void)observer_->GetLocalInstances([](const resource_view::InstanceInfo &info) {
        return !info.lowreliability();
    }).Then(
        litebus::Defer(GetAID(), &AbnormalProcessorActor::OnSchedulerAbnormaled, std::placeholders::_1, abnormaled));
}

bool AbnormalProcessorActor::OnSchedulerAbnormaled(const std::vector<std::string> &localInstances,
                                                   const std::shared_ptr<litebus::Promise<bool>> abnormaled)
{
    std::string instances;
    for (auto instanceID: localInstances) {
        instances = instances + "|" + instanceID;
    }
    if (instances.empty()) {
        YRLOG_WARN("All local instances have been taken over. ready to exit");
        litebus::Async(GetAID(), &AbnormalProcessorActor::CommitSuicide);
        abnormaled->SetValue(false);
        return false;
    }
    YRLOG_WARN("instances({}) does not to be taken over. keep waiting..", instances);
    abnormalWatchTimer_ =
            litebus::AsyncAfter(queryIntervalMs_, GetAID(), &AbnormalProcessorActor::SchedulerAbnormaled, abnormaled);
    return true;
}

void AbnormalProcessorActor::CommitSuicide()
{
    auto selfKey = ABNORMAL_PREFIX + id_;
    ASSERT_IF_NULL(metaStoreClient_);
    ASSERT_IF_NULL(raiseWrapper_);
    (void)metaStoreClient_->Delete(selfKey, { false, false })
        .Then([raiseWrapper(raiseWrapper_), selfKey](const std::shared_ptr<DeleteResponse> &deleteResponse) {
            if (deleteResponse->status.IsError()) {
                YRLOG_WARN("failed to delete abnormal information code ({}), which may cause another restart",
                           fmt::underlying(deleteResponse->status.StatusCode()));
            }
            YRLOG_ERROR("local is abnormal, raise SIGINT to exit");
            (void)raiseWrapper->Raise(SIGINT);
            return false;
        });
}

}  // namespace functionsystem::local_scheduler
