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

#ifndef FUNCTION_PROXY_COMMON_OBSERVER_CONTROL_OBSERVER_H
#define FUNCTION_PROXY_COMMON_OBSERVER_CONTROL_OBSERVER_H

#include <async/future.hpp>

#include "meta_storage_accessor/meta_storage_accessor.h"
#include "common/metadata/metadata.h"
#include "common/resource_view/resource_type.h"
#include "common/status/status.h"
#include "function_proxy/common/observer/observer_actor.h"

namespace functionsystem::function_proxy {
class ControlPlaneObserver : public TenantObserver {
public:
    explicit ControlPlaneObserver(const std::shared_ptr<ObserverActor> &actor);

    ~ControlPlaneObserver() noexcept override;
    ControlPlaneObserver(const ControlPlaneObserver &) = delete;
    ControlPlaneObserver &operator=(const ControlPlaneObserver &) = delete;

    virtual void SetDriverEventCbFunc(const DriverEventCbFunc &driverCbFunc);
    virtual void SetInstanceInfoSyncerCbFunc(const InstanceInfoSyncerCbFunc &instanceInfoSyncerCbFunc);
    virtual void SetUpdateFuncMetasFunc(const UpdateFuncMetasFunc &updateFuncMetasFunc);
    virtual void SetTrafficReportCbFunc(const TrafficReportCbFunc &trafficReportCbFunc);

    /**
     * register callback to meta store accessor
     */
    void Register() const;

    /**
     * put instance to meta store
     * @param instanceInfo: InstanceInfo struct
     * @return: Future Status
     */
    virtual litebus::Future<Status> PutInstance(const resource_view::InstanceInfo &instanceInfo) const;

    /**
     * delete instance info
     * @param instanceID
     * @return
     */
    virtual litebus::Future<Status> DelInstance(const std::string &instanceID) const;

    /**
     * get instanceInfo by instanceID
     * @param instanceID
     * @return instanceInfo
     */
    virtual litebus::Future<litebus::Option<resource_view::InstanceInfo>> GetInstanceInfoByID(
        const std::string &instanceID) const;

    /**
     * get all instanceInfo of a function agent by funcAgentID
     * @param funcAgentID
     * @return all instanceInfo of a function agent
     */
    virtual litebus::Future<litebus::Option<InstanceInfoMap>> GetAgentInstanceInfoByID(
        const std::string &funcAgentID) const;

    /**
     * get function meta by funcKey
     * @param funcKey
     * @return future option of FunctionMeta
     */
    virtual litebus::Future<litebus::Option<FunctionMeta>> GetFuncMeta(const std::string &funcKey) const;

    /**
     * get aid of instance ctrl in local scheduler by proxyID
     * @param proxyID
     * @return future option of aid of instance ctrl in local scheduler
     */
    virtual litebus::Future<litebus::Option<litebus::AID>> GetLocalSchedulerAID(const std::string &proxyID) const;

    /**
     * get all instanceInfo on this node
     *
     * @return all instanceInfo on this node
     */
    virtual litebus::Future<litebus::Option<InstanceInfoMap>> GetLocalInstanceInfo() const;

    virtual litebus::Future<InstanceInfoMap> GetAllInstanceInfos() const;

    /**
     * judge the function is or not system function
     * @param function function key
     * @return is or not
     */
    virtual litebus::Future<bool> IsSystemFunction(const std::string &function) const;

    virtual litebus::Future<std::vector<std::string>> GetLocalInstances(
        const std::function<bool(const resource_view::InstanceInfo &)> &filter);

    virtual void Attach(const std::shared_ptr<InstanceListener> &listener) const;

    virtual void Detach(const std::shared_ptr<InstanceListener> &listener) const;

    virtual litebus::Future<Status> PutInstanceEvent(const resource_view::InstanceInfo &instanceInfo, bool synced,
                                                     int64_t modRevision);

    virtual void FastPutRemoteInstanceEvent(const resource_view::InstanceInfo &instanceInfo, bool synced,
                                            int64_t modRevision);

    virtual litebus::Future<Status> DelInstanceEvent(const std::string &instanceID, int64_t modRevision);

    virtual void WatchInstance(const std::string &instanceID, int64_t revision = 0);

    virtual litebus::Future<bool> IsInstanceWatched(const std::string &instanceID);

    virtual litebus::Future<resource_view::InstanceInfo> GetAndWatchInstance(const std::string &instanceID);

    virtual litebus::Future<resource_view::InstanceInfo> GetOrWatchInstance(const std::string &instanceID);

    virtual void CancelWatchInstance(const std::string &instanceID);

    litebus::Future<bool> InstanceSyncDone();

    void AttachTenantListener(const std::shared_ptr<TenantListener> &listener) override;

    void DetachTenantListener(const std::shared_ptr<TenantListener> &listener) override;

    void NotifyUpdateTenantInstance(const TenantEvent &event) override;

    void NotifyDeleteTenantInstance(const TenantEvent &event) override;

private:
    std::shared_ptr<ObserverActor> observerActor_;
};
}  // namespace functionsystem::function_proxy

#endif  // FUNCTION_PROXY_COMMON_OBSERVER_CONTROL_OBSERVER_H
