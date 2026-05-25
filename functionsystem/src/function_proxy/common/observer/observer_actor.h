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

#ifndef FUNCTION_PROXY_COMMON_OBSERVER_H
#define FUNCTION_PROXY_COMMON_OBSERVER_H

#include "actor/actor.hpp"
#include "async/option.hpp"
#include "common/meta_store_adapter/instance_operator.h"
#include "function_proxy/busproxy/instance_view/instance_view.h"
#include "function_proxy/busproxy/registry/constants.h"
#include "function_proxy/common/data_view/local_scheduler_view/local_scheduler_view.h"
#include "function_proxy/common/data_view/proxy_view/proxy_view.h"
#include "function_proxy/common/posix_client/data_plane_client/data_interface_client_manager_proxy.h"
#include "instance_observer.h"
#include "meta_storage_accessor/meta_storage_accessor.h"
#include "meta_store_client/key_value/watcher.h"
#include "meta_store_client/meta_store_struct.h"
#include "meta_store_client/watch_client.h"
#include "common/utils/meta_store_kv_operation.h"
#include "common/metadata/metadata.h"
#include "common/proto/pb/message_pb.h"
#include "common/proto/pb/posix_pb.h"
#include "common/resource_view/resource_type.h"
#include "tenant_observer.h"

namespace functionsystem::function_proxy {

// key: instance id
using InstanceInfoMap = std::unordered_map<std::string, resource_view::InstanceInfo>;
using KillInstanceCbFunc = std::function<void(const std::string &instanceID)>;
using FunctionAccessorEventCbFunc = std::function<void(const resource_view::InstanceInfo &instanceInfo)>;
using DriverEventCbFunc = std::function<void(const resource_view::InstanceInfo &instanceInfo)>;
using InstanceStatusToRunningCbFunc = std::function<void(const resource_view::InstanceInfo &instanceInfo)>;
using InstanceInfoSyncerCbFunc = std::function<void(const resource_view::RouteInfo &routeInfo)>;
using UpdateFuncMetasFunc =
    std::function<void(bool isAdd, const std::unordered_map<std::string, FunctionMeta> &funcMetas)>;

using TrafficReportCbFunc = std::function<void(const std::string &instanceID, const size_t &size)>;

const int SERVICE_TTL = 300000;  // ms

struct ObserverParam {
    std::string servicesPath;
    std::string libPath;
    std::string functionMetaPath;
    bool enableIpv4TenantIsolation{ true };  // for coverage
    bool enableTenantAffinity{ true };       // for coverage
    bool isMetaStoreEnabled{ false };
    bool isPartialWatchInstances{ false };
    int serviceTTL;
};

// Service registration information
struct RegisterInfo {
    // Service registration key
    std::string key;

    // Service registration value
    struct ProxyMeta meta;
};

[[maybe_unused]] inline RegisterInfo GetServiceRegistryInfo(const std::string &nodeID, const litebus::AID &aid)
{
    // fmt: /yr/busproxy/business/yrk/tenant/0/node/{nodeID}
    std::string key = BUSPROXY_PATH_PREFIX + "/0/node/" + nodeID;
    RegisterInfo reg{
        .key = key,
        .meta = { .node = nodeID, .aid = std::string(aid), .ak = std::move(aid.GetAK()) },
    };
    return reg;
}

[[maybe_unused]] inline std::string Dump(ProxyMeta &proxyMeta)
{
    return nlohmann::json{ { "aid", proxyMeta.aid }, { "node", proxyMeta.node }, { "ak", proxyMeta.ak } }.dump();
}

[[maybe_unused]] inline bool TtlValidate(int ttl)
{
    return ttl >= MIN_TTL && ttl <= MAX_TTL;
}

class ObserverActor : public litebus::ActorBase, public InstanceObserver {
public:
    ObserverActor(const std::string &name, const std::string &nodeID,
                  const std::shared_ptr<MetaStorageAccessor> &metaStorageAccessor, ObserverParam &&observerParam)
        : ActorBase(name),
          nodeID_(nodeID),
          metaStorageAccessor_(metaStorageAccessor),
          observerParam_(std::move(observerParam)),
          instanceView_(std::make_shared<busproxy::InstanceView>(nodeID)),
          proxyView_(std::make_shared<ProxyView>()),
          localSchedulerView_(std::make_shared<LocalSchedulerView>()),
          isMetaStoreEnabled_(observerParam_.isMetaStoreEnabled),
          isPartialWatchInstances_(observerParam_.isPartialWatchInstances)
    {
        if (metaStorageAccessor != nullptr) {
            instanceOperator_ = std::make_shared<InstanceOperator>(metaStorageAccessor->GetMetaClient());
        }
        Attach(instanceView_);
        instanceView_->BindProxyView(proxyView_);
    }

    ~ObserverActor() override
    {
        instanceListenerList_.clear();
        tenantListenerList_.clear();
    }

    virtual void SetDriverEventCbFunc(const DriverEventCbFunc &driverCbFunc)
    {
        driverEventCbFunc_ = driverCbFunc;
    }

    virtual void SetInstanceInfoSyncerFunc(const InstanceInfoSyncerCbFunc &instanceInfoSyncerCbFunc)
    {
        instanceInfoSyncerCbFunc_ = instanceInfoSyncerCbFunc;
    }

    virtual void SetUpdateFuncMetasFunc(const UpdateFuncMetasFunc &updateFuncMetasFunc)
    {
        updateFuncMetasFunc_ = updateFuncMetasFunc;
        if (updateFuncMetasFunc_) {
            updateFuncMetasFunc_(true, funcMetaMap_);
            updateFuncMetasFunc_(true, systemFuncMetaMap_);
        }
    }

    virtual void SetTrafficReportCbFunc(const TrafficReportCbFunc &trafficReportCbFunc)
    {
        trafficReportCbFunc_ = trafficReportCbFunc;
    }

    virtual void BindDataInterfaceClientManager(
        const std::shared_ptr<DataInterfaceClientManagerProxy> &dataInterfaceClientManager)
    {
        dataInterfaceClientManager_ = dataInterfaceClientManager;
        instanceView_->BindDataInterfaceClientManager(dataInterfaceClientManager);
    }

    /**
     * register callback to meta store accessor
     */
    Status Register();

    /**
     * callback of update instance event
     * @param events: instance event info
     */
    void UpdateInstanceEvent(const std::vector<WatchEvent> &events, bool synced);

    /**
     * callback of update instanceRoute event
     * @param events: instanceRoute event info
     */
    void UpdateInstanceRouteEvent(const std::vector<WatchEvent> &events, bool synced);

    /**
     * callback of update function meta event
     * @param events: function meta event info
     */
    void UpdateFuncMetaEvent(const std::vector<WatchEvent> &events);

    /**
     * callback of update function meta event
     * @param events: function meta event info
     */
    virtual void UpdateProxyEvent(const std::vector<WatchEvent> &events);

    /**
     * put instance info to meta store
     * @param instanceInfo: instance info
     * @return result Status
     */
    litebus::Future<Status> PutInstance(const resource_view::InstanceInfo &instanceInfo, bool isForceUpdate = false);

    /**
     * delete instance info in meta store
     * @param instanceID
     * @return result Status
     */
    litebus::Future<Status> DelInstance(const std::string &instanceID);

    /**
     * get instanceInfo by instanceID
     * @param instanceID
     * @return instanceInfo
     */
    virtual litebus::Future<litebus::Option<resource_view::InstanceInfo>> GetInstanceInfoByID(
        const std::string &instanceID);

    /**
     * get all instanceInfo of a function agent by funcAgentID
     * @param funcAgentID
     * @return all instanceInfo of a function agent
     */
    virtual litebus::Option<InstanceInfoMap> GetAgentInstanceInfoByID(const std::string &funcAgentID);

    /**
     * get all instanceInfo on this node
     *
     * @return all instanceInfo on this node
     */
    virtual litebus::Option<InstanceInfoMap> GetLocalInstanceInfo();

    virtual litebus::Future<InstanceInfoMap> GetAllInstanceInfos();

    /**
     * get function meta by funcKey
     * @param funcKey
     * @return option of function meta
     */
    virtual litebus::Future<litebus::Option<FunctionMeta>> GetFuncMeta(const std::string &funcKey);

    /**
     * get aid of instance ctrl in local scheduler by proxyID
     * @param proxyID
     * @return option of AID
     */
    litebus::Future<litebus::Option<litebus::AID>> GetLocalSchedulerAID(const std::string &proxyID);

    /**
     * judge the function is or not system function
     * @param function function key
     * @return is or not
     */
    bool IsSystemFunction(const std::string &function)
    {
        return systemFuncMetaMap_.find(function) != systemFuncMetaMap_.end();
    }

    /**
     * get the instance located in current node or local scheduler, the driver would not be returned
 	 * @param filter A function that defines the filter criteria. It takes an
                     InstanceInfo object and returns true if the instance meets
                     the criteria, false otherwise.
 	 * @return the list of instance id that match the filter
 	 */
    std::vector<std::string> GetLocalInstances(
        const std::function<bool(const resource_view::InstanceInfo &)> &filter);

    void Attach(const std::shared_ptr<InstanceListener> &listener) override;

    void Detach(const std::shared_ptr<InstanceListener> &listener) override;

    Status PutInstanceEvent(const resource_view::InstanceInfo &instanceInfo, bool isForceUpdate, int64_t modRevision);

    void FastPutRemoteInstanceEvent(const resource_view::InstanceInfo &instanceInfo, bool synced, int64_t modRevision);

    litebus::Future<Status> DelInstanceEvent(const std::string &instanceID, int64_t modRevision = -1);

    litebus::Future<bool> InstanceSyncDone();

    litebus::Future<Status> SubscribeInstanceEvent(const std::string &subscriber, const std::string &targetInstance,
                                                   bool ignoreNonExist);
    litebus::Future<Status> TrySubscribeInstanceEvent(const std::string &subscriber, const std::string &targetInstance,
                                                      bool ignoreNonExist);

    void NotifyMigratingRequest(const std::string &instanceID);

    void ReportTraffic(const std::string &instanceID, const size_t &size);

    void InstanceRouteUpdate(const std::string &instanceID, const resources::InstanceInfo &instanceInfo);

    void AttachTenantListener(const std::shared_ptr<TenantListener> &listener);

    void DetachTenantListener(const std::shared_ptr<TenantListener> &listener);

    void NotifyUpdateTenantInstance(const TenantEvent &event);

    void NotifyDeleteTenantInstance(const TenantEvent &event);

    void OnTenantInstanceEvent(const std::string &instanceID, const resource_view::InstanceInfo &instanceInfo);

    litebus::Future<resource_view::InstanceInfo> GetInstanceRouteInfo(const std::string &instanceID);

    litebus::Future<resource_view::InstanceInfo> OnGetInstanceFromMetaStore(
        const litebus::Future<std::shared_ptr<GetResponse>> &getResponse, const std::string &instanceID);

    litebus::Future<InstanceInfoMap> OnGetInstancesFromMetaStore(
        const litebus::Future<std::shared_ptr<GetResponse>> &getResponse);

    void WatchInstance(const std::string &instanceID, int64_t revision = 0);
    bool IsInstanceWatched(const std::string &instanceID);

    void OnWatchInstance(const std::string &instanceID, const litebus::Future<std::shared_ptr<Watcher>> &watcher);

    litebus::Future<resource_view::InstanceInfo> GetAndWatchInstance(const std::string &instanceID);

    litebus::Future<resource_view::InstanceInfo> GetOrWatchInstance(const std::string &instanceID);

    void CancelWatchInstance(const std::string &instanceID);

    void BindInternalIAM(const std::shared_ptr<InternalIAM> &internalIAM);

protected:
    void Init() override{};
    void Finalize() override{};

private:
    void ProcFuncMetaEvent(const std::string &funcKey, const WatchEvent &event);

    void OnPutMeta(const litebus::Future<bool> &isSystem, const std::string &funcKey, const FunctionMeta &funcMeta);

    void SetInstanceInfo(const std::string &instanceID, const resource_view::InstanceInfo &info);

    void DelInstanceInfo(const std::string &instanceID);

    void CloseDataInterfaceClient(const std::string &instanceID);

    void NotifyUpdateInstance(const std::string &instanceID, const resource_view::InstanceInfo &instanceInfo,
                              bool isForceUpdate) override;
    void NotifyDeleteInstance(const std::string &instanceID, int64_t modRevision) override;

    void SetInstanceBillingContext(const resource_view::InstanceInfo &instanceInfo, bool synced);

    litebus::Future<litebus::Option<FunctionMeta>> GetFuncMetaFromMetaStore(const std::string &funcKey);
    Status OnGetFuncMetaFromMetaStore(const std::string &funcKey, const litebus::Option<FunctionMeta> &funcMeta);
    litebus::Future<litebus::Option<ProxyMeta>> GetProxyFromMetaStore(const std::string &key);
    Status OnGetProxyFromMetaStore(const std::string &proxyID, const litebus::Option<ProxyMeta> &funcMeta);
    void PutProxyMeta(const ProxyMeta &proxyMeta);

    void RemoveQueryKeyMetaCache(const std::string &key);

    litebus::Future<SyncResult> BusProxySyncer(const std::shared_ptr<GetResponse> &getResponse);

    litebus::Future<SyncResult> InstanceInfoSyncer(const std::shared_ptr<GetResponse> &getResponse);

    void DeleteInstanceRouteEvent(const ::std::shared_ptr<GetResponse> &getResponse, const std::string &instanceID);

    litebus::Future<SyncResult> DealWithInstanceNotInEtcd(
        const std::shared_ptr<GetResponse> &getResponse, const std::string &instanceID, SyncResult syncResult,
        std::unordered_map<std::string, resource_view::InstanceInfo>::iterator localInstance);

    litebus::Future<SyncResult> PartialInstanceInfoSyncer(const std::shared_ptr<GetResponse> &getResponse,
                                                          const std::string &instanceID);

    litebus::Future<SyncResult> FunctionMetaSyncer(const std::shared_ptr<GetResponse> &getResponse);

    SyncResult OnSyncer(const std::shared_ptr<GetResponse> &getResponse, std::vector<WatchEvent> &events,
                        std::string prefixKey);

    void HandleInstanceEvent(bool synced, const WatchEvent &event, std::string &instanceID);

    void HandleRouteEvent(bool synced, const WatchEvent &event, std::string &instanceID);

    std::string nodeID_;
    std::shared_ptr<MetaStorageAccessor> metaStorageAccessor_;
    std::shared_ptr<InstanceOperator> instanceOperator_;
    ObserverParam observerParam_;
    DriverEventCbFunc driverEventCbFunc_;
    InstanceStatusToRunningCbFunc instanceStatusToRunningCbFunc_;
    InstanceInfoSyncerCbFunc instanceInfoSyncerCbFunc_;
    UpdateFuncMetasFunc updateFuncMetasFunc_;
    TrafficReportCbFunc trafficReportCbFunc_;

    // for busproxy
    std::shared_ptr<DataInterfaceClientManagerProxy> dataInterfaceClientManager_;
    std::shared_ptr<busproxy::InstanceView> instanceView_;

    std::shared_ptr<ProxyView> proxyView_;
    std::shared_ptr<LocalSchedulerView> localSchedulerView_;

    // key is instanceID
    InstanceInfoMap instanceInfoMap_;
    // first level key is functionAgentID, second level key is instanceID
    std::unordered_map<std::string, InstanceInfoMap> agentInstanceInfoMap_;
    // instanceID, instance key mod_revision
    std::unordered_map<std::string, int64_t> instanceModRevisionMap_;
    // all instance on this node
    InstanceInfoMap localInstanceInfo_;

    // map key is functionKey
    std::unordered_map<std::string, FunctionMeta> funcMetaMap_;
    std::set<std::string> localFuncMetaSet_;
    std::unordered_map<std::string, FunctionMeta> systemFuncMetaMap_;
    // when function meta is not found, need to fetch it from etcd
    // to avoid query etcd every time, need to cache query record
    std::unordered_map<std::string, litebus::Timer> queryMetaStoreTimerMap_;
    std::unordered_map<std::string, std::shared_ptr<litebus::Promise<litebus::Option<FunctionMeta>>>>
        queryFuncMetaPromiseMap_;
    std::unordered_map<std::string, std::shared_ptr<litebus::Promise<litebus::Option<litebus::AID>>>>
        queryProxyPromiseMap_;

    // key is etcd event's key, for delete
    std::unordered_map<std::string, TenantEvent> lastTenantEventCacheMap_;

    // Instance Listener
    std::list<std::shared_ptr<InstanceListener>> instanceListenerList_;
    // Tenant Listener
    std::list<std::shared_ptr<TenantListener>> tenantListenerList_;

    litebus::Promise<bool> instanceSyncDone_;
    bool isMetaStoreEnabled_;

    bool isPartialWatchInstances_;
    std::unordered_map<std::string, std::shared_ptr<Watcher>> instanceWatchers_;

    std::shared_ptr<InternalIAM> internalIAM_;
};

}  // namespace functionsystem::function_proxy

#endif  // FUNCTION_PROXY_COMMON_OBSERVER_H
