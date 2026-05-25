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

#ifndef RUNTIME_MANAGER_EXECUTOR_EXECUTOR_ACTOR_RUNTIME_EXECUTOR_H
#define RUNTIME_MANAGER_EXECUTOR_EXECUTOR_ACTOR_RUNTIME_EXECUTOR_H

#include <sys/stat.h>

#include <chrono>
#include <memory>
#include <shared_mutex>

#include "async/defer.hpp"
#include "common/file_monitor/monitor_callback_actor.h"
#include "common/metrics/metrics_adapter.h"
#include "common/proto/pb/message_pb.h"
#include "common/status/status.h"
#include "common/utils/capability.h"
#include "common/utils/cmd_tool.h"
#include "common/utils/files.h"
#include "config/build.h"
#include "exec/exec.hpp"
#include "executor.h"
#include "runtime_manager/config/flags.h"
#include "runtime_manager/std_monitor/std_monitor.h"
#include "runtime_manager/utils/std_redirector.h"
#include "utils/volume_mount.h"
#include "virtual_env_manager/virtual_env_manager.h"

namespace functionsystem::runtime_manager {

const int CAP_LEN = 4;
const std::string PKG_TYPE_WHEEL = "WHEEL";
const std::string PKG_TYPE_TARBALL = "TARBALL";

class RuntimeExecutor : public Executor {
public:
    explicit RuntimeExecutor(const std::string &name, const litebus::AID &functionAgentAID);

    virtual ~RuntimeExecutor() override = default;

    litebus::Future<messages::StartInstanceResponse> StartInstance(
        const std::shared_ptr<messages::StartInstanceRequest> &request, const std::vector<int> &cardIDs) override;

    litebus::Future<Status> StopInstance(const std::shared_ptr<messages::StopInstanceRequest> &request,
                                         bool oomKilled = false) override;

    litebus::Future<messages::SnapshotRuntimeResponse> SnapshotRuntime(
        const std::shared_ptr<messages::SnapshotRuntimeRequest> &request) override;

    std::map<std::string, messages::RuntimeInstanceInfo> GetRuntimeInstanceInfos() override;

    void UpdatePrestartRuntimePromise(pid_t pid) override;

    void ClearCapability() override
    {
        cap_value_t cap_list[CAP_LEN] = { CAP_SYS_ADMIN, CAP_SETGID, CAP_SETUID };
        ClearCapabilities(cap_list, CAP_LEN);
    }

    litebus::Future<messages::UpdateCredResponse> UpdateCredForRuntime(
        const std::shared_ptr<messages::UpdateCredRequest> &request) override;

    // for test
    [[maybe_unused]] void ProtectedHookRuntimeCredentialByID(std::vector<std::function<void()>> &initHook, int userID,
                                                             int groupID) const
    {
        HookRuntimeCredentialByID(initHook, userID, groupID);
    }

    // for test
    [[maybe_unused]] bool ProtectedCheckPrestartRuntimeRetry(const std::string &runtimeID, const std::string &language,
                                                             const int retryTimes)
    {
        return CheckPrestartRuntimeRetry(runtimeID, language, retryTimes);
    }

    // for test
    [[maybe_unused]] int CheckPrestartRuntimePromise()
    {
        int cnt = 0;
        for (auto iter = prestartRuntimePromiseMap_.begin(); iter != prestartRuntimePromiseMap_.end(); ++iter) {
            if (iter->second->GetFuture().IsError() || iter->second->GetFuture().IsOK()) {
                cnt++;
            } else {
                return 0;
            }
        }
        return cnt;
    }

    // for test
    [[maybe_unused]] Status ProtectedFillMetricsTLSConfig(::messages::TLSConfig &tlsConfig) const
    {
        return FillMetricsTLSConfig(tlsConfig);
    }

    // for test
    [[maybe_unused]] Status ProtectedFillMetricsTLSConfigFromStsP12(::messages::TLSConfig &tlsConfig) const
    {
        return FillMetricsTLSConfigFromStsP12(tlsConfig);
    }

    // for test
    [[maybe_unused]] void SetStdRedirectors(const std::string logName, const std::shared_ptr<StdRedirector> redirector)
    {
        stdRedirectors_[logName] = redirector;
    }

    litebus::Future<bool> StopAllRuntimes();

    void CheckRuntimesExited(const std::chrono::steady_clock::time_point &start, const litebus::Promise<bool> &promise);

    Status GetBuildArgs(const std::string &language, const std::string &port,
                        const std::shared_ptr<messages::StartInstanceRequest> &request, std::vector<std::string> &args);

    std::vector<std::string> GetBuildArgsForPrestart(const std::string &runtimeID, const std::string &language,
                                                     const std::string &port);

    litebus::Future<Status> NotifyInstancesDiskUsageExceedLimit(const std::string &description,
                                                                const int limit) override;

protected:
    std::shared_ptr<VolumeMount> mounter_;
    void Init() override;

    void Finalize() override;

    void InitPrestartRuntimePool() override;

    void InitVirtualEnvIdleTimeLimit() override;

    PrestartProcess GetRuntimeFromPool(const std::string &language, const std::string &schedulePolicy);

    static Status PostStartExecHook(const messages::RuntimeConfig &config, const std::string &instanceID);

private:
    std::map<std::string, messages::RuntimeInstanceInfo> runtimeInstanceInfoMap_;

    std::set<std::string> prestartRuntimeIDs_;

    std::unordered_map<std::string, std::shared_ptr<StdRedirector>> stdRedirectors_;

    std::shared_ptr<StdRedirector> GetStdRedirector(const std::string &logName);

    std::shared_ptr<StdRedirector> GetStdRedirector(const std::string &runtimeID,
                                                    const StdRedirectParam &stdRedirectParam);

    void StopRedirectorByRuntimeID(const std::string &runtimeID);

    litebus::Future<messages::StartInstanceResponse> StartInstanceWithoutPrestart(
        const std::shared_ptr<messages::StartInstanceRequest> &request, const std::string &language,
        const std::vector<int> &cardIDs);

    bool NeedToRollUserLog() const;

    std::shared_ptr<litebus::Exec> StartRuntimeByRuntimeID(
        const std::map<std::string, std::string> startRuntimeParams, const std::vector<std::string> &buildArgs,
        const Envs &envs, const std::vector<std::function<void()>> childInitHook) const;

    std::string GetExecPath(const std::string &language) const;

    std::string GetExecPathFromRuntimeConfig(const messages::RuntimeConfig &config) const;

    std::string GetLanguageArg(const std::string &language) const;

    std::map<std::string, std::string> CombineEnvs(const Envs &envs) const;

    std::map<std::string, pid_t> runtime2PID_;

    std::unordered_set<std::string> innerOomKilledruntimes_;

    std::unordered_set<pid_t> runtimeToExit_;

    Status StopInstanceByRuntimeID(const std::string &runtimeID, const std::string &requestID, bool oomKilled = false);

    void KillProcess(const pid_t &pid, bool force = false);

    bool ShouldUseProcessGroup() const;

    void StopMonitoringFD(const std::string &runtimeID, const std::string &requestID);

    void TerminateImmediately(pid_t pid, std::string_view processType);

    void SendGracefulTermination(pid_t pid, std::string_view processType);

    std::function<std::pair<Status, std::vector<std::string>>(const std::string &,
                                                              const std::shared_ptr<messages::StartInstanceRequest> &)>
        getBuildArgs_;

    std::pair<Status, std::vector<std::string>> GetCppBuildArgs(
        const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const;

    std::pair<Status, std::vector<std::string>> GetGoBuildArgs(
        const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const;

    std::pair<Status, std::vector<std::string>> GetPythonBuildArgs(
        const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const;

    std::pair<Status, std::vector<std::string>> GetJavaBuildArgsDefault(
        const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const;

    std::pair<Status, std::vector<std::string>> GetJavaBuildArgsForJava11(
        const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const;

    std::pair<Status, std::vector<std::string>> GetJavaBuildArgsForJava17(
        const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const;

    std::pair<Status, std::vector<std::string>> GetJavaBuildArgsForJava21(
        const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const;

    std::pair<Status, std::vector<std::string>> GetJavaBuildArgs(
        const std::string &port, const std::vector<std::string> &jvmArgs,
        const std::shared_ptr<messages::StartInstanceRequest> &request) const;

    std::pair<Status, std::vector<std::string>> GetNodejsBuildArgs(
        const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const;

    std::pair<Status, std::vector<std::string>> WrapMassifBuildArgs(
        const std::string &languageExecPath, const std::vector<std::string> &languageBuildArgs) const;

    std::shared_ptr<litebus::Exec> CreateMassifWrapExec(const std::string &runtimeID,
                                                        const std::string &languageExecPath,
                                                        const std::vector<std::string> &languageBuildArgs,
                                                        const std::map<std::string, std::string> &combineEnvs,
                                                        const std::vector<std::function<void()>> childInitHook) const;

    std::pair<Status, std::vector<std::string>> GetPosixCustomBuildArgs(
        const std::string &, const std::shared_ptr<messages::StartInstanceRequest> &request) const;

    std::map<const std::string, std::pair<Status, std::vector<std::string>> (RuntimeExecutor::*)(
                                    const std::string &, const std::shared_ptr<messages::StartInstanceRequest> &) const>
        buildArgsFunc_ = { { CPP_LANGUAGE, &RuntimeExecutor::GetCppBuildArgs },
                           { GO_LANGUAGE, &RuntimeExecutor::GetGoBuildArgs },
                           { JAVA_LANGUAGE, &RuntimeExecutor::GetJavaBuildArgsDefault },
                           { JAVA11_LANGUAGE, &RuntimeExecutor::GetJavaBuildArgsForJava11 },
                           { JAVA17_LANGUAGE, &RuntimeExecutor::GetJavaBuildArgsForJava17 },
                           { JAVA21_LANGUAGE, &RuntimeExecutor::GetJavaBuildArgsForJava21 },
                           { POSIX_CUSTOM_RUNTIME, &RuntimeExecutor::GetPosixCustomBuildArgs },
                           { NODE_JS, &RuntimeExecutor::GetNodejsBuildArgs },
                           { PYTHON_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgs },
                           { PYTHON3_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgs },
                           { PYTHON36_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgs },
                           { PYTHON37_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgs },
                           { PYTHON38_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgs },
                           { PYTHON39_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgs },
                           { PYTHON310_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgs },
                           { PYTHON311_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgs } };

    void StartPrestartRuntimeByLanguage(const std::string &language, const int startCount);

    bool StartPrestartRuntimeByRuntimeID(const std::string &runtimeID, const std::string &language,
                                         const std::string &execPath, const int retryTimes);

    void WaitPrestartRuntimeExit(const std::string &runtimeID, const std::string &language, const std::string &execPath,
                                 const int retryTimes, const std::shared_ptr<litebus::Exec> &execPtr);

    std::function<std::vector<std::string>(const std::string &, const std::string &, const std::string &)>
        getBuildArgsPrestart_;

    std::vector<std::string> GetCppBuildArgsForPrestart(const std::string &runtimeID, const std::string &port,
                                                        const std::string &language) const;

    std::vector<std::string> GetPythonBuildArgsForPrestart(const std::string &runtimeID, const std::string &port,
                                                           const std::string &language) const;

    std::vector<std::string> GetJavaBuildArgsForPrestart(const std::string &runtimeID, const std::string &port,
                                                         const std::string &language) const;
    std::map<const std::string, std::vector<std::string> (RuntimeExecutor::*)(const std::string &, const std::string &,
                                                                              const std::string &) const>
        buildArgsFuncPrestart_ = { { CPP_LANGUAGE, &RuntimeExecutor::GetCppBuildArgsForPrestart },
                                   { JAVA_LANGUAGE, &RuntimeExecutor::GetJavaBuildArgsForPrestart },
                                   { JAVA11_LANGUAGE, &RuntimeExecutor::GetJavaBuildArgsForPrestart },
                                   { JAVA17_LANGUAGE, &RuntimeExecutor::GetJavaBuildArgsForPrestart },
                                   { JAVA21_LANGUAGE, &RuntimeExecutor::GetJavaBuildArgsForPrestart },
                                   { PYTHON_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgsForPrestart },
                                   { PYTHON3_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgsForPrestart },
                                   { PYTHON36_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgsForPrestart },
                                   { PYTHON37_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgsForPrestart },
                                   { PYTHON38_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgsForPrestart },
                                   { PYTHON39_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgsForPrestart },
                                   { PYTHON310_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgsForPrestart },
                                   { PYTHON311_LANGUAGE, &RuntimeExecutor::GetPythonBuildArgsForPrestart } };

    void HookRuntimeCredentialByID(std::vector<std::function<void()>> &initHook, int userID, int groupID) const;
    [[nodiscard]]     std::vector<std::function<void()>> BuildInitHook(
        const std::shared_ptr<messages::StartInstanceRequest> &request) const;

    [[nodiscard]] std::vector<std::function<void()>> BuildInitHookForPrestart() const;

    StatusCode CheckRuntimeCredential(const std::shared_ptr<messages::StartInstanceRequest> &request);

    bool CheckPrestartRuntimeRetry(const std::string &runtimeID, const std::string &language, const int retryTimes);

    messages::StartInstanceResponse GenSuccessStartInstanceResponse(
        const std::shared_ptr<messages::StartInstanceRequest> &request, const std::shared_ptr<litebus::Exec> &execPtr,
        const std::string &port);

    void KillOtherPrestartRuntimeProcess();

    litebus::Future<messages::StartInstanceResponse> StartRuntime(
        const std::shared_ptr<messages::StartInstanceRequest> &request, const std::string &language,
        const std::string &port, const Envs &envs, const std::vector<std::string> &args);

    std::shared_ptr<litebus::Exec> StartRuntimeByRuntimeIDWithRetry(
        const std::map<std::string, std::string> &startRuntimeParams, const std::vector<std::string> &buildArgs,
        const Envs &envs, const std::vector<std::function<void()>> childInitHook,
        const messages::RuntimeInstanceInfo &info);

    Status WriteProtoToRuntime(const std::string &requestID, const std::string &runtimeID,
                               const ::messages::TLSConfig &tlsConfig,
                               const std::shared_ptr<litebus::Exec> execPtr) const;
    Status WriteJsonToRuntime(const std::string &requestID, const std::string &runtimeID,
                              const ::messages::TLSConfig &tlsConfig,
                              const std::shared_ptr<litebus::Exec> execPtr) const;
    Status FillMetricsTLSConfig(::messages::TLSConfig &tlsConfig) const;
    Status FillMetricsTLSConfigFromStsP12(::messages::TLSConfig &tlsConfig) const;

    void ReportInfo(const std::string &instanceID, const std::string runtimeID, const pid_t &pid,
                    const functionsystem::metrics::MeterTitle &title);

    Status CreateSubDir(const std::shared_ptr<messages::StartInstanceRequest> &request);

    void InheritEnv(std::map<std::string, std::string> &combineEnvs) const;

    void ConfigRuntimeRedirectLog(litebus::ExecIO &stdOut, litebus::ExecIO &stdErr, const std::string &runtimeID) const;

    void StartRuntimeStdRedirection(const std::string &runtimeID,
                                    const litebus::Option<int> stdOut, const litebus::Option<int> stdErr);

    std::pair<Status, std::string> GetPythonExecPath(
        const google::protobuf::Map<std::string, std::string> &deployOptions,
        const messages::RuntimeInstanceInfo &info) const;

    std::pair<Status, std::string> HandleWorkingDirectory(
        const std::shared_ptr<messages::StartInstanceRequest> &request,
        const messages::RuntimeInstanceInfo &info) const;

    Status HandleCondaConfig(const google::protobuf::Map<std::string, std::string> &deployOptions,
                             const std::string &deployDir, const messages::RuntimeInstanceInfo &info) const;

    Status HandleCondaCommand(const google::protobuf::Map<std::string, std::string> &deployOptions,
                              const std::string &condaEnvFile, const messages::RuntimeInstanceInfo &info) const;

    std::pair<Status, std::vector<std::string>> PythonBuildFinalArgs(const std::string &port,
                                                                     const std::string &execPath,
                                                                     const std::string &deployDir,
                                                                     const messages::RuntimeInstanceInfo &info) const;

    int64_t gracefulShutdownTime_{ 0 };

    litebus::AID functionAgentAID_;
    std::shared_ptr<MonitorCallBackActor> monitorCallBackActor_{ nullptr };
    std::shared_ptr<CmdTool> cmdTool_;
    std::shared_ptr<VirtualEnvManager> virtualEnvMgr_{ nullptr };
    std::shared_ptr<StdMonitor> stdMonitor_{ nullptr };
    StdRedirectParam stdRedirectParam_;
    std::string pkgType_;
};

class RuntimeExecutorProxy : public ExecutorProxy {
public:
    explicit RuntimeExecutorProxy(const std::shared_ptr<RuntimeExecutor> &executor) : ExecutorProxy(executor) {};

    ~RuntimeExecutorProxy() override = default;

    /**
     * Start Instance when receive message from function agent.
     *
     * @param request Include start instance arguments.
     * @return response Include start instance result arguments.
     */
    litebus::Future<::messages::StartInstanceResponse> StartInstance(
        const std::shared_ptr<messages::StartInstanceRequest> &request, const std::vector<int> &cardIDs) override;

    /**
     * Stop Instance when receive message from function agent.
     *
     * @param request Include stop instance arguments.
     * @param oomKilled is inner oom killed by runtime-manager
     * @return response Include stop instance result arguments.
     */
    litebus::Future<Status> StopInstance(const std::shared_ptr<messages::StopInstanceRequest> &request,
                                         bool oomKilled = false) override;

    /**
     * Snapshot Runtime when receive message from function agent.
     *
     * @param request Include snapshot arguments.
     * @return response Include snapshot result with checkpoint info.
     */
    litebus::Future<messages::SnapshotRuntimeResponse> SnapshotRuntime(
        const std::shared_ptr<messages::SnapshotRuntimeRequest> &request) override;

    /**
     * Get runtime instance infos.
     *
     * @return Runtime infos.
     */
    litebus::Future<std::map<std::string, messages::RuntimeInstanceInfo>> GetRuntimeInstanceInfos() override;

    void UpdatePrestartRuntimePromise(pid_t pid) override;

    void ClearCapability() override
    {
        litebus::Async(executor_->GetAID(), &RuntimeExecutor::ClearCapability);
    }

    litebus::Future<bool> GracefulShutdown() override
    {
        return litebus::Async(executor_->GetAID(), &RuntimeExecutor::StopAllRuntimes);
    }
};

}  // namespace functionsystem::runtime_manager

#endif  // RUNTIME_MANAGER_EXECUTOR_EXECUTOR_ACTOR_RUNTIME_EXECUTOR_H
