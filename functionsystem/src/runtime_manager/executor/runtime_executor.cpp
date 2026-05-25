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

#include "runtime_executor.h"

#include <google/protobuf/util/json_util.h>
#include <yaml-cpp/yaml.h>
#include <sys/eventfd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <nlohmann/json.hpp>
#include <regex>
#include <thread>
#include <unordered_set>

#include "async/asyncafter.hpp"
#include "async/collect.hpp"
#include "common/hex/hex.h"
#include "common/logs/logging.h"
#include "common/resource_view/resource_type.h"
#include "common/utils/exec_utils.h"
#include "common/utils/files.h"
#include "common/utils/numa_binding.h"
#include "common/utils/generate_message.h"
#include "common/utils/path.h"
#include "common/utils/struct_transfer.h"
#include "config/build.h"
#include "port/port_manager.h"
#include "utils/os_utils.hpp"
#include "utils/utils.h"

namespace functionsystem::runtime_manager {
using json = nlohmann::json;

const int RETRY_TIMES = 2;
const int YAML_INDENT_SIZE = 2;
const std::string MONOPOLY = "monopoly";
const std::string IS_PRESTART = "IS_PRESTART";
const std::string RUNTIME_DIR = "RUNTIME_DIR";
const std::string PRESTART_FLAG = "1";
const std::string PARAM_EXEC_PATH = "execPath";
const std::string PARAM_RUNTIME_ID = "runtimeID";
const std::string PARAM_LANGUAGE = "language";
const std::string CPP_NEW_EXEC_PATH = "/cpp/bin/runtime";
const std::string GO_NEW_EXEC_PATH = "/go/bin/goruntime";
const std::string GLOG_LOG_DIR = "GLOG_log_dir";
const std::string YR_LOG_LEVEL = "YR_LOG_LEVEL";
const std::string PYTHON_PATH = "PYTHONPATH";
const std::string PATH = "PATH";
const std::string PYTHON_LOG_CONFIG_PATH = "PYTHON_LOG_CONFIG";
const std::string ENABLE_DIS_CONV_CALL_STACK = "ENABLE_DIS_CONV_CALL_STACK";
const std::string BASH_PATH = "/bin/bash";
const std::string MAX_LOG_SIZE_MB_ENV = "YR_MAX_LOG_SIZE_MB";
const std::string MAX_LOG_FILE_NUM_ENV = "YR_MAX_LOG_FILE_NUM";
const std::string RUNTIME_DS_CONNECT_TIMEOUT_ENV = "DS_CONNECT_TIMEOUT_SEC";
const std::string RUNTIME_METRICS_CONFIG = "RUNTIME_METRICS_CONFIG";
const std::string RUNTIME_METRICS_CONFIG_FILE = "RUNTIME_METRICS_CONFIG_FILE";
const std::string PROMETHEUS_PULL_EXPORTER = "prometheusPullExporter";
const std::string CERT_SOURCE_STS_P12 = "stsP12";
const std::vector<std::string> languages = {
    CPP_LANGUAGE,       GO_LANGUAGE,        JAVA_LANGUAGE,        JAVA11_LANGUAGE,
    JAVA17_LANGUAGE,    JAVA21_LANGUAGE,    PYTHON_LANGUAGE,      PYTHON3_LANGUAGE,
    PYTHON36_LANGUAGE,  PYTHON37_LANGUAGE,  PYTHON38_LANGUAGE,    PYTHON39_LANGUAGE,
    PYTHON310_LANGUAGE, PYTHON311_LANGUAGE, POSIX_CUSTOM_RUNTIME, NODE_JS
};
const std::string VALGRIND_TOOL_PREFIX = "--tool=";
const std::string MASSIF_TIME_UNIT_PREFIX = "--time-unit=";
const std::string MASSIF_MAX_THREADS_PREFIX = "--max-threads=";
const std::string MASSIF_OUT_FILE_PREFIX = "--massif-out-file=";
const std::string MASSIF_DETAILED_FREQ = "--detailed-freq=";
const std::string CPP_PROGRAM_NAME = "cppruntime";
const std::string GO_PROGRAM_NAME = "goruntime";
const std::string VALGRIND_PROGRAM_NAME = "valgrind";
const std::string RUNTIME_ID_ARG_PREFIX = "-runtimeId=";
const std::string INSTANCE_ID_ARG_PREFIX = "-instanceId=";
const std::string LOG_LEVEL_PREFIX = "-logLevel=";
const std::string GRPC_ADDRESS_PREFIX = "-grpcAddress=";
const std::string CONFIG_PATH_PREFIX = "-runtimeConfigPath=";
const std::string JOB_ID_PREFIX = "-jobId=job-";
const std::string PYTHON_JOB_ID_PREFIX = "job-";
const std::string RUNTIME_LAYER_DIR_NAME = "layer";
const std::string RUNTIME_FUNC_DIR_NAME = "func";
const std::string PYTHON_PRESTART_DEPLOY_DIR = "/dcache";
const std::string JAVA_SYSTEM_PROPERTY_FILE = "-Dlog4j2.configurationFile=file:";
const std::string JAVA_SYSTEM_LIBRARY_PATH = "-Djava.library.path=";
const std::string JAVA_LOG_LEVEL = "-DlogLevel=";
const std::string JAVA_JOB_ID = "-DjobId=job-";
const std::string JAVA_MAIN_CLASS = "org.yuanrong.runtime.server.RuntimeServer";
const std::string PYTHON_SERVER_PATH = "/python/yr/main/yr_runtime_main.py";
const std::string PYTHON_SERVER_PATH_IN_WHEEL = "/../../main/yr_runtime_main.py";

const std::string YR_JAVA_RUNTIME_PATH = "/java/yr-runtime-1.0.0.jar";
const std::string POST_START_EXEC_REGEX = R"(^(uv )?pip3.[0-9]* install [a-zA-Z0-9\-\s:/\.=_]* && pip3.[0-9]* check$)";
// should be read from deploy request in the future
const int DEFAULT_RETRY_RESTART_CACHE_RUNTIME = 3;
const int MAX_USER_ID = 65535;
const int MAX_GROUP_ID = 65535;
const int AGENT_ID = 1002;
const int MIN_VALID_ID = -1;
const int INITIAL_USER_ID = 1000;
const int INITIAL_GROUP_ID = 1000;
const uint8_t KILL_PROCESS_TIMEOUT = 5;
const int MAX_WRITE_LENGTH = 102400;
const uint32_t WAIT_RUNTIMES_EXITED_INTERVAL = 1000;  // ms

const std::string INSTANCE_WORK_DIR_ENV = "INSTANCE_WORK_DIR";
const std::string YR_NOSET_ASCEND_RT_VISIBLE_DEVICES = "YR_NOSET_ASCEND_RT_VISIBLE_DEVICES";
const static std::string ASCEND_RT_VISIBLE_DEVICES = "ASCEND_RT_VISIBLE_DEVICES";
const std::string YR_NOSET_CUDA_VISIBLE_DEVICES = "YR_NOSET_CUDA_VISIBLE_DEVICES";
const static std::string CUDA_VISIBLE_DEVICES = "CUDA_VISIBLE_DEVICES";

const std::string CONDA_PROGRAM_NAME = "conda";
const std::string CONDA_ENV_FILE = "env.yaml";
const std::string WHITE_LIST_ENV_PREFIX = "YR_";

const std::string CHDIR_PATH_CONFIG = "CHDIR_PATH";

// Exclude environment variables passed to the runtime
const std::vector<std::string> EXCLUDE_ENV_KEYS_PASSED_TO_RUNTIME = {
    UNZIPPED_WORKING_DIR
};  // job working_dir file unzipped path

const int DEFAULT_STD_MONITOR_EVENT = EPOLLIN | EPOLLHUP | EPOLLERR;

std::function<void()> SetRuntimeIdentity(int userID, int groupID)
{
    return [groupID, userID]() {
        std::cout << "userID: " << userID << ", groupID: " << groupID << std::endl;
        int r = setuid(userID);
        if (r == -1) {
            std::cerr << "failed to set uid: " << userID << ", get errno: " << errno
                      << ", reason: " << litebus::os::Strerror(errno) << std::endl;
            ::exit(errno);
        }
        r = setgid(groupID);
        if (r == -1) {
            std::cerr << "failed to set gid: " << groupID << ", get errno: " << errno << std::endl;
            ::exit(errno);
        }
    };
}

std::function<void()> SetSubProcessPgid()
{
    return []() {
        pid_t pid = getpid();
        int pgidRet = setpgid(pid, 0);
        if (pgidRet < 0) {
            std::cerr << "failed to set pgid: " << pid << ", get errno: " << errno << std::endl;
        }
    };
}

std::function<void()> ChdirHook(std::string dir)
{
    return [dir]() {
        if (chdir(dir.c_str()) != 0) {
            std::cerr << "failed to chdir: " << dir << ", get errno: " << errno << std::endl;
        }
    };
}

static std::vector<int> ParseNUMANodeIds(const std::string& idsStr)
{
    std::vector<int> selectedNodes;
    size_t pos = 0;
    while (pos < idsStr.length()) {
        size_t commaPos = idsStr.find(',', pos);
        std::string nodeIdStr = (commaPos == std::string::npos)
                                   ? idsStr.substr(pos)
                                   : idsStr.substr(pos, commaPos - pos);
        if (nodeIdStr.empty()) {
            if (commaPos == std::string::npos) {
                break;
            }
            pos = commaPos + 1;
            continue;
        }
        int nodeId = -1;
        try {
            nodeId = std::stoi(nodeIdStr);
        } catch (const std::exception& e) {
            YRLOG_WARN("Failed to parse NUMA node ID '{}': {}", nodeIdStr, e.what());
            if (commaPos == std::string::npos) {
                break;
            }
            pos = commaPos + 1;
            continue;
        }
        if (nodeId < 0) {
            if (commaPos == std::string::npos) {
                break;
            }
            pos = commaPos + 1;
            continue;
        }
        selectedNodes.push_back(nodeId);
        if (commaPos == std::string::npos) {
            break;
        }
        pos = commaPos + 1;
    }
    return selectedNodes;
}

static void AddSingleNodeBindingHook(std::vector<std::function<void()>>& initHook, int nodeId)
{
    (void)initHook.emplace_back([nodeId]() {
        auto status = functionsystem::utils::NUMABinding::BindToNUMANode(nodeId);
        if (!status.IsOk()) {
            YRLOG_WARN("Failed to bind to NUMA node {}: {}", nodeId, status.GetMessage());
        }
    });
}

static void AddMultiNodeBindingHook(std::vector<std::function<void()>>& initHook,
                                    const std::vector<int>& selectedNodes)
{
    std::vector<int> nodesCopy = selectedNodes;
    (void)initHook.emplace_back([nodesCopy]() {
        auto status = functionsystem::utils::NUMABinding::BindToNUMANodes(nodesCopy);
        if (!status.IsOk()) {
            YRLOG_WARN("Failed to bind to NUMA nodes [{}]: {}",
                       fmt::format("{}", fmt::join(nodesCopy, ", ")), status.GetMessage());
        }
    });
}

static void AddNUMABindingHook(std::vector<std::function<void()>>& initHook,
                               const std::shared_ptr<messages::StartInstanceRequest>& request)
{
    if (!request->has_scheduleoption()) {
        return;
    }
    const auto& extension = request->scheduleoption().extension();
    auto numaIDsIter = extension.find("NUMA_NODE_IDS");
    if (numaIDsIter == extension.end() || numaIDsIter->second.empty()) {
        return;
    }
    auto idsStr = numaIDsIter->second;
    std::vector<int> selectedNodes = ParseNUMANodeIds(idsStr);
    if (selectedNodes.empty()) {
        return;
    }
    YRLOG_INFO("{}|{}|Binding runtime to NUMA node(s) [{}]",
               request->runtimeinstanceinfo().requestid(),
               request->runtimeinstanceinfo().instanceid(),
               idsStr);
    if (selectedNodes.size() == 1) {
        AddSingleNodeBindingHook(initHook, selectedNodes[0]);
    } else {
        AddMultiNodeBindingHook(initHook, selectedNodes);
    }
}

std::function<void()> CondaActivate(const std::string &condaPrefix, const std::string &condaDefaultEnv)
{
    // if ${CONDA_PREFIX}/etc/profile.d/conda.sh not found, conda activate failed
    // User should ensure that conda.sh exists.
    return [condaPrefix, condaDefaultEnv]() {
        if (!CheckIllegalChars(condaPrefix) || !CheckIllegalChars(condaDefaultEnv)) {
            std::cerr << "conda activate invalid." << std::endl;
            return;
        }
        std::string command = fmt::format(
            "export {}=\"{}\"; export {}=\"{}\";"
            " . ${{{}}}/etc/profile.d/conda.sh &&"
            " conda activate ${{{}}} && python -V;",  // escape: {{{}}}
            CONDA_PREFIX, condaPrefix, CONDA_DEFAULT_ENV, condaDefaultEnv, CONDA_PREFIX, CONDA_DEFAULT_ENV);
        if (system(command.c_str()) != 0) {
            std::cerr << "conda activate failed." << std::endl;
        } else {
            std::cout << "conda activate finished." << std::endl;
        }
    };
}

std::string GetInstallationType()
{
    char result[PATH_MAX];
    ssize_t count = readlink("/proc/self/exe", result, PATH_MAX);
    std::string exePath(result, (count > 0) ? count : 0);

    if (exePath.find("inner") != std::string::npos) {
        return PKG_TYPE_TARBALL;
    }

    if (exePath.find("dist-packages") != std::string::npos ||
        exePath.find("site-packages") != std::string::npos ||
        exePath.find("venv") != std::string::npos) {
        return PKG_TYPE_WHEEL;
    }
    
    return PKG_TYPE_TARBALL;
}

RuntimeExecutor::RuntimeExecutor(const std::string &name, const litebus::AID &functionAgentAID) : Executor(name)
{
    mounter_ = std::make_shared<VolumeMount>();
    functionAgentAID_ = functionAgentAID;
    // start file monitor to watch instance subdir
    std::string mcName = "MonitorCallBack_" + litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    monitorCallBackActor_ = std::make_shared<MonitorCallBackActor>(mcName, functionAgentAID);
    cmdTool_ = std::make_shared<CmdTool>();
    litebus::Spawn(monitorCallBackActor_);
    // start std monitor
    stdMonitor_ = std::make_shared<StdMonitor>();
    stdMonitor_->Start();
}

void RuntimeExecutor::Init()
{
    pkgType_ = GetInstallationType();
    YRLOG_INFO("RuntimeManager installation type: {}", pkgType_);
}

void RuntimeExecutor::Finalize()
{
    for (auto iter : stdRedirectors_) {
        litebus::Terminate(iter.second->GetAID());
        litebus::Await(iter.second->GetAID());
    }

    litebus::Async(monitorCallBackActor_->GetAID(), &MonitorCallBackActor::DeleteAllMonitorAndRemoveDir);
    litebus::Terminate(monitorCallBackActor_->GetAID());
    litebus::Await(monitorCallBackActor_->GetAID());

    stdMonitor_->Stop();
    stdMonitor_ = nullptr;

    stdRedirectors_.clear();
    runtimeInstanceInfoMap_.clear();
    Executor::Finalize();
}

litebus::Future<Status> RuntimeExecutor::NotifyInstancesDiskUsageExceedLimit(const std::string &description,
                                                                             const int limit)
{
    std::list<litebus::Future<Status>> notifyFutures;
    for (const auto pair : runtimeInstanceInfoMap_) {
        const std::string &runtimeID = pair.first;
        const messages::RuntimeInstanceInfo &info = pair.second;
        auto requestID = litebus::os::Join("notify-instance-disk-usage-exceed-limit", runtimeID, '-');
        auto future = litebus::Async(monitorCallBackActor_->GetAID(), &MonitorCallBackActor::SendMessage, requestID,
                                     info.instanceid(), (int64_t)limit, description);
        notifyFutures.emplace_back(future);
        YRLOG_DEBUG("{}|{}|Notify instance DiskUsageExceedLimit", runtimeID, info.instanceid());
    }

    // after notify all instances
    auto promise = std::make_shared<litebus::Promise<Status>>();
    (void)litebus::Collect(notifyFutures).OnComplete([promise](const litebus::Future<std::list<Status>> &future) {
        if (future.IsError()) {
            YRLOG_ERROR("Collect future error");
            promise->SetValue(Status(StatusCode::FAILED));
            return;
        }
        for (auto status : future.Get()) {
            if (status.IsError()) {
                YRLOG_ERROR("Error occurs of notify instances");
                promise->SetValue(Status(StatusCode::FAILED));
                return;
            }
        }
        promise->SetValue(Status::OK());
    });
    return promise->GetFuture();
}

litebus::Future<bool> RuntimeExecutor::StopAllRuntimes()
{
    auto start = std::chrono::steady_clock::now();
    std::unordered_set<pid_t> runtimeToExit;
    YRLOG_INFO("{} runtimes need to stop", runtime2PID_.size());
    for (auto [runtimeID, pid] : runtime2PID_) {
        runtimeToExit_.insert(pid);
        auto ret = kill(pid, SIGTERM);
        YRLOG_INFO("stop runtime {} with pid {}, ret: {}, errno: {}", runtimeID, pid, ret, errno);
    }

    litebus::Promise<bool> promise;
    litebus::AsyncAfter(WAIT_RUNTIMES_EXITED_INTERVAL, GetAID(), &RuntimeExecutor::CheckRuntimesExited, start, promise);
    return promise.GetFuture();
}

void RuntimeExecutor::CheckRuntimesExited(const std::chrono::steady_clock::time_point &start,
                                          const litebus::Promise<bool> &promise)
{
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start).count();
    YRLOG_INFO("wait {} runtimes to graceful shutdown gracefulShutdownTime: {}", runtimeToExit_.size(),
               gracefulShutdownTime_);
    if (elapsed > gracefulShutdownTime_) {
        YRLOG_INFO("elapsed time {} exceed graceful shutdown max time {}", elapsed, gracefulShutdownTime_);
        promise.SetValue(false);
        return;
    }

    std::unordered_set<pid_t> cleared;
    for (auto pid : runtimeToExit_) {
        if (kill(pid, 0) != 0) {
            cleared.emplace(pid);
        } else {
            YRLOG_INFO("runtime with pid {} is still running", pid);
        }
    }
    for (auto pid : cleared) {
        runtimeToExit_.erase(pid);
    }
    if (runtimeToExit_.empty()) {
        YRLOG_INFO("all runtimes have exited");
        // clear all worker dir after all runtimes have exited
        litebus::Async(monitorCallBackActor_->GetAID(), &MonitorCallBackActor::DeleteAllMonitorAndRemoveDir);
        promise.SetValue(true);
        return;
    }

    litebus::AsyncAfter(WAIT_RUNTIMES_EXITED_INTERVAL, GetAID(), &RuntimeExecutor::CheckRuntimesExited, start, promise);
}

Status RuntimeExecutor::PostStartExecHook(const messages::RuntimeConfig &config, const std::string &instanceID)
{
    auto iter = config.posixenvs().find("POST_START_EXEC");
    if (iter == config.posixenvs().end()) {
        return Status::OK();
    }

    if (!std::regex_search(iter->second, std::regex(POST_START_EXEC_REGEX))) {
        return Status(FAILED, iter->second + " is not match the regular");
    }
    const auto &command = iter->second;
    auto result = ExecuteCommandByPopen(command, INT32_MAX, true, instanceID);
    if (result.empty() || result.find("ERROR") != std::string::npos) {
        YRLOG_ERROR(
            "failed to execute POST_START_EXEC command({}), error:\n"
            "---POST_START_EXEC begin---\n{}---POST_START_EXEC end---",
            command, result);
        return Status(FAILED,
                      "failed to execute POST_START_EXEC command(" + command + ") in pre start, code: " + result);
    }
    YRLOG_DEBUG(
        "execute POST_START_EXEC command({}), output:\n"
        "---POST_START_EXEC begin---\n{}---POST_START_EXEC end---",
        command, result);
    return Status::OK();
}

inline bool IsCondaExist()
{
    auto path = LookPath(CONDA_PROGRAM_NAME);
    if (path.IsNone()) {
        return false;
    }
    return true;
}

litebus::Future<messages::StartInstanceResponse> RuntimeExecutor::StartInstance(
    const std::shared_ptr<messages::StartInstanceRequest> &request, const std::vector<int> &cardIDs)
{
    const auto &info = request->runtimeinstanceinfo();
    if (auto res(PostStartExecHook(info.runtimeconfig(), info.instanceid())); res.IsError()) {
        YRLOG_ERROR("{}|{}|failed to execute pre start hook, error: {}", info.traceid(), info.requestid(),
                    res.ToString());
        return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_POST_START_EXEC_FAILED, res.ToString());
    }

    if (auto res(mounter_->MountVolumesForFunction(info.runtimeconfig().funcmountconfig())); res.IsError()) {
        YRLOG_ERROR("{}|{}|failed to mount volumes for function, error: {}", info.traceid(), info.requestid(),
                    res.ToString());
        return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_MOUNT_VOLUME_FAILED);
    }
    gracefulShutdownTime_ = request->runtimeinstanceinfo().gracefulshutdowntime();

    if (request->runtimeinstanceinfo().runtimeconfig().subdirectoryconfig().isenable()
        && CreateSubDir(request).IsError()) {
        YRLOG_ERROR("{}|{}|create sub dir failed", info.traceid(), info.requestid());
        return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_CREATE_EXEC_FAILED);
    }

    std::string language = request->runtimeinstanceinfo().runtimeconfig().language();
    const auto &deployOptions = request->runtimeinstanceinfo().deploymentconfig().deployoptions();
    if (deployOptions.find(CONDA_PREFIX) != deployOptions.end()) {
        if (!IsCondaExist()) {
            YRLOG_ERROR("{}|{}|{} not found in path", info.traceid(), info.requestid(), CONDA_PROGRAM_NAME);
            return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_CONDA_PARAMS_INVALID,
                                                CONDA_PROGRAM_NAME + " not found in path");
        }
        if (deployOptions.find(CONDA_DEFAULT_ENV) == deployOptions.end()) {
            YRLOG_ERROR("{}|{}|CONDA_DEFAULT_ENV must be set", info.traceid(), info.requestid());
            return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_CONDA_PARAMS_INVALID,
                                                "CONDA_DEFAULT_ENV must be set");
        }
        if (auto it = deployOptions.find(CONDA_COMMAND);
            it != deployOptions.end()
            && (!CheckIllegalChars(it->second)
                || !litebus::strings::StartsWithPrefix(it->second, CONDA_PROGRAM_NAME))) {
            YRLOG_ERROR("{}|{}|conda command({}) is not valid", info.traceid(), info.requestid(), it->second);
            return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_CONDA_PARAMS_INVALID,
                                                "conda command(" + it->second + ") is not valid");
        }
    }

    (void)transform(language.begin(), language.end(), language.begin(), ::tolower);
    PrestartProcess prestartProcess = GetRuntimeFromPool(language, request->scheduleoption().schedpolicyname());
    if (prestartProcess.execPtr == nullptr) {
        return StartInstanceWithoutPrestart(request, language, cardIDs);
    }
    const auto &port = prestartProcess.port;
    const auto &execPtr = prestartProcess.execPtr;
    const auto &runtimeID = prestartProcess.runtimeID;
    request->mutable_runtimeinstanceinfo()->set_runtimeid(runtimeID);
    nlohmann::json envJson(CombineEnvs(GenerateEnvs(config_, request, port, cardIDs)));
    std::string env;
    try {
        env = envJson.dump() + "\n";
    } catch (std::exception &e) {
        YRLOG_ERROR("dump envJson failed, error: {}", e.what());
    }
    if (env.size() > MAX_WRITE_LENGTH) {
        YRLOG_ERROR("{}|{}|env info is too long, runtimeID: {}", info.traceid(), info.requestid(), runtimeID);
        return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_CREATE_EXEC_FAILED);
    }
    if (write(execPtr->GetIn().Get(), env.c_str(), env.size()) == -1) {
        YRLOG_ERROR("{}|{}|failed to write env info, runtimeID: {}, errno: {}", info.traceid(), info.requestid(),
                    runtimeID, errno);
        return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_CREATE_EXEC_FAILED);
    }
    runtime2PID_[runtimeID] = execPtr->GetPid();
    pids_.insert(execPtr->GetPid());
    runtimeInstanceInfoMap_[runtimeID] = request->runtimeinstanceinfo();
    (void)runtime2Exec_.insert(std::make_pair(runtimeID, execPtr));
    YRLOG_INFO("{}|{}|start instance success, instanceID({}) runtimeID({}) PID({}) IP({}) Port({})", info.traceid(),
               info.requestid(), info.instanceid(), info.runtimeid(), execPtr->GetPid(), config_.ip, port);
    return GenSuccessStartInstanceResponse(request, execPtr, port);
}

Status RuntimeExecutor::CreateSubDir(const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    auto parentPath = request->runtimeinstanceinfo().runtimeconfig().subdirectoryconfig().parentdirectory();
    if (!IsPathWriteable(parentPath, config_.runtimeUID, config_.runtimeGID)) {
        // if parent doesn't exist or is not writeable, set parentPath to /tmp
        parentPath = "/tmp";
    }
    std::string workDir;
    if (request->runtimeinstanceinfo().runtimeconfig().subdirectoryconfig().isenable()) {
        workDir = litebus::os::Join(parentPath, request->runtimeinstanceinfo().instanceid());
        if (litebus::os::Mkdir(workDir, false).IsSome()) {
            // mkdir return none when ok
            YRLOG_ERROR("failed to mkdir ({}), msg: {}", workDir, litebus::os::Strerror(errno));
            return Status(StatusCode::FAILED);
        }

        // set 750 permission, dir is still owned by sn, which will prevent snuser change dir permissions
        int result = chmod(workDir.c_str(), 0770);
        if (result != 0) {
            YRLOG_ERROR("failed to execute chmod error msg: {}", litebus::os::Strerror(errno));
            (void)litebus::os::Rmdir(workDir);
            return Status(StatusCode::FAILED);
        }
    } else {
        workDir = parentPath;
    }

    (*request->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->mutable_posixenvs())[INSTANCE_WORK_DIR_ENV] =
        workDir;

    litebus::Async(monitorCallBackActor_->GetAID(), &MonitorCallBackActor::AddToMonitorMap,
                   request->runtimeinstanceinfo().instanceid(), workDir, request);
    return Status::OK();
}

litebus::Future<messages::StartInstanceResponse> RuntimeExecutor::StartInstanceWithoutPrestart(
    const std::shared_ptr<messages::StartInstanceRequest> &request, const std::string &language,
    const std::vector<int> &cardIDs)
{
    const auto &info = request->runtimeinstanceinfo();
    std::string runtimeID = request->runtimeinstanceinfo().runtimeid();
    std::string port;
    auto tlsConfig = request->runtimeinstanceinfo().runtimeconfig().tlsconfig();
    RuntimeFeatures features;
    if (tlsConfig.enableservermode()) {
        port = tlsConfig.posixport();
        features.serverMode = false;
    } else {
        port = PortManager::GetInstance().RequestPort(runtimeID);
        features.serverMode = true;
        features.serverPort = port;
    }
    YRLOG_DEBUG("enableservermode = {}, port = {}", tlsConfig.enableservermode(), port);
    if (port.empty()) {
        YRLOG_ERROR("{}|{}|port resource is not available, can not start instanceID({}), runtimeID({})", info.traceid(),
                    info.requestid(), info.instanceid(), runtimeID);
        return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_PORT_UNAVAILABLE);
    }
    if (CheckRuntimeCredential(request) != StatusCode::SUCCESS) {
        YRLOG_ERROR("{}|{}|CheckRuntimeCredential failed, instanceID({}), runtimeID({})", info.traceid(),
                    info.requestid(), info.instanceid(), runtimeID);
        return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_PARAMS_INVALID);
    }
    std::vector<std::string> args;
    if (auto status = GetBuildArgs(language, port, request, args); status.IsError()) {
        YRLOG_ERROR("{}|{}|get build args failed, can not start instanceID({}), runtimeID({})", info.traceid(),
                    info.requestid(), info.instanceid(), runtimeID);
        return GenFailStartInstanceResponse(request, status.StatusCode(), status.GetMessage());
    }
    YRLOG_INFO("{}|{}|advance to start instanceID({}) runtimeID({})", info.traceid(), info.requestid(),
               info.instanceid(), runtimeID);

    // Set the runtime environment variable for direct connection.
    if (config_.runtimeDirectConnectionEnable) {
        if (tlsConfig.enableservermode()) {
            auto runtimeServerPort = PortManager::GetInstance().RequestPort(runtimeID);
            if (runtimeServerPort.empty()) {
                YRLOG_WARN("direct runtime server runtimeServerPort resource is not available for runtime({})",
                           runtimeID);
                features.runtimeDirectConnectionEnable = false;
            } else {
                YRLOG_DEBUG("allocate port({}) for runtime({}) direct connection", runtimeServerPort, runtimeID);
                features.runtimeDirectConnectionEnable = true;
                features.directRuntimeServerPort = runtimeServerPort;
            }
        } else {
            YRLOG_DEBUG("reuse port({}) for runtime({}) direct connection", port, runtimeID);
            features.runtimeDirectConnectionEnable = true;
            features.directRuntimeServerPort = port;
        }
    }
    features.cleanStreamProducerEnable = config_.cleanStreamProducerEnable;
    return StartRuntime(request, language, port, GenerateEnvs(config_, request, port, cardIDs, features), args);
}

litebus::Future<messages::StartInstanceResponse> RuntimeExecutor::StartRuntime(
    const std::shared_ptr<messages::StartInstanceRequest> &request, const std::string &language,
    const std::string &port, const Envs &envs, const std::vector<std::string> &args)
{
    const auto &info = request->runtimeinstanceinfo();
    std::string execPath;
    if (litebus::strings::StartsWithPrefix(language, PYTHON_LANGUAGE)) {
        const auto &deployOptions = request->runtimeinstanceinfo().deploymentconfig().deployoptions();
        auto [execPathStatus, condaExecPath] = GetPythonExecPath(deployOptions, info);
        if (execPathStatus.IsError()) {
            return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_EXEC_PATH_NOT_FOUND);
        }
        execPath = condaExecPath;
    } else {
        execPath = GetExecPathFromRuntimeConfig(info.runtimeconfig());
    }
    YRLOG_DEBUG("{}|{}|language({}) executor path: {}", info.traceid(), info.requestid(), language, execPath);
    if (execPath.empty()) {
        YRLOG_ERROR("{}|{}|execPath is not found, start instanceID({}) failed, runtimeID({})", info.traceid(),
                    info.requestid(), info.instanceid(), info.runtimeid());
        return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_EXEC_PATH_NOT_FOUND,
                                            "Executable path of " + language + " is not found");
    }

    std::shared_ptr<litebus::Exec> execPtr = StartRuntimeByRuntimeIDWithRetry(
        { { PARAM_EXEC_PATH, execPath }, { PARAM_RUNTIME_ID, info.runtimeid() }, { PARAM_LANGUAGE, language } }, args,
        envs, BuildInitHook(request), info);
    if (execPtr == nullptr || execPtr->GetPid() == -1) {
        YRLOG_ERROR("{}|{}|failed to create exec, instanceID({}), runtimeID({}), errno({}), errorMsg({})",
                    info.traceid(), info.requestid(), info.instanceid(), info.runtimeid(), errno, strerror(errno));
        return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_CREATE_EXEC_FAILED);
    }

    auto tlsConfig = request->runtimeinstanceinfo().runtimeconfig().tlsconfig();
    auto fillMetricsTLSStatus = FillMetricsTLSConfig(tlsConfig);
    if (fillMetricsTLSStatus.IsError()) {
        YRLOG_WARN("failed to fill metrics tls config, error: {}", fillMetricsTLSStatus.ToString());
    }
    Status result;
    if (config_.isProtoMsgToRuntime) {
        result =
            WriteProtoToRuntime(request->runtimeinstanceinfo().requestid(), request->runtimeinstanceinfo().runtimeid(),
                                tlsConfig, execPtr);
    } else {
        result =
            WriteJsonToRuntime(request->runtimeinstanceinfo().requestid(), request->runtimeinstanceinfo().runtimeid(),
                               tlsConfig, execPtr);
    }
    if (result.IsError()) {
        return GenFailStartInstanceResponse(request, result.StatusCode());
    }
    if (config_.separatedRedirectRuntimeStd && NeedToRollUserLog()) {
        StartRuntimeStdRedirection(info.runtimeid(), execPtr->GetOut(), execPtr->GetErr());
    }
    if (!config_.separatedRedirectRuntimeStd) {
        litebus::Async(GetStdRedirector(config_.nodeID)->GetAID(), &StdRedirector::StartRuntimeStdRedirection,
                       info.runtimeid(), info.instanceid(), execPtr->GetOut(), execPtr->GetErr());
    }
    YRLOG_INFO("{}|{}|start instance success, instanceID({}), runtimeID({}), PID({}), IP({}), Port({})", info.traceid(),
               info.requestid(), info.instanceid(), info.runtimeid(), execPtr->GetPid(), config_.ip, port);
    runtime2PID_[info.runtimeid()] = execPtr->GetPid();
    pids_.insert(execPtr->GetPid());
    runtimeInstanceInfoMap_[info.runtimeid()] = request->runtimeinstanceinfo();
    (void)runtime2Exec_.insert(std::make_pair(info.runtimeid(), execPtr));

    if (const auto &deployOptions = info.deploymentconfig().deployoptions();
        deployOptions.contains(CONDA_DEFAULT_ENV) && virtualEnvMgr_ != nullptr) {
        virtualEnvMgr_->AddEnvReferInfos(deployOptions.at(CONDA_DEFAULT_ENV),
                                         request->runtimeinstanceinfo().runtimeid());
    }
    return GenSuccessStartInstanceResponse(request, execPtr, port);
}


void RuntimeExecutor::StartRuntimeStdRedirection(const std::string &runtimeID,
                                                 const litebus::Option<int> stdOut, const litebus::Option<int> stdErr)
{
    const StdRedirectParam stdRedirectParam = {
        .maxLogLength          = config_.userLogBufferFlushThreshold,
        .flushDuration         = config_.userLogAutoFlushIntervalMs,
        .stdRollingMaxFileSize = config_.userLogRollingSizeLimitMb,
        .stdRollingMaxFiles    = config_.userLogRollingFileCountLimit,
        .exportMode            = FILE_EXPORTER,
        .logFileExtensions     = DEFAULT_LOG_FILE_OUT_EXTENSIONS,
    };

    if (stdOut.IsSome()) {
        std::function<void(int, int)> ReadDataCallback =
            [aid(GetStdRedirector(runtimeID, stdRedirectParam)->GetAID())](int fd, int event) {
                litebus::Async(aid, &StdRedirector::ReadStdLogRealTime, fd, event);
            };
        stdMonitor_->AddFd(stdOut.Get(), DEFAULT_STD_MONITOR_EVENT, ReadDataCallback);
    }

    if (stdErr.IsSome()) {
        std::function<void(int, int)> ReadDataCallback =
            [aid(GetStdRedirector(runtimeID, stdRedirectParam)->GetAID())](int fd, int event) {
                litebus::Async(aid, &StdRedirector::ReadStdLogRealTime, fd, event);
            };
        stdMonitor_->AddFd(stdErr.Get(), DEFAULT_STD_MONITOR_EVENT, ReadDataCallback);
    }
}

namespace {
bool ReadFileContent(const std::string &path, std::string &content)
{
    if (path.empty()) {
        return false;
    }
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        YRLOG_WARN("failed to open metrics tls cert file {}", path);
        return false;
    }
    content.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
    return !content.empty();
}

litebus::Option<json> LoadRuntimeMetricsConfig()
{
    if (auto config = litebus::os::GetEnv(RUNTIME_METRICS_CONFIG); config.IsSome() && !config.Get().empty()) {
        try {
            return json::parse(config.Get());
        } catch (const std::exception &e) {
            YRLOG_WARN("failed to parse {}, error: {}", RUNTIME_METRICS_CONFIG, e.what());
            return litebus::None();
        }
    }
    std::string configFile;
    if (auto file = litebus::os::GetEnv(RUNTIME_METRICS_CONFIG_FILE); file.IsSome() && !file.Get().empty()) {
        configFile = file.Get();
    }
    if (configFile.empty()) {
        return litebus::None();
    }

    std::ifstream file(configFile);
    if (!file.is_open()) {
        YRLOG_WARN("failed to open runtime metrics config file {}", configFile);
        return litebus::None();
    }
    try {
        json config;
        file >> config;
        return config;
    } catch (const std::exception &e) {
        YRLOG_WARN("failed to parse runtime metrics config file {}, error: {}", configFile, e.what());
        return litebus::None();
    }
}

litebus::Option<json> FindPrometheusPullExporterConfig(const json &metricsConfig)
{
    if (!metricsConfig.is_object() || !metricsConfig.contains("backends") || !metricsConfig.at("backends").is_array()) {
        return litebus::None();
    }
    for (const auto &backend : metricsConfig.at("backends")) {
        if (!backend.is_object() || !backend.contains("immediatelyExport")) {
            continue;
        }
        const auto &immediateExport = backend.at("immediatelyExport");
        if (!immediateExport.is_object() || !immediateExport.contains("exporters") ||
            !immediateExport.at("exporters").is_array()) {
            continue;
        }
        for (const auto &exporter : immediateExport.at("exporters")) {
            if (!exporter.is_object() || !exporter.contains(PROMETHEUS_PULL_EXPORTER)) {
                continue;
            }
            return exporter.at(PROMETHEUS_PULL_EXPORTER);
        }
    }
    return litebus::None();
}

void ClearMetricsTLSConfig(::messages::TLSConfig &tlsConfig)
{
    tlsConfig.clear_metricsrootcertdata();
    tlsConfig.clear_metricscertdata();
    tlsConfig.clear_metricskeydata();
}

bool HasMetricsTLSConfig(const ::messages::TLSConfig &tlsConfig)
{
    return !tlsConfig.metricsrootcertdata().empty() || !tlsConfig.metricscertdata().empty() ||
           !tlsConfig.metricskeydata().empty();
}

bool IsMetricsTLSConfigComplete(const ::messages::TLSConfig &tlsConfig)
{
    return !tlsConfig.metricsrootcertdata().empty() && !tlsConfig.metricscertdata().empty() &&
           !tlsConfig.metricskeydata().empty();
}

void SetMetricsTLSConfigFromBase64(::messages::TLSConfig &tlsConfig, const json &initConfig)
{
    tlsConfig.set_metricsrootcertdata(initConfig.value("rootCertData", ""));
    tlsConfig.set_metricscertdata(initConfig.value("certData", ""));
    tlsConfig.set_metricskeydata(initConfig.value("keyData", ""));
}

void SetMetricsTLSConfigFromFiles(::messages::TLSConfig &tlsConfig, const json &initConfig)
{
    std::string rootCertData;
    std::string certData;
    std::string keyData;
    if (ReadFileContent(initConfig.value("rootCertFile", ""), rootCertData) &&
        ReadFileContent(initConfig.value("certFile", ""), certData) &&
        ReadFileContent(initConfig.value("keyFile", ""), keyData)) {
        tlsConfig.set_metricsrootcertdata(Base64Encode(rootCertData));
        tlsConfig.set_metricscertdata(Base64Encode(certData));
        tlsConfig.set_metricskeydata(Base64Encode(keyData));
    }
}
}  // namespace

Status RuntimeExecutor::FillMetricsTLSConfigFromStsP12(::messages::TLSConfig & /* tlsConfig */) const
{
    return Status::OK();
}

Status RuntimeExecutor::FillMetricsTLSConfig(::messages::TLSConfig &tlsConfig) const
{
    auto metricsConfig = LoadRuntimeMetricsConfig();
    if (metricsConfig.IsNone()) {
        return Status::OK();
    }
    auto exporterConfig = FindPrometheusPullExporterConfig(metricsConfig.Get());
    if (exporterConfig.IsNone()) {
        return Status::OK();
    }
    const auto &exporter = exporterConfig.Get();
    if (!exporter.value("enable", false) || !exporter.contains("initConfig") ||
        !exporter.at("initConfig").is_object()) {
        return Status::OK();
    }
    const auto &initConfig = exporter.at("initConfig");
    if (!initConfig.value("isSSLEnable", false)) {
        return Status::OK();
    }
    if (initConfig.value("certSource", "") == CERT_SOURCE_STS_P12) {
        auto status = FillMetricsTLSConfigFromStsP12(tlsConfig);
        if (status.IsError()) {
            return status;
        }
    } else if (initConfig.contains("rootCertData") || initConfig.contains("certData") ||
               initConfig.contains("keyData")) {
        SetMetricsTLSConfigFromBase64(tlsConfig, initConfig);
    } else {
        SetMetricsTLSConfigFromFiles(tlsConfig, initConfig);
    }
    if (HasMetricsTLSConfig(tlsConfig) && !IsMetricsTLSConfigComplete(tlsConfig)) {
        YRLOG_WARN("metrics tls config is incomplete, skip passing it to runtime");
        ClearMetricsTLSConfig(tlsConfig);
    }
    return Status::OK();
}

Status RuntimeExecutor::WriteProtoToRuntime(const std::string &requestID, const std::string &runtimeID,
                                            const ::messages::TLSConfig &tlsConfig,
                                            const std::shared_ptr<litebus::Exec> execPtr) const
{
    std::vector<uint8_t> buff;
    buff.resize(tlsConfig.ByteSizeLong());
    tlsConfig.SerializeToArray(buff.data(), buff.size());
    auto writeRes = write(execPtr->GetIn().Get(), buff.data(), buff.size());
    if (writeRes == -1) {
        YRLOG_ERROR("{}|write tls config failed!,runtimeID({}), errno({}), errorMsg({})", requestID, runtimeID, errno,
                    strerror(errno));
        return Status(StatusCode::RUNTIME_MANAGER_CREATE_EXEC_FAILED);
    }
    return Status(StatusCode::SUCCESS);
}

Status RuntimeExecutor::WriteJsonToRuntime(const std::string &requestID, const std::string &runtimeID,
                                           const ::messages::TLSConfig &tlsConfig,
                                           const std::shared_ptr<litebus::Exec> execPtr) const
{
    std::string tlsConfigStr;
    if (!google::protobuf::util::MessageToJsonString(tlsConfig, &tlsConfigStr).ok()) {
        YRLOG_ERROR("{}|invalid tls config, instanceID({}), runtimeID({})", requestID, runtimeID);
        return Status(StatusCode::RUNTIME_MANAGER_BUILD_ARGS_INVALID);
    }
    tlsConfigStr = tlsConfigStr + "\n";
    if (tlsConfigStr.size() > MAX_WRITE_LENGTH) {
        YRLOG_ERROR("{}|write tls config is too long!,runtimeID({})", requestID, runtimeID);
        return Status(StatusCode::RUNTIME_MANAGER_CREATE_EXEC_FAILED);
    }
    auto writeRes = write(execPtr->GetIn().Get(), tlsConfigStr.c_str(), tlsConfigStr.size());
    for (char &ch : tlsConfigStr) {
        ch = '0';
    }
    if (writeRes == -1) {
        YRLOG_ERROR("{}|write tls config failed!, runtimeID({}), errno({}), errorMsg({})", requestID, runtimeID, errno,
                    strerror(errno));
        return Status(StatusCode::RUNTIME_MANAGER_CREATE_EXEC_FAILED);
    }
    return Status(StatusCode::SUCCESS);
}

std::shared_ptr<litebus::Exec> RuntimeExecutor::StartRuntimeByRuntimeIDWithRetry(
    const std::map<std::string, std::string> &startRuntimeParams, const std::vector<std::string> &buildArgs,
    const Envs &envs, const std::vector<std::function<void()>> childInitHook, const messages::RuntimeInstanceInfo &info)
{
    std::shared_ptr<litebus::Exec> execPtr;
    for (int i = 0; i < RETRY_TIMES; i++) {
        execPtr = StartRuntimeByRuntimeID(startRuntimeParams, buildArgs, envs, childInitHook);
        if (execPtr == nullptr || execPtr->GetPid() == -1) {
            YRLOG_WARN("{}|{}|failed to create exec, instanceID({}), runtimeID({}), errno({}), errorMsg({})",
                       info.traceid(), info.requestid(), info.instanceid(), info.runtimeid(), errno, strerror(errno));
            continue;
        }
        litebus::Async(GetAID(), &RuntimeExecutor::ReportInfo, info.instanceid(), info.runtimeid(), execPtr->GetPid(),
                       functionsystem::metrics::MeterTitle{ "yr_app_instance_start_time", " start timestamp", "ms" });
        return execPtr;
    }
    return nullptr;
}

litebus::Future<Status> RuntimeExecutor::StopInstance(const std::shared_ptr<messages::StopInstanceRequest> &request,
                                                      bool oomKilled)
{
    std::string runtimeID = request->runtimeid();
    std::string requestID = request->requestid();

    if (virtualEnvMgr_ != nullptr) {
        virtualEnvMgr_->RmEnvReferInfos(runtimeID);
    }
    return StopInstanceByRuntimeID(runtimeID, requestID, oomKilled);
}

litebus::Future<messages::SnapshotRuntimeResponse> RuntimeExecutor::SnapshotRuntime(
    const std::shared_ptr<messages::SnapshotRuntimeRequest> &request)
{
    messages::SnapshotRuntimeResponse response;
    response.set_requestid(request->requestid());
    response.set_code(static_cast<int32_t>(StatusCode::GRPC_UNIMPLEMENTED));
    response.set_message("Snapshot is not supported for process-based runtime");
    YRLOG_WARN("{}|SnapshotRuntime is not supported for RuntimeExecutor", request->requestid());
    return response;
}

std::shared_ptr<StdRedirector> RuntimeExecutor::GetStdRedirector(const std::string &logName)
{
    if (stdRedirectors_.find(logName) != stdRedirectors_.end()) {
        return stdRedirectors_[logName];
    }
    auto path = litebus::os::Join(config_.runtimeLogPath, config_.runtimeStdLogDir);
    if (!litebus::os::ExistPath(path)) {
        YRLOG_WARN("std log path {} not found, try to make dir", path);
        if (!litebus::os::Mkdir(path).IsNone()) {
            YRLOG_WARN("failed to make dir {}, msg: {}", path, litebus::os::Strerror(errno));
            return nullptr;
        }
    }
    auto logFileName = logName + STD_POSTFIX;
    auto stdLogFilePath = litebus::os::Join(path, logFileName);
    YRLOG_INFO("{} not found, create a new redirector log file: {}", logName, stdLogFilePath);
    auto stdRedirectParam = StdRedirectParam{};
    stdRedirectParam.exportMode = config_.userLogExportMode;
    auto redirector = std::make_shared<StdRedirector>(path, logFileName, stdRedirectParam);
    (void)litebus::Spawn(redirector);
    (void)litebus::Async(redirector->GetAID(), &StdRedirector::Start);
    stdRedirectors_[logName] = redirector;
    return redirector;
}

std::shared_ptr<StdRedirector> RuntimeExecutor::GetStdRedirector(const std::string &runtimeID,
                                                                 const StdRedirectParam &stdRedirectParam)
{
    if (stdRedirectors_.find(runtimeID) != stdRedirectors_.end()) {
        return stdRedirectors_[runtimeID];
    }
    auto path = litebus::os::Join(config_.runtimeLogPath, config_.runtimeStdLogDir);
    auto logFileName = runtimeID + stdRedirectParam.logFileExtensions;
    YRLOG_INFO("{} not found, create a new redirector log file: {} {}", runtimeID, path, logFileName);
    auto redirector = std::make_shared<StdRedirector>(path, logFileName, stdRedirectParam);
    (void)litebus::Spawn(redirector);
    (void)litebus::Async(redirector->GetAID(), &StdRedirector::Start);
    stdRedirectors_[runtimeID] = redirector;
    return redirector;
}

void RuntimeExecutor::StopRedirectorByRuntimeID(const std::string &runtimeID)
{
    auto iter = stdRedirectors_.find(runtimeID);
    if (iter == stdRedirectors_.end()) {
        return;
    }
    litebus::Terminate(iter->second->GetAID());
    litebus::Await(iter->second->GetAID());
    stdRedirectors_.erase(iter);
}

void RuntimeExecutor::ReportInfo(const std::string &instanceID, const std::string runtimeID, const pid_t &pid,
                                 const functionsystem::metrics::MeterTitle &title)
{
    auto timeStamp =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();

    functionsystem::metrics::MeterData data{
        static_cast<double>(timeStamp),
        {
            { "instance_id", instanceID },
            { "node_id", config_.nodeID },
            { "ip", config_.ip },
            { "runtime_id", runtimeID },
            { "pid", std::to_string(pid) },
        },
    };
    functionsystem::metrics::MetricsAdapter::GetInstance().ReportGauge(title, data);
}

void RuntimeExecutor::ConfigRuntimeRedirectLog(litebus::ExecIO &stdOut, litebus::ExecIO &stdErr,
                                               const std::string &runtimeID) const
{
    auto path = litebus::os::Join(config_.runtimeLogPath, config_.runtimeStdLogDir);
    if (!litebus::os::ExistPath(path)) {
        YRLOG_WARN("std log path {} not found, try to make dir", path);
        if (!litebus::os::Mkdir(path).IsNone()) {
            YRLOG_WARN("failed to make dir {}, msg: {}", path, litebus::os::Strerror(errno));
            return;
        }
    }
    char realPath[PATH_MAX] = { 0 };
    if (realpath(path.c_str(), realPath) == nullptr) {
        YRLOG_WARN("real path std log file {} failed, errno: {}, {}", path, errno, litebus::os::Strerror(errno));
        return;
    }

    auto outFile = litebus::os::Join(std::string(realPath), runtimeID + ".out");
    if (!litebus::os::ExistPath(outFile) && TouchFile(outFile) != 0) {
        YRLOG_WARN("create std out log file {} failed.", outFile, litebus::os::Strerror(errno));
        return;
    }
    stdOut = litebus::ExecIO::CreateFileIO(outFile);

    auto errFile = litebus::os::Join(std::string(realPath), runtimeID + ".err");
    if (!litebus::os::ExistPath(errFile) && TouchFile(errFile) != 0) {
        YRLOG_WARN("create std err log file {} failed.", errFile, litebus::os::Strerror(errno));
        return;
    }
    stdErr = litebus::ExecIO::CreateFileIO(errFile);
}

bool RuntimeExecutor::NeedToRollUserLog() const
{
    return config_.userLogRollingFileCountLimit != 0;
}

std::shared_ptr<litebus::Exec> RuntimeExecutor::StartRuntimeByRuntimeID(
    const std::map<std::string, std::string> startRuntimeParams, const std::vector<std::string> &buildArgs,
    const Envs &envs, const std::vector<std::function<void()>> childInitHook) const
{
    const auto &execPath = startRuntimeParams.at(PARAM_EXEC_PATH);
    auto language = startRuntimeParams.at(PARAM_LANGUAGE);
    const std::map<std::string, std::string> combineEnvs = CombineEnvs(envs);
    auto runtimeID = startRuntimeParams.at(PARAM_RUNTIME_ID);
    if (config_.massifEnable
        && (language.find(CPP_LANGUAGE) != std::string::npos || language.find(GO_LANGUAGE) != std::string::npos)) {
        return CreateMassifWrapExec(runtimeID, execPath, buildArgs, combineEnvs, childInitHook);
    }
    litebus::ExecIO stdOut = litebus::ExecIO::CreatePipeIO();
    auto stdErr = stdOut;
    if (config_.userLogExportMode == functionsystem::runtime_manager::FILE_EXPORTER
        && config_.separatedRedirectRuntimeStd
        && !NeedToRollUserLog()) {
        ConfigRuntimeRedirectLog(stdOut, stdErr, runtimeID);
    }
    std::string cmd = execPath;
    for (const auto &arg : buildArgs) {
        cmd += " ";
        cmd += arg;
    }
    // java has jvm args check so ignore here
    if (language.find(JAVA_LANGUAGE_PREFIX) == std::string::npos && !CheckIllegalChars(cmd)) {
        return nullptr;
    }

    YRLOG_INFO("start {} runtime({}), execute final cmd: {}", language, runtimeID, cmd);
    if (language.find(JAVA_LANGUAGE) != std::string::npos || language.find(JAVA11_LANGUAGE) != std::string::npos
        || language.find(POSIX_CUSTOM_RUNTIME) != std::string::npos) {
        return litebus::Exec::CreateExec(cmd, combineEnvs, litebus::ExecIO::CreatePipeIO(), stdOut, stdErr,
                                         childInitHook, {}, false);
    } else {
        return litebus::Exec::CreateExec(execPath, buildArgs, combineEnvs, litebus::ExecIO::CreatePipeIO(), stdOut,
                                         stdErr, childInitHook, {}, false);
    }
}

std::string RuntimeExecutor::GetExecPath(const std::string &language) const
{
    std::string languageArg = GetLanguageArg(language);
    std::string languageCmd = language;
    YRLOG_DEBUG("ready to GetExecPath, language: {}, languageArg: {}", language, languageArg);
    if (languageArg == CPP_LANGUAGE) {
        return config_.runtimePath + CPP_NEW_EXEC_PATH;
    } else if (languageArg == GO_LANGUAGE) {
        return config_.runtimePath + GO_NEW_EXEC_PATH;
    } else if (languageArg == NODE_JS) {
        languageCmd = NODE_JS_CMD;
    } else if (languageArg == JAVA_LANGUAGE) {
        languageCmd = JAVA_LANGUAGE;
    } else if (languageArg == JAVA11_LANGUAGE) {
        languageCmd = JAVA11_LANGUAGE;
    } else if (languageArg == JAVA17_LANGUAGE) {
        languageCmd = JAVA17_LANGUAGE;
    } else if (languageArg == JAVA21_LANGUAGE) {
        languageCmd = JAVA21_LANGUAGE;
    }
    auto path = LookPath(languageCmd);
    if (path.IsNone()) {
        YRLOG_ERROR("GetExecPath failed, path is null");
        return "";
    }
    YRLOG_INFO("GetExecPath, execPath: {}", path.Get());
    return path.Get();
}

std::string RuntimeExecutor::GetExecPathFromRuntimeConfig(const messages::RuntimeConfig &config) const
{
    const std::string &language = config.language();
    if (language == POSIX_CUSTOM_RUNTIME) {
        // custom-runtime Case1: compatible with job entrypoint, like "python script.py"
        auto workingDirIter = config.posixenvs().find(UNZIPPED_WORKING_DIR);
        if (workingDirIter != config.posixenvs().end() && !workingDirIter->second.empty()) {
            std::string entrypoint = config.entryfile();
            if (entrypoint.empty()) {
                YRLOG_ERROR("empty job entrypoint is invalid");
                return "";
            }
            YRLOG_DEBUG("job entrypoint: {}", entrypoint);
            return entrypoint;
        }

        // custom-runtime Case2: C++ FaaS entrypoint, like "start.sh"
        auto delegateBootstrapIter = config.posixenvs().find(ENV_DELEGATE_BOOTSTRAP);
        auto delegateDownloadIter = config.posixenvs().find(ENV_DELEGATE_DOWNLOAD);
        if (delegateBootstrapIter != config.posixenvs().end() && delegateDownloadIter != config.posixenvs().end()) {
            std::stringstream ss;
            ss << delegateDownloadIter->second << "/" << delegateBootstrapIter->second;
            YRLOG_DEBUG("posix custom runtime entry file : {}", ss.str());
            return ss.str();
        }
        // custom-runtime Cases
        return config.entryfile();
    }
    return GetExecPath(language);
}

std::string RuntimeExecutor::GetLanguageArg(const std::string &language) const
{
    std::string res = language;
    for (const auto &lang : languages) {
        if (language.find(lang) != std::string::npos) {
            YRLOG_DEBUG("GetLanguageArg find lang: {}", lang);
            res = lang;
            return res;
        }
    }
    YRLOG_DEBUG("cannot support this language: {}", res);
    return res;
}

std::map<std::string, std::string> RuntimeExecutor::CombineEnvs(const Envs &envs) const
{
    auto posixEnvs = envs.posixEnvs;
    auto customEnvs = envs.customResourceEnvs;
    auto userEnvs = envs.userEnvs;
    std::map<std::string, std::string> combineEnvs = posixEnvs;
    combineEnvs.insert(customEnvs.begin(), customEnvs.end());
    // userEnvs and override posixEnvs and customEnvs
    for (auto &pair : std::as_const(userEnvs)) {
        auto iter = combineEnvs.find(pair.first);
        if (iter == combineEnvs.end()) {
            combineEnvs.insert(pair);
            continue;
        }

        if (pair.first == LD_LIBRARY_PATH) {
            combineEnvs[pair.first] = iter->second + ":" + pair.second;
            continue;
        }

        combineEnvs[pair.first] = pair.second;
    }
    // framework envs needed by runtime override userEnvs
    combineEnvs[YR_LOG_LEVEL] = config_.runtimeLogLevel;
    combineEnvs[GLOG_LOG_DIR] = config_.runtimeLogPath;
    combineEnvs["YR_LOG_PATH"] = config_.runtimeLogPath;
    combineEnvs[PYTHON_LOG_CONFIG_PATH] = config_.pythonLogConfigPath;
    combineEnvs[MAX_LOG_SIZE_MB_ENV] = std::to_string(config_.runtimeMaxLogSize);
    combineEnvs[MAX_LOG_FILE_NUM_ENV] = std::to_string(config_.runtimeMaxLogFileNum);
    // set distributed convergent call stack enable environment variable
    combineEnvs[ENABLE_DIS_CONV_CALL_STACK] = config_.enableDisConvCallStack ? "true" : "false";
    std::string pythonPath = "";
    if (pkgType_ == PKG_TYPE_WHEEL) {
        pythonPath = config_.runtimePath + "/../../../";
    }
    if (!config_.pythonDependencyPath.empty()) {
        (void)pythonPath.append(":" + config_.pythonDependencyPath);
    }

    // python job working dir after unzip
    auto workingDirIter = combineEnvs.find(UNZIPPED_WORKING_DIR);
    if (workingDirIter != combineEnvs.end() && !workingDirIter->second.empty()) {
        (void)pythonPath.append(":" + workingDirIter->second);
    }
    if (combineEnvs.find(PYTHON_PATH) != combineEnvs.end()) {
        auto userDefined = combineEnvs[PYTHON_PATH];
        pythonPath = userDefined.append(":" + pythonPath);
    }
    combineEnvs[PYTHON_PATH] = pythonPath;

    // exclude envs to runtime process
    for (const std::string &str : EXCLUDE_ENV_KEYS_PASSED_TO_RUNTIME) {
        combineEnvs.erase(str);
    }

    // add runtime ds-client connection timeout env
    combineEnvs[RUNTIME_DS_CONNECT_TIMEOUT_ENV] = std::to_string(config_.runtimeDsConnectTimeout);

    InheritEnv(combineEnvs);
    return combineEnvs;
}

void RuntimeExecutor::InheritEnv(std::map<std::string, std::string> &combineEnvs) const
{
    char **env = environ;
    for (; *env; ++env) {
        std::string envStr = *env;
        auto equalPos = envStr.find('=');
        if (equalPos == std::string::npos) {
            continue;
        }
        auto key = envStr.substr(0, equalPos);
        auto val = envStr.substr(equalPos + 1);
        if (litebus::strings::StartsWithPrefix(key, WHITE_LIST_ENV_PREFIX)) {
            if (combineEnvs.find(key) == combineEnvs.end()) {
                combineEnvs[key] = val;
            }
            continue;
        }
        if (config_.inheritEnv) {
            if (key == PATH) {
                combineEnvs[key] = (combineEnvs[key].empty() ? "" : combineEnvs[key] + ":") + val;
                continue;
            }
            if (combineEnvs.find(key) != combineEnvs.end()) {
                continue;
            }
            combineEnvs[key] = val;
        }
    }

    // if set YR_NOSET_ASCEND_RT_VISIBLE_DEVICES , ASCEND_RT_VISIBLE_DEVICES will not set
    if (combineEnvs.find(YR_NOSET_ASCEND_RT_VISIBLE_DEVICES) != combineEnvs.end()) {
        (void)combineEnvs.erase(ASCEND_RT_VISIBLE_DEVICES);
    }
    // if set YR_NOSET_CUDA_VISIBLE_DEVICES , CUDA_VISIBLE_DEVICES will not set
    if (combineEnvs.find(YR_NOSET_CUDA_VISIBLE_DEVICES) != combineEnvs.end()) {
        (void)combineEnvs.erase(CUDA_VISIBLE_DEVICES);
    }
}

void RuntimeExecutor::StopMonitoringFD(const std::string &runtimeID, const std::string &requestID)
{
    auto execPtr = GetExecByRuntimeID(runtimeID);
    if (execPtr == nullptr) {
        YRLOG_WARN("{}|{}|runtime has already been killed.", requestID, runtimeID);
        return;
    }

    auto stdOut = execPtr->GetOut();
    if (stdOut.IsSome()) {
        stdMonitor_->RemoveFd(stdOut.Get());
    }

    auto stdErr = execPtr->GetErr();
    if (stdErr.IsSome()) {
        stdMonitor_->RemoveFd(stdErr.Get());
    }
}

Status RuntimeExecutor::StopInstanceByRuntimeID(const std::string &runtimeID, const std::string &requestID,
                                                bool oomKilled)
{
    auto pidIter = runtime2PID_.find(runtimeID);
    if (pidIter == runtime2PID_.end()) {
        if (innerOomKilledruntimes_.find(runtimeID) != innerOomKilledruntimes_.end()) {
            YRLOG_DEBUG("{}|runtime({}) already deleted by oomMonitor.", requestID, runtimeID);
            innerOomKilledruntimes_.erase(runtimeID);
            return Status::OK();  // for adapting instance exit clean logic in function_proxy
        }
        YRLOG_ERROR("{}|can not find pid to stop runtime({}).", requestID, runtimeID);
        return Status(RUNTIME_MANAGER_RUNTIME_PROCESS_NOT_FOUND);
    }

    YRLOG_INFO("{}|kill process({}) of runtime({}).", requestID, pidIter->second, runtimeID);
    KillProcess(pidIter->second, oomKilled);

    auto infoIter = runtimeInstanceInfoMap_.find(runtimeID);
    std::string instanceID = "";
    if (infoIter != runtimeInstanceInfoMap_.end()) {
        instanceID = infoIter->second.instanceid();
        runtimeInstanceInfoMap_.erase(runtimeID);
    }
    functionsystem::metrics::MeterTitle title{ "yr_instance_stop_time", "stop timestamp", "num" };
    litebus::Async(GetAID(), &RuntimeExecutor::ReportInfo, instanceID, runtimeID, pidIter->second, title);

    // clear work dir if exist
    litebus::Async(monitorCallBackActor_->GetAID(), &MonitorCallBackActor::DeleteFromMonitorMap, instanceID);
    (void)runtime2PID_.erase(runtimeID);
    StopMonitoringFD(runtimeID, requestID);
    StopRedirectorByRuntimeID(runtimeID);
    (void)runtime2Exec_.erase(runtimeID);
    (void)pids_.erase(pidIter->second);

    if (oomKilled) {
        innerOomKilledruntimes_.insert(runtimeID);
    }
    return Status::OK();
}

bool RuntimeExecutor::ShouldUseProcessGroup() const
{
    const auto yrEnv = litebus::os::GetEnv("YR_BARE_MENTAL");
    return yrEnv.IsNone() || yrEnv.Get().empty();
}

void RuntimeExecutor::TerminateImmediately(pid_t pid, std::string_view processType)
{
    YRLOG_INFO("kill {}: {}", processType, std::abs(pid));

    if (kill(pid, SIGKILL) != 0) {
        YRLOG_ERROR("kill {}({}) failed, errno({})", processType, std::abs(pid), errno);
    } else {
        YRLOG_INFO("SIGKILL killed {}: {}", processType, std::abs(pid));
    }
}

void RuntimeExecutor::SendGracefulTermination(pid_t pid, std::string_view processType)
{
    YRLOG_INFO("kill {}: {}", processType, std::abs(pid));

    // Send initial SIGINT
    (void)kill(pid, SIGINT);

    // Setup delayed SIGKILL
    litebus::TimerTools::AddTimer(
        config_.killProcessTimeoutSeconds * litebus::SECTOMILLI, "TriggerSignalKill", [pid, processType]() {
            if (kill(pid, 0) != 0) {
                YRLOG_INFO("SIGINT killed {}: {}", processType, std::abs(pid));
                return;
            }

            if (kill(pid, SIGKILL) != 0) {
                YRLOG_ERROR("kill {}({}) failed, errno({})", processType, std::abs(pid), errno);
            }

            YRLOG_INFO("SIGKILL killed {}: {}", processType, std::abs(pid));
        });
}

void RuntimeExecutor::KillProcess(const pid_t &pid, bool force)
{
    const bool useProcessGroup = ShouldUseProcessGroup();
    const pid_t targetPid = useProcessGroup ? -pid : pid;
    const std::string_view processType = useProcessGroup ? "process group" : "process";

    if (force) {
        TerminateImmediately(targetPid, processType);
        return;
    }

    SendGracefulTermination(targetPid, processType);
}

std::map<std::string, messages::RuntimeInstanceInfo> RuntimeExecutor::GetRuntimeInstanceInfos()
{
    return runtimeInstanceInfoMap_;
}

Status RuntimeExecutor::GetBuildArgs(const std::string &language, const std::string &port,
                                     const std::shared_ptr<messages::StartInstanceRequest> &request,
                                     std::vector<std::string> &args)
{
    auto info = request->runtimeinstanceinfo();
    if (chdir(config_.runtimePath.c_str()) != 0) {
        YRLOG_WARN("{}|{}|enter runtimePath failed, path: {}", info.traceid(), info.requestid(), config_.runtimePath);
    }
    std::string langArg = GetLanguageArg(language);
    if (buildArgsFunc_.find(langArg) == buildArgsFunc_.end()) {
        YRLOG_ERROR("{}|{}|RuntimeExecutor does not support this language: {}", info.traceid(), info.requestid(),
                    langArg);
        return Status(StatusCode::PARAMETER_ERROR, "runtimeExecutor does not support this language: " + langArg);
    }

    YRLOG_DEBUG("{}|{}|find buildArgsFunc for lang: {}", info.traceid(), info.requestid(), language);
    auto langBuild = buildArgsFunc_[langArg];
    getBuildArgs_ = std::bind(langBuild, this, std::placeholders::_1, std::placeholders::_2);
    auto [status, args_local] = getBuildArgs_(port, request);
    args = std::move(args_local);
    return status;
}

std::pair<Status, std::vector<std::string>> RuntimeExecutor::GetCppBuildArgs(
    const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const
{
    YRLOG_DEBUG("{}|{}|GetCppBuildArgs start", request->runtimeinstanceinfo().traceid(),
                request->runtimeinstanceinfo().requestid());
    std::string address = GetPosixAddress(config_, port);
    auto confPath = litebus::os::Join(config_.runtimeConfigPath, "runtime.json");

    auto resultPair = HandleWorkingDirectory(request, request->runtimeinstanceinfo());
    if (resultPair.first.IsError()) {
        return { resultPair.first, {} };
    }

    return { Status::OK(),
             { CPP_PROGRAM_NAME, RUNTIME_ID_ARG_PREFIX + request->runtimeinstanceinfo().runtimeid(),
               LOG_LEVEL_PREFIX + config_.runtimeLogLevel,
               JOB_ID_PREFIX + Utils::GetJobIDFromTraceID(request->runtimeinstanceinfo().traceid()),
               GRPC_ADDRESS_PREFIX + address, CONFIG_PATH_PREFIX + confPath } };
}

YAML::Node ConvertJsonToYaml(const json &j)
{
    YAML::Node node;
    if (j.is_object()) {
        for (auto &[key, value] : j.items()) {
            node[key] = ConvertJsonToYaml(value);
        }
    } else if (j.is_array()) {
        for (const auto &element : j) {
            node.push_back(ConvertJsonToYaml(element));
        }
    } else if (j.is_string()) {
        node = j.get<std::string>();
    }
    return node;
}

inline Status CondaEnvJsonToYaml(const std::string &jsonStr, std::string &outYamlStr, const std::string &envName)
{
    try {
        std::string sanitizedStr = jsonStr;
        std::replace(sanitizedStr.begin(), sanitizedStr.end(), '\'', '\"');
        json j = json::parse(sanitizedStr);
        if (!j.contains("name") || !j["name"].is_string() || j["name"].get<std::string>().empty()) {
            j["name"] = envName;  // name is required in conda
        }
        YAML::Node yamlNode = ConvertJsonToYaml(j);
        YAML::Emitter emitter;
        emitter.SetIndent(YAML_INDENT_SIZE);
        emitter << yamlNode;
        if (!emitter.good()) {
            YRLOG_ERROR("YAML emitter error: {}", emitter.GetLastError());
            return Status(StatusCode::FAILED, "Emitter error");
        }
        outYamlStr = emitter.c_str();
        return Status::OK();
    } catch (const json::exception &e) {
        YRLOG_ERROR("JSON parse error: {}", e.what());
        return Status(StatusCode::FAILED, "Invalid JSON format");
    } catch (const YAML::Exception &e) {
        YRLOG_ERROR("YAML conversion error: {}", e.what());
        return Status(StatusCode::FAILED, "YAML generation failed");
    }
}

inline bool IsEnableConda(const google::protobuf::Map<std::string, std::string> &deployOptions)
{
    return deployOptions.count(CONDA_PREFIX) && deployOptions.count(CONDA_DEFAULT_ENV);
}

std::pair<Status, std::string> RuntimeExecutor::GetPythonExecPath(
    const google::protobuf::Map<std::string, std::string> &deployOptions,
    const messages::RuntimeInstanceInfo &info) const
{
    // 插件返回的execPath不局限于python，后续可以统一处理execPath，不需要再分语言了
    auto execPathIter = deployOptions.find(EXEC_PATH);
    if (execPathIter != deployOptions.end()) {
        YRLOG_INFO("{}|{}|python execPath: {}", info.traceid(), info.requestid(), execPathIter->second);
        return { Status::OK(), execPathIter->second };
    }

    if (!IsEnableConda(deployOptions)) {
        return { Status::OK(), GetExecPath(info.runtimeconfig().language()) };
    }

    const auto &condaPrefix = deployOptions.at(CONDA_PREFIX);
    const auto &condaEnv = deployOptions.at(CONDA_DEFAULT_ENV);
    const std::string execPath = litebus::os::Join(
        litebus::os::Join(litebus::os::Join(litebus::os::Join(condaPrefix, "envs"), condaEnv), "bin"), "python");

    YRLOG_INFO("{}|{}|conda python env's execPath: {}", info.traceid(), info.requestid(), execPath);
    return { Status::OK(), execPath };
}

std::pair<Status, std::string> RuntimeExecutor::HandleWorkingDirectory(
    const std::shared_ptr<messages::StartInstanceRequest> &request, const messages::RuntimeInstanceInfo &info) const
{
    const auto &posixEnvs = info.runtimeconfig().posixenvs();
    auto workingDirIter = posixEnvs.find(UNZIPPED_WORKING_DIR);
    auto fileIter = posixEnvs.find(YR_WORKING_DIR);
    if (workingDirIter == posixEnvs.end() || fileIter == posixEnvs.end()) {
        return { Status::OK(), info.deploymentconfig().deploydir() };
    }
    if (workingDirIter->second.empty() || fileIter->second.empty()) {
        YRLOG_ERROR("{}|{}|params working dir({}) or unzipped dir({}) is empty", info.traceid(), info.requestid(),
                    fileIter->second, workingDirIter->second);
        return { Status(StatusCode::RUNTIME_MANAGER_WORKING_DIR_FOR_APP_NOTFOUND,
                        "params working dir or unzipped dir is empty"),
                 "" };
    }

    if (workingDirIter->second.find(".img") != std::string::npos) {
        YRLOG_WARN("{}|{}| not support mount img now {}", info.traceid(), info.requestid(),
                   workingDirIter->second);
        return { Status(StatusCode::RUNTIME_MANAGER_WORKING_DIR_FOR_APP_NOTFOUND, "not support mount img now"), "" };
    }

    char canonicalPath[PATH_MAX];
    if (realpath(workingDirIter->second.c_str(), canonicalPath) == nullptr) {
        return { Status(StatusCode::RUNTIME_MANAGER_WORKING_DIR_FOR_APP_NOTFOUND, "cannot resolve path"), "" };
    }

    if (access(canonicalPath, R_OK | W_OK | X_OK) != 0) {
        return { Status(StatusCode::RUNTIME_MANAGER_WORKING_DIR_FOR_APP_NOTFOUND, "insufficient directory permissions"),
                 "" };
    }

    (*request->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->mutable_deployoptions())[CHDIR_PATH_CONFIG] =
        workingDirIter->second;
    YRLOG_DEBUG("change working dir to {}", workingDirIter->second);
    return { Status::OK(), workingDirIter->second };
}

Status RuntimeExecutor::HandleCondaConfig(const google::protobuf::Map<std::string, std::string> &deployOptions,
                                          const std::string &deployDir, const messages::RuntimeInstanceInfo &info) const
{
    auto it = deployOptions.find(CONDA_CONFIG);
    if (!IsEnableConda(deployOptions)) {
        return Status::OK();
    }

    // case: specified conda env but not exist
    const auto &condaPrefix = deployOptions.at(CONDA_PREFIX);
    const auto &condaEnvName = deployOptions.at(CONDA_DEFAULT_ENV);

    const std::string condaEnvPath = litebus::os::Join(litebus::os::Join(condaPrefix, "envs"), condaEnvName);
    if (!CheckIllegalChars(condaEnvPath)) {
        YRLOG_ERROR("condaEnvPath is not a valid value");
        return Status(StatusCode::RUNTIME_MANAGER_CONDA_PARAMS_INVALID, "condaEnvPath is not a valid value");
    }
    if (it == deployOptions.end() && !litebus::os::ExistPath(condaEnvPath)) {
        YRLOG_ERROR("{}|{}|specified conda env({}) not exists on node({})", info.traceid(), info.requestid(),
                    condaEnvName, config_.nodeID);
        return Status(StatusCode::RUNTIME_MANAGER_CONDA_ENV_NOT_EXIST,
                      "specified conda env " + condaEnvName + " not exists on node " + config_.nodeID);
    }

    if (it == deployOptions.end()) {
        return Status::OK();
    }

    if (!CheckIllegalChars(deployDir)) {
        YRLOG_ERROR("deployDir is not a valid value");
        return Status(StatusCode::RUNTIME_MANAGER_CONDA_PARAMS_INVALID, "deployDir is not a valid value");
    }
    const std::string condaEnvFile = litebus::os::Join(deployDir, CONDA_ENV_FILE);
    if (litebus::os::ExistPath(condaEnvFile)) {
        YRLOG_WARN("{}|{}|conda env.yaml exists, removing: {}", info.traceid(), info.requestid(), condaEnvFile);
        (void)litebus::os::Rm(condaEnvFile);
    }

    std::string outYamlStr;
    if (auto status = CondaEnvJsonToYaml(it->second, outYamlStr, condaEnvName); status.IsError()) {
        return Status(StatusCode::RUNTIME_MANAGER_CONDA_ENV_FILE_WRITE_FAILED, status.RawMessage());
    }

    if (!Write(condaEnvFile, outYamlStr)) {
        YRLOG_ERROR("{}|{}|write conda env yaml({}) failed", info.traceid(), info.requestid(), condaEnvFile);
        return Status(StatusCode::RUNTIME_MANAGER_CONDA_PARAMS_INVALID,
                      "write conda env yaml(" + condaEnvFile + ") failed");
    }

    return HandleCondaCommand(deployOptions, condaEnvFile, info);
}

Status RuntimeExecutor::HandleCondaCommand(const google::protobuf::Map<std::string, std::string> &deployOptions,
                                           const std::string &condaEnvFile,
                                           const messages::RuntimeInstanceInfo &info) const
{
    auto it = deployOptions.find(CONDA_COMMAND);
    if (it == deployOptions.end()) {
        YRLOG_ERROR("{}|{}|CONDA_COMMAND need be set", info.traceid(), info.requestid());
        return Status(StatusCode::RUNTIME_MANAGER_CONDA_ENV_FILE_WRITE_FAILED, "CONDA_COMMAND need be set");
    }
    std::string condaCommand = it->second;
    const auto index = condaCommand.find(CONDA_ENV_FILE);
    if (index != std::string::npos) {
        condaCommand = condaCommand.replace(index, CONDA_ENV_FILE.size(), condaEnvFile);
    }
    YRLOG_DEBUG("condaCommand: {}", condaCommand);
    const std::vector<std::string> condaCreateResult = cmdTool_->GetCmdResultWithError(condaCommand);

    // Verify condaCommand result
    bool isEnvCreated = std::any_of(condaCreateResult.begin(), condaCreateResult.end(), [](const std::string &line) {
        return line.find("To activate this environment") != std::string::npos;
    });
    if (isEnvCreated) {
        return Status::OK();
    }

    // Error info
    std::stringstream output;
    for (const auto &line : condaCreateResult) {
        output << line << "\n";
    }
    YRLOG_ERROR("{}|{}|conda command({}) failed on node({}). Output ({} lines):\n{}", info.traceid(), info.requestid(),
                condaCommand, config_.nodeID, condaCreateResult.size(), output.str());
    return Status(
        StatusCode::RUNTIME_MANAGER_CONDA_ENV_FILE_WRITE_FAILED,
        "conda command failed on node " + config_.nodeID + ": " + condaCommand + "\n"
        "Possible reasons:\n"
        "1. Invalid conda environment configuration\n"
        "2. Missing LibRuntime dependencies, Check user_func_std.log for details\n"
        "3. Others, please check the output of the conda command for details:\n" + output.str()
    );
}

std::pair<Status, std::vector<std::string>> RuntimeExecutor::PythonBuildFinalArgs(
    const std::string &port, const std::string &execPath, const std::string &deployDir,
    const messages::RuntimeInstanceInfo &info) const
{
    std::string jobID = PYTHON_JOB_ID_PREFIX + Utils::GetJobIDFromTraceID(info.traceid());
    std::string address = GetPosixAddress(config_, port);
    std::string pythonServerPath = PYTHON_SERVER_PATH;
    if (pkgType_ == PKG_TYPE_WHEEL) {
        pythonServerPath = PYTHON_SERVER_PATH_IN_WHEEL;
    }

    return { Status::OK(),
             { execPath, "-u", config_.runtimePath + pythonServerPath, "--rt_server_address", address, "--deploy_dir",
               deployDir, "--runtime_id", info.runtimeid(), "--job_id", jobID, "--log_level",
               config_.runtimeLogLevel } };
}

std::pair<Status, std::vector<std::string>> RuntimeExecutor::GetPythonBuildArgs(
    const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const
{
    const auto &info = request->runtimeinstanceinfo();
    const auto &deployOptions = info.deploymentconfig().deployoptions();

    auto [execPathStatus, execPath] = GetPythonExecPath(deployOptions, info);
    if (execPathStatus.IsError()) {
        return { execPathStatus, {} };
    }

    auto [workDirStatus, deployDir] = HandleWorkingDirectory(request, info);
    if (workDirStatus.IsError()) {
        return { workDirStatus, {} };
    }

    if (deployDir.empty()) {
        YRLOG_ERROR("{}|{}|python deploy dir is empty, cannot set build args", info.traceid(), info.requestid());
        return { Status(StatusCode::RUNTIME_MANAGER_DEPLOY_DIR_IS_EMPTY, "deploy dir is empty"), {} };
    }

    YRLOG_DEBUG("{}|{}|python deploy dir: {}", info.traceid(), info.requestid(), deployDir);
    if (!litebus::os::ExistPath(deployDir)) {
        if (!litebus::os::Mkdir(deployDir).IsNone()) {
            YRLOG_WARN("{}|{}|failed to make dir deployDir({}), msg: {}", request->runtimeinstanceinfo().traceid(),
                       request->runtimeinstanceinfo().requestid(), deployDir, litebus::os::Strerror(errno));
            return { Status(StatusCode::RUNTIME_MANAGER_CONDA_PARAMS_INVALID, "failed to make dir deployDir"), {} };
        }
    }

    if (auto status = HandleCondaConfig(deployOptions, deployDir, info); status.IsError()) {
        return { status, {} };
    }

    return PythonBuildFinalArgs(port, execPath, deployDir, info);
}

std::pair<Status, std::vector<std::string>> RuntimeExecutor::GetNodejsBuildArgs(
    const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const
{
    std::string memorySize = "";
    std::string address = GetPosixAddress(config_, port);
    auto resources = request->runtimeinstanceinfo().runtimeconfig().resources().resources();
    for (auto resource : resources) {
        if (resource.first == resource_view::MEMORY_RESOURCE_NAME && resource.second.mutable_scalar()->value() > 0) {
            if (resource.second.mutable_scalar()->value() >= std::numeric_limits<int>::max()) {
                YRLOG_DEBUG("{} scalar exceeds max int value", resource_view::MEMORY_RESOURCE_NAME);
                continue;
            }
            memorySize = "--max-old-space-size=" + std::to_string(int(resource.second.mutable_scalar()->value()));
            break;
        }
    }

    if (memorySize != "") {
        return { Status::OK(),
                 { memorySize, "/home/snuser/runtime/nodejs/wrapper.js", "--rt_server_address=" + address,
                   "--runtime_id=" + request->runtimeinstanceinfo().runtimeid(),
                   "--job_id=" + Utils::GetJobIDFromTraceID(request->runtimeinstanceinfo().traceid()),
                   "--log_level=" + config_.runtimeLogLevel } };
    }
    return { Status::OK(),
             { "/home/snuser/runtime/nodejs/wrapper.js", "--rt_server_address=" + address,
               "--runtime_id=" + request->runtimeinstanceinfo().runtimeid(),
               "--job_id=" + Utils::GetJobIDFromTraceID(request->runtimeinstanceinfo().traceid()),
               "--log_level=" + config_.runtimeLogLevel } };
}

std::pair<Status, std::vector<std::string>> RuntimeExecutor::WrapMassifBuildArgs(
    const std::string &languageExecPath, const std::vector<std::string> &languageBuildArgs) const
{
    std::string massifOutFile = config_.runtimeLogPath + "/" + "massif-%p.out";
    std::string massifToolName = "massif";
    std::string timeUnit = "B";
    std::string threadsNum = "10000";
    std::string detailedFreq = "1";
    std::vector<std::string> wrapMassifBuildArgs{ VALGRIND_PROGRAM_NAME,
                                                  VALGRIND_TOOL_PREFIX + massifToolName,
                                                  MASSIF_TIME_UNIT_PREFIX + timeUnit,
                                                  MASSIF_MAX_THREADS_PREFIX + threadsNum,
                                                  MASSIF_OUT_FILE_PREFIX + massifOutFile,
                                                  MASSIF_DETAILED_FREQ + detailedFreq };
    size_t languageProgramNameIndex = 0;
    if (languageBuildArgs.size() > languageProgramNameIndex
        && (languageBuildArgs[languageProgramNameIndex] == CPP_PROGRAM_NAME
            || languageBuildArgs[languageProgramNameIndex] == GO_PROGRAM_NAME)) {
        wrapMassifBuildArgs.push_back(languageExecPath);
        wrapMassifBuildArgs.insert(wrapMassifBuildArgs.end(), languageBuildArgs.begin() + 1, languageBuildArgs.end());
    }
    return { Status::OK(), wrapMassifBuildArgs };
}

std::shared_ptr<litebus::Exec> RuntimeExecutor::CreateMassifWrapExec(
    const std::string &runtimeID, const std::string &languageExecPath,
    const std::vector<std::string> &languageBuildArgs, const std::map<std::string, std::string> &combineEnvs,
    const std::vector<std::function<void()>> childInitHook) const
{
    litebus::ExecIO stdOut = litebus::ExecIO::CreatePipeIO();
    auto [status, wrapMassifArgs] = WrapMassifBuildArgs(languageExecPath, languageBuildArgs);
    std::ignore = status;
    auto path = LookPath(VALGRIND_PROGRAM_NAME);
    if (path.IsNone()) {
        YRLOG_ERROR("Get valgrind ExecPath failed, path is null");
        return nullptr;
    }
    auto valgrindExecPath = path.Get();
    YRLOG_INFO("Get valgrind ExecPath, execPath: {}", valgrindExecPath);

    std::string cmd = valgrindExecPath;
    for (const auto &arg : wrapMassifArgs) {
        cmd += " ";
        cmd += arg;
    }
    if (!CheckIllegalChars(cmd)) {
        YRLOG_ERROR("final cmd: {} is invalid", cmd);
        return nullptr;
    }
    YRLOG_INFO("start valgrind wrap runtime({}), execute final cmd: {}", runtimeID, cmd);
    return litebus::Exec::CreateExec(valgrindExecPath, wrapMassifArgs, combineEnvs, litebus::ExecIO::CreatePipeIO(),
                                     stdOut, stdOut, childInitHook, {}, false);
}

std::pair<Status, std::vector<std::string>> RuntimeExecutor::GetJavaBuildArgsDefault(
    const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const
{
    return GetJavaBuildArgs(port, config_.jvmArgs, request);
}

std::pair<Status, std::vector<std::string>> RuntimeExecutor::GetJavaBuildArgsForJava11(
    const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const
{
    return GetJavaBuildArgs(port, config_.jvmArgsForJava11, request);
}

std::pair<Status, std::vector<std::string>> RuntimeExecutor::GetJavaBuildArgsForJava17(
    const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const
{
    return GetJavaBuildArgs(port, config_.jvmArgsForJava17, request);
}

std::pair<Status, std::vector<std::string>> RuntimeExecutor::GetJavaBuildArgsForJava21(
    const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const
{
    return GetJavaBuildArgs(port, config_.jvmArgsForJava21, request);
}

std::pair<Status, std::vector<std::string>> RuntimeExecutor::GetJavaBuildArgs(
    const std::string &port, const std::vector<std::string> &jvmArgs,
    const std::shared_ptr<messages::StartInstanceRequest> &request) const
{
    YRLOG_DEBUG("{}|{}|GetJavaBuildArgs start", request->runtimeinstanceinfo().traceid(),
                request->runtimeinstanceinfo().requestid());
    std::string deployDir = request->runtimeinstanceinfo().deploymentconfig().deploydir();
    std::string jarPath = deployDir;
    if (request->scheduleoption().schedpolicyname() != MONOPOLY_SCHEDULE) {
        std::string bucketID = request->runtimeinstanceinfo().deploymentconfig().bucketid();
        std::string objectID = request->runtimeinstanceinfo().deploymentconfig().objectid();
        jarPath =
            deployDir + "/" + RUNTIME_LAYER_DIR_NAME + "/" + RUNTIME_FUNC_DIR_NAME + "/" + bucketID + "/" + objectID;
    }
    std::string javaClassPath = config_.runtimePath + YR_JAVA_RUNTIME_PATH + ":" + jarPath;
    std::string address = GetPosixAddress(config_, port);
    std::vector<std::string> args = jvmArgs;
    auto resources = request->runtimeinstanceinfo().runtimeconfig().resources().resources();
    for (auto resource : resources) {
        if (resource.first == resource_view::MEMORY_RESOURCE_NAME) {
            auto memVal = resource.second.mutable_scalar()->value();
            memVal = memVal > config_.maxJvmMemory ? config_.maxJvmMemory : memVal;
            if (memVal > 0) {
                // use memory value(defined in metadata or scheduling options) to set java heap memory: Xmx
                std::string memStr = std::to_string(int(memVal));
                (void)args.emplace_back("-Xmx" + memStr + "m");
            }
            break;
        }
    }
    std::string jobID = Utils::GetJobIDFromTraceID(request->runtimeinstanceinfo().traceid());
    (void)args.emplace_back("-cp");
    (void)args.emplace_back(javaClassPath);
    (void)args.emplace_back(JAVA_LOG_LEVEL + config_.runtimeLogLevel);
    if (auto path = request->runtimeinstanceinfo().runtimeconfig().posixenvs().find(ENV_DELEGATE_DOWNLOAD);
        path != request->runtimeinstanceinfo().runtimeconfig().posixenvs().end()) {
        (void)args.emplace_back(JAVA_SYSTEM_PROPERTY_FILE + config_.javaSystemProperty + ',' + path->second
                                + "/config/log4j2.xml");
        YRLOG_INFO("append java system property file: {}/config/log4j2.xml", path->second);
    } else {
        (void)args.emplace_back(JAVA_SYSTEM_PROPERTY_FILE + config_.javaSystemProperty);
    }
    (void)args.emplace_back(JAVA_SYSTEM_LIBRARY_PATH + config_.javaSystemLibraryPath);
    (void)args.emplace_back("-XX:ErrorFile=" + config_.runtimeLogPath + "/exception/BackTrace_"
                            + request->runtimeinstanceinfo().runtimeid() + ".log");
    (void)args.emplace_back(JAVA_JOB_ID + jobID);
    (void)args.emplace_back(JAVA_MAIN_CLASS);
    (void)args.emplace_back(address);
    (void)args.emplace_back(request->runtimeinstanceinfo().runtimeid());
    return { Status::OK(), args };
}

std::pair<Status, std::vector<std::string>> RuntimeExecutor::GetGoBuildArgs(
    const std::string &port, const std::shared_ptr<messages::StartInstanceRequest> &request) const
{
    YRLOG_DEBUG("{}|{}|GetGoBuildArgs start, instance({}), runtime({})", request->runtimeinstanceinfo().traceid(),
                request->runtimeinstanceinfo().requestid(), request->runtimeinstanceinfo().instanceid(),
                request->runtimeinstanceinfo().runtimeid());
    std::string address = GetPosixAddress(config_, port);
    return { Status::OK(),
             { GO_PROGRAM_NAME, RUNTIME_ID_ARG_PREFIX + request->runtimeinstanceinfo().runtimeid(),
               INSTANCE_ID_ARG_PREFIX + request->runtimeinstanceinfo().instanceid(),
               LOG_LEVEL_PREFIX + config_.runtimeLogLevel, GRPC_ADDRESS_PREFIX + address } };
}

std::pair<Status, std::vector<std::string>> RuntimeExecutor::GetPosixCustomBuildArgs(
    const std::string &, const std::shared_ptr<messages::StartInstanceRequest> &request) const
{
    YRLOG_DEBUG("{}|{}|GetPosixCustomBuildArgs start", request->runtimeinstanceinfo().traceid(),
                request->runtimeinstanceinfo().requestid());

    // entry script case
    if (request->runtimeinstanceinfo().runtimeconfig().posixenvs().find(ENV_DELEGATE_BOOTSTRAP)
            != request->runtimeinstanceinfo().runtimeconfig().posixenvs().end()
        && request->runtimeinstanceinfo().runtimeconfig().posixenvs().find(ENV_DELEGATE_DOWNLOAD)
               != request->runtimeinstanceinfo().runtimeconfig().posixenvs().end()) {
        YRLOG_DEBUG("posix custom runtime will use user define entry file");
        return { Status::OK(), {} };
    }

    // job working dir case
    auto iter = request->runtimeinstanceinfo().runtimeconfig().posixenvs().find(UNZIPPED_WORKING_DIR);
    auto fileIter = request->runtimeinstanceinfo().runtimeconfig().posixenvs().find(YR_WORKING_DIR);
    if (auto endIter = request->runtimeinstanceinfo().runtimeconfig().posixenvs().end();
        iter != endIter && fileIter != endIter) {
        YRLOG_DEBUG("posix custom runtime will use user defined job entrypoint");
        if (iter->second.empty() || fileIter->second.empty()) {
            YRLOG_ERROR("{}|{}|params working dir({}) or unzipped dir({}) is empty",
                        request->runtimeinstanceinfo().traceid(), request->runtimeinstanceinfo().requestid(),
                        fileIter->second, iter->second);
            return { Status(StatusCode::RUNTIME_MANAGER_WORKING_DIR_FOR_APP_NOTFOUND,
                            "params working dir or unzipped dir is empty"),
                     {} };
        }

        if (!litebus::os::ExistPath(iter->second)) {
            YRLOG_ERROR("{}|{}|enter working dir failed, path: {}", request->runtimeinstanceinfo().traceid(),
                        request->runtimeinstanceinfo().requestid(), iter->second);
            return { Status(StatusCode::RUNTIME_MANAGER_WORKING_DIR_FOR_APP_NOTFOUND, "job working dir is invalid"),
                     {} };
        }

        (*request->mutable_runtimeinstanceinfo()
              ->mutable_deploymentconfig()
              ->mutable_deployoptions())[CHDIR_PATH_CONFIG] = iter->second;
        YRLOG_DEBUG("change job entrypoint execute dir to {}", iter->second);
        return { Status::OK(), {} };
    }

    std::string entryFile = request->runtimeinstanceinfo().runtimeconfig().entryfile();
    if (entryFile.empty()) {
        YRLOG_ERROR("{}|{}|entryFile is empty", request->runtimeinstanceinfo().traceid(),
                    request->runtimeinstanceinfo().requestid());
        return { Status(StatusCode::RUNTIME_MANAGER_EXECUTABLE_PATH_INVALID, "entryFile is empty"), {} };
    }
    YRLOG_DEBUG("entrypoint: {}", entryFile);
    return { Status::OK(), { entryFile } };
}

std::vector<std::function<void()>> RuntimeExecutor::BuildInitHook(
    const std::shared_ptr<messages::StartInstanceRequest> &request) const
{
    std::vector<std::function<void()>> initHook{ litebus::ChildInitHook::EXITWITHPARENT() };
    (void)initHook.emplace_back(SetSubProcessPgid());
    if (config_.setCmdCred) {
        auto funcMountUser = request->runtimeinstanceinfo().runtimeconfig().funcmountconfig().funcmountuser();
        HookRuntimeCredentialByID(initHook, funcMountUser.userid(), funcMountUser.groupid());
    }
    auto deployOptions = request->runtimeinstanceinfo().deploymentconfig().deployoptions();
    if (IsEnableConda(deployOptions)) {
        auto it = deployOptions.find(CONDA_PREFIX);
        auto it2 = deployOptions.find(CONDA_DEFAULT_ENV);
        if (it != deployOptions.end() && it2 != deployOptions.end()) {
            YRLOG_DEBUG("process add conda activate hook");
            (void)initHook.emplace_back(CondaActivate(it->second, it2->second));
        }
    }

    if (auto iter = deployOptions.find(CHDIR_PATH_CONFIG); iter != deployOptions.end() && !iter->second.empty()) {
        YRLOG_DEBUG("{}|{} process add chdir({}) hook",
                    request->runtimeinstanceinfo().requestid(),
                    request->runtimeinstanceinfo().instanceid(),
                    iter->second);
        (void)initHook.emplace_back(ChdirHook(iter->second));
    }
    AddNUMABindingHook(initHook, request);
    return initHook;
}

std::vector<std::function<void()>> RuntimeExecutor::BuildInitHookForPrestart() const
{
    std::vector<std::function<void()>> initHook{ litebus::ChildInitHook::EXITWITHPARENT() };
    (void)initHook.emplace_back(SetSubProcessPgid());
    if (config_.setCmdCred) {
        HookRuntimeCredentialByID(initHook, DEFAULT_USER_ID, DEFAULT_GROUP_ID);
    }
    return initHook;
}

void RuntimeExecutor::HookRuntimeCredentialByID(std::vector<std::function<void()>> &initHook, int userID,
                                                int groupID) const
{
    if (userID == 0 || userID == MIN_VALID_ID) {
        userID = config_.runtimeUID;
    }
    if (groupID == 0 || groupID == MIN_VALID_ID) {
        groupID = config_.runtimeGID;
    }
    YRLOG_INFO("HookRuntimeCredential with userID: {}, groupID: {}", userID, groupID);
    (void)initHook.emplace_back(SetRuntimeIdentity(userID, groupID));
}


StatusCode RuntimeExecutor::CheckRuntimeCredential(const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    auto funcMountUser = request->runtimeinstanceinfo().runtimeconfig().funcmountconfig().funcmountuser();
    int userID = funcMountUser.userid();
    int groupID = funcMountUser.groupid();
    if (userID < MIN_VALID_ID || groupID < MIN_VALID_ID) {
        YRLOG_ERROR("{}|{}|cannot set ID smaller than -1. userID({}), groupID({}), instance({}), runtime({})",
                    request->runtimeinstanceinfo().traceid(), request->runtimeinstanceinfo().requestid(), userID,
                    groupID, request->runtimeinstanceinfo().instanceid(), request->runtimeinstanceinfo().runtimeid());
        return StatusCode::PARAMETER_ERROR;
    }
    if (userID == INITIAL_USER_ID || userID == AGENT_ID || userID > MAX_USER_ID) {
        YRLOG_ERROR("{}|{}|userID value: {} is invalid, instance({}), runtime({}))",
                    request->runtimeinstanceinfo().traceid(), request->runtimeinstanceinfo().requestid(), userID,
                    request->runtimeinstanceinfo().instanceid(), request->runtimeinstanceinfo().runtimeid());
        return StatusCode::PARAMETER_ERROR;
    }
    if (groupID == INITIAL_GROUP_ID || groupID == AGENT_ID || groupID > MAX_GROUP_ID) {
        YRLOG_ERROR("{}|{}|groupID value: {} is invalid, instance({}), runtime({}))",
                    request->runtimeinstanceinfo().traceid(), request->runtimeinstanceinfo().requestid(), groupID,
                    request->runtimeinstanceinfo().instanceid(), request->runtimeinstanceinfo().runtimeid());
        return StatusCode::PARAMETER_ERROR;
    }
    return StatusCode::SUCCESS;
}

messages::StartInstanceResponse RuntimeExecutor::GenSuccessStartInstanceResponse(
    const std::shared_ptr<messages::StartInstanceRequest> &request, const std::shared_ptr<litebus::Exec> &execPtr,
    const std::string &port)
{
    messages::StartInstanceResponse response;
    response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    response.set_message("start instance success");
    response.set_requestid(request->runtimeinstanceinfo().requestid());

    auto instanceResponse = response.mutable_startruntimeinstanceresponse();
    instanceResponse->set_runtimeid(request->runtimeinstanceinfo().runtimeid());
    instanceResponse->set_address(GetPosixAddress(config_, port));
    YRLOG_DEBUG("{}|{}|instance address: ip: {}, port: {}", request->runtimeinstanceinfo().traceid(),
                request->runtimeinstanceinfo().requestid(), config_.ip, port);
    instanceResponse->set_port(port);
    instanceResponse->set_pid(execPtr->GetPid());
    return response;
}

std::vector<std::string> RuntimeExecutor::GetBuildArgsForPrestart(const std::string &runtimeID,
                                                                  const std::string &language, const std::string &port)
{
    if (chdir(config_.runtimePath.c_str()) != 0) {
        YRLOG_WARN("enter runtimePath failed, path: {}", config_.runtimePath);
    }
    std::string langArg = GetLanguageArg(language);
    if (buildArgsFuncPrestart_.find(langArg) == buildArgsFuncPrestart_.end()) {
        YRLOG_ERROR("RuntimeExecutor does not support this language: {}", langArg);
        return std::vector<std::string>();
    }
    YRLOG_DEBUG("find buildArgsFunc for lang: {}", language);
    auto langBuild = buildArgsFuncPrestart_[langArg];
    getBuildArgsPrestart_ =
        std::bind(langBuild, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
    return getBuildArgsPrestart_(runtimeID, port, langArg);
}

std::vector<std::string> RuntimeExecutor::GetCppBuildArgsForPrestart(const std::string &runtimeID,
                                                                     const std::string &port,
                                                                     const std::string &language) const
{
    YRLOG_DEBUG("GetCppBuildArgs start {}", language);
    std::string address = GetPosixAddress(config_, port);
    auto confPath = litebus::os::Join(config_.runtimeConfigPath, "runtime.json");
    return { CPP_PROGRAM_NAME, RUNTIME_ID_ARG_PREFIX + runtimeID, LOG_LEVEL_PREFIX + config_.runtimeLogLevel,
             GRPC_ADDRESS_PREFIX + address, CONFIG_PATH_PREFIX + confPath };
}

std::vector<std::string> RuntimeExecutor::GetPythonBuildArgsForPrestart(const std::string &runtimeID,
                                                                        const std::string &port,
                                                                        const std::string &language) const
{
    YRLOG_DEBUG("GetPythonBuildArgs start {}", language);
    std::string execPath = GetExecPath(language);
    std::string address = GetPosixAddress(config_, port);
    std::string pythonServerPath = PYTHON_SERVER_PATH;
    if (pkgType_ == PKG_TYPE_WHEEL) {
        pythonServerPath = PYTHON_SERVER_PATH_IN_WHEEL;
    }
    return { execPath,
             "-u",
             config_.runtimePath + pythonServerPath,
             "--rt_server_address",
             address,
             "--deploy_dir",
             PYTHON_PRESTART_DEPLOY_DIR,
             "--runtime_id",
             runtimeID,
             "--log_level",
             config_.runtimeLogLevel };
}

std::vector<std::string> RuntimeExecutor::GetJavaBuildArgsForPrestart(const std::string &runtimeID,
                                                                      const std::string &port,
                                                                      const std::string &language) const
{
    YRLOG_DEBUG("GetJavaBuildArgs start {}", language);
    std::string javaClassPath = config_.runtimePath + YR_JAVA_RUNTIME_PATH;
    std::string address = GetPosixAddress(config_, port);
    std::vector<std::string> args;
    if (language == JAVA11_LANGUAGE) {
        args = config_.jvmArgsForJava11;
    } else {
        args = config_.jvmArgs;
    }
    (void)args.emplace_back("-cp");
    (void)args.emplace_back(javaClassPath);
    (void)args.emplace_back(JAVA_LOG_LEVEL + config_.runtimeLogLevel);
    (void)args.emplace_back(JAVA_SYSTEM_PROPERTY_FILE + config_.javaSystemProperty);
    (void)args.emplace_back(JAVA_SYSTEM_LIBRARY_PATH + config_.javaSystemLibraryPath);
    (void)args.emplace_back(JAVA_MAIN_CLASS);
    (void)args.emplace_back(address);
    (void)args.emplace_back(runtimeID);
    return args;
}

void RuntimeExecutor::InitPrestartRuntimePool()
{
    for (const auto &prestartConfig : config_.runtimePrestartConfigs) {
        if (prestartConfig.second > 0) {
            StartPrestartRuntimeByLanguage(prestartConfig.first, prestartConfig.second);
        }
    }
}

void RuntimeExecutor::InitVirtualEnvIdleTimeLimit()
{
    virtualEnvMgr_ = std::make_shared<VirtualEnvManager>(config_.virtualEnvIdleTimeLimit);
}

void RuntimeExecutor::StartPrestartRuntimeByLanguage(const std::string &language, const int startCount)
{
    std::string execPath = GetExecPath(language);
    YRLOG_INFO("ready to prestart runtime for {}, startCount is {}, execPath: {}", language, startCount, execPath);
    if (execPath.empty()) {
        YRLOG_ERROR("execPath is not found, prestart runtime failed for {}", language);
        return;
    }
    for (int i = 0; i < startCount; i++) {
        std::string runtimeID = RUNTIME_UUID_PREFIX + litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        if (!StartPrestartRuntimeByRuntimeID(runtimeID, language, execPath, 0)) {
            YRLOG_ERROR("stop to prestart runtime for {}", language);
            break;
        }
    }
}

bool RuntimeExecutor::StartPrestartRuntimeByRuntimeID(const std::string &runtimeID, const std::string &language,
                                                      const std::string &execPath, const int retryTimes)
{
    if (!CheckPrestartRuntimeRetry(runtimeID, language, retryTimes)) {
        return true;
    }
    YRLOG_INFO("start to prestart runtime, runtimeID: {} retryTimes: {}", runtimeID, retryTimes);
    std::string port = PortManager::GetInstance().RequestPort(runtimeID);
    if (port.empty()) {
        YRLOG_ERROR("port resource is not available, can not start instance, runtimeID: {}", runtimeID);
        return true;
    }
    auto args = GetBuildArgsForPrestart(runtimeID, language, port);
    if (args.empty()) {
        YRLOG_ERROR("get build args failed, can not start runtime, runtimeID: {}", runtimeID);
        return false;
    }
    Envs envs = {
        .posixEnvs = {
            {IS_PRESTART, PRESTART_FLAG}, {RUNTIME_DIR, config_.runtimePath},
            {ENABLE_DIS_CONV_CALL_STACK, config_.enableDisConvCallStack ? "true" : "false"}
        },
        .customResourceEnvs = {},
        .userEnvs = {}
    };
    std::string tlsJson = "{}\n";
    std::shared_ptr<litebus::Exec> execPtr;
    for (int j = 0; j < RETRY_TIMES; j++) {
        execPtr = StartRuntimeByRuntimeID(
            { { PARAM_EXEC_PATH, execPath }, { PARAM_RUNTIME_ID, runtimeID }, { PARAM_LANGUAGE, language } }, args,
            envs, BuildInitHookForPrestart());
        if (execPtr != nullptr) {
            if (write(execPtr->GetIn().Get(), tlsJson.c_str(), tlsJson.size()) == -1) {
                YRLOG_ERROR("write tls config failed!, runtimeID: {}, errno: {}", runtimeID, errno);
                continue;
            }
            break;
        }
    }
    if (execPtr == nullptr) {
        YRLOG_ERROR("failed to create exec, runtimeID: {}", runtimeID);
        return false;
    }
    YRLOG_INFO("prestart instance success runtimeID: {} PID: {} IP: {} Port: {}", runtimeID, execPtr->GetPid(),
               config_.ip, port);
    PrestartProcess prestartProcess = { .port = port, .runtimeID = "", .execPtr = nullptr };
    prestartProcess.execPtr = execPtr;
    prestartProcess.runtimeID = runtimeID;
    prestartRuntimePool_[language].push_back(prestartProcess);
    prestartRuntimeIDs_.insert(runtimeID);
    WaitPrestartRuntimeExit(runtimeID, language, execPath, retryTimes, execPtr);
    return true;
}

void RuntimeExecutor::WaitPrestartRuntimeExit(const std::string &runtimeID, const std::string &language,
                                              const std::string &execPath, const int retryTimes,
                                              const std::shared_ptr<litebus::Exec> &execPtr)
{
    auto promise = std::make_shared<litebus::Promise<bool>>();
    (void)prestartRuntimePromiseMap_.emplace(execPtr->GetPid(), promise);
    (void)promise->GetFuture().OnComplete(
        [from(GetAID()), runtimeID, language, execPath, retryTimes](const litebus::Future<bool> &status) {
            if (status.IsOK()) {
                litebus::Async(from, &RuntimeExecutor::StartPrestartRuntimeByRuntimeID, runtimeID, language, execPath,
                               retryTimes + 1);
            }
        });
}

bool RuntimeExecutor::CheckPrestartRuntimeRetry(const std::string &runtimeID, const std::string &language,
                                                const int retryTimes)
{
    if (retryTimes <= 0) {
        return true;
    }
    (void)PortManager::GetInstance().ReleasePort(runtimeID);
    if (retryTimes >= DEFAULT_RETRY_RESTART_CACHE_RUNTIME) {
        YRLOG_WARN("prestart runtime have reached max retry times: {}, runtimeID: {}", retryTimes, runtimeID);
        return false;
    }
    return prestartRuntimeIDs_.find(runtimeID) != prestartRuntimeIDs_.end();
}

PrestartProcess RuntimeExecutor::GetRuntimeFromPool(const std::string &language, const std::string &schedulePolicy)
{
    PrestartProcess prestartProcess;
    if (prestartRuntimePool_.find(language) == prestartRuntimePool_.end()) {
        if (schedulePolicy == MONOPOLY && prestartRuntimePool_.size() > 0) {
            litebus::Async(GetAID(), &RuntimeExecutor::KillOtherPrestartRuntimeProcess);
        }
        return prestartProcess;
    }
    while (prestartRuntimePool_[language].size() > 0) {
        auto front = prestartRuntimePool_[language].front();
        auto execPtr = front.execPtr;
        if (execPtr == nullptr) {
            YRLOG_WARN("get runtime from pool execPtr is null, runtimeID: {}", front.runtimeID);
            prestartRuntimePool_[language].pop_front();
            prestartRuntimeIDs_.erase(front.runtimeID);
            continue;
        }
        auto processPromise = prestartRuntimePromiseMap_.find(execPtr->GetPid());
        if (processPromise == prestartRuntimePromiseMap_.end() || processPromise->second->GetFuture().IsError()
            || processPromise->second->GetFuture().IsOK()) {
            YRLOG_WARN("failed to get runtime from pool runtime maybe exit, runtimeID: {}", front.runtimeID);
            prestartRuntimePool_[language].pop_front();
            prestartRuntimeIDs_.erase(front.runtimeID);
            prestartRuntimePromiseMap_.erase(execPtr->GetPid());
            continue;
        }
        prestartRuntimePool_[language].pop_front();
        prestartRuntimeIDs_.erase(front.runtimeID);
        prestartRuntimePromiseMap_.erase(execPtr->GetPid());
        if (schedulePolicy != MONOPOLY) {
            litebus::Async(GetAID(), &RuntimeExecutor::StartPrestartRuntimeByLanguage, language, 1);
        } else {
            litebus::Async(GetAID(), &RuntimeExecutor::KillOtherPrestartRuntimeProcess);
        }
        return front;
    }
    return prestartProcess;
}

void RuntimeExecutor::KillOtherPrestartRuntimeProcess()
{
    for (auto item : prestartRuntimePool_) {
        while (item.second.size() > 0) {
            auto runtime = item.second.front();
            item.second.pop_front();
            prestartRuntimeIDs_.erase(runtime.runtimeID);
            auto execPtr = runtime.execPtr;
            if (execPtr == nullptr) {
                continue;
            }
            auto processPromise = prestartRuntimePromiseMap_.find(execPtr->GetPid());
            if (processPromise == prestartRuntimePromiseMap_.end() || processPromise->second->GetFuture().IsError()
                || processPromise->second->GetFuture().IsOK()) {
                prestartRuntimePromiseMap_.erase(execPtr->GetPid());
                continue;
            }
            prestartRuntimePromiseMap_.erase(execPtr->GetPid());
            YRLOG_INFO("kill other runtime runtimeID: {}, pid: {}", runtime.runtimeID, execPtr->GetPid());
            KillProcess(execPtr->GetPid());
        }
    }
}

void RuntimeExecutor::UpdatePrestartRuntimePromise(pid_t pid)
{
    auto processPromise = prestartRuntimePromiseMap_.find(pid);
    if (processPromise != prestartRuntimePromiseMap_.end()) {
        processPromise->second->SetValue(true);
    }
}

litebus::Future<messages::UpdateCredResponse> RuntimeExecutor::UpdateCredForRuntime(
    const std::shared_ptr<messages::UpdateCredRequest> &request)
{
    auto requestID = request->requestid();
    auto runtimeID = request->runtimeid();
    auto token = request->token();
    auto salt = request->salt();

    messages::UpdateCredResponse response;
    response.set_requestid(requestID);
    auto execPtr = GetExecByRuntimeID(runtimeID);
    if (execPtr == nullptr) {
        YRLOG_WARN("{}|{}|runtime has already been killed.", requestID, runtimeID);
        response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        return response;
    }
    ::messages::TLSConfig tlsConfig;
    auto infoIter = runtimeInstanceInfoMap_.find(runtimeID);
    if (infoIter != runtimeInstanceInfoMap_.end()) {
        tlsConfig = infoIter->second.runtimeconfig().tlsconfig();
    }
    tlsConfig.set_salt(request->salt());
    tlsConfig.set_token(request->token());
    tlsConfig.mutable_tenantcredentials()->CopyFrom(request->tenantcredentials());
    auto fillMetricsTLSStatus = FillMetricsTLSConfig(tlsConfig);
    if (fillMetricsTLSStatus.IsError()) {
        YRLOG_WARN("failed to fill metrics tls config, error: {}", fillMetricsTLSStatus.ToString());
    }
    Status result = config_.isProtoMsgToRuntime ? WriteProtoToRuntime(requestID, runtimeID, tlsConfig, execPtr)
                                                : WriteJsonToRuntime(requestID, runtimeID, tlsConfig, execPtr);
    if (result.IsError()) {
        response.set_code(static_cast<int32_t>(result.StatusCode()));
        response.set_message(result.ToString());
        return response;
    }
    response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    return response;
}

litebus::Future<::messages::StartInstanceResponse> RuntimeExecutorProxy::StartInstance(
    const std::shared_ptr<messages::StartInstanceRequest> &request, const std::vector<int> &cardIDs)
{
    return litebus::Async(executor_->GetAID(), &RuntimeExecutor::StartInstance, request, cardIDs);
}

litebus::Future<Status> RuntimeExecutorProxy::StopInstance(
    const std::shared_ptr<messages::StopInstanceRequest> &request, bool oomKilled)
{
    return litebus::Async(executor_->GetAID(), &RuntimeExecutor::StopInstance, request, oomKilled);
}

litebus::Future<messages::SnapshotRuntimeResponse> RuntimeExecutorProxy::SnapshotRuntime(
    const std::shared_ptr<messages::SnapshotRuntimeRequest> &request)
{
    return litebus::Async(executor_->GetAID(), &RuntimeExecutor::SnapshotRuntime, request);
}

litebus::Future<std::map<std::string, messages::RuntimeInstanceInfo>> RuntimeExecutorProxy::GetRuntimeInstanceInfos()
{
    return litebus::Async(executor_->GetAID(), &RuntimeExecutor::GetRuntimeInstanceInfos);
}

void RuntimeExecutorProxy::UpdatePrestartRuntimePromise(pid_t pid)
{
    return litebus::Async(executor_->GetAID(), &RuntimeExecutor::UpdatePrestartRuntimePromise, pid);
}

}  // namespace functionsystem::runtime_manager
