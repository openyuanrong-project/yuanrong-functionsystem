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

#include "logmanager_actor.h"
#include "common/logs/logging.h"
#include "common/proto/pb/posix/message.pb.h"
#include "common/utils/files.h"
#include "manager/runtime_manager.h"
#include "async/asyncafter.hpp"
#include "async/defer.hpp"
#include "async/collect.hpp"

#include <ctime>
#include <cstring>
#include <regex>
#include <sstream>

namespace functionsystem::runtime_manager {

namespace {
const uint32_t MILLISECONDS_PRE_SECOND = 1000;
const int EXCEPTION_DIR_LEN = 9;
const std::string RUNTIME_UUID_PREFIX = "runtime-";
const std::string RUNTIME_LOG_REGEX_PATTERN =
    "(" + RUNTIME_UUID_PREFIX + "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}(-[0-9a-f]{12})?)";
const std::regex JAVA_RUNTIME_LOG_REGEX_PATTERN_REGEX(RUNTIME_LOG_REGEX_PATTERN);
const std::string PYTHON_RUNTIME_LOG_REGEX_PATTERN =
    "^" + RUNTIME_LOG_REGEX_PATTERN + "(-\\d{14})?(\\.\\d*)?\\.log(\\.gz)?(\\.\\d+)?$";
const std::regex PYTHON_RUNTIME_LOG_REGEX_PATTERN_REGEX(PYTHON_RUNTIME_LOG_REGEX_PATTERN);
// The logs of cpp runtime exist in multiple formats, contains soft link
const std::string CPP_RUNTIME_LOG_REGEX_PATTERN = "^\\S+" + RUNTIME_LOG_REGEX_PATTERN + "\\S*(\\.log)?(\\.gz)?$";
const std::regex CPP_RUNTIME_LOG_REGEX_PATTERN_REGEX(CPP_RUNTIME_LOG_REGEX_PATTERN);
const std::string LIB_RUNTIME_LOG_REGEX_PATTERN =
    "^job-[0-9a-f]{8}-" + RUNTIME_LOG_REGEX_PATTERN + "(-\\d{14})?(\\.\\d*)?\\.log(\\.gz)?$";
const std::regex LIB_RUNTIME_LOG_REGEX_PATTERN_REGEX(LIB_RUNTIME_LOG_REGEX_PATTERN);
const std::string STD_OUTPUT_LOG_REGEX_PATTERN = "^" + RUNTIME_LOG_REGEX_PATTERN + ".*\\.(out|err)$";
const std::regex STD_OUTPUT_LOG_REGEX_PATTERN_REGEX(STD_OUTPUT_LOG_REGEX_PATTERN);
const std::string DS_CLIENT_LOG_REGEX_PATTERN =
    R"(ds_client_(?:access_)?(\d+)(?:\.(?:INFO|DEBUG|ERROR|WARNING))?(?:\.log)?)";
const std::regex DS_CLIENT_LOG_REGEX_PATTERN_REGEX(DS_CLIENT_LOG_REGEX_PATTERN);

// for reuse log
const std::regex REUSE_LOG_PREFIX_PATTERN_REGEX(R"(^((YR_\d+_\d{6})).*\.log(\.gz)?(\.\d+)?$)");
const std::regex REUSE_LOG_PREFIX_WITH_GZ_REGEX(R"(^((YR_\d+_\d{6})).*\.log\.gz(\.\d+)?$)");
const int MAX_COUNTER = 999999;
const int MAX_COUNTER_DIGIT = 6;
const size_t MAX_DELETE_FILE_NUM = 50;

}  // namespace


bool ExpiredLogQueue::IsLogFileExist(const std::shared_ptr<RuntimeLogFile> &logFile)
{
    return filePathSet_.find(logFile->GetFilePath()) != filePathSet_.end();
}

void ExpiredLogQueue::AddLogFile(const std::shared_ptr<RuntimeLogFile> &logFile, bool needAddSet)
{
    if (logFile == nullptr) {
        YRLOG_WARN("log file is null.");
        return;
    }
    if (needAddSet && IsLogFileExist(logFile)) {
        YRLOG_DEBUG("log file({}) already exists in ExpiredLogQueue", logFile->GetFilePath());
        return;
    }
    if (needAddSet) { // reuse log don't need to add
        filePathSet_.insert(logFile->GetFilePath());
    }
    queue_.push(logFile);
    YRLOG_DEBUG("AddLogFile: {} is Dir {}, modificationTime: {}, curtime: {}", logFile->GetFilePath(), logFile->IsDir(),
                logFile->GetModificationTime(), std::time(nullptr));
}

size_t ExpiredLogQueue::GetLogCount() const
{
    return queue_.size();
}

void ExpiredLogQueue::Reset()
{
    filePathSet_.clear();
    std::priority_queue<
        std::shared_ptr<RuntimeLogFile>,
        std::vector<std::shared_ptr<RuntimeLogFile>>,
        RuntimeLogFileComparator> empty;
    queue_.swap(empty);
}

bool ExpiredLogQueue::DeleteOldestRuntimeLogFile()
{
    if (queue_.empty()) {
        YRLOG_DEBUG("queue_ is empty");
        return false;
    }
    auto oldestLog = queue_.top();
    YRLOG_DEBUG("{} is to be deleted", oldestLog->GetFilePath());
    queue_.pop();
    filePathSet_.erase(oldestLog->GetFilePath());
    if (oldestLog->IsDir()) {
        if (litebus::os::Rmdir(oldestLog->GetFilePath(), true).IsSome()) {
            YRLOG_ERROR("failed to rm expired runtime log({})", oldestLog->GetFilePath());
            return false;
        }
    } else {
        if (litebus::os::Rm(oldestLog->GetFilePath()).IsSome()) {
            YRLOG_DEBUG("failed to rm expired runtime log({}), it has already been deleted", oldestLog->GetFilePath());
            return false;
        }
    }
    YRLOG_DEBUG("expired runtime log({}) deleted", oldestLog->GetFilePath());
    return true;
}

std::shared_ptr<RuntimeLogFile> ExpiredLogQueue::PopLogFile()
{
    if (queue_.empty()) {
        YRLOG_DEBUG("queue_ is empty");
        return nullptr;
    }
    auto oldestLog = queue_.top();
    queue_.pop();
    return oldestLog;
}

LogManagerActor::LogManagerActor(const std::string &name, const litebus::AID &runtimeManagerAID, bool logReuse)
    : ActorBase(name), runtimeManagerAID_(runtimeManagerAID), logReuse_(logReuse)
{
    expiredLogQueue_ = std::make_shared<ExpiredLogQueue>();
    YRLOG_DEBUG("enable log prefix reuse: {}.", logReuse_);
}

litebus::Future<bool> LogManagerActor::CppAndPythonRuntimeLogProcess(const bool &isActive, const std::string &runtimeID,
                                                                     const std::string &filePath,
                                                                     const time_t &nowTimeStamp)
{
    auto promise = std::make_shared<litebus::Promise<bool>>();
    if (isActive) {
        YRLOG_DEBUG("runtime({}) is active, not delete it's file", runtimeID);
        return true;
    }
    return NeedCleanFile(runtimeID, filePath, nowTimeStamp);
}

litebus::Future<bool> LogManagerActor::DsClientLogProcess(const bool &isActive, const pid_t &pid,
                                                          const std::string &filePath, const time_t &nowTimeStamp)
{
    auto promise = std::make_shared<litebus::Promise<bool>>();
    if (isActive) {
        YRLOG_DEBUG("ds client({}) is active, not delete it's file", pid);
        return true;
    }

    auto fileInfoOption = GetFileInfo(filePath);
    if (fileInfoOption.IsNone()) {
        YRLOG_WARN("Failed to get file info for {}", filePath);
        promise->SetFailed(litebus::Status::KERROR);
        return promise->GetFuture();
    }

    // The accuracy is maintained to the level of seconds.
    auto modificationTime = fileInfoOption.Get().st_mtime;
    // Check if the log file is expired based on its modification time.
    if (nowTimeStamp - modificationTime >= logExpirationConfig_.timeThreshold) {
        YRLOG_DEBUG("Log file {} is expired", filePath);
        expiredLogQueue_->AddLogFile(std::make_shared<RuntimeLogFile>("", filePath, modificationTime, false),
                                     !logReuse_);
    }
    promise->SetValue(true);
    return promise->GetFuture();
}

litebus::Future<bool> LogManagerActor::JavaRuntimeDirProcess(const bool &isActive, const std::string &runtimeID,
                                                             const std::string &filePath, const time_t &nowTimeStamp)
{
    auto promise = std::make_shared<litebus::Promise<bool>>();
    if (isActive) {
        YRLOG_DEBUG("java runtime({}) is active, not delete it's file", runtimeID);
        return true;
    }

    bool isSubFilesAllExpired = true;
    time_t dirModificationTime = 0;
    auto subFilesOption = litebus::os::Ls(filePath);
    if (subFilesOption.IsSome()) {
        auto subFiles = subFilesOption.Get();
        for (const auto &subFile : subFiles) {
            auto subFilePath = litebus::os::Join(filePath, subFile, '/');
            auto subFileInfoOption = GetFileInfo(subFilePath);
            if (subFileInfoOption.IsNone()) {
                YRLOG_WARN("Failed to get file info for {}", subFilePath);
                continue;
            }
            auto modificationTime = subFileInfoOption.Get().st_mtime;
            YRLOG_DEBUG("{} modificationTime: {}", subFilePath, modificationTime);
            if (modificationTime > dirModificationTime) {
                dirModificationTime = modificationTime;
            }

            // Check if the log file is expired based on its modification time.
            YRLOG_DEBUG("nowTimeStamp: {}, modificationTime: {}, sub: {}, timeThreshold: {}", nowTimeStamp,
                        modificationTime, nowTimeStamp - modificationTime, logExpirationConfig_.timeThreshold);
            if (nowTimeStamp - modificationTime >= logExpirationConfig_.timeThreshold) {
                YRLOG_DEBUG("Log file {} is expired", subFilePath);
                expiredLogQueue_->AddLogFile(
                    std::make_shared<RuntimeLogFile>(runtimeID, subFilePath, modificationTime, false), !logReuse_);
            } else {
                isSubFilesAllExpired &= false;
            }
        }
        // the directory is also count as one expired log file
        if (isSubFilesAllExpired) {
            expiredLogQueue_->AddLogFile(
                std::make_shared<RuntimeLogFile>(runtimeID, filePath, dirModificationTime, true), !logReuse_);
        }
    }
    promise->SetValue(true);
    return promise->GetFuture();
}

void LogManagerActor::Init()
{
    YRLOG_INFO("Init LogManagerActor.");
}

void LogManagerActor::Finalize()
{
}

void LogManagerActor::SetConfig(const Flags &flags)
{
    YRLOG_DEBUG("Init LogManagerActor config");
    runtimeLogsPath_ = flags.GetRuntimeLogPath();
    runtimeStdLogDir_ = flags.GetRuntimeStdLogDir();
    logExpirationConfig_ = {.enable = flags.GetLogExpirationEnable(),
        .cleanupInterval = flags.GetLogExpirationCleanupInterval(),
        .timeThreshold = flags.GetLogExpirationTimeThreshold(),
        .maxFileCount = flags.GetLogExpirationMaxFileCount()};
}

void LogManagerActor::StopScanLogs()
{
    if (!logExpirationConfig_.enable) {
        YRLOG_DEBUG("runtime expired log manage disabled");
        return;
    }
    (void)litebus::TimerTools::Cancel(scanLogsTimer_);
}

std::string LogManagerActor::GetJavaRuntimeIDFromLogDirName(const std::string &file, const std::string &filePath)
{
    std::string runtimeID;
    std::smatch matchResult;
    if (std::regex_match(file, matchResult, JAVA_RUNTIME_LOG_REGEX_PATTERN_REGEX)) {
        YRLOG_DEBUG("Processing java runtime log file {}", filePath);
        if (matchResult.size() > 1) {
            runtimeID = matchResult[1].str();
            YRLOG_DEBUG("Extracted java runtimeId: {}", runtimeID);
        }
    }
    return runtimeID;
}

std::string LogManagerActor::GetRuntimeIDFromLogFileName(const std::string &file, const std::string &filePath)
{
    std::string runtimeID;
    std::smatch matchResult;
    if (std::regex_match(file, matchResult, PYTHON_RUNTIME_LOG_REGEX_PATTERN_REGEX)) {
        YRLOG_DEBUG("Processing python runtime log file {}", filePath);
        if (matchResult.size() > 1) {
            runtimeID = matchResult[1].str();
            YRLOG_DEBUG("Extracted python runtimeId: {}", runtimeID);
        }
    } else if (std::regex_match(file, matchResult, LIB_RUNTIME_LOG_REGEX_PATTERN_REGEX)) {
        YRLOG_DEBUG("Processing lib runtime log file {}", filePath);
        if (matchResult.size() > 1) {
            runtimeID = matchResult[1].str();
            YRLOG_DEBUG("Extracted lib runtimeId: {}", runtimeID);
        }
    } else if (std::regex_match(file, matchResult, STD_OUTPUT_LOG_REGEX_PATTERN_REGEX)) {
        YRLOG_DEBUG("Processing std output/error runtime log file {}", filePath);
        if (matchResult.size() > 1) {
            runtimeID = matchResult[1].str();
            YRLOG_DEBUG("Extracted std output/error runtimeId: {}", runtimeID);
        }
    } else if (std::regex_match(file, matchResult, CPP_RUNTIME_LOG_REGEX_PATTERN_REGEX)) { // last match
        YRLOG_DEBUG("Processing cpp runtime log file {}", filePath);
        if (matchResult.size() > 1) {
            runtimeID = matchResult[1].str();
            YRLOG_DEBUG("Extracted cpp runtimeId: {}", runtimeID);
        }
    }
    return runtimeID;
}

pid_t LogManagerActor::GetDsClientPidFromLogFileName(const std::string &file, const std::string &filePath)
{
    pid_t pid = 0;
    std::smatch matchResult;
    if (std::regex_match(file, matchResult, DS_CLIENT_LOG_REGEX_PATTERN_REGEX)) {
        YRLOG_DEBUG("Processing ds client log file {}", filePath);
        if (matchResult.size() > 1) {
            pid = std::stoll(matchResult[1].str());
            YRLOG_DEBUG("Extracted ds client PID: {}", pid);
        }
    }
    return pid;
}

litebus::Future<bool> LogManagerActor::IsRuntimeActive(const std::string &runtimeID) const
{
    return litebus::Async(runtimeManagerAID_, &RuntimeManager::IsRuntimeActive, runtimeID);
}

litebus::Future<bool> LogManagerActor::IsRuntimeActiveByPid(const pid_t &pid) const
{
    return litebus::Async(runtimeManagerAID_, &RuntimeManager::IsRuntimeActiveByPid, pid);
}

litebus::Future<bool> LogManagerActor::CollectAddFilesFuture(const std::list<litebus::Future<bool>> &adds)
{
    auto promise = std::make_shared<litebus::Promise<bool>>();
    (void)litebus::Collect(adds).OnComplete([promise](const litebus::Future<std::list<bool>> &future) {
        if (future.IsError()) {
            YRLOG_DEBUG("Collect future error");
            promise->SetValue(false);
            return;
        }
        bool isError = false;
        for (auto resp : future.Get()) {
            if (resp) {
                continue;
            }
            isError = true;
            YRLOG_WARN("error occurs of files in this scanning round");
        }
        YRLOG_DEBUG("done. result: {}, size is {}", !isError, future.Get().size());
        promise->SetValue(!isError);
    });
    return promise->GetFuture();
}

void LogManagerActor::ScanLogsRegularly()
{
    if (!logExpirationConfig_.enable) {
        YRLOG_DEBUG("runtime expired log manage disabled");
        return;
    }

    YRLOG_DEBUG("start ScanLogs");
    if (!litebus::os::ExistPath(runtimeLogsPath_)) {
        YRLOG_WARN("{} is not exist", runtimeLogsPath_);
        return;
    }

    auto filesOption = litebus::os::Ls(runtimeLogsPath_);
    if (filesOption.IsNone() || filesOption.Get().empty()) {
        YRLOG_WARN("no log file in {}", runtimeLogsPath_);
        return;
    }

    if (logReuse_) {
        ReuseLogScanAndClean(filesOption.Get());
    } else {
        NoReuseLogScanAndClean(filesOption.Get());
    }
}

void LogManagerActor::NoReuseLogScanAndClean(const std::vector<std::string> &files)
{
    time_t nowTimeStamp = std::time(nullptr);
    std::list<litebus::Future<bool>> adds;
    for (const auto &file : files) {
        auto filePath = litebus::os::Join(runtimeLogsPath_, file, '/');
        bool isDir = !IsFile(filePath);
        YRLOG_DEBUG("Processing filePath: {}", filePath);
        if (isDir) {
            // Skip the exception and instance folders, which are for storing function exception
            // and user console output logs respectively.
            if (strncmp(file.c_str(), "exception", EXCEPTION_DIR_LEN) == 0 ||
                strncmp(file.c_str(), runtimeStdLogDir_.c_str(), runtimeStdLogDir_.size()) == 0) {
                continue;
            }

            // If it is a directory, check the latest modification time of the files in the directory.
            std::string runtimeID = GetJavaRuntimeIDFromLogDirName(file, filePath);
            if (runtimeID.empty()) {
                continue;
            }
            auto future = IsRuntimeActive(runtimeID).Then(
                litebus::Defer(GetAID(), &LogManagerActor::JavaRuntimeDirProcess, std::placeholders::_1, runtimeID,
                               filePath, nowTimeStamp));
            adds.emplace_back(future);
        } else {
            std::smatch matchResult;
            if (std::regex_match(file, matchResult, DS_CLIENT_LOG_REGEX_PATTERN_REGEX)) {
                pid_t pid = GetDsClientPidFromLogFileName(file, filePath);
                if (pid == 0) {
                    continue;
                }
                auto future = IsRuntimeActiveByPid(pid).Then(
                    litebus::Defer(GetAID(), &LogManagerActor::DsClientLogProcess, std::placeholders::_1, pid,
                                   filePath, nowTimeStamp));
                adds.emplace_back(future);
            } else {
                std::string runtimeID = GetRuntimeIDFromLogFileName(file, filePath);
                if (runtimeID.empty()) {
                    continue;
                }
                auto future = IsRuntimeActive(runtimeID).Then(
                    litebus::Defer(GetAID(), &LogManagerActor::CppAndPythonRuntimeLogProcess, std::placeholders::_1,
                                   runtimeID, filePath, nowTimeStamp));
                adds.emplace_back(future);
            }
        }
    }

    // CleanLogs after expired log files added to queue
    CollectAddFilesFuture(adds).OnComplete(litebus::Defer(GetAID(), &LogManagerActor::CleanLogs))
        .OnComplete(litebus::Defer(GetAID(), &LogManagerActor::StartScanLogs));
}

litebus::Future<bool> LogManagerActor::CleanLogs()
{
    auto logCount = expiredLogQueue_->GetLogCount();
    YRLOG_DEBUG("expiredLogQueue_ count: {}, maxFileCount:{}", logCount, logExpirationConfig_.maxFileCount);
    if (logCount <= (size_t)logExpirationConfig_.maxFileCount) {
        return true;
    }

    size_t toDeleteCount = logCount - logExpirationConfig_.maxFileCount;
    if (!logReuse_) {
        for (size_t i = 0; i < toDeleteCount; ++i) {
            expiredLogQueue_->DeleteOldestRuntimeLogFile();
        }
        return true;
    }

    // reuse log recycle
    litebus::Promise<bool> promise;
    std::vector<std::shared_ptr<RuntimeLogFile>> toBeDeleteFiles;
    for (size_t i = 0; i < toDeleteCount; ++i) {
        auto file = expiredLogQueue_->PopLogFile();
        if (file == nullptr) {
            YRLOG_WARN("get null file.");
            break;
        }
        toBeDeleteFiles.emplace_back(file);
    }
    // to avoid blocking runtime-manager acquire or release log prefix while delete too many file
    PartitionDeleteFile(toBeDeleteFiles, 0, promise);
    return promise.GetFuture();
}

void LogManagerActor::PartitionDeleteFile(const std::vector<std::shared_ptr<RuntimeLogFile>> &toBeDeleteFiles,
                                          size_t index, litebus::Promise<bool> promise)
{
    if (index >= toBeDeleteFiles.size()) {
        promise.SetValue(true);
        return;
    }
    size_t curDeleteNum = 0;
    for (; index < toBeDeleteFiles.size() && curDeleteNum < MAX_DELETE_FILE_NUM; index++) {
        curDeleteNum++;
        if (toBeDeleteFiles[index]->IsDir()) {
            if (litebus::os::Rmdir(toBeDeleteFiles[index]->GetFilePath(), true).IsSome()) {
                YRLOG_ERROR("failed to rm expired runtime log dir({})", toBeDeleteFiles[index]->GetFilePath());
            }
            continue;
        }
        if (litebus::os::Rm(toBeDeleteFiles[index]->GetFilePath()).IsSome()) {
            YRLOG_DEBUG("failed to rm expired runtime log({}), it has already been deleted",
                        toBeDeleteFiles[index]->GetFilePath());
        }
    }
    if (index < toBeDeleteFiles.size()) {
        litebus::Async(GetAID(), &LogManagerActor::PartitionDeleteFile, toBeDeleteFiles, index, promise);
    } else {
        promise.SetValue(true);
    }
    YRLOG_DEBUG("current delete: {}, total: {}, rest: {}", curDeleteNum, index, toBeDeleteFiles.size() - index);
}

void LogManagerActor::ReuseLogScanAndClean(const std::vector<std::string> &files)
{
    time_t nowTimeStamp = std::time(nullptr);
    std::list<litebus::Future<bool>> adds;
    // each recycle need to clean last cache because log may be reused
    expiredLogQueue_->Reset();
    for (const auto &file : files) {
        auto filePath = litebus::os::Join(runtimeLogsPath_, file, '/');
        if (!IsFile(filePath)) {
            continue;
        }
        std::smatch matchResult;
        if (std::regex_match(file, matchResult, DS_CLIENT_LOG_REGEX_PATTERN_REGEX)) {
            pid_t pid = GetDsClientPidFromLogFileName(file, filePath);
            if (pid == 0) {
                continue;
            }
            auto future = IsRuntimeActiveByPid(pid).Then(
                litebus::Defer(GetAID(), &LogManagerActor::DsClientLogProcess, std::placeholders::_1, pid,
                               filePath, nowTimeStamp));
            adds.emplace_back(future);
            continue;
        }
        if (std::regex_match(file, matchResult, REUSE_LOG_PREFIX_PATTERN_REGEX)) {
            auto iter = logPrefix2RuntimeID_.find(matchResult[1].str());
            if (iter == logPrefix2RuntimeID_.end()) {  // not found mean runtime is release it
                auto future =
                    litebus::Async(GetAID(), &LogManagerActor::RecycleReuseLog, false, "", file, nowTimeStamp);
                adds.emplace_back(future);
                continue;
            }
            auto future = IsRuntimeActive(iter->second).Then(
                litebus::Defer(GetAID(), &LogManagerActor::RecycleReuseLog, std::placeholders::_1,
                               iter->second, file, nowTimeStamp));
            adds.emplace_back(future);
        }
    }
    // CleanLogs after expired log files added to queue
    CollectAddFilesFuture(adds).OnComplete(litebus::Defer(GetAID(), &LogManagerActor::CleanLogs))
        .OnComplete(litebus::Defer(GetAID(), &LogManagerActor::StartScanLogs));
}

void LogManagerActor::StartScanLogs()
{
    scanLogsTimer_ = litebus::AsyncAfter(logExpirationConfig_.cleanupInterval * MILLISECONDS_PRE_SECOND, GetAID(),
                                         &LogManagerActor::ScanLogsRegularly);
}

litebus::Future<bool> LogManagerActor::RecycleReuseLog(const bool &isActive, const std::string &runtimeID,
                                                       const std::string &file, const time_t &nowTimeStamp)
{
    std::smatch matchResult;
    if (isActive && !std::regex_match(file, matchResult, REUSE_LOG_PREFIX_WITH_GZ_REGEX)) {
        YRLOG_DEBUG("runtime({}) is active and reusing, not delete it's log file {}", runtimeID, file);
        return true;
    }
    auto filePath = litebus::os::Join(runtimeLogsPath_, file, '/');
    return NeedCleanFile(runtimeID, filePath, nowTimeStamp);
}

litebus::Future<bool> LogManagerActor::NeedCleanFile(const std::string &runtimeID, const std::string &filePath,
                                                     const time_t &nowTimeStamp)
{
    auto promise = std::make_shared<litebus::Promise<bool>>();
    auto fileInfoOption = GetFileInfo(filePath);
    if (fileInfoOption.IsNone()) {
        YRLOG_WARN("Failed to get file info for {}", filePath);
        promise->SetFailed(litebus::Status::KERROR);
        return promise->GetFuture();
    }

    // The accuracy is maintained to the level of seconds.
    auto modificationTime = fileInfoOption.Get().st_mtime;
    // Check if the log file is expired based on its modification time.
    if (nowTimeStamp - modificationTime >= logExpirationConfig_.timeThreshold) {
        expiredLogQueue_->AddLogFile(std::make_shared<RuntimeLogFile>(runtimeID, filePath, modificationTime, false),
                                     !logReuse_);
    }
    promise->SetValue(true);
    return promise->GetFuture();
}

litebus::Future<std::string> LogManagerActor::AcquireLogPrefix(const std::string &runtimeID)
{
    YRLOG_DEBUG("try to acquire log prefix for runtime {}.", runtimeID);
    std::string logPrefix;
    if (!logPrefixDequeue_.empty()) {
        logPrefix = logPrefixDequeue_.front();
        logPrefixDequeue_.pop_front();
    } else {
        logCount_ = (logCount_ + 1) % MAX_COUNTER;
        std::ostringstream oss;
        oss << "YR_" << actorPid_ << "_" << std::setw(MAX_COUNTER_DIGIT) << std::setfill('0') << logCount_;
        logPrefix = oss.str();
    }

    logPrefix2RuntimeID_[logPrefix] = runtimeID;
    runtimeID2LogPrefix_[runtimeID] = logPrefix;

    YRLOG_INFO("assign log prefix {} to runtime {}.", logPrefix, runtimeID);
    return logPrefix;
}

void LogManagerActor::ReleaseLogPrefix(const std::string &runtimeID)
{
    YRLOG_DEBUG("try to release log prefix for runtime {}.", runtimeID);
    auto runtimeIter = runtimeID2LogPrefix_.find(runtimeID);
    if (runtimeIter == runtimeID2LogPrefix_.end()) {
        YRLOG_WARN("can not find runtime {} log prefix.", runtimeID);
        return;
    }
    auto logPrefix = runtimeIter->second;
    runtimeID2LogPrefix_.erase(runtimeID);

    auto prefixIter = logPrefix2RuntimeID_.find(logPrefix);
    if (prefixIter == logPrefix2RuntimeID_.end()) {
        YRLOG_WARN("can not find log prefix {} for runtime {}.", logPrefix, runtimeID);
        return;
    }

    if (prefixIter->second != runtimeID) {
        YRLOG_WARN("log prefix {} is used by runtime {}, not current runtime {}.", logPrefix, prefixIter->second,
                   runtimeID);
        return;
    }
    logPrefix2RuntimeID_.erase(logPrefix);
    logPrefixDequeue_.push_front(logPrefix);
    YRLOG_INFO("release log prefix {} from runtime {}.", logPrefix, runtimeID);
}

}  // namespace functionsystem::runtime_manager