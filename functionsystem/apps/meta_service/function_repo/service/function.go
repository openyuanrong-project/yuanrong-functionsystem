/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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

// Package service is processing service codes

package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	common "meta_service/common/constants"
	"meta_service/common/functionhandler"
	"meta_service/common/logger/log"
	"meta_service/common/snerror"
	"meta_service/common/timeutil"
	"meta_service/common/types"
	"meta_service/common/urnutils"
	"meta_service/function_repo/errmsg"
	"meta_service/function_repo/model"
	"meta_service/function_repo/pkgstore"
	"meta_service/function_repo/server"
	"meta_service/function_repo/storage"
	"meta_service/function_repo/storage/publish"
	"meta_service/function_repo/utils"
	"meta_service/function_repo/utils/constants"
)

const (
	formatIntBase int = 10
	nilMapValue       = "{}"
)

type uncontrolledInfo struct {
	ctx      server.Context
	bucketID string
	objectID string
}

type functionInstanceAttribute struct {
	minInstance   int64
	maxInstance   int64
	concurrentNum int
}

// CreateFunctionInfo create function
func CreateFunctionInfo(ctx server.Context, req model.FunctionCreateRequest, isAdmin bool) (model.FunctionVersion,
	error,
) {
	// start transaction
	txn := storage.GetTxnByKind(ctx, req.Kind)
	defer txn.Cancel()

	version := urnutils.GetFunctionVersion(req.Kind)
	f, err := storage.GetFunctionVersion(txn, req.Name, version)
	if err == nil {
		log.GetLogger().Errorf("function name %s is exist", f.Function.Name)
		return model.FunctionVersion{}, errmsg.New(errmsg.FunctionNameExist)
	}
	err = checkFunctionLayerList(storage.GetTxnByKind(ctx, ""), req.Layers, req.Runtime)
	if err != nil {
		log.GetLogger().Errorf("failed to check layer :%s", err.Error())
		return model.FunctionVersion{}, err
	}

	// Validate rootfs specification if provided
	err = ValidateRootfsSpec(&req.RootfsSpecMeta)
	if err != nil {
		log.GetLogger().Errorf("failed to validate rootfs specification: %s", err.Error())
		return model.FunctionVersion{}, err
	}

	functionVersion, err := buildFunctionVersion(req)
	if err != nil {
		log.GetLogger().Errorf("failed to build function version when creating :%s", err.Error())
		return model.FunctionVersion{}, err
	}
	functionVersion.FunctionVersion.Package.CodeUploadType = req.CodeUploadType
	err = storage.CreateFunctionVersion(txn, functionVersion)
	if err != nil {
		log.GetLogger().Errorf("failed to create function when saving function: %s", err.Error())
		return model.FunctionVersion{}, err
	}
	err = storage.CreateFunctionStatus(txn, functionVersion.Function.Name,
		functionVersion.FunctionVersion.Version)
	if err != nil {
		log.GetLogger().Errorf("failed to create function status when saving function: %s", err.Error())
		return model.FunctionVersion{}, err
	}
	if req.CodeUploadType != common.S3StorageType || !isAdmin {
		err = publish.SavePublishFuncVersion(txn, functionVersion)
		if err != nil {
			log.GetLogger().Errorf("failed to create function mete data when saving function: %s", err.Error())
			return model.FunctionVersion{}, err
		}
	}
	err = txn.Commit()
	if err != nil {
		log.GetLogger().Errorf("failed to create function when committing: %s", err.Error())
		return model.FunctionVersion{}, err
	}
	return buildFuncResult(ctx, functionVersion)
}

func buildFuncResult(ctx server.Context,
	functionVersion storage.FunctionVersionValue,
) (model.FunctionVersion, error) {
	t, err := ctx.TenantInfo()
	if err != nil {
		log.GetLogger().Errorf("failed to create function can not get tenant info: %s", err.Error())
		return model.FunctionVersion{}, err
	}
	key := storage.FunctionVersionKey{
		TenantInfo:      t,
		FunctionName:    functionVersion.Function.Name,
		FunctionVersion: functionVersion.FunctionVersion.Version,
	}
	ret := buildFunctionVersionModel(key, functionVersion)
	return ret, nil
}

func checkFunctionLayerList(txn storage.Transaction, urns []string, runtime string) error {
	layerMap := make(map[string]string, len(urns))
	for _, urn := range urns {
		info, err := ParseLayerInfo(txn.GetCtx(), urn)
		if err != nil {
			log.GetLogger().Errorf("failed to get layer query info :%s", err.Error())
			return err
		}
		val, err := storage.GetLayerVersionTx(txn, info.LayerName, info.LayerVersion)
		if err != nil {
			log.GetLogger().Errorf("failed to get layer version :%s", err.Error())
			return err
		}

		var found bool
		for _, rt := range val.CompatibleRuntimes {
			if rt == runtime {
				found = true
				break
			}
		}
		// not exit
		if !found {
			return errmsg.NewParamError("runtime %s of layer [%s] is not compatible with "+
				"function's runtime [%s]", val.CompatibleRuntimes, info.LayerName, runtime)
		}

		// repetitive
		if layerMap[urn] == "" {
			layerMap[urn] = urn
		} else {
			return errmsg.NewParamError(
				"the URN list of layer's version is not unique, layer name is [%s]", info.LayerName)
		}
	}
	return nil
}

// UpdateFunctionInfo update function
func UpdateFunctionInfo(ctx server.Context, f model.FunctionQueryInfo,
	fv model.FunctionUpdateRequest, isAdmin bool) (
	model.FunctionVersion, error,
) {
	// start transaction
	txn := storage.NewTxn(ctx)
	defer txn.Cancel()

	funcVersion, err := storage.GetFunctionByFunctionNameAndVersion(ctx, f.FunctionName, f.FunctionVersion, "")
	if err != nil {
		log.GetLogger().Errorf("failed to get function by function name and version :%s", err.Error())
		return model.FunctionVersion{}, err
	}
	if funcVersion.FunctionVersion.RevisionID != fv.RevisionID {
		log.GetLogger().Errorf("revisionId is not the same as latest versions %s ,%s",
			funcVersion.FunctionVersion.RevisionID, fv.RevisionID)
		return model.FunctionVersion{}, errmsg.New(errmsg.RevisionIDError)
	}
	err = checkFunctionLayerList(txn, fv.Layers, funcVersion.FunctionVersion.Runtime)
	if err != nil {
		log.GetLogger().Errorf("failed to check layer :%s", err.Error())
		return model.FunctionVersion{}, err
	}

	// Validate rootfs specification if provided
	err = ValidateRootfsSpec(&fv.RootfsSpecMeta)
	if err != nil {
		log.GetLogger().Errorf("failed to validate rootfs specification: %s", err.Error())
		return model.FunctionVersion{}, err
	}

	err = buildUpdateFunctionVersion(fv, &funcVersion)
	if err != nil {
		log.GetLogger().Errorf("failed to build update function version :%s", err.Error())
		return model.FunctionVersion{}, err
	}
	funcVersion.FunctionVersion.Package.CodeUploadType = fv.CodeUploadType
	err = storage.UpdateFunctionVersion(txn, funcVersion)
	if err != nil {
		log.GetLogger().Errorf("failed to update function version :%s", err.Error())
		return model.FunctionVersion{}, err
	}
	if fv.CodeUploadType != common.S3StorageType || !isAdmin {
		err = publish.SavePublishFuncVersion(txn, funcVersion)
		if err != nil {
			log.GetLogger().Errorf("failed to create function mete data when saving function: %s", err.Error())
			return model.FunctionVersion{}, err
		}
	}
	err = txn.Commit()
	if err != nil {
		log.GetLogger().Errorf("failed to commit when update :%s", err.Error())
		return model.FunctionVersion{}, err
	}
	return buildFuncResult(ctx, funcVersion)
}

func buildBasicUpdateFunctionVersion(request model.FunctionUpdateRequest,
	fv *storage.FunctionVersionValue,
) {
	fv.FunctionVersion.Handler = getChangeStringValue(fv.FunctionVersion.Handler, request.Handler)
	fv.FunctionVersion.CPU = getChangeInt64Value(fv.FunctionVersion.CPU, request.CPU)
	fv.FunctionVersion.Memory = getChangeInt64Value(fv.FunctionVersion.Memory, request.Memory)
	fv.FunctionVersion.Timeout = getChangeInt64Value(fv.FunctionVersion.Timeout, request.Timeout)
	fv.Function.Description = getChangeStringValue(fv.Function.Description, request.Description)
	fv.FunctionVersion.HookHandler = getHookHandler(request.Kind, fv.FunctionVersion.Runtime, request.Handler,
		request.HookHandler)
	if request.ExtendedHandler != nil {
		fv.FunctionVersion.ExtendedHandler = request.ExtendedHandler
	}
	if request.ExtendedTimeout != nil {
		fv.FunctionVersion.ExtendedTimeout = request.ExtendedTimeout
	}
	fv.FunctionVersion.Package.StorageType = request.StorageType
	fv.FunctionVersion.Package.CodePath = request.CodePath
	fv.FunctionVersion.CacheInstance = request.CacheInstance
	fv.FunctionVersion.Device = request.Device
	poolLabel := utils.GetPoolLabels(request.ResourceAffinitySelectors)
	if len(poolLabel) == 0 {
		poolLabel = utils.GetPoolLabels(request.SchedulePolicies)
	}
	fv.FunctionVersion.PoolLabel = poolLabel
	fv.FunctionVersion.PoolID = request.PoolID
	fv.FunctionVersion.EnableMetrics = request.EnableMetrics
	fv.FunctionVersion.EnableAgentSession = request.EnableAgentSession
	fv.FunctionVersion.PriorityAZ = request.PriorityAZ
	fv.FunctionVersion.IdleTime = request.IdleTime
	fv.FunctionVersion.IsFuncPublic = request.IsFuncPublic
	setFunctionS3CodePath(fv, request)
	fv.FunctionVersion.WarmupType = request.WarmupType
	fv.FunctionVersion.RootfsSpecMeta = request.RootfsSpecMeta
	logRootfsUpdate(request)
	setFunctionScaleConfig(fv, request)
}

func setFunctionS3CodePath(fv *storage.FunctionVersionValue, request model.FunctionUpdateRequest) {
	fv.FunctionVersion.Package.BucketID = request.S3CodePath.BucketID
	fv.FunctionVersion.Package.ObjectID = request.S3CodePath.ObjectID
	fv.FunctionVersion.Package.BucketUrl = request.S3CodePath.BucketUrl
	fv.FunctionVersion.Package.Token = request.S3CodePath.Token
	fv.FunctionVersion.Package.Signature = request.S3CodePath.Sha512
}

func logRootfsUpdate(request model.FunctionUpdateRequest) {
	if request.RootfsSpecMeta.Type != "" {
		log.GetLogger().Infof("updating function rootfs: type=%s, runtime=%s, imageurl=%s, readonly=%v",
			request.RootfsSpecMeta.Type, request.RootfsSpecMeta.Runtime,
			request.RootfsSpecMeta.ImageURL, request.RootfsSpecMeta.ReadOnly)
	}
}

func setFunctionScaleConfig(fv *storage.FunctionVersionValue, request model.FunctionUpdateRequest) {
	fv.FunctionVersion.ScalePolicy = request.ScalePolicy
	fv.FunctionVersion.SchedulePolicy = request.SchedulePolicy
	fv.FunctionVersion.CustomContainerConfig = request.ExtendedMetaData.CustomContainerConfig
	fv.FunctionVersion.CustomHealthCheck = request.ExtendedMetaData.CustomHealthCheck
	fv.FunctionVersion.AutoScaleConfig = storage.AutoScaleConfig{
		SLAQuota:      request.AutoScaleConfig.SLAQuota,
		ScaleDownTime: request.AutoScaleConfig.ScaleDownTime,
		BurstScaleNum: request.AutoScaleConfig.BurstScaleNum,
	}
}

func buildUpdateFunctionVersion(request model.FunctionUpdateRequest,
	fv *storage.FunctionVersionValue,
) error {
	buildBasicUpdateFunctionVersion(request, fv)
	if request.CustomResources != nil {
		bytes, err := json.Marshal(request.CustomResources)
		if err != nil {
			log.GetLogger().Errorf("failed to get marshal customResources :%s", err.Error())
			return err
		}
		fv.FunctionVersion.CustomResources = getChangeStringValue(fv.FunctionVersion.CustomResources, string(bytes))
	}
	if request.Environment != nil {
		env, err := utils.EncodeEnv(request.Environment)
		if err != nil {
			log.GetLogger().Errorf("failed to get encode env info :%s", err.Error())
			return err
		}
		fv.FunctionVersion.Environment = getChangeStringValue(fv.FunctionVersion.Environment, env)
	}
	if request.ConcurrentNum != "" {
		concurrentNum, err := strconv.Atoi(request.ConcurrentNum)
		if err != nil {
			log.GetLogger().Errorf("failed to get conversion str concurrentNum %s :%s", concurrentNum, err.Error())
			return err
		}
		fv.FunctionVersion.ConcurrentNum = getChangeIntValue(fv.FunctionVersion.ConcurrentNum, concurrentNum)
	}
	if request.MinInstance != "" {
		minInstance, err := strconv.ParseInt(request.MinInstance, 10, 64)
		if err != nil {
			log.GetLogger().Errorf("failed to get conversion str minInstance %s :%s", minInstance, err.Error())
			return err
		}
		fv.FunctionVersion.MinInstance = minInstance
	}
	if request.MaxInstance != "" {
		maxInstance, err := strconv.ParseInt(request.MaxInstance, 10, 64)
		if err != nil {
			log.GetLogger().Errorf("failed to get conversion str maxInstance %s :%s", maxInstance, err.Error())
			return err
		}
		fv.FunctionVersion.MaxInstance = maxInstance
	}
	err := updateFunctionLayer(request, fv)
	if err != nil {
		log.GetLogger().Errorf("failed to get layers %s :%s", request.Layers, err.Error())
		return err
	}
	fv.FunctionVersion.RevisionID = utils.GetUTCRevisionID()
	fv.Function.UpdateTime = utils.NowTimeF()
	return nil
}

func updateFunctionLayer(request model.FunctionUpdateRequest, fv *storage.FunctionVersionValue) error {
	if len(request.Layers) == 0 {
		fv.FunctionLayer = nil
		return nil
	}
	functionLayer := make([]storage.FunctionLayer, len(request.Layers), len(request.Layers))
	for index, data := range request.Layers {
		name, versionStr := utils.InterceptNameAndVersionFromLayerURN(data)
		version, err := strconv.Atoi(versionStr)
		if err != nil {
			log.GetLogger().Errorf("failed to conversion str version %s ", versionStr)
			return err
		}
		functionLayer[index] = storage.FunctionLayer{
			Name:    name,
			Version: version,
			Order:   index,
		}
	}
	fv.FunctionLayer = functionLayer
	return nil
}

func getChangeStringValue(ov, v string) string {
	if v != "" {
		return v
	}
	return ov
}

func getChangeIntValue(ov, v int) int {
	if v != 0 {
		return v
	}
	return ov
}

func getChangeInt64Value(ov, v int64) int64 {
	if v != 0 {
		return v
	}
	return ov
}

// build FunctionVersion struct
func buildFunctionVersion(request model.FunctionCreateRequest) (storage.FunctionVersionValue, error) {
	functionLayer := make([]storage.FunctionLayer, len(request.Layers), len(request.Layers))
	for index, data := range request.Layers {
		name, versionStr := utils.InterceptNameAndVersionFromLayerURN(data)
		version, err := strconv.Atoi(versionStr)
		if err != nil {
			log.GetLogger().Errorf("failed to conversion str version %s ", versionStr)
			return storage.FunctionVersionValue{}, err
		}
		functionLayer[index] = storage.FunctionLayer{
			Name:    name,
			Version: version,
			Order:   index,
		}
	}
	var env string
	var err error = nil
	if request.Environment != nil {
		env, err = utils.EncodeEnv(request.Environment)
		if err != nil {
			log.GetLogger().Errorf("failed to encode env %s ", err.Error())
			return storage.FunctionVersionValue{}, err
		}
	}
	var customResources []byte
	if request.CustomResources != nil {
		customResources, err = json.Marshal(request.CustomResources)
		if err != nil {
			log.GetLogger().Warnf("failed to marshal customResources %s ", err)
			return storage.FunctionVersionValue{}, err
		}
	}
	return functionVersionResult(request, env, functionLayer, string(customResources))
}

func functionVersionResult(request model.FunctionCreateRequest, env string,
	functionLayer []storage.FunctionLayer, customResources string,
) (storage.FunctionVersionValue, error) {
	w, err := tranWorkerParams(request)
	if err != nil {
		log.GetLogger().Errorf("failed to tran worker params :%s", err.Error())
		return storage.FunctionVersionValue{}, err
	}
	function := storage.Function{
		Name:        request.Name,
		Description: request.Description,
		Tag:         request.Tags,
		CreateTime:  utils.NowTimeF(),
	}
	version := getFunctionVersion(request, env, w, customResources)
	value := storage.FunctionVersionValue{
		Function:        function,
		FunctionVersion: version,
		FunctionLayer:   functionLayer,
	}
	return value, nil
}

func getHookHandler(kind, runtime, handler string, hookMap map[string]string) map[string]string {
	hookHandler := functionhandler.FunctionHookHandlerInfo{}
	if len(hookMap) > 0 {
		hookHandler.InitHandler = hookMap[functionhandler.InitHandler]
		hookHandler.CallHandler = hookMap[functionhandler.CallHandler]
		hookHandler.CheckpointHandler = hookMap[functionhandler.CheckpointHandler]
		hookHandler.RecoverHandler = hookMap[functionhandler.RecoverHandler]
		hookHandler.ShutdownHandler = hookMap[functionhandler.ShutdownHandler]
		hookHandler.SignalHandler = hookMap[functionhandler.SignalHandler]
	}
	mapBuilder := functionhandler.GetBuilder(kind, runtime, handler)
	if mapBuilder != nil {
		return mapBuilder.HookHandler(runtime, hookHandler)
	}
	return map[string]string{}
}

func getFunctionVersion(request model.FunctionCreateRequest, env string,
	w functionInstanceAttribute, customResources string,
) storage.FunctionVersion {
	poolLabel := utils.GetPoolLabels(request.ResourceAffinitySelectors)
	if len(poolLabel) == 0 {
		poolLabel = utils.GetPoolLabels(request.SchedulePolicies)
	}

	// Log rootfs specification if provided
	if request.RootfsSpecMeta.Type != "" {
		log.GetLogger().Infof("creating function with rootfs: type=%s, runtime=%s, imageurl=%s, readonly=%v",
			request.RootfsSpecMeta.Type, request.RootfsSpecMeta.Runtime,
			request.RootfsSpecMeta.ImageURL, request.RootfsSpecMeta.ReadOnly)
	}

	version := storage.FunctionVersion{
		Package: storage.Package{
			StorageType: request.StorageType,
			LocalPackage: storage.LocalPackage{
				CodePath: request.CodePath,
			},
			S3Package: storage.S3Package{
				BucketID:  request.S3CodePath.BucketID,
				ObjectID:  request.S3CodePath.ObjectID,
				BucketUrl: request.S3CodePath.BucketUrl,
				Token:     request.S3CodePath.Token,
				Signature: request.S3CodePath.Sha512,
			},
		},
		RevisionID:         utils.GetUTCRevisionID(),
		Handler:            request.Handler,
		CPU:                request.CPU,
		Memory:             request.Memory,
		Runtime:            utils.GetRuntimeName(request.Kind, request.Runtime),
		Timeout:            request.Timeout,
		Version:            utils.GetDefaultVersion(),
		Environment:        env,
		CustomResources:    customResources,
		Description:        utils.GetDefaultVersion(),
		PublishTime:        utils.NowTimeF(),
		MinInstance:        w.minInstance,
		MaxInstance:        w.maxInstance,
		ConcurrentNum:      w.concurrentNum,
		CacheInstance:      request.CacheInstance,
		HookHandler:        getHookHandler(request.Kind, request.Runtime, request.Handler, request.HookHandler),
		ExtendedHandler:    request.ExtendedHandler,
		ExtendedTimeout:    request.ExtendedTimeout,
		Device:             request.Device,
		PoolLabel:          poolLabel,
		PoolID:             request.PoolID,
		EnableAgentSession: request.EnableAgentSession,
		PriorityAZ:             request.PriorityAZ,
		EnableMetrics:          request.EnableMetrics,
		IsFuncPublic:           request.IsFuncPublic,
		IdleTime:               request.IdleTime,
		WarmupType:             request.WarmupType,
		RootfsSpecMeta:         request.RootfsSpecMeta,
		ScalePolicy:            request.ScalePolicy,
		SchedulePolicy:         request.SchedulePolicy,
		AutoScaleConfig: storage.AutoScaleConfig{
			SLAQuota:      request.AutoScaleConfig.SLAQuota,
			ScaleDownTime: request.AutoScaleConfig.ScaleDownTime,
			BurstScaleNum: request.AutoScaleConfig.BurstScaleNum,
		},
		CustomContainerConfig:  request.ExtendedMetaData.CustomContainerConfig,
		CustomHealthCheck:      request.ExtendedMetaData.CustomHealthCheck,
	}
	if request.Kind == common.Faas {
		version.Kind = common.Faas
		version.Version = utils.GetDefaultFaaSVersion()
		version.Description = utils.GetDefaultFaaSVersion()
		version.FuncName = urnutils.GetPureFaaSFunctionName(request.Name)
		version.Service = urnutils.GetPureFaaSService(request.Name)
		version.IsBridgeFunction = false
		version.EnableAuthInHeader = false
	}
	return version
}

func tranWorkerParams(request model.FunctionCreateRequest) (functionInstanceAttribute, error) {
	minInstance, err := strconv.ParseInt(request.MinInstance, 10, 64)
	if err != nil {
		log.GetLogger().Errorf("failed to convert str %s :%s ", "minInstance", request.MinInstance)
		return functionInstanceAttribute{}, err
	}
	maxInstance, err := strconv.ParseInt(request.MaxInstance, 10, 64)
	if err != nil {
		log.GetLogger().Errorf("failed to convert str %s :%s ", "maxInstance", request.MaxInstance)
		return functionInstanceAttribute{}, err
	}
	concurrentNum, err := strconv.Atoi(request.ConcurrentNum)
	if err != nil {
		log.GetLogger().Errorf("failed to convert str %s :%s ", "concurrentNum", request.ConcurrentNum)
		return functionInstanceAttribute{}, err
	}
	return functionInstanceAttribute{minInstance, maxInstance, concurrentNum}, nil
}

func deletePackage(txn storage.Transaction, ctx server.Context, fvs []storage.FunctionVersionValue) (err error) {
	err = txn.Commit()
	if err != nil {
		log.GetLogger().Errorf("failed to commit delete function :%s", err.Error())
		return err
	}

	err = deleteFunctionPackage(ctx, fvs)
	if err != nil {
		log.GetLogger().Errorf("failed to delete function package :%s", err.Error())
		return err
	}
	return nil
}

// DeleteResponseForMeta delete function info
func DeleteResponseForMeta(ctx server.Context, tenantInfo server.TenantInfo, funcName string,
	funVersion, kind string,
) (response model.FunctionDeleteResponse, err error) {
	// start transaction
	txn := storage.GetTxnByKind(ctx, kind)
	defer txn.Cancel()

	// check whether the function exists
	fvs, err := storage.GetFunctionVersions(txn, funcName)
	if err != nil {
		log.GetLogger().Errorf("failed to get function version :%s", err.Error())
		return model.FunctionDeleteResponse{}, err
	}
	if len(fvs) > 0 {
		// use real kind type
		kind = fvs[0].FunctionVersion.Kind
	}
	if utils.IsLatestVersion(funVersion) {
		err = deleteAllFunctions(txn, funcName)
		if err != nil {
			log.GetLogger().Errorf("failed to delete functions :%s", err.Error())
			return model.FunctionDeleteResponse{}, err
		}
		publish.DeleteAllPublishFunction(txn, funcName, kind, tenantInfo)
		publish.DeleteTraceChainInfo(txn, funcName, tenantInfo)
		err := publish.DeleteAliasByFuncNameEtcd(txn, funcName)
		if err != nil {
			log.GetLogger().Errorf("failed to delete alias by function name :%s", err.Error())
			return model.FunctionDeleteResponse{}, err
		}
	} else {
		err = deleteFunctionByVersion(txn, funcName, funVersion)
		publish.DeletePublishFunction(txn, funcName, tenantInfo, funVersion, kind)
		if err != nil {
			log.GetLogger().Errorf("failed to delete publishings :%s", err.Error())
			return model.FunctionDeleteResponse{}, err
		}
	}
	// storage.FunctionVersion->model.FunctionValue
	fvLen := len(fvs)
	modelFvs := make([]model.FunctionVersion, fvLen)
	for i := 0; i < fvLen; i++ {
		modelFvs[i], err = buildFuncResult(ctx, fvs[i])
		if err != nil {
			return model.FunctionDeleteResponse{}, err
		}
	}
	err = txn.Commit()
	if err != nil {
		log.GetLogger().Errorf("failed to commit delete function :%s", err.Error())
		return model.FunctionDeleteResponse{}, err
	}

	return model.FunctionDeleteResponse{
		Total:    len(modelFvs),
		Versions: modelFvs,
	}, nil
}

// DeleteResponse delete function info
func DeleteResponse(ctx server.Context, funcName string,
	funVersion, kind string,
) error {
	// start transaction
	txn := storage.GetTxnByKind(ctx, kind)
	defer txn.Cancel()

	// check whether the function exists
	fvs, err := storage.GetFunctionVersions(txn, funcName)
	if err != nil {
		log.GetLogger().Errorf("failed to get function version :%s", err.Error())
		return err
	}

	err = canDelFunctionPackage(txn, funVersion, fvs)
	if err != nil {
		log.GetLogger().Errorf("failed to check function package :%s", err.Error())
		return err
	}
	return deletePackage(txn, ctx, fvs)
}

func canDelFunctionPackage(txn storage.Transaction, fv string, fvs []storage.FunctionVersionValue) error {
	for index, data := range fvs {
		var bucketID, objectID string
		// if the default function version is used, delete all functions
		if fv != utils.GetDefaultVersion() && fv != utils.GetDefaultFaaSVersion() {
			if data.FunctionVersion.Version != fv {
				fvs[index].Function.Name = constants.NilStringValue
				continue
			}

			bucketID = data.FunctionVersion.Package.BucketID
			objectID = data.FunctionVersion.Package.ObjectID

			referred, err := storage.IsObjectReferred(txn, bucketID, objectID)
			if err != nil {
				log.GetLogger().Errorf("failed to check object referred :%s", err.Error())
				return err
			}
			if referred {
				fvs[index].Function.Name = constants.NilStringValue
				continue
			}
		}
		err := storage.AddUncontrolledTx(txn, bucketID, objectID)
		if err != nil {
			log.GetLogger().Errorf("failed to add uncontrolled")
			return err
		}
	}
	return nil
}

func deleteFunctionPackage(ctx server.Context, fvs []storage.FunctionVersionValue) error {
	for _, v := range fvs {
		if v.Function.Name == constants.NilStringValue {
			continue
		}
		if v.FunctionVersion.Package.CodeUploadType != common.S3StorageType {
			continue
		}
		bucketID := v.FunctionVersion.Package.BucketID
		objectID := v.FunctionVersion.Package.ObjectID
		err := pkgstore.Delete(bucketID, objectID)
		if err != nil {
			log.GetLogger().Warnf("failed to delete bucket file, bucketId is %s, objectId is %s,err：%s",
				bucketID, objectID, err.Error())
		}
		// remove uncontrolled
		err = storage.RemoveUncontrolled(ctx, bucketID, objectID)
		if err != nil {
			log.GetLogger().Warnf("failed to remove uncontrolled package :%s ", err.Error())
		}
	}
	return nil
}

func deleteFunctionByVersion(txn storage.Transaction, funcName string, funVersion string) error {
	_, err := storage.GetAliasNumByFunctionName(txn, funcName)
	if err != nil {
		log.GetLogger().Errorf("failed to get alias num by function name version :%s", err.Error())
		return err
	}
	exist, err := storage.AliasRoutingExist(txn, funcName, funVersion)
	if err != nil {
		log.GetLogger().Errorf("failed to check alias routing :%s", err.Error())
		return err
	}
	if exist {
		log.GetLogger().Errorf("failed to check function alias route info "+
			"there is other alias list use the version %s", funVersion)
		return snerror.NewWithFmtMsg(errmsg.FunctionVersionDeletionForbidden,
			errmsg.ErrorMessage(errmsg.FunctionVersionDeletionForbidden))
	}

	err = storage.DeleteFunctionVersion(txn, funcName, funVersion)
	if err != nil {
		log.GetLogger().Errorf("failed to delete function version :%s", err.Error())
		return err
	}
	err = storage.DeleteFunctionStatus(txn, funcName, funVersion)
	if err != nil {
		log.GetLogger().Errorf("failed to delete function status :%s", err.Error())
		return err
	}

	if err := DeleteTriggerByFuncNameVersion(txn, funcName, funVersion); err != nil {
		log.GetLogger().Errorf("failed to delete trigger register info, error: %s", err.Error())
		return err
	}

	return nil
}

func deleteAllFunctions(txn storage.Transaction, funcName string) error {
	err := storage.DeleteFunctionVersions(txn, funcName)
	if err != nil {
		log.GetLogger().Errorf("failed to delete function versions :%s", err.Error())
		return err
	}
	err = storage.DeleteFunctionStatuses(txn, funcName)
	if err != nil {
		log.GetLogger().Errorf("failed to delete function status :%s", err.Error())
		return err
	}
	// delete alias
	err = storage.DeleteAliasByFunctionName(txn, funcName)
	if err != nil {
		log.GetLogger().Errorf("failed to delete alias by function name :%s", err.Error())
		return err
	}

	err = DeleteTriggerByFuncName(txn, funcName)
	if err != nil {
		log.GetLogger().Errorf("failed to delete trigger info by function name, error:%s", err.Error())
		return err
	}

	return nil
}

// GetFunction return function info
func GetFunction(ctx server.Context, req model.FunctionGetRequest) (resp model.FunctionGetResponse, err error) {
	if req.AliasName != "" {
		return getFunctionByAlias(ctx, req.FunctionName, req.AliasName, req.Kind)
	}
	return getFunctionByVersion(ctx, req.FunctionName, req.FunctionVersion, req.Kind)
}

// GetFunctionVersionList return function version list info
func GetFunctionVersionList(ctx server.Context, f string, pi int, ps int) (resp model.FunctionVersionListGetResponse,
	err error,
) {
	// start transaction
	txn := storage.NewTxn(ctx)
	defer txn.Cancel()
	tuples, total, err := storage.GetFunctionVersionList(ctx, f, "", pi, ps)
	if err != nil {
		log.GetLogger().Errorf("failed to get function version list :%s", err.Error())
		return model.FunctionVersionListGetResponse{}, err
	}
	versions := make([]model.FunctionVersion, len(tuples), len(tuples))
	for i, tuple := range tuples {
		versions[i] = buildFunctionVersionModel(tuple.Key, tuple.Value)
	}
	return model.FunctionVersionListGetResponse{
		Versions: versions,
		Total:    total,
	}, nil
}

// GetFunctionList return function list info
func GetFunctionList(c server.Context, f string, q string, k string,
	pi int, ps int,
) (model.FunctionListGetResponse, error) {
	// start transaction
	txn := storage.GetTxnByKind(c, k)
	defer txn.Cancel()
	tuples, total, err := storage.GetFunctionList(c, f, q, k, pi, ps)
	if err != nil {
		log.GetLogger().Errorf("failed to get function version list :%s", err.Error())
		return model.FunctionListGetResponse{}, nil
	}
	versions := make([]model.FunctionVersion, len(tuples), len(tuples))
	for i, tuple := range tuples {
		versions[i] = buildFunctionVersionModel(tuple.Key, tuple.Value)
	}
	return model.FunctionListGetResponse{
		Total:           total,
		FunctionVersion: versions,
	}, nil
}

func getFunctionByVersion(c server.Context, f string,
	v string, kind string,
) (model.FunctionGetResponse, error) {
	version := v
	if version == "" {
		if kind == constants.Faas {
			version = utils.GetDefaultFaaSVersion()
		} else {
			version = utils.GetDefaultVersion()
		}
	}
	var fv storage.FunctionVersionValue
	var err error
	fv, err = storage.GetFunctionByFunctionNameAndVersion(c, f, version, kind)
	if err != nil {
		log.GetLogger().Errorf("failed to get function by function name and version :%s", err.Error())
		return model.FunctionGetResponse{}, err
	}
	return buildGetFunctionResponse(c, fv, "")
}

func getFunctionByAlias(c server.Context, f string, a string,
	kind string,
) (model.FunctionGetResponse, error) {
	version := storage.GetFunctionByFunctionNameAndAlias(c, f, a)
	return getFunctionByVersion(c, f, version, kind)
}

func buildGetFunctionResponse(c server.Context, version storage.FunctionVersionValue,
	dl string,
) (model.FunctionGetResponse, error) {
	t, err := c.TenantInfo()
	if err != nil {
		log.GetLogger().Errorf("failed to get tenant info, error :%s", err.Error())
		return model.FunctionGetResponse{}, err
	}
	dateTime := buildTime(version)
	var function model.Function
	function = buildFunction(dateTime, t, version)

	env, err := utils.DecodeEnv(version.FunctionVersion.Environment)
	if err != nil {
		log.GetLogger().Errorf("failed to decode env :%s", err.Error())
		return model.FunctionGetResponse{}, err
	}
	var layerURNs []string
	for _, v := range version.FunctionLayer {
		layer, err := GetLayerVersion(c, v.Name, v.Version)
		if err != nil {
			log.GetLogger().Errorf("failed to get layer info :%s", err.Error())
			return model.FunctionGetResponse{}, err
		}
		layerURNs = append(layerURNs, layer.LayerVersionURN)
	}
	functionVersionURN := switchBuildFuncVersionURN(t, version)
	v := buildFunctionVersionInfo(function, functionVersionURN, version, layerURNs, env)
	return model.FunctionGetResponse{
		FunctionVersion: v,
		Created:         version.Function.CreateTime,
	}, nil
}

func switchBuildFuncVersionURN(t server.TenantInfo, version storage.FunctionVersionValue) string {
	return utils.BuildFunctionVersionURNWithTenant(t, version.Function.Name, version.FunctionVersion.Version)
}

func buildFunctionVersionInfo(function model.Function, functionVersionURN string,
	version storage.FunctionVersionValue, layerURNs []string, env map[string]string,
) model.FunctionVersion {
	v := model.FunctionVersion{
		Function:           function,
		FunctionVersionURN: functionVersionURN,
		RevisionID:         version.FunctionVersion.RevisionID,
		StorageType:        version.FunctionVersion.Package.StorageType,
		CodeSize:           version.FunctionVersion.Package.Size,
		CodeSha256:         version.FunctionVersion.Package.Signature,
		CodePath:           version.FunctionVersion.Package.CodePath,
		BucketID:           version.FunctionVersion.Package.BucketID,
		ObjectID:           version.FunctionVersion.Package.ObjectID,
		Handler:            version.FunctionVersion.Handler,
		Layers:             layerURNs,
		CPU:                version.FunctionVersion.CPU,
		Memory:             version.FunctionVersion.Memory,
		Runtime:            version.FunctionVersion.Runtime,
		Timeout:            version.FunctionVersion.Timeout,
		VersionNumber:      version.FunctionVersion.Version,
		VersionDesc:        version.FunctionVersion.Description,
		Environment:        env,
		StatefulFlag:       version.FunctionVersion.StatefulFlag,
		LastModified:       version.Function.UpdateTime,
		Published:          version.FunctionVersion.PublishTime,
		MinInstance:        version.FunctionVersion.MinInstance,
		MaxInstance:        version.FunctionVersion.MaxInstance,
		ConcurrentNum:      version.FunctionVersion.ConcurrentNum,
		EnableMetrics:      version.FunctionVersion.EnableMetrics,
		Status:             version.FunctionVersion.Status,
		InstanceNum:        version.FunctionVersion.InstanceNum,
		Device:             version.FunctionVersion.Device,
		Kind:               version.FunctionVersion.Kind,
		RootfsSpecMeta:     version.FunctionVersion.RootfsSpecMeta,
	}
	if version.FunctionVersion.CustomResources != "" {
		err := json.Unmarshal([]byte(version.FunctionVersion.CustomResources), &v.CustomResources)
		if err != nil {
			// ignore customResource unmarshal error
			log.GetLogger().Warnf("failed to unmarshal customResources :%s", err.Error())
		}
	}
	return v
}

func buildFunction(dateTime model.DateTime, t server.TenantInfo,
	version storage.FunctionVersionValue,
) model.Function {
	function := model.Function{
		DateTime:            dateTime,
		FunctionURN:         utils.BuildFunctionURNWithTenant(t, version.Function.Name),
		FunctionName:        version.Function.Name,
		TenantID:            t.TenantID,
		BusinessID:          t.BusinessID,
		ProductID:           t.ProductID,
		ReversedConcurrency: version.Function.ReversedConcurrency,
		Description:         version.Function.Description,
		Tag:                 version.Function.Tag,
	}
	return function
}

func buildTime(version storage.FunctionVersionValue) model.DateTime {
	dateTime := model.DateTime{
		CreateTime: version.Function.CreateTime,
		UpdateTime: version.Function.UpdateTime,
	}
	return dateTime
}

// GetUploadFunctionCodeResponse is upload function package
func GetUploadFunctionCodeResponse(ctx server.Context, info model.FunctionQueryInfo,
	req model.FunctionCodeUploadRequest,
) (model.FunctionVersion, error) {
	name := utils.RemoveServiceID(info.FunctionName) + "-" +
		strconv.FormatInt(timeutil.NowUnixMillisecond(), formatIntBase)
	tempFile, err := getUploadFile(ctx)
	if err != nil {
		log.GetLogger().Errorf("failed to get package :%s", err.Error())
		return model.FunctionVersion{}, err
	}
	pkg, err := pkgstore.NewPackage(ctx, name, tempFile, req.FileSize)
	if err != nil {
		log.GetLogger().Errorf("failed to new package :%s", err.Error())
		return model.FunctionVersion{}, err
	}
	defer pkg.Close()

	// start transaction
	txn := storage.GetTxnByKind(ctx, req.Kind)
	defer txn.Cancel()

	fv, err := storage.GetFunctionVersion(txn, info.FunctionName, info.FunctionVersion)
	if err != nil {
		log.GetLogger().Errorf("failed to get function name by function name and version :%s", err.Error())
		return model.FunctionVersion{}, err
	}
	if fv.FunctionVersion.RevisionID != req.RevisionID {
		return model.FunctionVersion{}, errmsg.New(errmsg.RevisionIDError)
	}

	uploader, err := pkgstore.NewUploader(ctx, pkg)
	if err != nil {
		log.GetLogger().Errorf("failed to new uploader :%s", err.Error())
		return model.FunctionVersion{}, err
	}

	uInfo := newUncontrolled(ctx, uploader.BucketID(), uploader.ObjectID())
	if err = storage.AddUncontrolled(ctx, uploader.BucketID(), uploader.ObjectID()); err != nil {
		log.GetLogger().Errorf("failed to add uncontrolled :%s", err.Error())
		return model.FunctionVersion{}, err
	}
	var rollback bool
	defer removeUncontrolledInfos(uInfo, rollback)
	if err = upload(uploader); err != nil {
		return model.FunctionVersion{}, err
	}
	oldBucketID, oldObjectID := updateFunctionPackageInfo(&fv, pkg, uploader)
	resp, err := getUploadFunctionCodeHelper(txn, fv, oldBucketID, oldObjectID)
	if err != nil {
		return failUploadFunctionCodeHelper(ctx, uploader, rollback, err)
	}

	return resp, nil
}

func getUploadFile(ctx server.Context) (io.Reader, error) {
	contentType := ctx.Gin().Request.Header.Get(common.HeaderDataContentType)
	ctx.Gin().Request.Header.Set("Content-Type", contentType)
	err := ctx.Gin().Request.ParseMultipartForm(common.MaxUploadMemorySize)
	if err != nil {
		return nil, err
	}
	if len(ctx.Gin().Request.MultipartForm.File["file"]) < 1 {
		log.GetLogger().Errorf("failed to get MultipartForm file")
		return nil, errors.New("multipartForm is empty")
	}
	tmpFile, err := ctx.Gin().Request.MultipartForm.File["file"][0].Open()
	if err != nil {
		return nil, err
	}
	return tmpFile, nil
}

func failUploadFunctionCodeHelper(ctx server.Context, uploader pkgstore.Uploader,
	rollback bool, failErr error,
) (model.FunctionVersion, error) {
	log.GetLogger().Errorf("failed to get upload function code :%s", failErr.Error())
	if e := uploader.Rollback(); e == nil {
		storage.RemoveUncontrolled(ctx, uploader.BucketID(), uploader.ObjectID())
	} else {
		rollback = true
	}
	return model.FunctionVersion{}, failErr
}

func removeUncontrolledInfos(v uncontrolledInfo, rollback bool) {
	if !rollback {
		err := storage.RemoveUncontrolled(v.ctx, v.bucketID, v.objectID)
		if err != nil {
			log.GetLogger().Warnf("failed to remove uncontrolled :%s", err.Error())
		}
	}
}

func newUncontrolled(ctx server.Context, bucketID string, objectID string) uncontrolledInfo {
	return uncontrolledInfo{
		ctx:      ctx,
		bucketID: bucketID,
		objectID: objectID,
	}
}

func updateFunctionPackageInfo(fv *storage.FunctionVersionValue, pkg pkgstore.Package,
	uploader pkgstore.Uploader,
) (string, string) {
	oldBucketID := fv.FunctionVersion.Package.BucketID
	oldObjectID := fv.FunctionVersion.Package.ObjectID
	fv.FunctionVersion.Package.Signature = pkg.Signature()
	fv.FunctionVersion.Package.BucketID = uploader.BucketID()
	fv.FunctionVersion.Package.ObjectID = uploader.ObjectID()
	fv.FunctionVersion.Package.Size = pkg.Size()
	return oldBucketID, oldObjectID
}

func upload(uploader pkgstore.Uploader) error {
	if err := uploader.Upload(); err != nil {
		log.GetLogger().Errorf("failed to upload function package :%s", err.Error())
		return err
	}
	return nil
}

func getUploadFunctionCodeHelper(txn storage.Transaction, fv storage.FunctionVersionValue,
	oldBucketID, oldObjectID string,
) (model.FunctionVersion, error) {
	err := storage.UpdateFunctionVersion(txn, fv)
	if err != nil {
		log.GetLogger().Errorf("failed to update function version :%s", err.Error())
		return model.FunctionVersion{}, err
	}

	if err := publish.SavePublishFuncVersion(txn, fv); err != nil {
		log.GetLogger().Errorf("failed to savePublishFuncVersion :%s", err.Error())
		return model.FunctionVersion{}, err
	}

	// tries to delete old
	if oldBucketID != "" && oldObjectID != "" {
		ref, err := storage.IsObjectReferred(txn, oldBucketID, oldObjectID)
		if err != nil {
			log.GetLogger().Warnf("failed to check object referred :%s", err.Error())
		}
		if err == nil && !ref {
			err = pkgstore.Delete(oldBucketID, oldObjectID)
			if err != nil {
				log.GetLogger().Errorf("failed to delete function package ：%s", err.Error())
			}
		}
	}
	if err := txn.Commit(); err != nil {
		log.GetLogger().Errorf("failed to commit upload info :%s")
		return model.FunctionVersion{}, err
	}

	tenantInfo, err := txn.GetCtx().TenantInfo()
	if err != nil {
		log.GetLogger().Errorf("failed to get tenantInfo :%s", err.Error())
		return model.FunctionVersion{}, err
	}
	key := storage.FunctionVersionKey{
		TenantInfo:      tenantInfo,
		FunctionName:    fv.Function.Name,
		FunctionVersion: fv.FunctionVersion.Version,
	}
	return buildFunctionVersionModel(key, fv), nil
}

func buildEnvironment(env *map[string]string, v storage.FunctionVersionValue) {
	if len(v.FunctionVersion.Environment) != 0 {
		value, err := utils.DecryptETCDValue(v.FunctionVersion.Environment)
		if err != nil {
			log.GetLogger().Warnf("failed to decrypt ETCD value :%s", err.Error())
		}
		if value != nilMapValue {
			err = json.Unmarshal([]byte(value), &env)
			if err != nil {
				log.GetLogger().Warnf("failed to unmarshal env when Convert2Model :%s", err.Error())
			}
		}
	}
}

func buildFunctionVersionModel(key storage.FunctionVersionKey,
	v storage.FunctionVersionValue,
) model.FunctionVersion {
	env := make(map[string]string, common.DefaultMapSize)
	buildEnvironment(&env, v)
	funcVer := buildFunctionVersionEntity(key, v, env)
	funcVer.FuncLayer = make([]model.FunctionLayer, len(v.FunctionLayer), len(v.FunctionLayer))
	for i, l := range v.FunctionLayer {
		funcVer.FuncLayer[i].Name = l.Name
		funcVer.FuncLayer[i].Version = l.Version
		funcVer.FuncLayer[i].Order = l.Order
	}
	return funcVer
}

func functionEntity(key storage.FunctionVersionKey,
	v storage.FunctionVersionValue,
) (string, model.Function) {
	var function model.Function
	function = model.Function{
		FunctionName:        v.Function.Name,
		TenantID:            key.TenantID,
		BusinessID:          key.BusinessID,
		ProductID:           key.ProductID,
		ReversedConcurrency: v.Function.ReversedConcurrency,
		Description:         v.Function.Description,
		Tag:                 v.Function.Tag,
		FunctionURN:         utils.BuildFunctionURN(key.BusinessID, key.TenantID, key.ProductID, v.Function.Name),
		DateTime: model.DateTime{
			CreateTime: v.Function.CreateTime,
			UpdateTime: v.Function.UpdateTime,
		},
	}
	functionVersionURN := utils.BuildFunctionVersionURN(key.BusinessID, key.TenantID, key.ProductID,
		v.Function.Name, v.FunctionVersion.Version)
	return functionVersionURN, function
}

func buildFunctionVersionEntity(key storage.FunctionVersionKey, v storage.FunctionVersionValue,
	env map[string]string,
) model.FunctionVersion {
	functionVersionURN, function := functionEntity(key, v)
	funcVer := model.FunctionVersion{
		Function:           function,
		CodeSize:           v.FunctionVersion.Package.Size,
		CodeSha256:         v.FunctionVersion.Package.Signature,
		RevisionID:         v.FunctionVersion.RevisionID,
		Handler:            v.FunctionVersion.Handler,
		CPU:                v.FunctionVersion.CPU,
		Memory:             v.FunctionVersion.Memory,
		Runtime:            v.FunctionVersion.Runtime,
		Timeout:            v.FunctionVersion.Timeout,
		VersionNumber:      v.FunctionVersion.Version,
		Environment:        env,
		BucketID:           v.FunctionVersion.Package.BucketID,
		ObjectID:           v.FunctionVersion.Package.ObjectID,
		VersionDesc:        v.FunctionVersion.Description,
		StatefulFlag:       v.FunctionVersion.StatefulFlag,
		Published:          v.FunctionVersion.PublishTime,
		MinInstance:        v.FunctionVersion.MinInstance,
		MaxInstance:        v.FunctionVersion.MaxInstance,
		ConcurrentNum:      v.FunctionVersion.ConcurrentNum,
		EnableMetrics:      v.FunctionVersion.EnableMetrics,
		FunctionVersionURN: functionVersionURN,
		Device:             v.FunctionVersion.Device,
		Kind:               v.FunctionVersion.Kind,
		RootfsSpecMeta:     v.FunctionVersion.RootfsSpecMeta,
		PriorityAZ:         v.FunctionVersion.PriorityAZ,
	}
	return funcVer
}

// ValidateRootfsSpec validates rootfs specification metadata with the following rules:
// 1. If all fields are empty, validation passes
// 2. If path is set, type must be 'local' and vice versa
// 3. If imageurl is set, type must be 'image' and vice versa
// 4. If type is 's3', StorageInfo.Endpoint, Bucket, Object must all be set
// 5. If any field is set but runtime is empty, runtime defaults to 'runsc'
func ValidateRootfsSpec(rootfs *types.RootfsSpecMeta) error {
	if rootfs == nil {
		return nil
	}

	// Check if rootfs is completely empty
	hasPath := rootfs.Path != ""
	hasImageURL := rootfs.ImageURL != ""
	hasStorageInfo := rootfs.StorageInfo.Endpoint != "" || rootfs.StorageInfo.Bucket != "" ||
		rootfs.StorageInfo.Object != "" || rootfs.StorageInfo.AccessKey != "" || rootfs.StorageInfo.SecretKey != ""
	hasType := rootfs.Type != ""
	hasMountPoint := rootfs.MountPoint != ""
	hasReadOnly := rootfs.ReadOnly

	// Rule 1: If all fields are empty, validation passes
	if !hasPath && !hasImageURL && !hasStorageInfo && !hasType && !hasMountPoint && !hasReadOnly && rootfs.Runtime == "" {
		return nil
	}

	// Rule 5: If any field is set but runtime is empty, default to 'runsc'
	if (hasPath || hasImageURL || hasStorageInfo || hasType || hasMountPoint) && rootfs.Runtime == "" {
		rootfs.Runtime = "runsc"
		log.GetLogger().Infof("rootfs runtime not specified, defaulting to 'runsc'")
	}

	// Rule 2: Path and Type validation (path requires type='local')
	if hasPath {
		if rootfs.Type != "local" {
			log.GetLogger().Errorf("rootfs path is set, but type is not 'local': %s", rootfs.Type)
			return errmsg.NewParamError("when rootfs path is set, type must be 'local'")
		}
	}
	if rootfs.Type == "local" && !hasPath {
		log.GetLogger().Errorf("rootfs type is 'local', but path is not set")
		return errmsg.NewParamError("when rootfs type is 'local', path must be set")
	}

	// Rule 3: ImageURL and Type validation (imageurl requires type='image')
	if hasImageURL {
		if rootfs.Type != "image" {
			log.GetLogger().Errorf("rootfs imageurl is set, but type is not 'image': %s", rootfs.Type)
			return errmsg.NewParamError("when rootfs imageurl is set, type must be 'image'")
		}
	}
	if rootfs.Type == "image" && !hasImageURL {
		log.GetLogger().Errorf("rootfs type is 'image', but imageurl is not set")
		return errmsg.NewParamError("when rootfs type is 'image', imageurl must be set")
	}

	// Rule 4: S3 configuration validation
	if rootfs.Type == "s3" {
		if rootfs.StorageInfo.Endpoint == "" {
			log.GetLogger().Errorf("rootfs type is 's3', but endpoint is not set")
			return errmsg.NewParamError("when rootfs type is 's3', storageInfo.endpoint must be set")
		}
		if rootfs.StorageInfo.Bucket == "" {
			log.GetLogger().Errorf("rootfs type is 's3', but bucket is not set")
			return errmsg.NewParamError("when rootfs type is 's3', storageInfo.bucket must be set")
		}
		if rootfs.StorageInfo.Object == "" {
			log.GetLogger().Errorf("rootfs type is 's3', but object is not set")
			return errmsg.NewParamError("when rootfs type is 's3', storageInfo.object must be set")
		}
	}

	// Validate that S3 storageInfo is only set when type is 's3'
	if hasStorageInfo && rootfs.Type != "s3" {
		log.GetLogger().Errorf("rootfs storageInfo is set, but type is not 's3': %s", rootfs.Type)
		return errmsg.NewParamError("storageInfo can only be set when rootfs type is 's3'")
	}

	// Validate type value
	if hasType && rootfs.Type != "local" && rootfs.Type != "image" && rootfs.Type != "s3" {
		log.GetLogger().Errorf("invalid rootfs type: %s, must be 'local', 'image', or 's3'", rootfs.Type)
		return errmsg.NewParamError(fmt.Sprintf("invalid rootfs type: %s, must be 'local', 'image', or 's3'", rootfs.Type))
	}

	return nil
}
