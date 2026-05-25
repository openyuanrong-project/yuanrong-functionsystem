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

package publish

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"

	common "meta_service/common/constants"
	"meta_service/common/functionhandler"
	"meta_service/common/logger/log"
	"meta_service/common/metadata"
	"meta_service/common/types"
	"meta_service/function_repo/config"
	"meta_service/function_repo/errmsg"
	"meta_service/function_repo/model"
	"meta_service/function_repo/pkgstore"
	"meta_service/function_repo/server"
	"meta_service/function_repo/storage"
	"meta_service/function_repo/utils"
	"meta_service/function_repo/utils/constants"
)

const (
	// the length of envMap
	envMapLength = 50
	// default init handler timeout
	defaultInitializerTimeout = 30
	// default pre_stop handler timeout
	defaultPreStopTimeout = 0
	maxPreStopTimeout     = 180
)

// AddTrigger published a create event of trigger by function name, version and trigger id in a transaction.
func AddTrigger(txn *storage.Txn, funcName, verOrAlias string, info storage.TriggerValue, value interface{}) error {
	ctx := txn.GetCtx()
	t, err := ctx.TenantInfo()
	if err != nil {
		log.GetLogger().Errorf("failed to get tenant info, error: %s", err.Error())
		return err
	}
	tKey := BuildTriggerRegisterKey(t, funcName, info.TriggerType, verOrAlias, info.TriggerID)
	b, err := json.Marshal(value)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal version value: %s", err.Error())
		return errmsg.MarshalError
	}
	txn.Put(tKey, string(b))
	return nil
}

// DeleteTrigger published a delete event of trigger by function name, version and trigger id in a transaction.
func DeleteTrigger(txn storage.Transaction, funcName, verOrAlias string, info storage.TriggerValue) error {
	ctx := txn.GetCtx()
	t, err := ctx.TenantInfo()
	if err != nil {
		log.GetLogger().Errorf("failed to get tenant info, error: %s", err.Error())
		return err
	}
	tKey := BuildTriggerRegisterKey(t, funcName, info.TriggerType, verOrAlias, info.TriggerID)
	txn.Delete(tKey)
	return nil
}

// DeleteTriggerByFuncName published a delete event of trigger by function name in a transaction.
func DeleteTriggerByFuncName(txn storage.Transaction, funcName string) error {
	ctx := txn.GetCtx()
	t, err := ctx.TenantInfo()
	if err != nil {
		log.GetLogger().Errorf("failed to get tenant info, error: %s", err.Error())
		return err
	}
	tKey := BuildTriggerRegisterKey(t, funcName, model.HTTPType, "", "")
	txn.DeleteRange(tKey)
	return nil
}

// SavePublishFuncVersion save the published function version value
func SavePublishFuncVersion(txn storage.Transaction,
	funcVersionValue storage.FunctionVersionValue,
) error {
	info, err := txn.GetCtx().TenantInfo()
	if err != nil {
		log.GetLogger().Errorf("failed to get tenantInfo error: %s", err.Error())
		return err
	}

	key := BuildFunctionRegisterKey(info, funcVersionValue.Function.Name,
		funcVersionValue.FunctionVersion.Version, funcVersionValue.FunctionVersion.Kind)
	value, err := getVersionValue(info, txn, funcVersionValue)
	if err != nil {
		return err
	}

	txn.Put(key, string(value))
	return nil
}

func getVersionValue(info server.TenantInfo, txn storage.Transaction,
	funcVersionValue storage.FunctionVersionValue,
) ([]byte, error) {
	var (
		value interface{}
		err   error
	)
	if funcVersionValue.FunctionVersion.Kind == common.Faas {
		value, err = buildFaaSFunctionVersionValue(txn, funcVersionValue, info)
		if err != nil {
			log.GetLogger().Errorf("failed to buildFaaSFunctionVersionValue: %s", err.Error())
			return nil, err
		}
	} else {
		value, err = buildFunctionVersionValue(txn, funcVersionValue, info)
		if err != nil {
			log.GetLogger().Errorf("failed to buildFunctionVersionValue: %s", err.Error())
			return nil, err
		}
	}
	versionValue, err := json.Marshal(value)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal version value: %s", err.Error())
		return nil, errmsg.MarshalError
	}
	return versionValue, nil
}

func buildFaaSFunctionVersionValue(txn storage.Transaction, fv storage.FunctionVersionValue,
	tenantInfo server.TenantInfo,
) (metadata.FaaSFuncMeta, error) {
	var info metadata.FaaSFuncMeta
	if err := buildEnv(fv, &info.EnvMetaData); err != nil {
		log.GetLogger().Errorf("failed to build environment: %s", err.Error())
		return info, err
	}
	return buildFaaSFuncMetaData(txn, fv, tenantInfo, info)
}

func buildFaaSFuncMetaData(txn storage.Transaction, fv storage.FunctionVersionValue, tenantInfo server.TenantInfo,
	info metadata.FaaSFuncMeta,
) (metadata.FaaSFuncMeta, error) {
	var err error
	info.FuncMetaData.Name = fv.Function.Name
	info.FuncMetaData.TenantID = tenantInfo.TenantID
	info.FuncMetaData.BusinessID = tenantInfo.BusinessID
	info.FuncMetaData.FunctionDescription = fv.Function.Description
	info.FuncMetaData.FunctionURN, tenantInfo.TenantID = buildFunctionURN(tenantInfo, fv)
	info.FuncMetaData.FunctionVersionURN = utils.BuildFunctionVersionURN(tenantInfo.BusinessID, tenantInfo.TenantID,
		tenantInfo.ProductID, fv.Function.Name, fv.FunctionVersion.Version)
	info.FuncMetaData.ReversedConcurrency = fv.Function.ReversedConcurrency
	info.FuncMetaData.CreationTime = fv.Function.CreateTime
	info.FuncMetaData.Handler = fv.FunctionVersion.Handler
	info.FuncMetaData.Runtime = fv.FunctionVersion.Runtime
	info.FuncMetaData.Tags = fv.Function.Tag
	info.FuncMetaData.RevisionID = fv.FunctionVersion.RevisionID
	info.FuncMetaData.CodeSize = int(fv.FunctionVersion.Package.Size)
	info.FuncMetaData.CodeSha512 = fv.FunctionVersion.Package.Signature
	info.FuncMetaData.Timeout = fv.FunctionVersion.Timeout
	info.FuncMetaData.Version = fv.FunctionVersion.Version
	info.FuncMetaData.FuncName = fv.FunctionVersion.FuncName
	info.FuncMetaData.Service = fv.FunctionVersion.Service
	info.FuncMetaData.VersionDescription = fv.FunctionVersion.Description
	info.FuncMetaData.IdleTime = fv.FunctionVersion.IdleTime
	info.FuncMetaData.IsFuncPublic = fv.FunctionVersion.IsFuncPublic
	info.FuncMetaData.AutoScaleConfig = metadata.AutoScaleConfig{
		SLAQuota:      fv.FunctionVersion.AutoScaleConfig.SLAQuota,
		ScaleDownTime: fv.FunctionVersion.AutoScaleConfig.ScaleDownTime,
		BurstScaleNum: fv.FunctionVersion.AutoScaleConfig.BurstScaleNum,
	}
	info.FuncMetaData.IsStatefulFunction = fv.FunctionVersion.StatefulFlag != 0
	info.FuncMetaData.Layers, err = getFaaSLayerBucket(storage.GetTxnByKind(txn.GetCtx(), ""),
		fv.FunctionLayer, tenantInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to get bucket layer info: %s", err.Error())
		return metadata.FaaSFuncMeta{}, err
	}
	info.FuncMetaData.IsBridgeFunction = false
	info.FuncMetaData.EnableAuthInHeader = false
	info.CodeMetaData, err = buildCodeMetaData(fv, tenantInfo.BusinessID)
	if err != nil {
		log.GetLogger().Errorf("failed to get bucket layer info: %s", err.Error())
		return metadata.FaaSFuncMeta{}, err
	}
	info.ResourceMetaData.CPU = fv.FunctionVersion.CPU
	info.ResourceMetaData.Memory = fv.FunctionVersion.Memory
	info.ResourceMetaData.CustomResources = fv.FunctionVersion.CustomResources
	info.ResourceMetaData.EnableDynamicMemory = false
	info.ExtendedMetaData.Initializer = getInitializer(fv.FunctionVersion.ExtendedHandler,
		fv.FunctionVersion.ExtendedTimeout)
	info.ExtendedMetaData.PreStop = getPreStop(fv.FunctionVersion.ExtendedHandler,
		fv.FunctionVersion.ExtendedTimeout)
	err = checkPreStopTime(info.ExtendedMetaData.PreStop)
	if err != nil {
		return metadata.FaaSFuncMeta{}, err
	}
	info.ExtendedMetaData.PriorityAZ = fv.FunctionVersion.PriorityAZ
	info.ExtendedMetaData.EnableMetrics = fv.FunctionVersion.EnableMetrics
	info.ExtendedMetaData.EnableAgentSession = fv.FunctionVersion.EnableAgentSession
	info.ExtendedMetaData.CustomContainerConfig = metadata.CustomContainerConfig{
		ControlPath: fv.FunctionVersion.CustomContainerConfig.ControlPath,
		Image:       fv.FunctionVersion.CustomContainerConfig.Image,
		Command:     fv.FunctionVersion.CustomContainerConfig.Command,
		Args:        fv.FunctionVersion.CustomContainerConfig.Args,
		WorkingDir:  fv.FunctionVersion.CustomContainerConfig.WorkingDir,
		UID:         fv.FunctionVersion.CustomContainerConfig.UID,
		GID:         fv.FunctionVersion.CustomContainerConfig.GID,
	}
	info.ExtendedMetaData.CustomHealthCheck = metadata.CustomHealthCheck{
		TimeoutSeconds:   fv.FunctionVersion.CustomHealthCheck.TimeoutSeconds,
		PeriodSeconds:    fv.FunctionVersion.CustomHealthCheck.PeriodSeconds,
		FailureThreshold: fv.FunctionVersion.CustomHealthCheck.FailureThreshold,
	}
	err = buildFaaSInstanceMetaData(txn, fv, tenantInfo, &info.InstanceMetaData)
	if err != nil {
		log.GetLogger().Errorf("failed to build faas instance meta data, error: %s", err.Error())
		return metadata.FaaSFuncMeta{}, err
	}
	info.RootfsSpecMeta = buildRootFsSpecMeta(fv.FunctionVersion.RootfsSpecMeta)
	return info, nil
}

func buildFaaSInstanceMetaData(txn storage.Transaction, fv storage.FunctionVersionValue, tenantInfo server.TenantInfo,
	instance *metadata.FaaSInstanceMetaData,
) error {
	instance.MaxInstance = fv.FunctionVersion.MaxInstance
	instance.MinInstance = fv.FunctionVersion.MinInstance
	instance.ConcurrentNum = fv.FunctionVersion.ConcurrentNum
	instance.PoolID = fv.FunctionVersion.PoolID
	instance.PoolLabel = fv.FunctionVersion.PoolLabel
	instance.IdleMode = false
	instance.ScalePolicy = fv.FunctionVersion.ScalePolicy
	instance.SchedulePolicy = fv.FunctionVersion.SchedulePolicy
	key := BuildInstanceRegisterKey(tenantInfo, fv.Function.Name,
		fv.FunctionVersion.Version, constants.DefaultClusterID)
	data := make(map[string]interface{})
	data["instanceMetaData"] = *instance
	value, err := json.Marshal(data)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal version value: %s", err.Error())
		return err
	}
	txn.Put(key, string(value))
	return nil
}

func getFaaSLayerBucket(txn storage.Transaction, layers []storage.FunctionLayer,
	info server.TenantInfo,
) ([]*metadata.FaaSLayer, error) {
	res := make([]*metadata.FaaSLayer, len(layers), len(layers))
	for i, layer := range layers {
		val, bucket, err := getBucketInfo(txn, layer, info)
		if err != nil {
			return nil, err
		}
		res[i] = &metadata.FaaSLayer{
			BucketURL: bucket.URL,
			ObjectID:  val.Package.ObjectID,
			BucketID:  val.Package.BucketID,
			AppID:     bucket.AppID,
			Sha256:    val.Package.Signature,
		}
	}
	return res, nil
}

func getInitializer(extendHandler map[string]string, extendTimeout map[string]int) metadata.Initializer {
	if len(extendHandler) == 0 {
		return metadata.Initializer{}
	}
	initHandler, ok := extendHandler[functionhandler.ExtendedInitializer]
	if !ok {
		return metadata.Initializer{}
	}

	if len(extendTimeout) == 0 {
		return metadata.Initializer{Handler: initHandler, Timeout: int64(defaultInitializerTimeout)}
	}
	initTimeout, ok := extendTimeout[functionhandler.ExtendedInitializer]
	if !ok {
		return metadata.Initializer{Handler: initHandler, Timeout: int64(defaultInitializerTimeout)}
	}

	return metadata.Initializer{
		Handler: initHandler,
		Timeout: int64(initTimeout),
	}
}

func getPreStop(extendHandler map[string]string, extendTimeout map[string]int) metadata.PreStop {
	if len(extendHandler) == 0 {
		return metadata.PreStop{}
	}
	preStopHandler, ok := extendHandler[functionhandler.ExtendedPreStop]
	if !ok {
		return metadata.PreStop{}
	}

	if len(extendTimeout) == 0 {
		return metadata.PreStop{Handler: preStopHandler, Timeout: int64(defaultPreStopTimeout)}
	}

	preStopTimeout, ok := extendTimeout[functionhandler.ExtendedPreStop]
	if !ok {
		return metadata.PreStop{Handler: preStopHandler, Timeout: int64(defaultPreStopTimeout)}
	}

	return metadata.PreStop{
		Handler: preStopHandler,
		Timeout: int64(preStopTimeout),
	}
}

func checkPreStopTime(preStop metadata.PreStop) error {
	if preStop.Timeout > maxPreStopTimeout || preStop.Timeout < defaultPreStopTimeout {
		return errors.New("preStop timeOut must between 0 and 180")
	}
	return nil
}

func buildFunctionVersionValue(txn storage.Transaction, fv storage.FunctionVersionValue,
	tenantInfo server.TenantInfo,
) (metadata.Function, error) {
	var info metadata.Function
	if err := buildEnv(fv, &info.EnvMetaData); err != nil {
		log.GetLogger().Errorf("failed to build environment: %s", err.Error())
		return info, err
	}
	return buildFuncMetaData(txn, fv, tenantInfo, info)
}

func buildFuncMetaData(txn storage.Transaction, fv storage.FunctionVersionValue, tenantInfo server.TenantInfo,
	info metadata.Function,
) (metadata.Function, error) {
	info.FuncMetaData.Name = fv.Function.Name
	info.FuncMetaData.FunctionDescription = fv.Function.Description
	info.FuncMetaData.FunctionURN, tenantInfo.TenantID = buildFunctionURN(tenantInfo, fv)
	info.FuncMetaData.TenantID = tenantInfo.TenantID
	info.FuncMetaData.BusinessID = tenantInfo.BusinessID
	info.FuncMetaData.FunctionVersionURN = utils.BuildFunctionVersionURN(tenantInfo.BusinessID, tenantInfo.TenantID,
		tenantInfo.ProductID, fv.Function.Name, fv.FunctionVersion.Version)
	info.FuncMetaData.ReversedConcurrency = fv.Function.ReversedConcurrency
	info.FuncMetaData.CreationTime = fv.Function.CreateTime
	info.FuncMetaData.Tags = fv.Function.Tag
	info.FuncMetaData.RevisionID = fv.FunctionVersion.RevisionID
	info.FuncMetaData.CodeSize = fv.FunctionVersion.Package.Size
	info.FuncMetaData.CodeSha512 = fv.FunctionVersion.Package.Signature
	info.FuncMetaData.Handler = fv.FunctionVersion.Handler
	info.ResourceMetaData.CPU = fv.FunctionVersion.CPU
	info.ResourceMetaData.Memory = fv.FunctionVersion.Memory
	info.ResourceMetaData.CustomResources = fv.FunctionVersion.CustomResources
	info.FuncMetaData.Runtime = fv.FunctionVersion.Runtime
	info.FuncMetaData.Timeout = fv.FunctionVersion.Timeout
	info.FuncMetaData.Version = fv.FunctionVersion.Version
	info.FuncMetaData.VersionDescription = fv.FunctionVersion.Description
	info.FuncMetaData.StatefulFlag = fv.FunctionVersion.StatefulFlag != 0
	info.FuncMetaData.HookHandler = fv.FunctionVersion.HookHandler
	info.FuncMetaData.IdleTime = fv.FunctionVersion.IdleTime
	info.FuncMetaData.IsFuncPublic = fv.FunctionVersion.IsFuncPublic
	info.WarmupType = fv.FunctionVersion.WarmupType
	info.RootfsSpecMeta = buildRootFsSpecMeta(fv.FunctionVersion.RootfsSpecMeta)
	var err error
	info.CodeMetaData, err = buildCodeMetaData(fv, tenantInfo.BusinessID)
	if err != nil {
		log.GetLogger().Errorf("failed to get bucket layer info: %s", err.Error())
		return metadata.Function{}, err
	}
	info.FuncMetaData.Layers, err = getLayerBucket(txn, fv.FunctionLayer, tenantInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to get bucket layer info: %s", err.Error())
		return metadata.Function{}, err
	}
	info.ExtendedMetaData.InstanceMetaData.MaxInstance = fv.FunctionVersion.MaxInstance
	info.ExtendedMetaData.InstanceMetaData.MinInstance = fv.FunctionVersion.MinInstance
	info.ExtendedMetaData.InstanceMetaData.ConcurrentNum = fv.FunctionVersion.ConcurrentNum
	info.ExtendedMetaData.InstanceMetaData.CacheInstance = fv.FunctionVersion.CacheInstance
	info.ExtendedMetaData.ExtendedHandler = fv.FunctionVersion.ExtendedHandler
	info.ExtendedMetaData.ExtendedTimeout = fv.FunctionVersion.ExtendedTimeout
	info.ExtendedMetaData.Device = fv.FunctionVersion.Device
	info.ExtendedMetaData.PriorityAZ = fv.FunctionVersion.PriorityAZ
	info.ExtendedMetaData.EnableMetrics = fv.FunctionVersion.EnableMetrics
	info.ExtendedMetaData.EnableAgentSession = fv.FunctionVersion.EnableAgentSession
	info.ExtendedMetaData.CustomContainerConfig = metadata.CustomContainerConfig{
		ControlPath: fv.FunctionVersion.CustomContainerConfig.ControlPath,
		Image:       fv.FunctionVersion.CustomContainerConfig.Image,
		Command:     fv.FunctionVersion.CustomContainerConfig.Command,
		Args:        fv.FunctionVersion.CustomContainerConfig.Args,
		WorkingDir:  fv.FunctionVersion.CustomContainerConfig.WorkingDir,
		UID:         fv.FunctionVersion.CustomContainerConfig.UID,
		GID:         fv.FunctionVersion.CustomContainerConfig.GID,
	}
	info.ExtendedMetaData.CustomHealthCheck = metadata.CustomHealthCheck{
		TimeoutSeconds:   fv.FunctionVersion.CustomHealthCheck.TimeoutSeconds,
		PeriodSeconds:    fv.FunctionVersion.CustomHealthCheck.PeriodSeconds,
		FailureThreshold: fv.FunctionVersion.CustomHealthCheck.FailureThreshold,
	}
	return info, nil
}

func buildRootFsSpecMeta(rootfs types.RootfsSpecMeta) metadata.RootfsSpecMeta {
	return metadata.RootfsSpecMeta{
		Runtime:  rootfs.Runtime,
		Type:     rootfs.Type,
		ImageURL: rootfs.ImageURL,
		ReadOnly: rootfs.ReadOnly,
		StorageInfo: metadata.RootfsStorageInfo{
			Endpoint:  rootfs.StorageInfo.Endpoint,
			Bucket:    rootfs.StorageInfo.Bucket,
			Object:    rootfs.StorageInfo.Object,
			AccessKey: rootfs.StorageInfo.AccessKey,
			SecretKey: rootfs.StorageInfo.SecretKey,
		},
		MountPoint: rootfs.MountPoint,
	}
}

func buildCodeMetaData(fv storage.FunctionVersionValue, businessID string) (metadata.CodeMetaData, error) {
	codeMetaData := metadata.CodeMetaData{
		CodeUploadType: fv.FunctionVersion.Package.CodeUploadType,
		Sha512:         fv.FunctionVersion.Package.Signature,
	}
	codeMetaData.StorageType = fv.FunctionVersion.Package.StorageType
	storageType := codeMetaData.StorageType
	if storageType == common.LocalStorageType || storageType == common.CopyStorageType ||
		storageType == common.WorkingDirStorageType {
		codeMetaData.LocalMetaData.CodePath = fv.FunctionVersion.Package.CodePath
	} else {
		pkg := fv.FunctionVersion.Package
		codeMetaData.S3MetaData.ObjectID = fv.FunctionVersion.Package.ObjectID
		codeMetaData.S3MetaData.BucketID = fv.FunctionVersion.Package.BucketID
		if fv.FunctionVersion.Package.BucketUrl != "" {
			codeMetaData.S3MetaData.BucketURL = fv.FunctionVersion.Package.BucketUrl
		} else {
			bucketCfg, err := pkgstore.FindBucket(businessID, pkg.BucketID)
			if err != nil {
				log.GetLogger().Errorf("failed to find bucket info: %s", err.Error())
				return metadata.CodeMetaData{}, err
			}
			codeMetaData.S3MetaData.BucketURL = bucketCfg.URL
			codeMetaData.S3MetaData.AppID = bucketCfg.AppID
		}
	}
	return codeMetaData, nil
}

func buildFunctionURN(tenantInfo server.TenantInfo, fv storage.FunctionVersionValue) (string, string) {
	return utils.BuildFunctionURN(tenantInfo.BusinessID, tenantInfo.TenantID,
		tenantInfo.ProductID, fv.Function.Name), tenantInfo.TenantID
}

func buildEnv(fv storage.FunctionVersionValue, env *metadata.EnvMetaData) error {
	if config.RepoCfg.DecryptAlgorithm == "NO_CRYPTO" {
		env.CryptoAlgorithm = "NO_CRYPTO"
		env.EnvKey = ""
		environment, err := getEnvironmentText(fv)
		if err != nil {
			return errors.New("failed to getEnvironment: " + err.Error())
		}
		env.Environment = environment
	} else {
		key, err := generateEnvKey()
		if err != nil {
			return errors.New("failed to generate EnvKey: " + err.Error())
		}
		env.Environment, err = getEnvironment([]byte(key), fv)
		if err != nil {
			return errors.New("failed to getEnvironment: " + err.Error())
		}
		env.EnvKey, err = utils.EncryptETCDValue(key)
		if err != nil {
			return errors.New("failed to encryptKey key: " + err.Error())
		}
		env.CryptoAlgorithm = "GCM"
	}
	return nil
}

func getLayerBucket(txn storage.Transaction, layers []storage.FunctionLayer,
	info server.TenantInfo,
) ([]metadata.CodeMetaData, error) {
	res := make([]metadata.CodeMetaData, len(layers), len(layers))
	for i, layer := range layers {
		val, bucket, err := getBucketInfo(txn, layer, info)
		if err != nil {
			return nil, err
		}
		res[i] = metadata.CodeMetaData{
			Sha512: val.Package.Signature,
			S3MetaData: metadata.S3MetaData{
				BucketURL: bucket.URL,
				ObjectID:  val.Package.ObjectID,
				BucketID:  val.Package.BucketID,
				AppID:     bucket.AppID,
			},
		}
	}
	return res, nil
}

func getBucketInfo(txn storage.Transaction, layer storage.FunctionLayer,
	info server.TenantInfo,
) (storage.LayerValue, config.BucketConfig, error) {
	val, err := storage.GetLayerVersionTx(txn, layer.Name, layer.Version)
	if err != nil {
		if err == errmsg.KeyNotFoundError {
			log.GetLogger().Errorf(
				"failed to get layer version, layer name %s, layer version %d does not exist",
				layer.Name, layer.Version)
			return storage.LayerValue{}, config.BucketConfig{}, err
		}
		log.GetLogger().Errorf("failed to get layer version: %s", err.Error())
		return storage.LayerValue{}, config.BucketConfig{}, err
	}
	bucketID := val.Package.BucketID
	objectID := val.Package.ObjectID
	bucket, err := pkgstore.FindBucket(info.BusinessID, bucketID)
	if err != nil {
		log.GetLogger().Errorf(
			"failed to get bucket, layer name %s, layer version %s, bucketId %s objectID %s",
			layer.Name, layer.Version, bucketID, objectID)
		return storage.LayerValue{}, config.BucketConfig{}, err
	}
	return val, bucket, nil
}

func generateEnvKey() (string, error) {
	k := make([]byte, constants.RandomKeySize, constants.RandomKeySize)
	if _, err := rand.Read(k); err != nil {
		log.GetLogger().Errorf("failed to generate randomKey by rand.Read(): %s", err.Error())
		return "", err
	}
	return hex.EncodeToString(k[:]), nil
}

func getEnvironment(key []byte, funcVersion storage.FunctionVersionValue) (string, error) {
	envText, err := getEnvironmentText(funcVersion)
	if err != nil {
		log.GetLogger().Errorf("failed to getEnvironmentText: %s", err.Error())
		return "", err
	}
	return envText, nil
}

func getEnvironmentText(funcVersion storage.FunctionVersionValue) (string, error) {
	envMap := make(map[string]string, envMapLength)
	envPrefix := config.RepoCfg.FunctionCfg.DefaultCfg.EnvPrefix
	if funcVersion.FunctionVersion.Kind == common.Faas {
		envPrefix = ""
	}
	if len(funcVersion.FunctionVersion.Environment) != 0 {
		env, err := utils.DecodeEnv(funcVersion.FunctionVersion.Environment)
		if err != nil {
			log.GetLogger().Errorf("failed to decode environment: %s", err.Error())
			return "", err
		}
		for key, value := range env {
			envMap[envPrefix+key] = value
		}
	}

	envMapValue, err := json.Marshal(envMap)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal environment: %s", err.Error())
		return "", err
	}
	return string(envMapValue), nil
}

// DeleteAllPublishFunction delete all published version by function name
func DeleteAllPublishFunction(txn storage.Transaction, name, kind string, tenantInfo server.TenantInfo) {
	path := BuildFunctionRegisterKey(tenantInfo, name, "", kind)
	txn.DeleteRange(path)
	path = BuildInstanceRegisterKey(tenantInfo, name, "", constants.DefaultClusterID)
	txn.DeleteRange(path)
	for cluster := range config.RepoCfg.ClusterID {
		path = BuildInstanceRegisterKey(tenantInfo, name, "", cluster)
		txn.DeleteRange(path)
	}
}

// DeletePublishFunction delete a specified version by function version and function name
func DeletePublishFunction(txn storage.Transaction, name string, tenantInfo server.TenantInfo, funcVersion,
	kind string,
) {
	path := BuildFunctionRegisterKey(tenantInfo, name, funcVersion, kind)
	txn.Delete(path)
	path = BuildInstanceRegisterKey(tenantInfo, name, funcVersion, constants.DefaultClusterID)
	txn.Delete(path)
	for cluster := range config.RepoCfg.ClusterID {
		path = BuildInstanceRegisterKey(tenantInfo, name, funcVersion, cluster)
		txn.DeleteRange(path)
	}
}

// DeleteAliasByFuncNameEtcd -
func DeleteAliasByFuncNameEtcd(txn storage.Transaction, funcName string) error {
	t, err := txn.GetCtx().TenantInfo()
	if err != nil {
		log.GetLogger().Errorf("failed to get tenant info, error: %s", err.Error())
		return err
	}

	txn.DeleteRange(BuildAliasRegisterKey(t, funcName, ""))
	return nil
}

// CreateAliasEtcd -
func CreateAliasEtcd(txn *storage.Txn, funcName string, aliasEtcd AliasEtcd) error {
	t, err := txn.GetCtx().TenantInfo()
	if err != nil {
		log.GetLogger().Errorf("failed to get tenant info, error: %s", err.Error())
		return err
	}
	str, err := json.Marshal(aliasEtcd)
	if err != nil {
		log.GetLogger().Errorf("marshal aliasEtcd value failed, error: %s", err.Error())
		return errmsg.MarshalError
	}
	txn.Put(BuildAliasRegisterKey(t, funcName, aliasEtcd.Name), string(str))

	return nil
}
