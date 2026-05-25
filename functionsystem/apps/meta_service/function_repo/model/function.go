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

// Package model is reset api model
package model

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"meta_service/common/base"
	"meta_service/common/types"
	"meta_service/function_repo/config"
	"meta_service/function_repo/errmsg"
)

// CustomPoolType -
const CustomPoolType = "custom"

// FunctionQueryInfo -
type FunctionQueryInfo struct {
	FunctionName, FunctionVersion, AliasName string
}

// S3CodePathInfo -
type S3CodePathInfo struct {
	BucketID  string `form:"bucketId" json:"bucketId"`
	ObjectID  string `form:"objectId" json:"objectId"`
	BucketUrl string `form:"bucketUrl" json:"bucketUrl"`
	Token     string `form:"token" json:"token"`
	Sha512    string `form:"sha512" json:"sha512"`
}

// ResourceAffinitySelector -
type ResourceAffinitySelector struct {
	Group    string `form:"group" json:"group"`
	Priority int    `form:"priority" json:"priority"`
}

// AutoScaleConfig -
type AutoScaleConfig struct {
	SLAQuota      int `json:"slaQuota" valid:"optional" default:"-1"`
	ScaleDownTime int `json:"scaleDownTime" valid:"optional" default:"-1"`
	BurstScaleNum int `json:"burstScaleNum" valid:"optional" default:"-1"`
}

type FunctionBasicInfo struct {
	Layers                    []string                   `json:"layers"`
	Memory                    int64                      `json:"memory"`
	CPU                       int64                      `json:"cpu"`
	PoolType                  string                     `json:"poolType"`
	MinInstance               string                     `json:"minInstance"`
	MaxInstance               string                     `json:"maxInstance"`
	ConcurrentNum             string                     `json:"concurrentNum"`
	Timeout                   int64                      `json:"timeout" valid:"required"`
	Handler                   string                     `json:"handler" valid:"maxstringlength(64)"`
	Kind                      string                     `form:"kind" json:"kind"`
	Environment               map[string]string          `json:"environment"`
	Description               string                     `json:"description" valid:"maxstringlength(1024)"`
	StorageType               string                     `json:"storageType"`
	CodePath                  string                     `json:"codePath"`
	FunctionType              string                     `json:"functionType" valid:",optional"`
	FunctionUpdateTime        string                     `json:"functionUpdateTime" valid:",optional"`
	FuncName                  string                     `json:"func_name" valid:",optional"`
	Service                   string                     `json:"service" valid:",optional"`
	IsBridgeFunction          bool                       `json:"isBridgeFunction" valid:"optional"`
	EnableAuthInHeader        bool                       `json:"enable_auth_in_header" valid:"optional"`
	CacheInstance             int                        `json:"cacheInstance"`
	HookHandler               map[string]string          `json:"hookHandler"`
	ExtendedHandler           map[string]string          `form:"extendedHandler" json:"extendedHandler"`
	ExtendedTimeout           map[string]int             `form:"extendedTimeout" json:"extendedTimeout"`
	CustomResources           map[string]float64         `json:"customResources"`
	Device                    types.Device               `form:"device" json:"device,omitempty"`
	S3CodePath                S3CodePathInfo             `form:"s3CodePath" json:"s3CodePath"`
	SchedulePolicies          []ResourceAffinitySelector `form:"schedulePolicies" json:"schedulePolicies"`
	ResourceAffinitySelectors []ResourceAffinitySelector `form:"resourceAffinitySelectors" json:"resourceAffinitySelectors"`
	CodeUploadType            string                     `form:"codeUploadType" json:"codeUploadType"`
	PoolID                    string                     `form:"poolId" json:"poolId" valid:"optional,poolId~pool id can contain only lowercase letters;digits and - it cannot start or end with - and cannot exceed 40 characters or less than 1 characters"`
	EnableMetrics             bool                       `form:"enableMetrics" json:"enableMetrics"`
	EnableAgentSession        bool                       `form:"enableAgentSession" json:"enableAgentSession"`
	IdleTime                  int64                      `form:"idleTime" json:"idleTime"`
	WarmupType                string                     `json:"warmup" valid:",optional"`
	RootfsSpecMeta            types.RootfsSpecMeta       `json:"rootfs" valid:",optional"`
	AutoScaleConfig           AutoScaleConfig            `json:"autoScaleConfig" valid:",optional"`
	SchedulePolicy            string                     `json:"schedulePolicy" valid:",optional"`
	ScalePolicy               string                     `json:"scalePolicy" valid:",optional"`
	PriorityAZ                string                     `json:"priorityAz" valid:",optional"`
	IsFuncPublic              bool                       `json:"isFuncPublic" valid:"optional"`
	ExtendedMetaData          types.ExtendedMetaData     `form:"extendedMetaData" json:"extendedMetaData" valid:",optional"`
}

// FunctionCreateRequest -
type FunctionCreateRequest struct {
	FunctionBasicInfo
	Name                string            `json:"name" valid:"required,functionName~format is 0@{serviceName}@{funcName} or 0-{serviceName}-{funcName}; serviceName can only contains letters、digits and cannot exceed 16 characters; funcName can only contains lowercase letters、digits、- and cannot exceed 127 characters or end with -"`
	Runtime             string            `json:"runtime" valid:"required"`
	ReversedConcurrency int               `json:"reversedConcurrency,omitempty"`
	Tags                map[string]string `json:"tags"`
	Kind                string            `json:"kind"`
}

func (c *FunctionBasicInfo) validateCommon(configs *config.Configs) error {
	// initialize the maximum and minimum number of instances
	c.setDefault(*configs)
	minInstance, err := strconv.Atoi(c.MinInstance)
	if err != nil {
		return errmsg.NewParamError(fmt.Sprintf("%s must be an integer", "minInstance"))
	}
	maxInstance, err := strconv.Atoi(c.MaxInstance)
	if err != nil {
		return errmsg.NewParamError(fmt.Sprintf("%s must be an integer", "maxInstance"))
	}
	concurrentNum, err := strconv.Atoi(c.ConcurrentNum)
	if err != nil {
		return errmsg.NewParamError(fmt.Sprintf("%s must be an integer", "concurrentNum"))
	}
	err = checkWorkerParams(minInstance, maxInstance, *configs, concurrentNum)
	if err != nil {
		return err
	}
	return nil
}

// Validate create function request
func (c *FunctionCreateRequest) Validate(configs config.Configs) error {
	if err := checkLayersNum(len(c.Layers), configs); err != nil {
		return err
	}
	if c.PoolType != CustomPoolType && reflect.DeepEqual(c.Device, types.Device{}) {
		if err := checkCPUAndMemory(c.CPU, c.Memory); err != nil {
			return err
		}
	}
	if err := checkRuntime(configs, c.Runtime); err != nil {
		return err
	}
	return c.validateCommon(&configs)
}

func (c *FunctionBasicInfo) setDefault(configs config.Configs) {
	if c.ConcurrentNum == "" {
		c.ConcurrentNum = configs.FunctionCfg.DefaultCfg.DefaultConcurrentNum
	}
	if c.MinInstance == "" {
		c.MinInstance = configs.FunctionCfg.DefaultCfg.DefaultMinInstance
	}
	if c.MaxInstance == "" {
		c.MaxInstance = configs.FunctionCfg.DefaultCfg.DefaultMaxInstance
	}
	if c.Timeout == 0 {
		c.Timeout = configs.FunctionCfg.DefaultCfg.Timeout
	}
	if c.AutoScaleConfig.SLAQuota == 0 {
		c.AutoScaleConfig.SLAQuota = -1
	}
	if c.AutoScaleConfig.ScaleDownTime == 0 {
		c.AutoScaleConfig.ScaleDownTime = -1
	}
	if c.AutoScaleConfig.BurstScaleNum == 0 {
		c.AutoScaleConfig.BurstScaleNum = -1
	}
}

func checkRuntime(configs config.Configs, runtime string) error {
	runtimeList := configs.RuntimeType
	err := errmsg.NewParamError("runtime " + runtime + " is not valid")
	for _, v := range runtimeList {
		if v == runtime {
			err = nil
			break
		}
	}
	if err != nil {
		return err
	}
	return nil
}

// CodeDownloadRequest -
type CodeDownloadRequest struct {
	FunctionName    string
	FunctionVersion string
	AliasName       string
}

// FunctionGetRequest -
type FunctionGetRequest struct {
	FunctionName    string
	FunctionVersion string
	AliasName       string
	Kind            string
}

// FunctionGetResponse -
type FunctionGetResponse struct {
	FunctionVersion
	Created string `json:"created"`
}

// FunctionVersionListGetResponse -
type FunctionVersionListGetResponse struct {
	Versions []FunctionVersion `json:"versions"`
	Total    int               `json:"total"`
}

// FunctionInfo describes function info
type FunctionInfo struct {
	base.FunctionCommonInfo
	FuncLayer []Layer      `json:"funcLayer"`
	Device    types.Device `json:"device,omitempty"`
	Kind      string       `json:"kind,omitempty"`
}

// GetFunctionResponse is response of getting function
type GetFunctionResponse struct {
	FunctionInfo
	Created string `json:"created"`
}

// PageInfo is page info of query response
type PageInfo struct {
	PageIndex string `json:"pageIndex"`
	PageSize  string `json:"pageSize"`
	Total     uint64 `json:"total"`
}

// GetFunctionVersionsResponse is response of all versions of a function
type GetFunctionVersionsResponse struct {
	Functions []GetFunctionResponse `json:"functions"`
	PageInfo  PageInfo              `json:"pageInfo"`
}

// FunctionListGetResponse -
type FunctionListGetResponse struct {
	FunctionVersion []FunctionVersion `json:"functions"`
	Total           int               `json:"total"`
}

// FunctionDeleteResponse is alia of FunctionVersionListGetResponse
type FunctionDeleteResponse FunctionVersionListGetResponse

// FunctionDraftGetRequest -
type FunctionDraftGetRequest struct {
	FunctionName    string
	FunctionVersion string
	AliasName       string
}

// FunctionPolicyRequest -
type FunctionPolicyRequest struct {
	TrustedTenantID string
	SourceProvider  string
	SourceAccount   string
	SourceID        string
	Effect          string
	Action          string
	RevisionID      string
}

// FunctionPolicyResponse -
type FunctionPolicyResponse struct{}

// DateTime time
type DateTime struct {
	CreateTime string `json:"createTime"`
	UpdateTime string `json:"updateTime"`
}

// FunctionLayer is functionVersion's layer
type FunctionLayer struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
	Order   int    `json:"order"`
}

// FunctionCodeUploadRequest is upload function package request
type FunctionCodeUploadRequest struct {
	RevisionID string
	Kind       string
	FileSize   int64
}

// FunctionCodeUploadResponse is upload function package response
type FunctionCodeUploadResponse struct {
	FunctionVersionURN string `json:"functionVersionUrn"`
}

// PublishResponse -
type PublishResponse = FunctionGetResponse

// PublishRequest -
type PublishRequest struct {
	RevisionID    string `json:"revisionId"`
	VersionDesc   string `json:"versionDesc"`
	Kind          string `json:"kind"`
	VersionNumber string `json:"versionNumber"`
}

// FunctionUpdateRequest update request
type FunctionUpdateRequest struct {
	FunctionBasicInfo
	RevisionID string `json:"revisionId"`
}

// Validate verify and update function request
func (c *FunctionUpdateRequest) Validate(configs config.Configs) error {
	// If the request runtime is nil, the request will not be updated.
	if err := checkLayersNum(len(c.Layers), configs); err != nil {
		return err
	}

	if c.PoolType != CustomPoolType && reflect.DeepEqual(c.Device, types.Device{}) {
		if err := checkCPUAndMemory(c.CPU, c.Memory); err != nil {
			return err
		}
	}

	if err := checkRuntime(configs, config.RepoCfg.RuntimeType[0]); err != nil {
		return err
	}
	return c.validateCommon(&configs)
}

func checkLayersNum(layerLen int, configs config.Configs) error {
	if layerLen > configs.FunctionCfg.LayerMax {
		return errmsg.NewParamError("the number of function reference layers "+
			"cannot exceed %d", configs.FunctionCfg.LayerMax)
	}
	return nil
}

func checkCPUAndMemory(cpu, memory int64) error {
	if cpu < config.RepoCfg.FunctionCfg.DefaultCfg.MinCPU || cpu > config.RepoCfg.FunctionCfg.DefaultCfg.MaxCPU {
		return errmsg.NewParamError(fmt.Sprintf("CPU not in range [%d, %d]", config.RepoCfg.FunctionCfg.DefaultCfg.MinCPU,
			config.RepoCfg.FunctionCfg.DefaultCfg.MaxCPU))
	} else if memory < config.RepoCfg.FunctionCfg.DefaultCfg.MinMemory || memory > config.RepoCfg.FunctionCfg.
		DefaultCfg.MaxMemory {
		return errmsg.NewParamError(fmt.Sprintf("memory not in range [%d, %d]",
			config.RepoCfg.FunctionCfg.DefaultCfg.MinMemory, config.RepoCfg.FunctionCfg.DefaultCfg.MaxMemory))
	}
	return nil
}

// check Worker Params
func checkWorkerParams(minInstance int, maxInstance int, configs config.Configs, concurrentNum int) error {
	if minInstance > maxInstance {
		return errmsg.NewParamError(fmt.Sprintf("the value of minInstance %d must be smaller "+
			"than that of maxInstance %d", minInstance, maxInstance))
	}
	if minInstance < 0 {
		return errmsg.NewParamError(fmt.Sprintf("the value of minInstance %d should be bigger than 0",
			minInstance))
	}
	maxInstanceUpperLimit, err := strconv.Atoi(configs.FunctionCfg.DefaultCfg.MaxInstanceUpperLimit)
	if err != nil {
		return errmsg.NewParamError(fmt.Sprintf("%s must be an integer", "configs.FunctionCfg."+
			"DefaultCfg.MaxInstanceUpperLimit"))
	}
	if maxInstance > maxInstanceUpperLimit {
		return errmsg.NewParamError(fmt.Sprintf("the value of maxInstance %d  must be smaller"+
			" than that of workerInstanceMax %d", maxInstance, maxInstanceUpperLimit))
	}
	concurrentNumUpperLimit, err := strconv.Atoi(configs.FunctionCfg.DefaultCfg.ConcurrentNumUpperLimit)
	if err != nil {
		return errmsg.NewParamError(fmt.Sprintf("%s must be an integer", "configs.FunctionCfg."+
			"DefaultCfg.ConcurrentNumUpperLimit"))
	}
	if concurrentNum > concurrentNumUpperLimit {
		return errmsg.NewParamError(fmt.Sprintf("the value of concurrentNum %d  must be smaller than that "+
			"of workerConcurrentNumMax %d", concurrentNum, concurrentNumUpperLimit))
	}
	if concurrentNum < 1 {
		return errmsg.NewParamError(fmt.Sprintf("the value of concurrentNum %d should be bigger than 1 ",
			concurrentNum))
	}
	return nil
}

// FunctionVersion indicates the function instance information of the interface.
type FunctionVersion struct {
	Function
	FunctionVersionURN string               `json:"functionVersionUrn"`
	RevisionID         string               `json:"revisionId"`
	StorageType        string               `json:"storageType"`
	CodeSize           int64                `json:"codeSize"`
	CodeSha256         string               `json:"codeSha256"`
	CodePath           string               `json:"codePath"`
	BucketID           string               `json:"bucketId"`
	ObjectID           string               `json:"objectId"`
	Handler            string               `json:"handler"`
	Layers             []string             `json:"layers"`
	CPU                int64                `json:"cpu"`
	Memory             int64                `json:"memory"`
	Runtime            string               `json:"runtime"`
	Timeout            int64                `json:"timeout"`
	VersionNumber      string               `json:"versionNumber"`
	VersionDesc        string               `json:"versionDesc"`
	Environment        map[string]string    `json:"environment"`
	CustomResources    map[string]float64   `json:"customResources"`
	StatefulFlag       int                  `json:"statefulFlag"`
	LastModified       string               `json:"lastModified"`
	Published          string               `json:"Published"`
	MinInstance        int64                `json:"minInstance"`
	MaxInstance        int64                `json:"maxInstance"`
	ConcurrentNum      int                  `json:"concurrentNum"`
	EnableMetrics      bool                 `json:"enableMetrics"`
	FuncLayer          []FunctionLayer      `json:"funcLayer"`
	Status             string               `json:"status"`
	InstanceNum        int64                `json:"instanceNum"`
	Device             types.Device         `json:"device,omitempty"`
	Kind               string               `json:"kind,omitempty"`
	RootfsSpecMeta     types.RootfsSpecMeta `json:"rootfs" valid:",optional"`
	PriorityAZ         string               `json:"priorityAz,omitempty"`
}

// Function is function entity
type Function struct {
	DateTime
	FunctionURN         string            `json:"functionUrn"`
	FunctionName        string            `json:"name"`
	TenantID            string            `json:"tenantId"`
	BusinessID          string            `json:"businessId"`
	ProductID           string            `json:"productId"`
	ReversedConcurrency int               `json:"reversedConcurrency"`
	Description         string            `json:"description"`
	Tag                 map[string]string `json:"tag"`
}

// FunctionForUser describes function info
type FunctionForUser struct {
	ID                  string               `json:"id"`
	CreateTime          string               `json:"createTime"`
	UpdateTime          string               `json:"updateTime"`
	FunctionURN         string               `json:"functionUrn"`
	FunctionName        string               `json:"name"`
	TenantID            string               `json:"tenantId"`
	BusinessID          string               `json:"businessId"`
	ProductID           string               `json:"productId"`
	ReversedConcurrency int                  `json:"reversedConcurrency"`
	Description         string               `json:"description"`
	Tag                 map[string]string    `json:"tag"`
	FunctionVersionURN  string               `json:"functionVersionUrn"`
	RevisionID          string               `json:"revisionId"`
	StorageType         string               `json:"storageType"`
	CodeSize            int64                `json:"codeSize"`
	CodeSha256          string               `json:"codeSha256"`
	CodePath            string               `json:"codePath"`
	BucketID            string               `json:"bucketId"`
	ObjectID            string               `json:"objectId"`
	Handler             string               `json:"handler"`
	Layers              []string             `json:"layers"`
	CPU                 int                  `json:"cpu"`
	Memory              int                  `json:"memory"`
	Runtime             string               `json:"runtime"`
	Timeout             int                  `json:"timeout"`
	VersionNumber       string               `json:"versionNumber"`
	VersionDesc         string               `json:"versionDesc"`
	Environment         map[string]string    `json:"environment"`
	CustomResources     map[string]float64   `json:"customResources"`
	StatefulFlag        int                  `json:"statefulFlag"`
	LastModified        string               `json:"lastModified"`
	Published           string               `json:"Published"`
	MinInstance         int                  `json:"minInstance"`
	MaxInstance         int                  `json:"maxInstance"`
	ConcurrentNum       int                  `json:"concurrentNum"`
	EnableMetrics       bool                 `json:"enableMetrics"`
	FuncLayer           []Layer              `json:"funcLayer"`
	Status              string               `json:"status"`
	InstanceNum         int                  `json:"instanceNum"`
	Device              types.Device         `json:"device,omitempty"`
	Kind                string               `json:"kind,omitempty"`
	RootfsSpecMeta      types.RootfsSpecMeta `json:"rootfs" valid:",optional"`
}

// GetFunctionResponseForUser is response of function info
type GetFunctionResponseForUser struct {
	FunctionForUser
	Created string `json:"created"`
}

// MarshalJSON marshals function info to json
func (r *GetFunctionResponseForUser) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Code     int                        `json:"code"`
		Message  string                     `json:"message"`
		Function GetFunctionResponseForUser `json:"function"`
	}{Message: "SUCCESS", Function: *r})
}

// BaseRequest base request
type BaseRequest struct {
	TraceID string `json:"-"`
}

// BaseResponse base response
type BaseResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ReserveInsBaseInfo basic info for reserved instance
type ReserveInsBaseInfo struct {
	FuncName      string `json:"funcName" valid:",required"`
	Version       string `json:"version" valid:",required"`
	TenantID      string `json:"-"`
	InstanceLabel string `json:"instanceLabel" valid:"maxstringlength(63)"`
}

// InstanceConfig instance config
type InstanceConfig struct {
	ClusterID   string `json:"clusterId" valid:"maxstringlength(64)"`
	MaxInstance int64  `json:"maxInstance" valid:"range(0|1000)"`
	MinInstance int64  `json:"minInstance" valid:"range(0|1000)"`
}

// ReserveInstanceConfig reserve instance config
type ReserveInstanceConfig struct {
	InstanceConfig InstanceConfig
	InstanceLabel  string
}

// CreateReserveInsRequest create reserved instance meta request
type CreateReserveInsRequest struct {
	BaseRequest
	ReserveInsBaseInfo
	InstanceConfigInfos []InstanceConfig `json:"instanceConfigInfos"`
}

// CreateReserveInsResponse create reserved instance meta response
type CreateReserveInsResponse struct {
	BaseResponse
	InstanceConfigInfos []InstanceConfig   `json:"instanceConfigInfos"`
	ReserveInsBaseInfo  ReserveInsBaseInfo `json:"reserveInsBaseInfo"`
}

// UpdateReserveInsRequest update reserved instance meta request
type UpdateReserveInsRequest struct {
	CreateReserveInsRequest
}

// UpdateReserveInsResponse update reserved instance meta response
type UpdateReserveInsResponse struct {
	CreateReserveInsResponse
}

// DeleteReserveInsRequest delete reserved instance meta request
type DeleteReserveInsRequest struct {
	BaseRequest
	ReserveInsBaseInfo
}

// DeleteReserveInsResponse delete reserved instance meta request
type DeleteReserveInsResponse struct {
	BaseResponse
}

// GetReserveInsRequest get reserved instance meta request
type GetReserveInsRequest struct {
	FuncName string `json:"funcName"`
	Version  string `json:"version"`
	TenantID string `json:"-"`
}

// GetReserveInsResult get reserved instance result
type GetReserveInsResult struct {
	InstanceLabel       string           `json:"instanceLabel"`
	InstanceConfigInfos []InstanceConfig `json:"instanceConfigInfos"`
}

// GetReserveInsResponse get reserved instance meta response
type GetReserveInsResponse struct {
	Total                int                   `json:"total"`
	GetReserveInsResults []GetReserveInsResult `json:"results"`
}
