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

// Package metadata define struct of metadata stored in storage like etcd
package metadata

import (
	"fmt"

	"meta_service/common/logger/logservice"
	"meta_service/common/trace"
	"meta_service/common/types"
	"meta_service/common/urnutils"
)

// Function define function meta info
type Function struct {
	FuncMetaData     FuncMetaData     `json:"funcMetaData" valid:",optional"`
	CodeMetaData     CodeMetaData     `json:"codeMetaData" valid:",optional"`
	EnvMetaData      EnvMetaData      `json:"envMetaData" valid:",optional"`
	ResourceMetaData ResourceMetaData `json:"resourceMetaData" valid:",optional"`
	ExtendedMetaData ExtendedMetaData `json:"extendedMetaData" valid:",optional"`
	WarmupType       string           `json:"warmup" valid:",optional"`
	RootfsSpecMeta   RootfsSpecMeta   `json:"rootfs" valid:",optional"`
}

// FuncMetaData define meta data of functions
type FuncMetaData struct {
	Layers                  []CodeMetaData    `json:"layers" valid:",optional"`
	Name                    string            `json:"name"`
	FunctionDescription     string            `json:"description" valid:"stringlength(1|1024)"`
	FunctionURN             string            `json:"functionUrn"`
	ReversedConcurrency     int               `json:"reversedConcurrency" valid:"int"`
	Tags                    map[string]string `json:"tags" valid:",optional"`
	FunctionUpdateTime      string            `json:"functionUpdateTime" valid:",optional"`
	FunctionVersionURN      string            `json:"functionVersionUrn"`
	CodeSize                int64             `json:"codeSize" valid:"int"`
	CodeSha256              string            `json:"codeSha256" valid:"stringlength(1|64),optional"`
	CodeSha512              string            `json:"codeSha512" valid:"stringlength(1|128),optional"`
	Handler                 string            `json:"handler" valid:"stringlength(1|255)"`
	Runtime                 string            `json:"runtime" valid:"stringlength(1|63)"`
	Timeout                 int64             `json:"timeout" valid:"required"`
	Version                 string            `json:"version" valid:"stringlength(1|16)"`
	VersionDescription      string            `json:"versionDescription" valid:"stringlength(1|1024)"`
	DeadLetterConfig        string            `json:"deadLetterConfig" valid:"stringlength(1|255)"`
	LatestVersionUpdateTime string            `json:"latestVersionUpdateTime" valid:",optional"`
	PublishTime             string            `json:"publishTime" valid:",optional"`
	BusinessID              string            `json:"businessId"  valid:"stringlength(1|32)"`
	TenantID                string            `json:"tenantId"`
	DomainID                string            `json:"domain_id" valid:",optional"`
	ProjectName             string            `json:"project_name" valid:",optional"`
	RevisionID              string            `json:"revisionId" valid:"stringlength(1|20),optional"`
	CreationTime            string            `json:"created" valid:",optional"`
	StatefulFlag            bool              `json:"statefulFlag"`
	HookHandler             map[string]string `json:"hookHandler" valid:",optional"`
	IdleTime                int64             `json:"idleTime" valid:",optional"`
	IsFuncPublic            bool              `json:"isFuncPublic" valid:"optional"`
}

// Layer define layer info
type Layer struct {
	AppID     string `json:"appId" valid:"stringlength(1|128),optional"`
	BucketID  string `json:"bucketId" valid:"stringlength(1|255),optional"`
	ObjectID  string `json:"objectId" valid:"stringlength(1|255),optional"`
	BucketURL string `json:"bucketUrl" valid:"url,optional"`
	Sha256    string `json:"sha256" valid:"optional"`
}

// EnvMetaData define env info of functions
type EnvMetaData struct {
	EnvKey            string `json:"envKey,omitempty" valid:",optional"`
	Environment       string `json:"environment" valid:",optional"`
	EncryptedUserData string `json:"encrypted_user_data" valid:",optional"`
	CryptoAlgorithm   string `json:"cryptoAlgorithm" valid:",optional"`
}

// ResourceMetaData include resource data such as cpu and memory
type ResourceMetaData struct {
	CPU             int64  `json:"cpu"`
	Memory          int64  `json:"memory"`
	CustomResources string `json:"customResources" valid:",optional"`
}

// InstanceMetaData define instance meta data of functions
type InstanceMetaData struct {
	MaxInstance   int64  `json:"maxInstance" valid:",optional"`
	MinInstance   int64  `json:"minInstance" valid:",optional"`
	ConcurrentNum int    `json:"concurrentNum" valid:",optional"`
	CacheInstance int    `json:"cacheInstance" valid:",optional"`
	PoolLabel     string `json:"poolLabel" valid:",optional"`
	PoolID        string `json:"poolId" valid:",optional"`
}

// ExtendedMetaData define external meta data of functions
type ExtendedMetaData struct {
	ImageName           string                    `json:"image_name" valid:",optional"`
	Role                types.Role                `json:"role" valid:",optional"`
	FuncMountConfig     types.FuncMountConfig     `json:"mount_config" valid:",optional"`
	StrategyConfig      StrategyConfig            `json:"strategy_config" valid:",optional"`
	ExtendConfig        string                    `json:"extend_config" valid:",optional"`
	Initializer         types.Initializer         `json:"initializer" valid:",optional"`
	PreStop             types.PreStop             `json:"pre_stop" valid:",optional"`
	EnterpriseProjectID string                    `json:"enterprise_project_id" valid:",optional"`
	LogTankService      logservice.LogTankService `json:"log_tank_service" valid:",optional"`
	TraceService        trace.Service             `json:"tracing_config" valid:",optional"`
	UserType            string                    `json:"user_type" valid:",optional"`
	InstanceMetaData    InstanceMetaData          `json:"instance_meta_data" valid:",optional"`
	ExtendedHandler     map[string]string         `json:"extended_handler" valid:",optional"`
	ExtendedTimeout     map[string]int            `json:"extended_timeout" valid:",optional"`
	Device              types.Device              `json:"device,omitempty" valid:",optional"`
	PriorityAZ          string                    `json:"priority_az,omitempty"`
	EnableMetrics       bool                      `json:"enable_metrics,omitempty"`
	EnableAgentSession  bool                      `json:"enable_agent_session,omitempty"`
	CustomContainerConfig CustomContainerConfig   `json:"custom_container_config" valid:",optional"`
	CustomHealthCheck     CustomHealthCheck       `json:"custom_health_check" valid:",optional"`
}

// FuncCode include function code file and link info
type FuncCode struct {
	File string `json:"file" valid:",optional"`
	Link string `json:"link" valid:",optional"`
}

// StrategyConfig include concurrency of function
type StrategyConfig struct {
	Concurrency int `json:"concurrency" valid:",optional"`
}

// FunctionKey contains key info of a function
type FunctionKey struct {
	BusinessID  string `json:"businessID" valid:"optional"`
	TenantID    string `json:"tenantID" valid:"optional"`
	ServiceID   string `json:"serviceID" valid:"optional"`
	FuncName    string `json:"functionName" valid:"optional"`
	FuncVersion string `json:"functionVersion" valid:"optional"`
}

// ToAnonymousString return the anonymous string of funcMeta
func (f *FunctionKey) ToAnonymousString() string {
	return fmt.Sprintf("%s-%s-%s-%s-%s", urnutils.Anonymize(f.TenantID), f.BusinessID, f.ServiceID, f.FuncName,
		f.FuncVersion)
}

// FullName return full function name(0-$serviceID-$funcName) of function
func (f *FunctionKey) FullName() string {
	return fmt.Sprintf("0-%s-%s", f.ServiceID, f.FuncName)
}

// RootfsSpecMeta defines rootfs specification metadata
type RootfsSpecMeta struct {
	Runtime     string            `json:"runtime" valid:",optional"`
	Type        string            `json:"type" valid:",optional"`
	ImageURL    string            `json:"imageurl" valid:",optional"`
	ReadOnly    bool              `json:"readonly" valid:",optional"`
	StorageInfo RootfsStorageInfo `json:"storageInfo" valid:",optional"`
	MountPoint  string            `json:"mountpoint" valid:",optional"`
}

// RootfsStorageInfo defines rootfs storage information
type RootfsStorageInfo struct {
	Endpoint  string `json:"endpoint" valid:",optional"`
	Bucket    string `json:"bucket" valid:",optional"`
	Object    string `json:"object" valid:",optional"`
	AccessKey string `json:"accessKey" valid:",optional"`
	SecretKey string `json:"secretKey" valid:",optional"`
}
