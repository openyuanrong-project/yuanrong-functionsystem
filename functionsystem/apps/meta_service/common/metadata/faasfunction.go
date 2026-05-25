/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// HTTPResponse is general http response
type HTTPResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// LogTankService -
type LogTankService struct {
	GroupID  string `json:"logGroupId" valid:",optional"`
	StreamID string `json:"logStreamId" valid:",optional"`
}

// TraceService -
type TraceService struct {
	TraceAK     string `json:"tracing_ak" valid:",optional"`
	TraceSK     string `json:"tracing_sk" valid:",optional"`
	ProjectName string `json:"project_name" valid:",optional"`
}

// Initializer include initializer handler and timeout
type Initializer struct {
	Handler string `json:"initializer_handler" valid:",optional"`
	Timeout int64  `json:"initializer_timeout" valid:",optional"`
}

// PreStop include pre_stop handler and timeout
type PreStop struct {
	Handler string `json:"pre_stop_handler" valid:",optional"`
	Timeout int64  `json:"pre_stop_timeout" valid:",optional"`
}

// FuncMountConfig function mount config
type FuncMountConfig struct {
	FuncMountUser FuncMountUser `json:"mount_user" valid:",optional"`
	FuncMounts    []FuncMount   `json:"func_mounts" valid:",optional"`
}

// FuncMountUser function mount user
type FuncMountUser struct {
	UserID  int `json:"user_id" valid:",optional"`
	GroupID int `json:"user_group_id" valid:",optional"`
}

// FuncMount function mount
type FuncMount struct {
	MountType      string `json:"mount_type" valid:",optional"`
	MountResource  string `json:"mount_resource" valid:",optional"`
	MountSharePath string `json:"mount_share_path" valid:",optional"`
	LocalMountPath string `json:"local_mount_path" valid:",optional"`
	Status         string `json:"status" valid:",optional"`
}

// Role include x_role and app_x_role
type Role struct {
	XRole    string `json:"xrole" valid:",optional"`
	AppXRole string `json:"app_xrole" valid:",optional"`
}

// FunctionDeploymentSpec define function deployment spec
type FunctionDeploymentSpec struct {
	BucketID  string `json:"bucket_id"`
	ObjectID  string `json:"object_id"`
	Layers    string `json:"layers"`
	DeployDir string `json:"deploydir"`
}

// InstanceResource describes the cpu and memory info of an instance
type InstanceResource struct {
	CPU             string           `json:"cpu"`
	Memory          string           `json:"memory"`
	CustomResources map[string]int64 `json:"customresources"`
}

// Worker define a worker
type Worker struct {
	Instances       []*Instance `json:"instances"`
	FunctionName    string      `json:"functionname"`
	FunctionVersion string      `json:"functionversion"`
	Tenant          string      `json:"tenant"`
	Business        string      `json:"business"`
}

// Instance define a instance
type Instance struct {
	IP             string `json:"ip"`
	Port           string `json:"port"`
	GrpcPort       string `json:"grpcPort"`
	InstanceID     string `json:"instanceID,omitempty"`
	DeployedIP     string `json:"deployed_ip"`
	DeployedNode   string `json:"deployed_node"`
	DeployedNodeID string `json:"deployed_node_id"`
	TenantID       string `json:"tenant_id"`
}

// ResourceStack stores properties of resource stack
type ResourceStack struct {
	StackID         string           `json:"id" valid:"required"`
	CPU             int64            `json:"cpu" valid:"required"`
	Mem             int64            `json:"mem" valid:"required"`
	CustomResources map[string]int64 `json:"customResources,omitempty" valid:"optional"`
}

// ResourceGroup stores properties of resource group
type ResourceGroup struct {
	GroupID         string                     `json:"id" valid:"required"`
	DeployOption    string                     `json:"deployOption" valid:"required"`
	GroupState      string                     `json:"groupState" valid:"required"`
	ResourceStacks  []ResourceStack            `json:"resourceStacks" valid:"required"`
	ScheduledStacks map[string][]ResourceStack `json:"scheduledStacks,omitempty" valid:"optional"`
}

// AffinityInfo is data affinity information
type AffinityInfo struct {
	AffinityRequest AffinityRequest
	AffinityNode    string // if AffinityNode is not empty, the affinity node has been calculated
	NeedToForward   bool
}

// AffinityRequest is affinity request parameter
type AffinityRequest struct {
	Strategy  string   `json:"strategy"`
	ObjectIDs []string `json:"object_ids"`
}

// GroupInfo stores groupID and stackID
type GroupInfo struct {
	GroupID string `json:"groupID"`
	StackID string `json:"stackID"`
}

// MetricsData shows the quantities of a specific resource
type MetricsData struct {
	TotalResource float32 `json:"totalResource"`
	InUseResource float32 `json:"inUseResource"`
}

// ResourceMetrics contains several resources' MetricsData
type ResourceMetrics map[string]MetricsData

// WorkerMetrics stores metrics used for scheduler
type WorkerMetrics struct {
	SystemResources ResourceMetrics
	// key levels: functionUrn instanceID
	FunctionResources map[string]map[string]ResourceMetrics
}

// UserAgency define AK/SK of user's agency
type UserAgency struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	Token     string `json:"token"`
}

// FaaSFuncCode include function code file and link info for FaaS
type FaaSFuncCode struct {
	File string `json:"file" valid:",optional"`
	Link string `json:"link" valid:",optional"`
}

// FaaSStrategyConfig -
type FaaSStrategyConfig struct {
	Concurrency int `json:"concurrency" valid:",optional"`
}

// FaaSFuncMeta define function meta info for FaaS
type FaaSFuncMeta struct {
	FuncMetaData     FaaSFuncMetaData     `json:"funcMetaData" valid:",optional"`
	CodeMetaData     CodeMetaData         `json:"codeMetaData" valid:",optional"`
	EnvMetaData      EnvMetaData          `json:"envMetaData" valid:",optional"`
	ResourceMetaData FaaSResourceMetaData `json:"resourceMetaData" valid:",optional"`
	InstanceMetaData FaaSInstanceMetaData `json:"instanceMetaData" valid:",optional"`
	ExtendedMetaData FaaSExtendedMetaData `json:"extendedMetaData" valid:",optional"`
	RootfsSpecMeta   RootfsSpecMeta       `json:"rootfs" valid:",optional"`
}

// FaaSFuncMetaData define meta data of functions for FaaS
type FaaSFuncMetaData struct {
	Layers              []*FaaSLayer      `json:"layers" valid:",optional"`
	Name                string            `json:"name"`
	FunctionDescription string            `json:"description" valid:"stringlength(1|1024)"`
	FunctionURN         string            `json:"functionUrn"`
	ReversedConcurrency int               `json:"reversedConcurrency" valid:"int"`
	TenantID            string            `json:"tenantId"`
	Tags                map[string]string `json:"tags" valid:",optional"`
	HostAliasConfig     map[string]string `json:"hostAliasConfig,omitempty"`
	FunctionUpdateTime  string            `json:"functionUpdateTime" valid:",optional"`
	FunctionVersionURN  string            `json:"functionVersionUrn"`
	RevisionID          string            `json:"revisionId" valid:"stringlength(1|20),optional"`
	CodeSize            int               `json:"codeSize" valid:"int"`
	CodeSha512          string            `json:"codeSha512" valid:"stringlength(1|128),optional"`
	Handler             string            `json:"handler" valid:"stringlength(1|255)"`
	Runtime             string            `json:"runtime" valid:"stringlength(1|63)"`
	Timeout             int64             `json:"timeout" valid:"required"`
	Version             string            `json:"version" valid:"stringlength(1|32)"`
	VersionDescription  string            `json:"versionDescription" valid:"stringlength(1|1024)"`
	DeadLetterConfig    string            `json:"deadLetterConfig" valid:"stringlength(1|255)"`
	BusinessID          string            `json:"businessId"  valid:"stringlength(1|32)"`
	FunctionType        string            `json:"functionType" valid:",optional"`
	FuncID              string            `json:"func_id" valid:",optional"`
	FuncName            string            `json:"func_name" valid:",optional"`
	DomainID            string            `json:"domain_id" valid:",optional"`
	ProjectName         string            `json:"project_name" valid:",optional"`
	Service             string            `json:"service" valid:",optional"`
	Dependencies        string            `json:"dependencies" valid:",optional"`
	EnableCloudDebug    string            `json:"enable_cloud_debug" valid:",optional"`
	IsStatefulFunction  bool              `json:"isStatefulFunction" valid:"optional"`
	IsBridgeFunction    bool              `json:"isBridgeFunction" valid:"optional"`
	IsStreamEnable      bool              `json:"isStreamEnable" valid:"optional"`
	Type                string            `json:"type" valid:"optional"`
	CreationTime        string            `json:"created" valid:",optional"`
	EnableAuthInHeader  bool              `json:"enable_auth_in_header" valid:"optional"`
	DNSDomainCfg        []DNSDomainInfo   `json:"dns_domain_cfg" valid:",optional"`
	VPCTriggerImage     string            `json:"vpcTriggerImage" valid:",optional"`
	IdleTime            int64             `json:"idleTime" valid:",optional"`
	AutoScaleConfig     AutoScaleConfig   `json:"autoScaleConfig" valid:"optional"`
	IsFuncPublic        bool              `json:"isFuncPublic" valid:"optional"`
}

// FaaSResourceMetaData include resource data such as cpu and memory
type FaaSResourceMetaData struct {
	CPU                 int64  `json:"cpu"`
	Memory              int64  `json:"memory"`
	GpuMemory           int64  `json:"gpu_memory"`
	EnableDynamicMemory bool   `json:"enable_dynamic_memory" valid:",optional"`
	CustomResources     string `json:"customResources" valid:",optional"`
	EnableTmpExpansion  bool   `json:"enable_tmp_expansion" valid:",optional"`
	EphemeralStorage    int    `json:"ephemeral_storage" valid:"int,optional"`
}

// FaaSInstanceMetaData define instance meta data of FG functions
type FaaSInstanceMetaData struct {
	MaxInstance    int64  `json:"maxInstance" valid:",optional"`
	MinInstance    int64  `json:"minInstance" valid:",optional"`
	ConcurrentNum  int    `json:"concurrentNum" valid:",optional"`
	InstanceType   string `json:"instanceType" valid:",optional"`
	IdleMode       bool   `json:"idleMode" valid:",optional"`
	PoolLabel      string `json:"poolLabel" valid:",optional"`
	PoolID         string `json:"poolId" valid:",optional"`
	DiskLimit      int64  `json:"diskLimit"   valid:",optional"`
	SchedulePolicy string `json:"schedulePolicy" valid:",optional"`
	ScalePolicy    string `json:"scalePolicy" valid:",optional"`
}

// ReserveInstanceMetaData meta data for reserved instance
type ReserveInstanceMetaData struct {
	InstanceMetaData FaaSInstanceMetaData `json:"instanceMetaData"`
}

// FaaSExtendedMetaData define external meta data of functions
type FaaSExtendedMetaData struct {
	ImageName             string                `json:"image_name" valid:",optional"`
	Role                  Role                  `json:"role" valid:",optional"`
	VpcConfig             *VpcConfig            `json:"func_vpc" valid:",optional"`
	EndpointTenantVpc     *VpcConfig            `json:"endpoint_tenant_vpc" valid:",optional"`
	FuncMountConfig       *FuncMountConfig      `json:"mount_config" valid:",optional"`
	StrategyConfig        FaaSStrategyConfig    `json:"strategy_config" valid:",optional"`
	ExtendConfig          string                `json:"extend_config" valid:",optional"`
	Initializer           Initializer           `json:"initializer" valid:",optional"`
	PreStop               PreStop               `json:"pre_stop" valid:",optional"`
	Heartbeat             Heartbeat             `json:"heartbeat" valid:",optional"`
	EnterpriseProjectID   string                `json:"enterprise_project_id" valid:",optional"`
	LogTankService        LogTankService        `json:"log_tank_service" valid:",optional"`
	TraceService          TraceService          `json:"tracing_config" valid:",optional"`
	CustomContainerConfig CustomContainerConfig `json:"custom_container_config" valid:",optional"`
	CustomHealthCheck     CustomHealthCheck     `json:"custom_health_check" valid:",optional"`
	AsyncConfigLoaded     bool                  `json:"async_config_loaded" valid:",optional"`
	RestoreHook           RestoreHook           `json:"restore_hook,omitempty" valid:",optional"`
	NetworkController     NetworkController     `json:"network_controller" valid:",optional"`
	UserAgency            UserAgency            `json:"user_agency" valid:",optional"`
	PriorityAZ            string                `json:"priority_az,omitempty"`
	EnableMetrics         bool                  `json:"enable_metrics,omitempty"`
	EnableAgentSession    bool                  `json:"enable_agent_session,omitempty"`
}

// Heartbeat define user custom heartbeat function config
type Heartbeat struct {
	// Handler define heartbeat function entry
	Handler string `json:"heartbeat_handler" valid:",optional"`
}

// CustomContainerConfig contains the metadata for custom container
type CustomContainerConfig struct {
	ControlPath string   `json:"control_path" valid:",optional"`
	Image       string   `json:"image" valid:",optional"`
	Command     []string `json:"command" valid:",optional"`
	Args        []string `json:"args" valid:",optional"`
	WorkingDir  string   `json:"working_dir" valid:",optional"`
	UID         int      `json:"uid" valid:",optional"`
	GID         int      `json:"gid" valid:",optional"`
}

// CustomHealthCheck custom health check
type CustomHealthCheck struct {
	TimeoutSeconds   int `json:"timeoutSeconds" valid:",optional"`
	PeriodSeconds    int `json:"periodSeconds" valid:",optional"`
	FailureThreshold int `json:"failureThreshold" valid:",optional"`
}

// RestoreHook include restorehook handler and timeout
type RestoreHook struct {
	Handler string `json:"restore_hook_handler,omitempty" valid:",optional"`
	Timeout int64  `json:"restore_hook_timeout,omitempty" valid:",optional"`
}

// NetworkController contains some special network settings
type NetworkController struct {
	DisablePublicNetwork bool      `json:"disable_public_network" valid:",optional"`
	TriggerAccessVpcs    []VpcInfo `json:"trigger_access_vpcs" valid:",optional"`
}

// VpcInfo contains the information of VPC access restriction
type VpcInfo struct {
	VpcName string `json:"vpc_name,omitempty"`
	VpcID   string `json:"vpc_id,omitempty"`
}

// VpcConfig include info of function vpc
type VpcConfig struct {
	ID         string `json:"id,omitempty"`
	DomainID   string `json:"domain_id,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	VpcName    string `json:"vpc_name,omitempty"`
	VpcID      string `json:"vpc_id,omitempty"`
	SubnetName string `json:"subnet_name,omitempty"`
	SubnetID   string `json:"subnet_id,omitempty"`
	TenantCidr string `json:"tenant_cidr,omitempty"`
	HostVMCidr string `json:"host_vm_cidr,omitempty"`
	Gateway    string `json:"gateway,omitempty"`
	Xrole      string `json:"xrole,omitempty"`
}

// FaaSLayer define layer info for FaaS
type FaaSLayer struct {
	BucketURL      string `json:"bucketUrl" valid:"url,optional"`
	ObjectID       string `json:"objectId" valid:"stringlength(1|255),optional"`
	BucketID       string `json:"bucketId" valid:"stringlength(1|255),optional"`
	AppID          string `json:"appId" valid:"stringlength(1|128),optional"`
	ETag           string `json:"etag" valid:"optional"`
	Link           string `json:"link" valid:"optional"`
	Name           string `json:"name" valid:",optional"`
	Sha256         string `json:"sha256" valid:"optional"`
	DependencyType string `json:"dependencyType" valid:",optional"`
}

// DNSDomainInfo dns domain info
type DNSDomainInfo struct {
	ID         string `json:"id"`
	DomainName string `json:"domain_name"`
	Type       string `json:"type" valid:",optional"`
	ZoneType   string `json:"zone_type" valid:",optional"`
}

// AutoScaleConfig -
type AutoScaleConfig struct {
	SLAQuota      int `json:"slaQuota" valid:"optional" default:"-1"`
	ScaleDownTime int `json:"scaleDownTime" valid:"optional" default:"-1"`
	BurstScaleNum int `json:"burstScaleNum" valid:"optional" default:"-1"`
}
