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

package storage

import (
	"encoding/json"
	"fmt"
	"time"

	"meta_service/function_repo/model"
	"meta_service/function_repo/server"

	"meta_service/common/constants"
	"meta_service/common/types"
)

// FunctionVersionKey defines key of function version entry.
type FunctionVersionKey struct {
	server.TenantInfo
	FunctionName    string
	FunctionVersion string
}

// FunctionStatusKey defines key of function status entry.
type FunctionStatusKey struct {
	FunctionVersionKey
}

// Function defines function metadata.
type Function struct {
	Name                string
	ReversedConcurrency int
	Version             string
	Description         string
	Tag                 map[string]string
	CreateTime          string
	UpdateTime          string
}

// S3Package defines s3 package metadata.
type S3Package struct {
	Signature string
	Size      int64
	BucketID  string
	ObjectID  string
	BucketUrl string
	Token     string
}

// LocalPackage defines local package metadata
type LocalPackage struct {
	CodePath string
}

// Package defines storage type metadata
type Package struct {
	StorageType    string
	CodeUploadType string
	S3Package
	LocalPackage
}

// FunctionVersion defines function version metadata.
type FunctionVersion struct {
	Package            Package
	RevisionID         string
	Handler            string
	CPU                int64
	Memory             int64
	Runtime            string
	Timeout            int64
	Version            string
	Environment        string
	CustomResources    string
	Description        string
	StatefulFlag       int
	PublishTime        string
	MinInstance        int64
	MaxInstance        int64
	ConcurrentNum      int
	Status             string
	Kind               string
	FunctionType       string
	FuncName           string
	Service            string
	IsBridgeFunction   bool
	EnableAuthInHeader bool
	InstanceNum        int64
	HookHandler        map[string]string
	ExtendedHandler    map[string]string
	ExtendedTimeout    map[string]int
	CacheInstance      int
	Device             types.Device
	PoolLabel          string
	PoolID             string
	EnableMetrics      bool
	EnableAgentSession bool
	ScalePolicy        string
	SchedulePolicy     string
	PriorityAZ         string
	IdleTime           int64
	AutoScaleConfig    AutoScaleConfig
	WarmupType         string
	RootfsSpecMeta        types.RootfsSpecMeta
	CustomContainerConfig types.CustomContainerConfig
	CustomHealthCheck     types.CustomHealthCheck
	IsFuncPublic          bool
}

// FunctionLayer defines layer info related to function version.
type FunctionLayer struct {
	Name    string
	Version int
	Order   int
}

// FunctionVersionValue defines function version entry in storage.
type FunctionVersionValue struct {
	Function        Function
	FunctionVersion FunctionVersion
	FunctionLayer   []FunctionLayer
}

// FunctionStatusValue defines function status entry in storage.
type FunctionStatusValue struct {
	Status      string
	InstanceNum int64
}

// UncontrolledObjectKey defines object info which is to be deleted.
type UncontrolledObjectKey struct {
	server.TenantInfo
	BucketID string
	ObjectID string
}

// UncontrolledObjectValue -
type UncontrolledObjectValue struct {
	CreateTime time.Time
}

// LayerKey defines key of layer version entry.
type LayerKey struct {
	server.TenantInfo
	LayerName    string
	LayerVersion int
}

// LayerValue defines layer version value in storage.
type LayerValue struct {
	Package            Package
	CompatibleRuntimes []string
	Description        string
	LicenseInfo        string
	CreateTime         time.Time
	UpdateTime         time.Time
}

// LayerCountIndexKey defines key of layer reference count index entry.
type LayerCountIndexKey struct {
	server.TenantInfo
	LayerName string
}

// LayerCountIndexValue -
type LayerCountIndexValue struct{}

// LayerFunctionIndexKey defines key of layer function index entry.
type LayerFunctionIndexKey struct {
	server.TenantInfo
	LayerName       string
	LayerVersion    int
	FunctionName    string
	FunctionVersion string
}

// LayerFunctionIndexValue -
type LayerFunctionIndexValue struct{}

// AliasKey defines key of alias entry.
type AliasKey struct {
	server.TenantInfo
	FunctionName string
	AliasName    string
}

// AliasValue defines alias entry value in storage.
type AliasValue struct {
	Name            string
	FunctionName    string
	FunctionVersion string
	RevisionID      string
	Description     string
	RoutingConfig   map[string]int
	CreateTime      time.Time
	UpdateTime      time.Time
}

// AliasRoutingIndexKey defines key of alias routing index entry.
type AliasRoutingIndexKey struct {
	server.TenantInfo
	FunctionName    string
	FunctionVersion string
}

// AliasRoutingIndexValue defines alias routing index entry in storage.
type AliasRoutingIndexValue struct {
	Name string
}

// TriggerKey defines key of trigger entry.
type TriggerKey struct {
	server.TenantInfo
	FunctionName   string
	VersionOrAlias string
	TriggerID      string
}

// PoolKey defines key of pool entry
type PoolKey struct {
	ID string
}

// PoolValue defines value of pool entry
type PoolValue struct {
	model.Pool
}

// TriggerSpec defines the behavior a trigger type.
type TriggerSpec interface {
	// Convert storage spec to model spec
	BuildModelSpec(model.TriggerSpec) (interface{}, error)
	save(txn Transaction, funcName, verOrAlias string) error
	delete(txn Transaction, funcName, verOrAlias string) error
}

// TriggerValue defines trigger entry in storage.
type TriggerValue struct {
	TriggerID   string
	FuncName    string
	TriggerType string
	RevisionID  string
	RawEtcdSpec json.RawMessage `json:"EtcdSpec"`
	EtcdSpec    TriggerSpec     `json:"-"`
	CreateTime  time.Time
	UpdateTime  time.Time
}

// localTrigger prevents recursive call to MarshalJSON and UnmarshalJSON
type localTrigger TriggerValue

// MarshalJSON implements json.Marshaler
func (t TriggerValue) MarshalJSON() ([]byte, error) {
	l := (*localTrigger)(&t)

	b, err := json.Marshal(l.EtcdSpec)
	if err != nil {
		return nil, err
	}
	l.RawEtcdSpec = json.RawMessage(b)
	return json.Marshal(l)
}

// UnmarshalJSON implements json.Unmarshaler
func (t *TriggerValue) UnmarshalJSON(b []byte) error {
	l := (*localTrigger)(t)

	if err := json.Unmarshal(b, l); err != nil {
		return err
	}

	switch l.TriggerType {
	case model.HTTPType:
		l.EtcdSpec = &HTTPTriggerEtcdSpec{}
	default:
		return fmt.Errorf("unkown type: %s", l.TriggerType)
	}
	return json.Unmarshal(l.RawEtcdSpec, l.EtcdSpec)
}

// TriggerFunctionIndexKey defines key of trigger function index entry.
type TriggerFunctionIndexKey struct {
	server.TenantInfo
	TriggerID string
}

// TriggerFunctionIndexValue defines trigger function index entry in storage.
type TriggerFunctionIndexValue struct {
	FunctionName   string
	VersionOrAlias string
}

// FunctionResourceIDIndexKey defines key of function resource ID index entry.
type FunctionResourceIDIndexKey struct {
	server.TenantInfo
	FunctionName   string
	VersionOrAlias string
	ResourceID     string
}

// FunctionResourceIDIndexValue -
type FunctionResourceIDIndexValue struct{}

// ObjectRefIndexKey defines key of object reference count index entry.
type ObjectRefIndexKey struct {
	server.TenantInfo
	BucketID string
	ObjectID string
}

// ObjectRefIndexValue defines object reference count index entry in storage.
type ObjectRefIndexValue struct {
	RefCnt uint64
}

// AliasRoutingIndexTuple defines AliasRouting index entry
type AliasRoutingIndexTuple struct {
	Key   AliasRoutingIndexKey
	Value AliasRoutingIndexValue
}

// TriggerTuple defines TriggerTuple index entry
type TriggerTuple struct {
	Key   TriggerKey
	Value TriggerValue
}

// TriggerFunctionIndexTuple defines TriggerFunctionIndexTuple index entry
type TriggerFunctionIndexTuple struct {
	Key   TriggerFunctionIndexKey
	Value TriggerFunctionIndexValue
}

// FunctionResourceIDIndexTuple defines  FunctionResourceIDIndexTuple index entry
type FunctionResourceIDIndexTuple struct {
	Key   FunctionResourceIDIndexKey
	Value FunctionResourceIDIndexValue
}

// ObjectRefIndexTuple defines ObjectRefIndexTuple index entry
type ObjectRefIndexTuple struct {
	Key   ObjectRefIndexKey
	Value ObjectRefIndexValue
}

// AliasTuple defines AliasTuple index entry
type AliasTuple struct {
	Key   AliasKey
	Value AliasValue
}

// LayerFunctionIndexTuple defines LayerFunctionIndexTuple index entry
type LayerFunctionIndexTuple struct {
	Key   LayerFunctionIndexKey
	Value LayerFunctionIndexValue
}

// FunctionVersionTuple defines FunctionVersionTuple index entry
type FunctionVersionTuple struct {
	Key   FunctionVersionKey
	Value FunctionVersionValue
}

// LayerCountIndexTuple defines LayerCountIndexTuple index entry
type LayerCountIndexTuple struct {
	Key   LayerCountIndexKey
	Value LayerCountIndexValue
}

// LayerTuple defines LayerTuple index entry
type LayerTuple struct {
	Key   LayerKey
	Value LayerValue
}

// UncontrolledObjectTuple defines UncontrolledObjectTuple index entry
type UncontrolledObjectTuple struct {
	Key   UncontrolledObjectKey
	Value UncontrolledObjectValue
}

// FunctionStatusTuple defines FunctionStatusTuple index entry
type FunctionStatusTuple struct {
	Key   FunctionStatusKey
	Value FunctionStatusValue
}

// PoolTuple defines PoolTuple index entry
type PoolTuple struct {
	Key   PoolKey
	Value PoolValue
}

// GetTxnByKind -
func GetTxnByKind(ctx server.Context, kind string) Transaction {
	if kind == constants.Faas {
		return NewMetaTxn(ctx)
	}
	return NewTxn(ctx)
}

// AutoScaleConfig -
type AutoScaleConfig struct {
	SLAQuota      int
	ScaleDownTime int
	BurstScaleNum int
}
