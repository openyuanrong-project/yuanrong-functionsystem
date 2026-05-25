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
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"meta_service/function_repo/config"
	"meta_service/function_repo/errmsg"
	"meta_service/function_repo/pkgstore"
	"meta_service/function_repo/server"
	"meta_service/function_repo/storage"
	"meta_service/function_repo/test/fakecontext"

	"meta_service/common/crypto"
	"meta_service/common/engine"
	"meta_service/common/metadata"

	codec2 "meta_service/common/codec"

	"github.com/agiledragon/gomonkey"
	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func getStorage(ctrl *gomock.Controller) (*engine.MockEngine, *engine.MockTransaction, *storage.Txn) {
	transaction := engine.NewMockTransaction(ctrl)
	eng := engine.NewMockEngine(ctrl)
	eng.EXPECT().BeginTx(gomock.Any()).Return(transaction)
	_ = storage.InitStorageByEng(eng, "test")

	ctx := fakecontext.NewMockContext()
	txn := storage.NewTxn(ctx)
	return eng, transaction, txn
}

func TestSavePublishFuncVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	if err := config.InitConfig("/home/sn/repo/config.json"); err != nil {
		t.Fatalf(err.Error())
	}

	_, tran, txn := getStorage(ctrl)

	layer, err := codec2.NewJSONCodec().Encode(storage.LayerValue{})
	if err != nil {
		t.Fatalf(err.Error())
	}
	tran.EXPECT().Get(gomock.Any()).Return(layer, nil)
	// can not assert value because of the environment encrypt
	tran.EXPECT().Put(gomock.Eq("/sn/functions/business/yrk/tenant/i1fe539427b24702acc11fbb4e134e17/function/Name/version/Version"), gomock.Any()).Return()

	functionVersionValue := storage.FunctionVersionValue{
		Function:        storage.Function{Name: "Name"},
		FunctionVersion: storage.FunctionVersion{Version: "Version", Environment: "7da374d89a8e872bac706113:d7fb221574a0cf6c8e04b0647545f42c83f5"},
		FunctionLayer:   []storage.FunctionLayer{},
	}
	functionVersionValue.FunctionLayer = append(functionVersionValue.FunctionLayer, storage.FunctionLayer{Name: "Name", Version: 0, Order: 1})
	if err := SavePublishFuncVersion(txn, functionVersionValue); err != nil {
		t.Fatalf("Save error %s", err)
	}
}

func TestCreateAliasEtcd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	_, tran, txn := getStorage(ctrl)

	info := AliasEtcd{Name: "a"}

	tran.EXPECT().Put(gomock.Eq("/sn/aliases/business/yrk/tenant/i1fe539427b24702acc11fbb4e134e17/function/funcName/a"), gomock.Any())
	err := CreateAliasEtcd(txn, "funcName", info)
	assert.Nil(t, err)
}

func TestSaveTraceChainInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eng := engine.NewMockEngine(ctrl)
	_ = storage.InitStorageByEng(eng, "test")

	ctx := fakecontext.NewMockContext()
	info, err := ctx.TenantInfo()
	assert.Nil(t, err)

	eng.EXPECT().Put(gomock.Any(), gomock.Eq("/sn/functionchains/business/yrk/tenant/i1fe539427b24702acc11fbb4e134e17/function/name"), gomock.Eq("value")).Return(nil)
	err = SaveTraceChainInfo(info, "name", "value")
	assert.Nil(t, err)

	eng.EXPECT().Put(gomock.Any(), gomock.Eq("/sn/functionchains/business/yrk/tenant/i1fe539427b24702acc11fbb4e134e17/function/name"), gomock.Eq("value")).Return(errmsg.EtcdInternalError)
	err = SaveTraceChainInfo(info, "name", "value")
	assert.Equal(t, errmsg.EtcdInternalError, err)
}

func TestAddTrigger(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	_, tran, txn := getStorage(ctrl)
	tran.EXPECT().Put(gomock.Any(), gomock.Any()).Return()
	Convey("Test AddTrigger", t, func() {
		err := AddTrigger(txn, "mock-func", "veroralias",
			storage.TriggerValue{TriggerID: "mockID", TriggerType: "mockType"},
			struct{}{},
		)
		So(err, ShouldBeNil)
	})
}

func TestDeleteTrigger(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	_, tran, txn := getStorage(ctrl)
	tran.EXPECT().Del(gomock.Any()).Return()
	Convey("Test DeleteTrigger", t, func() {
		err := DeleteTrigger(txn, "mock-func", "veroralias",
			storage.TriggerValue{TriggerID: "mockID", TriggerType: "mockType"})
		So(err, ShouldBeNil)
	})
}

func Test_getEnvironment(t *testing.T) {
	key := make([]byte, 1)
	funcVer := storage.FunctionVersionValue{}
	Convey("Test getEnvironment", t, func() {
		patch := gomonkey.ApplyFunc(getEnvironmentText, func(_ storage.FunctionVersionValue) (string, error) {
			return "", errors.New("mock err")
		})
		defer patch.Reset()
		_, err := getEnvironment(key, funcVer)
		So(err, ShouldNotBeNil)
	})
	Convey("Test getEnvironment 2", t, func() {
		patch := gomonkey.ApplyFunc(getEnvironmentText, func(_ storage.FunctionVersionValue) (string, error) {
			return "", nil
		}).ApplyFunc(crypto.Encrypt, func(content string, secret []byte) ([]byte, error) {
			return nil, errors.New("mock err 2")
		})
		defer patch.Reset()
		_, err := getEnvironment(key, funcVer)
		So(err, ShouldNotBeNil)
	})
}

func Test_buildFunctionVersionValue(t *testing.T) {
	Convey("Test buildFunctionVersionValue", t, func() {
		fv := storage.FunctionVersionValue{}
		tInfo := server.TenantInfo{}
		patch := gomonkey.ApplyFunc(buildEnv, func(fv storage.FunctionVersionValue, env *metadata.EnvMetaData) error {
			return errors.New("mock err")
		})
		defer patch.Reset()
		_, err := buildFunctionVersionValue(nil, fv, tInfo)
		So(err, ShouldNotBeNil)
	})
}

func Test_buildFaaSFunctionVersionValue(t *testing.T) {
	Convey("Test buildFaaSFunctionVersionValue", t, func() {
		patches := gomonkey.NewPatches()
		defer patches.Reset()
		fv := storage.FunctionVersionValue{
			FunctionVersion: storage.FunctionVersion{
				MaxInstance:   100,
				MinInstance:   0,
				ConcurrentNum: 100,
				Version:       "latest",
			},
			Function: storage.Function{
				Name: "0@faaspy@hello",
			},
			FunctionLayer: []storage.FunctionLayer{},
		}
		tInfo := server.TenantInfo{
			TenantID: "default",
		}
		Convey("failed to buildFaaSEnv", func() {
			Convey("failed to getEnvironment", func() {
				patches.ApplyFunc(generateEnvKey, func() (string, error) {
					return "", nil
				})
				patches.ApplyFunc(getEnvironment, func(key []byte,
					funcVersion storage.FunctionVersionValue,
				) (string, error) {
					return "", errors.New("get env failed")
				})
				_, err := buildFaaSFunctionVersionValue(nil, fv, tInfo)
				So(err, ShouldNotBeNil)
			})
			Convey("failed to generateEnvKey", func() {
				patches.ApplyFunc(generateEnvKey, func() (string, error) {
					return "", errors.New("generateEnvKey failed")
				})
				_, err := buildFaaSFunctionVersionValue(nil, fv, tInfo)
				So(err, ShouldNotBeNil)
			})
		})
		Convey("succeed to buildFaaSFunctionVersionValue", func() {
			patches.ApplyFunc(buildEnv, func(fv storage.FunctionVersionValue, env *metadata.EnvMetaData) error {
				return nil
			})
			ctx := server.NewContext(&gin.Context{Request: &http.Request{}})
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			_, tran, txn := getStorage(ctrl)
			tran.EXPECT().Put(gomock.Any(), gomock.Any()).Return()
			patches.ApplyFunc((*storage.Txn).GetCtx, func(_ *storage.Txn) server.Context {
				return ctx
			})
			patches.ApplyFunc(storage.GetTxnByKind, func(ctx server.Context, kind string) storage.Transaction {
				return txn
			})
			Convey("succeed to build value", func() {
				_, err := buildFaaSFunctionVersionValue(txn, fv, tInfo)
				So(err, ShouldBeNil)
			})
			Convey("succeed to getFaaSLayerBucket", func() {
				fv.FunctionLayer = []storage.FunctionLayer{
					{Name: "name", Version: 2, Order: 2},
				}
				patches.ApplyFunc(storage.GetLayerVersionTx, func(txn storage.Transaction, layerName string,
					layerVersion int,
				) (storage.LayerValue, error) {
					return storage.LayerValue{}, nil
				})
				patches.ApplyFunc(pkgstore.FindBucket, func(businessID string,
					bucketID string,
				) (config.BucketConfig, error) {
					return config.BucketConfig{}, nil
				})
				_, err := buildFaaSFunctionVersionValue(txn, fv, tInfo)
				So(err, ShouldBeNil)
			})
		})
	})
}

func Test_buildFaaSFuncMetaDataPriorityAZ(t *testing.T) {
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyFunc(getFaaSLayerBucket, func(storage.Transaction, []storage.FunctionLayer,
		server.TenantInfo) ([]*metadata.FaaSLayer, error) {
		return nil, nil
	})
	patches.ApplyFunc(buildCodeMetaData, func(storage.FunctionVersionValue, string) (metadata.CodeMetaData, error) {
		return metadata.CodeMetaData{}, nil
	})

	info, err := buildFaaSFuncMetaData(nil, storage.FunctionVersionValue{
		Function: storage.Function{
			Name:        "0@default@test",
			Description: "desc",
		},
		FunctionVersion: storage.FunctionVersion{
			Version:    "latest",
			Runtime:    "go1.x",
			Handler:    "main",
			Timeout:    30,
			PriorityAZ: "az1",
		},
	}, server.TenantInfo{
		BusinessID: "biz",
		TenantID:   "tenant",
		ProductID:  "product",
	}, metadata.FaaSFuncMeta{})

	assert.Nil(t, err)
	assert.Equal(t, "az1", info.ExtendedMetaData.PriorityAZ)
}

func Test_buildCodeMeteData(t *testing.T) {
	Convey("Test buildCodeMeteData", t, func() {
		fv := storage.FunctionVersionValue{
			FunctionVersion: storage.FunctionVersion{
				Package: storage.Package{},
			},
		}
		fv.FunctionVersion.Package.StorageType = "s3"
		fv.FunctionVersion.Package.CodeUploadType = ""
		fv.FunctionVersion.Package.BucketID = "bucket01"
		fv.FunctionVersion.Package.BucketUrl = "bucket-url"
		fv.FunctionVersion.Package.ObjectID = "obj1"
		fv.FunctionVersion.Package.Signature = "aaa"
		codeMetaData, err := buildCodeMetaData(fv, "yrk")
		So(err, ShouldBeNil)
		So(codeMetaData.Sha512, ShouldEqual, "aaa")
		fv.FunctionVersion.Package.BucketUrl = ""
		_, err = buildCodeMetaData(fv, "yrk")
		So(err, ShouldBeNil)
		fv.FunctionVersion.Package.StorageType = "local"
		_, err = buildCodeMetaData(fv, "yrk")
		So(err, ShouldBeNil)
		fv.FunctionVersion.Package.StorageType = "copy"
		_, err = buildCodeMetaData(fv, "yrk")
		So(err, ShouldBeNil)
		fv.FunctionVersion.Package.StorageType = "copy"
	})
}

func Test_generateEnvKey(t *testing.T) {
	Convey("Test generateEnvKey", t, func() {
		patch := gomonkey.ApplyFunc(rand.Read, func(b []byte) (n int, err error) {
			return 0, errors.New("mock err")
		})
		defer patch.Reset()
		_, err := generateEnvKey()
		So(err, ShouldNotBeNil)
	})
}

func TestCreateAliasEtcd2(t *testing.T) {
	Convey("Test CreateAliasEtcd 2", t, func() {
		txn := &storage.Txn{}
		ctx := fakecontext.NewMockContext()
		patch := gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
			return nil, errors.New("mock err")
		}).ApplyMethod(reflect.TypeOf(txn), "GetCtx", func(tx *storage.Txn) server.Context {
			return ctx
		})
		defer patch.Reset()
		Convey("with TenantInfo err", func() {
			ctx = fakecontext.NewContext()
			err := CreateAliasEtcd(txn, "mock-fun", AliasEtcd{})
			So(err, ShouldNotBeNil)
		})
		Convey("with json Marshal err", func() {
			ctx = fakecontext.NewMockContext()
			err := CreateAliasEtcd(txn, "mock-fun", AliasEtcd{})
			So(err, ShouldNotBeNil)
		})
	})
}
